# Consight
Personal project which aims to show arbitrage of crypto coins, currently aiming for Binance and Coinbase.
There are 2 main functionalities:
- Showing in real time the comparison of prices of crypto coins across different exchanges
- Showing comparison of coins for different exchanges for the last month (historical chart)

The UI will most likely be a mobile application. (TBD)

Currently there are multiple parts of the project 
- Ingestor
- Aggregations
- Event Storage
- Streams
- Monitoring
- BFF

Starting the project as a monolith as its easier for hotfixing and deployments, if a particular part has to be scaled it would be easy to separate it.
Each of these parts are in separate packages, they have no compile time dependencies and if they communicate its through Kafka.

# Stack
    - Java 21 
    - Spring 4
    - Kafka 4 (using KRaft)
    - Kafka Streams
    - Kafka UI (Monitoring for Kafka)
    - Redis 
    - Redis Insight (Monitoring for Redis)
    - TigerData (aka TimeScaleDB)
    - Monitoring Stack 
      - Vector (consumes monitor-topic from Kafka directly, no custom Java consumer)
      - Loki (log storage, queried via LogQL)
      - Grafana (dashboards/alerts, queries Loki)

# Coinsight Diagram
Refer to "draw.excalidraw" file in /diagram.
Drag and drop into https://excalidraw.com/ 
Visualizing the diagram here was too complex.

# Coinsight Ingestor
This app is used for consuming messages from Crypto Exchanges through a WebSocket.
It's responsibility is to convert those messages to Protobuf and then publish them to a dedicated Kafka topic per exchange.
The monitoring/logging is happening asynchronously, done in order to offload the service and fetch as many messages from the exchanges as we can.

If there is an exception we publish to a DLT(dead letter topic). After the problem is resolve I turn
on the DLT consumer and it will consume the message and try to reprocess it.
The message is published to the DLT raw in String format, unlike the other topics, which use Protobuf.
Dead letter strategy is having a consumer of the DLT behind a feature flag that is disabled by default, that consumer will take the messages from the DTL and try to republish them to the main topic.
It's expected that when the DLT consumer is enabled the errors shouldn't happen anymore and then republishing should be successful.

In order to ensure we have processing for each pair in order the message id is the crypto pair ("BTC-USDT", "BTC-USD", "ETH-USD" etc)
This makes sure that all events with the same message id are put in the same partition so only 1 consumer thread will read them, thus ensuring ordering.

Since at some point we'll scale the app to at least 2 instance that would mean that 2 instances have their own socket listening to exchange events.
This will lead to publishing the same event twice so we'll need a distributed lock to ensure that only 1 socket is working regardless of how much 
we scale horizontally.

# Coinsight Aggregations
The aggregations receive the input events that the ingestor pushes in the exchange topics.
When consuming we consume a batch of events, the events are split up into sub-batches by message id. 
Meaning we'll have sub-batches for "BTC-USDT", "BTC-USD" etc.
Each subbatch will be taken by a virtual thread as the work it'll do is network based and virtual threads are very good there.
The processed data will be writen to TimeScaleDB.
For every event we'll do an idempotency check to make sure the event hasn't been processed before, the idempotency storage will be 
the database even though its slower than Redis, we need to make sure that persistance of the data and idempotency record is IN THE SAME TRANSACTION.
If I put the idempotency records in Redis and then the DB fails on write it could lead to inconsistent data.
If the record is present in TimeScaleDB  then it's been processed before.

# Consight Streams
I'll be using Kafka Streams whose aim is to take all events per topic per message id and only output the latest one every 300 millis.
This is done because if I change the data all of the time the human eye won't be able to track the data so we want to have a window of 300 millis.
The Streams opens a intermediary topic in which we put all events for BTC-USDT(as an example) for the time window, at the end of the window the last event 
is pushed to the output topic.

# Coinsight BFF
When the user opens their mobile a socket is opened to the BFF.
The BFF listens to the output topic and whatever comes through it is send through the socket to the client.
But before the data is send an idempotency check is executed to make sure that the record hasn't been processed before.
The idempotency records are stored in Redis as it's faster and it stores its data in memory.
If the record is present in Redis then it's been processed before.

# Consight Event Storage 
It's good to have an event storage just in case I have to replay events. I'll be storing them in 
TimeScaleDB and compressing them to lower memory requirements.
TimeScale has aggressive compression algorithms, up to 90% less memory.
The event storage avoids Kafka as a single point of failure. 
There is a job (disabled by default) which aims to take the events from the DB and push them to the exchange topics, thus replaying them.
The way events will be consumed by exchanges is again through socket, the difference is the sockets in **Ingestor** push to Kafka and these 
will push to the event storage table in TimeScaleDB.

# Monitoring
Vector connects to Kafka directly as a consumer group member of `monitor-topic`, decodes the payload
(see `docker/vector/vector.yaml`), reshapes it, and ships it to Loki.
Loki stores the data and indexes it. Grafana is used as an UI for the indexed data.

Monitoring events (`MonitorEvent`/`DltEvent` in `coinsight.arbitrage.shared.model`) are plain Java records, published
as JSON via Spring Kafka's built-in `JsonSerializer`. `monitor-topic` is low volume and exists to be read (Grafana, or `kafka-console-consumer` while debugging).

Local Grafana testing requires admin/admin as credentials.

# Kafka
- 3 combined broker+controller nodes (KRaft, combined mode): each node participates in the Raft metadata quorum (controller role) AND serves message traffic (broker role)
- 3 is the minimum node count for the Raft quorum to tolerate 1 node failure (majority = 2 of 3)
- Combining roles instead of running dedicated controller-only nodes keeps the same HA guarantee with fewer containers, and gives replicated topics more brokers to place replicas on
- For every event we retrieve from any exchange we publish to the topic of that exchange
- For every log instead of logging and blocking the flow we publish an event to the monitor topic
- For the sake of saving $ I'm making the retention of the generally used topics to be **1 MINUTE!**
- The DLT topics will have a retention of 3 days, meaning I have 3 days to fix the broken logic and then re-enable the DLT consumer which re-emits the messages to the original topic
- The other topics will be created by the Apps that will use them
- [START] ``docker compose up -d``
- [TOPIC CREATION] ``docker exec -it broker1 /opt/kafka/bin/kafka-topics.sh --create --topic binance-topic --bootstrap-server broker1:19092 --partitions 10 --replication-factor 3 --config retention.ms=60000 --config segment.ms=300000``
    - create binance topic with 1 minute retention, segment rolls at 5 min so after 5 min a new segment is created and the old one can have its messages deleted
    - replication-factor is now 3 since all 3 nodes are full brokers (was 2 when only 2 nodes were broker-only)
- [TOPIC CREATION] ``docker exec -it broker1 /opt/kafka/bin/kafka-topics.sh --create --topic coinbase-topic --bootstrap-server broker1:19092 --partitions 10 --replication-factor 3 --config retention.ms=60000 --config segment.ms=300000``
    - create coinbase topic with 1 minute retention
- [TOPIC CREATION] ``docker exec -it broker1 /opt/kafka/bin/kafka-topics.sh --create --topic monitor-topic --bootstrap-server broker1:19092 --partitions 16 --replication-factor 3 --config retention.ms=60000 --config segment.ms=300000``
    - create monitoring topic with 1 minute retention
- [TOPIC CREATION] ``docker exec -it broker1 /opt/kafka/bin/kafka-topics.sh --create --topic binance-latest-topic --bootstrap-server broker1:19092 --partitions 10 --replication-factor 3 --config retention.ms=60000 --config segment.ms=300000``
    - create binance latest topic with 1 minute retention 
- [TOPIC CREATION] ``docker exec -it broker1 /opt/kafka/bin/kafka-topics.sh --create --topic binance-dlt-topic --bootstrap-server broker1:19092 --partitions 2 --replication-factor 3 --config retention.ms=259200000``
    - create binance dlt topic with 3 day retention
- [TOPIC CREATION] ``docker exec -it broker1 /opt/kafka/bin/kafka-topics.sh --create --topic coinbase-dlt-topic --bootstrap-server broker1:19092 --partitions 2 --replication-factor 3 --config retention.ms=259200000``
    - create coinbase dlt topic with 3 day retention
- [TOPIC CREATION] ``docker exec -it broker1 /opt/kafka/bin/kafka-topics.sh --create --topic monitor-dlt-topic --bootstrap-server broker1:19092 --partitions 2 --replication-factor 3 --config retention.ms=259200000``
    - create binance dlt topic with 3 day retention
- [VERIFY] ``docker exec broker1 /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server broker1:19092``
- [TOPIC DELETION] ``docker exec -it broker1 /opt/kafka/bin/kafka-topics.sh --delete --topic binance-topic --bootstrap-server broker1:19092``
- [END] ``docker compose down``
- [REMOVE VOLUMES] ``docker volume rm broker1-data broker2-data broker3-data``
- [ALTER PARTITIONS ON TOPIC] ``docker exec -it broker1 /opt/kafka/bin/kafka-topics.sh --alter --topic binance-topic --partitions 10 --bootstrap-server broker1:19092,broker2:19093,broker3:19094``
  - Keep in mind in Kafka you can only **increase** partitions  
- **[VERIFY EVENTS PRODUCED FOR TOPIC]**
- ``docker exec -it broker1 bash``
- ``cd ../../bin`` -> folder with scripts
- ``kafka-console-consumer --bootstrap-server broker1:19092 --topic read-topic --property print.key=true --property print.timestamp=true --property print.partition=true --from-beginning``
- OR ``docker exec broker1 kafka-console-consumer --bootstrap-server broker1:19092 --topic binance-topic --property print.key=true --property print.timestamp=true --property print.partition=true --from-beginning``

# Redis 
- I'm using Redis for this use cases
  - Distributed lock for the input of events. When we scale to 2+ instances of the app we'll also scale to 2+ input sockets of events.
    That would mean that we would duplicate events. Regardless of how many instance we have we need to make sure only one event comes in.
    The way we ensure is that is through a distributed lock. There's a job that runs and tries to take/maintain the ownership of the current instance.
    If the instance dies the job doesn't run and the instance loses ownership, meaning the other active instance will take ownershup.

# TimeScaleDB
- One main instance that will be used for all traffic and a replica on standby to ensure availability.
- For the event storage, when we write an event we know it won't be update we safely compress older data to save storage.

# Checkstyle
- Checkstyle is used to enforce standards for code quality
- use ``gradle checkstyleMain`` to use
- checkstyle rules can be found in ``config/checkstyle/checkstyle.xml``
- configured to exclude generate files

# PMD
- Another tool for more code quality and bug catching
- use ``gradle pmdMain`` to use
- pmd rules can be found in ``config/pmd/pmd.xml``

# Protobuf
- format for kafka messages will be in Protobuf as it allows for higher performance and smaller memory footprint
- execute ``gradle generateProto`` to generate classes from .proto files
- All of the prices are stored in int64 after being multiplied by 10^8
- This is done to avoid string conversions which are slower with bigger memory footprint
- Ingestor -> sets best_bid_price * 10^8 -> then the processor will divide by 10^8 to get the real price
- The scales for prices will be put in the events too, so that if they change they have to only change in the ingestor app
- Exception: `monitor-topic`/`monitor-dlt-topic` events are NOT protobuf - they're plain JSON records, see **Monitoring** above

# Client Testing 
- Run the client.js script in src/main/resources/static/client.js
- install node and npm before
- Run this ``node client.js`` after start the application

# Docker problems (Windows)
 - If you have problems with ports, use the commands belows
 - ``net stop winnat``
 - ``net start winnat``

# Redis Insight (local testing) 
 - Connect to the Redis container using ``redis://redis:6379``