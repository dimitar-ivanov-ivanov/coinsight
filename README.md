# Consight
Personal project which aims to show arbitrage of crypto coins, currently aiming for Binance and Coinbase.
There are 2 main functionalities:
- Showing in real time the comparison of prices of crypto coins across different exchanges
- Showing comparison of coins for different exchanges for the last month (historical chart)

The UI will most likely be a mobile application. (TBD)

Currently there are multiple parts of the project 
- Ingestor
- Aggregations
- Streams
- BFF
- Shared (contains shared component, in the future can be extracted as library if we split up the project into microservices)

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
      - Vector (log collector, consumes monitor-topic from Kafka directly, no custom Java consumer)
      - Loki (log storage, queried via LogQL)
      - Prometheus (metrics storage, scrapes the app's `/actuator/prometheus` endpoint)
      - Grafana (dashboards/alerts, queries Loki and Prometheus)

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

In order to ensure we have processing for each pair in order, the Kafka **partition key** for every exchange topic is the
crypto pair ("BTC-USDT", "BTC-USD", "ETH-USD" etc). Using crypto pair as the key makes sure that all events
for the same pair land in the same partition, so only 1 consumer thread ever reads them, ensuring ordering per pair.

Since at some point we'll scale the app to at least 2 instance that would mean that 2 instances have their own socket listening to exchange events.
This will lead to publishing the same event twice so we'll need a distributed lock to ensure that only 1 socket is working regardless of how much 
we scale horizontally.

# Coinsight Aggregations
Aggregations consumes from `binance-latest-topic`/`coinbase-latest-topic` - the windowed output of Streams, not the raw
exchange topics - one event at a time via a plain `@KafkaListener` per exchange (`BinanceAggregationsLatestConsumer`/
`CoinbaseAggregationsLatestConsumer`), each writing straight to TimescaleDB

No batching, no sub-batches by crypto pair, no virtual threads - the actual write is a single `JdbcTemplate` insert per
event. Idempotency is handled entirely by the DB itself: `ticks` has a `UNIQUE (time, crypto_pair, message_id)`
constraint, and the insert is `ON CONFLICT DO NOTHING`. A genuinely re-delivered event just silently doesn't insert a
second row - no exception thrown, no separate idempotency table, no need to coordinate a transaction across two
writes. See `src/main/resources/sql/timescale-schema.sql` for the schema.

# Consight Streams
I'll be using Kafka Streams whose aim is to take all events per topic per message id and only output the latest one every 300 millis.
This is done because if I change the data all of the time the human eye won't be able to track the data so we want to have a window of 300 millis.
The Streams opens a intermediary topic in which we put all events for BTC-USDT(as an example) for the time window, at the end of the window the last event 
is pushed to the output topic.

Both Binance and Coinbase have their own topology (`BinanceStream`/`CoinbaseStream`), sharing one embedded Kafka Streams application/application.id
(see `KafkaStreamsConfig`) rather than one each. Because of that, every serde is passed explicitly at each operator (`Consumed`/`Grouped`/`Materialized`/
`Produced`) instead of relying on the app's default value serde - a single global default can't correctly serve two different ticker types at once, so
neither topology depends on it.

# Coinsight BFF
When the user opens their mobile a socket is opened to the BFF.
The BFF listens to the output topics (`binance-latest-topic`/`coinbase-latest-topic`) and whatever comes through is sent through the socket to the client,
on `/topic/binance` and `/topic/coinbase` respectively.

There is no idempotency check here on purpose. A duplicate live tick just means the client renders the same price
twice in a row - cosmetically invisible for a live ticker, not worth an extra Redis round-trip per message for a
problem with no real user-facing symptom.

# Monitoring
Vector connects to Kafka directly as a consumer group member of `monitor-topic`, decodes the payload
(see `docker/vector/vector.yaml`), reshapes it, and ships it to Loki.
Loki stores the data and indexes it. Grafana is used as an UI for the indexed data.

Monitoring events (`MonitorEvent` in `coinsight.arbitrage.shared.model`) are plain Java records, published
as JSON via Spring Kafka's built-in `JsonSerializer`. `monitor-topic` is low volume and exists to be read (Grafana, or `kafka-console-consumer` while debugging).

Local Grafana testing requires admin/admin as credentials.

# Metrics (Prometheus)
The app exposes Micrometer metrics at `/actuator/prometheus` (only `health` and `prometheus` are exposed via
`management.endpoints.web.exposure.include` - not `include: *`, which would also leak env/beans/threaddump).
Prometheus (in `docker/docker-compose.yml`) scrapes this endpoint on a 15s interval. Since the app runs on the host
rather than in a container (same reason Kafka's bootstrap servers are `localhost:*`), the scrape target is
`host.docker.internal:8080`, not a service name - see `docker/prometheus/prometheus.yml`.
Grafana auto-provisions a Prometheus datasource the same way it does for Loki (see
`docker/grafana/provisioning/datasources/datasources.yaml`), so it's queryable immediately after `docker compose up`
with no manual setup

# Kafka
- 3 combined broker+controller nodes (KRaft, combined mode): each node participates in the Raft metadata quorum (controller role) AND serves message traffic (broker role)
- 3 is the minimum node count for the Raft quorum to tolerate 1 node failure (majority = 2 of 3)
- Combining roles instead of running dedicated controller-only nodes keeps the same HA guarantee with fewer containers, and gives replicated topics more brokers to place replicas on
- For every event we retrieve from any exchange we publish to the topic of that exchange
- For every log instead of logging and blocking the flow we publish an event to the monitor topic
- For the sake of saving $ I'm making the retention of the generally used topics to be **1 MINUTE!**
- The DLT topics will have a retention of 3 days, meaning I have 3 days to fix the broken logic and then re-enable the DLT consumer which re-emits the messages to the original topic
- The other topics will be created by the Apps that will use them
- [START] ``docker compose -f docker/docker-compose.yml up -d`` (or ``cd docker && docker compose up -d``)
- [TOPIC CREATION] ``docker exec -it broker1 /opt/kafka/bin/kafka-topics.sh --create --topic binance-topic --bootstrap-server broker1:19092 --partitions 10 --replication-factor 3 --config retention.ms=60000 --config segment.ms=300000``
    - create binance topic with 1 minute retention, segment rolls at 5 min so after 5 min a new segment is created and the old one can have its messages deleted
    - replication-factor is now 3 since all 3 nodes are full brokers (was 2 when only 2 nodes were broker-only)
- [TOPIC CREATION] ``docker exec -it broker1 /opt/kafka/bin/kafka-topics.sh --create --topic coinbase-topic --bootstrap-server broker1:19092 --partitions 10 --replication-factor 3 --config retention.ms=60000 --config segment.ms=300000``
    - create coinbase topic with 1 minute retention
- [TOPIC CREATION] ``docker exec -it broker1 /opt/kafka/bin/kafka-topics.sh --create --topic monitor-topic --bootstrap-server broker1:19092 --partitions 16 --replication-factor 3 --config retention.ms=60000 --config segment.ms=300000``
    - create monitoring topic with 1 minute retention
- [TOPIC CREATION] ``docker exec -it broker1 /opt/kafka/bin/kafka-topics.sh --create --topic binance-latest-topic --bootstrap-server broker1:19092 --partitions 10 --replication-factor 3 --config retention.ms=60000 --config segment.ms=300000``
    - create binance latest topic with 1 minute retention 
- [TOPIC CREATION] ``docker exec -it broker1 /opt/kafka/bin/kafka-topics.sh --create --topic coinbase-latest-topic --bootstrap-server broker1:19092 --partitions 10 --replication-factor 3 --config retention.ms=60000 --config segment.ms=300000``
  - create binance latest topic with 1 minute retention
- [TOPIC CREATION] ``docker exec -it broker1 /opt/kafka/bin/kafka-topics.sh --create --topic binance-dlt-topic --bootstrap-server broker1:19092 --partitions 2 --replication-factor 3 --config retention.ms=259200000``
    - create binance dlt topic with 3 day retention
- [TOPIC CREATION] ``docker exec -it broker1 /opt/kafka/bin/kafka-topics.sh --create --topic coinbase-dlt-topic --bootstrap-server broker1:19092 --partitions 2 --replication-factor 3 --config retention.ms=259200000``
    - create coinbase dlt topic with 3 day retention
- [VERIFY] ``docker exec broker1 /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server broker1:19092``
- [TOPIC DELETION] ``docker exec -it broker1 /opt/kafka/bin/kafka-topics.sh --delete --topic binance-topic --bootstrap-server broker1:19092``
- [END] ``docker compose -f docker/docker-compose.yml down``
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
- No migration tool (Flyway/Liquibase) is used - schema/hypertable/policy setup lives as plain SQL in
  ``src/main/resources/sql/timescale-schema.sql``, run manually against the container. `spring.jpa.hibernate.ddl-auto`
  is `none` on purpose - Hibernate has no concept of hypertables or continuous aggregates, so schema management can't
  go through it regardless of environment.
- Use DBeaver or tool to run the queries
- Currently only ONE tier is built - the raw `ticks` hypertable (partitioned by time and by crypto_pair) and a daily
  OHLC (open/high/low/close) continuous aggregate, `ticks_daily`, rolling it up automatically in the background.
- Retention is 1 month on the raw `ticks` table only, not on `ticks_daily` - continuous aggregates are backed by their
  own separate hypertable, so the daily rollup is kept independently and can outlive the raw data that fed it, since
  its storage cost is negligible compared to raw ticks.
- Further tiers (1-minute, 1-hour) are meant to follow the same pattern later, each rolling up from the tier below it
  rather than from raw data again, once there's an actual UI consumer to justify building them.

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
- Exception: `monitor-topic` events are NOT protobuf - they're plain JSON records, see **Monitoring** above

# Client Testing 
- Run the client.js script in src/main/resources/static/client.js
- install node and npm before
- Run this ``node client.js`` after start the application

# Docker problems (Windows)
 - If you have problems with ports, use the commands belows after opening a console terminal in admin mode
 - ``net stop winnat``
 - ``net start winnat``

# Redis Insight (local testing) 
 - Connect to the Redis container using ``redis://redis:6379``