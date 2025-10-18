package coinsight.arbitrage.ingestor.integration.test;

import coinsight.arbitrage.ingestor.config.KafkaConsumerConfig;
import coinsight.arbitrage.ingestor.integration.util.MockBinanceWebSocketClient;
import coinsight.arbitrage.ingestor.util.LeaderUtil;
import com.google.common.collect.ImmutableList;
import monitor.MonitorEventOuterClass.MonitorEvent;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;
import ticker.BinanceTickerOuterClass.BinanceTicker;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static coinsight.arbitrage.ingestor.integration.config.KafkaTestUtil.createBinanceConsumer;
import static coinsight.arbitrage.ingestor.integration.config.KafkaTestUtil.createMonitoringConsumer;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
@ActiveProfiles("test")
@SuppressWarnings({"PMD.ExcessiveImports", "PMD.UseUnderscoresInNumericLiterals"})
class BinanceIntegrationTest {

    private static final String BINANCE_TOPIC = "binance-topic";

    private static final String BINANCE_GROUP_ID = "binance-group";

    private static final String MONITORING_TOPIC = "monitor-topic";

    private static final String MONITOR_GROUP_ID = "monitor-group";

    private static final String BINANCE_DLT_TOPIC = "binance-dlt-topic";

    private static final String BINANCE_DLT_GROUP_ID = "binance-dlt-group";

    private static final String MONITORING_DLT_TOPIC = "monitor-dlt-topic";

    private static final ConfluentKafkaContainer KAFKA_CONTAINER =
        new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));

    private static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine")
            .withExposedPorts(6379);

    @Autowired
    private KafkaConsumerConfig actualConsumerConfig;

    @Value("${spring.application.environment}")
    private String environment;

    @Value("${spring.application.name}")
    private String serviceName;

    @Autowired
    private MockBinanceWebSocketClient client;

    @BeforeAll
    static void setUpClass() throws NoSuchFieldException, IllegalAccessException {
        KAFKA_CONTAINER.start();
        createTopics();

        REDIS.start();
    }

    private static void createTopics() {
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_CONTAINER.getBootstrapServers());

        try (AdminClient adminClient = AdminClient.create(props)) {
            // Topic name - partitions - replication
            NewTopic binanceTopic = new NewTopic(BINANCE_TOPIC, 1, (short) 1);
            NewTopic monitoringTopic = new NewTopic(MONITORING_TOPIC, 1, (short) 1);
            NewTopic binanceDltTopic = new NewTopic(BINANCE_DLT_TOPIC, 1, (short) 1);
            NewTopic monitoringDltTopic = new NewTopic(MONITORING_DLT_TOPIC, 1, (short) 1);
            adminClient.createTopics(List.of(binanceTopic, monitoringTopic, binanceDltTopic, monitoringDltTopic)).all().get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create topic", e);
        }
    }

    @AfterAll
    static void afterAll() {
        KAFKA_CONTAINER.stop();
        REDIS.stop();
    }

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        // Necessary Kafka config
        registry.add("spring.kafka.bootstrap-servers", KAFKA_CONTAINER::getBootstrapServers);

        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", REDIS::getFirstMappedPort);
    }

    @Test
    void given_pollFromBinanceStream_when_onMessage_then_emitToKafka() throws InterruptedException, UnknownHostException {
        // GIVEN
        String message = buildMessage();

        // WHEN
        client.onMessage(message);

        // Small delay to ensure message is published
        Thread.sleep(1000);

        // THEN -> read actual message from Kafka
        var consumer = createBinanceConsumer(KAFKA_CONTAINER.getBootstrapServers(), BINANCE_GROUP_ID, BINANCE_TOPIC);
        AtomicReference<Integer> eventId = new AtomicReference<>();
        await()
            .atMost(12, TimeUnit.SECONDS)
            .pollInterval(4, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                ConsumerRecords<String, BinanceTicker> records = consumer.poll(Duration.ofSeconds(5));
                List<ConsumerRecords<String, BinanceTicker>> recordsList = ImmutableList.of(records);

                assertFalse(recordsList.isEmpty(), "Expected message on Binance Kafka topic");
                assertEquals(1, recordsList.size());

                BinanceTicker actualEvent = recordsList.getFirst().iterator().next().value();
                eventId.set(actualEvent.getMessageId());
                assertEvent(actualEvent);
            });

        consumer.close();

        // assert no messages in DLT
        assertBinanceDltTopic();

        // assert message in monitoring topic with exact id of the binance event
        assertMonitoringEvent(eventId.get());

    }

    private String buildMessage() {
        return "{\n" +
            "  \"e\" : \"24hrTicker\",\n" +
            "  \"E\" : 1753971283028,\n" +
            "  \"s\" : \"ETHUSDT\",\n" +
            "  \"p\" : \"13.25000000\",\n" +
            "  \"P\" : \"0.350\",\n" +
            "  \"w\" : \"3802.98878497\",\n" +
            "  \"x\" : \"3780.50000000\",\n" +
            "  \"c\" : \"3793.74000000\",\n" +
            "  \"Q\" : \"0.95910000\",\n" +
            "  \"b\" : \"3793.74000000\",\n" +
            "  \"B\" : \"96.66040000\",\n" +
            "  \"a\" : \"3793.75000000\",\n" +
            "  \"A\" : \"18.42230000\",\n" +
            "  \"o\" : \"3780.49000000\",\n" +
            "  \"h\" : \"3878.67000000\",\n" +
            "  \"l\" : \"3677.65000000\",\n" +
            "  \"v\" : \"560495.97930000\",\n" +
            "  \"q\" : \"2131559923.29601500\",\n" +
            "  \"O\" : 1753884883013,\n" +
            "  \"C\" : 1753971283013,\n" +
            "  \"F\" : 2670054103,\n" +
            "  \"L\" : 2672872348,\n" +
            "  \"n\" : 2818246\n" +
            "}";
    }

    private void assertEvent(BinanceTicker event) {
        assertNotNull(event);

        assertEquals(1_753_971_283_028L, event.getEventTime());

        assertEquals("ETHUSDT", event.getCryptoPair());

        assertEquals(379_374_000_000L, event.getBestBidPrice());
        assertEquals(9_666_040_000L, event.getBestBidQty());
        assertEquals(379_375_000_000L, event.getBestAskPrice());
        assertEquals(1_842_230_000L, event.getBestAskQty());

        assertEquals(5_6049_597_930_000L, event.getVolume());

        assertNotNull(event.getTimestamp(), "Timestamp should be set");
        assertFalse(event.getTimestamp().isEmpty(), "Timestamp should not be empty");

        assertDoesNotThrow(() -> Instant.parse(event.getTimestamp()),
            "Timestamp should be a valid ISO instant");
    }

    private void assertBinanceDltTopic() throws InterruptedException {
        var dltConsumer = actualConsumerConfig.stringConsumerFactory(BINANCE_DLT_GROUP_ID).createConsumer();
        dltConsumer.subscribe(List.of(BINANCE_DLT_TOPIC));
        Thread.sleep(1000);
        await()
            .atMost(12, TimeUnit.SECONDS).pollInterval(4, TimeUnit.SECONDS).untilAsserted(() -> {
                ConsumerRecords<String, String> dltRecords = dltConsumer.poll(Duration.ofSeconds(2));
                assertFalse(dltRecords.iterator().hasNext());
            });
        dltConsumer.close();
    }

    private void assertMonitoringEvent(int actualEventId) throws InterruptedException {
        var monitoringConsumer =
            createMonitoringConsumer(KAFKA_CONTAINER.getBootstrapServers(), MONITOR_GROUP_ID, MONITORING_TOPIC);

        Thread.sleep(1000);

        await()
            .atMost(20, TimeUnit.SECONDS)
            .pollInterval(4, TimeUnit.SECONDS).untilAsserted(() -> {
                ConsumerRecords<String, MonitorEvent> monitoringRecords = monitoringConsumer.poll(Duration.ofSeconds(2));
                List<ConsumerRecords<String, MonitorEvent>> monitoringRecordsList = ImmutableList.of(monitoringRecords);

                assertFalse(monitoringRecordsList.isEmpty(), "Expected message on Monitoring Kafka topic");
                assertEquals(1, monitoringRecordsList.size());

                MonitorEvent monitorEvent = monitoringRecordsList.getFirst().iterator().next().value();
                String expectedMessage = "Binance Published message " + actualEventId;
                assertMonitoringEvent(monitorEvent, expectedMessage, "INFO");
            });
        monitoringConsumer.close();
    }

    private void assertMonitoringEvent(MonitorEvent event,
                                    String expectedMessage,
                                    String expectedLevel) throws UnknownHostException {

        assertEquals(expectedMessage, event.getMessage());
        assertEquals(expectedLevel, event.getLevel());

        assertNotNull(event.getMessageId());
        assertFalse(event.getMessageId().isEmpty());
        assertDoesNotThrow(() -> UUID.fromString(event.getMessageId()),
            "MessageId should be a valid UUID");

        assertNotNull(event.getTraceId());
        assertFalse(event.getTraceId().isEmpty());
        assertDoesNotThrow(() -> UUID.fromString(event.getTraceId()),
            "TraceId should be a valid UUID");

        // Timestamp validation
        assertNotNull(event.getTimestamp());
        assertFalse(event.getTimestamp().isEmpty());
        assertDoesNotThrow(() -> Instant.parse(event.getTimestamp()),
            "Timestamp should be a valid ISO instant");

        assertEquals(serviceName, event.getServiceName());
        assertEquals(environment, event.getEnvironment());
        assertEquals(InetAddress.getLocalHost().getHostName(), event.getHost());
        assertEquals(LeaderUtil.INSTANCE_ID, event.getInstanceId());
    }
}
