package coinsight.arbitrage.ingestor.integration.config;

import coinsight.arbitrage.shared.util.ProtobufDeserializer;
import monitor.MonitorEventOuterClass;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import ticker.BinanceTickerOuterClass;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Profile("test")
public final class KafkaTestUtil {

    private KafkaTestUtil() {
        // no instances
    }

    public static Consumer<String, BinanceTickerOuterClass.BinanceTicker> createBinanceConsumer(
        String servers, String groupId, String topic) {

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ProtobufDeserializer.class);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        var factory = new DefaultKafkaConsumerFactory<>(props,
            new StringDeserializer(),
            new ProtobufDeserializer<>(BinanceTickerOuterClass.BinanceTicker.parser()));


        Consumer<String, BinanceTickerOuterClass.BinanceTicker> consumer = factory.createConsumer();
        consumer.subscribe(List.of(topic));
        return consumer;
    }

    public static Consumer<String, MonitorEventOuterClass.MonitorEvent> createMonitoringConsumer(
        String servers, String groupId, String topic) {

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ProtobufDeserializer.class);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        var factory = new DefaultKafkaConsumerFactory<>(props,
            new StringDeserializer(),
            new ProtobufDeserializer<>(MonitorEventOuterClass.MonitorEvent.parser()));

        Consumer<String, MonitorEventOuterClass.MonitorEvent> consumer = factory.createConsumer();
        consumer.subscribe(List.of(topic));
        return consumer;
    }
}
