package coinsight.arbitrage.ingestor.config;

import coinbase.ticker.CoinbaseEvent;
import coinsight.arbitrage.shared.model.MonitorEvent;
import coinsight.arbitrage.shared.util.ProtobufSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import ticker.BinanceTickerOuterClass;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.compression-type}")
    private String compressionType;

    private Map<String, Object> baseProducerConfigs(Class serializerClass) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializerClass);
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
        return config;
    }

    private <V> ProducerFactory<String, V> protobufProducerFactory() {
        return new DefaultKafkaProducerFactory<>(baseProducerConfigs(ProtobufSerializer.class));
    }

    // Plain JSON, used only for monitor-topic/monitor-dlt-topic - see MonitorEvent/DltEvent
    // for why monitoring events are plain records instead of protobuf-derived.
    private <V> ProducerFactory<String, V> jsonProducerFactory() {
        Map<String, Object> config = baseProducerConfigs(JsonSerializer.class);
        // There's no Java consumer for these topics - Vector reads the raw JSON - so there's
        // no reason to leak Java class names into Kafka message headers via Spring's default
        // "__TypeId__" behavior.
        config.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaProducerFactory<>(config);
    }

    private <V> ProducerFactory<String, V> stringProducerFactory() {
        return new DefaultKafkaProducerFactory<>(baseProducerConfigs(StringSerializer.class));
    }

    @Bean
    public KafkaTemplate<String, BinanceTickerOuterClass.BinanceTicker> binanceTemplate() {
        return new KafkaTemplate<>(protobufProducerFactory());
    }

    /**
     * All the exchange dlt topics will receive the input message from the exchange. (raw event)
     * @return template used for publishing events
     */
    @Bean
    public KafkaTemplate<String, String> exchangeDltTemplate() {
        return new KafkaTemplate<>(stringProducerFactory());
    }

    @Bean
    public KafkaTemplate<String, CoinbaseEvent.CoinbaseTicker> coinbaseTemplate() {
        return new KafkaTemplate<>(protobufProducerFactory());
    }

    @Bean
    public KafkaTemplate<String, MonitorEvent> monitoringTemplate() {
        return new KafkaTemplate<>(jsonProducerFactory());
    }
}
