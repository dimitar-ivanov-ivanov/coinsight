package coinsight.arbitrage.ingestor.config;

import coinbase.ticker.CoinbaseEvent;
import coinsight.arbitrage.shared.util.ProtobufSerializer;
import monitor.MonitorDtlEvent;
import monitor.MonitorEventOuterClass;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
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
    public KafkaTemplate<String, MonitorEventOuterClass.MonitorEvent> monitoringTemplate() {
        return new KafkaTemplate<>(protobufProducerFactory());
    }

    @Bean
    public KafkaTemplate<String, MonitorDtlEvent.DltEvent> monitoringDltTemplate() {
        return new KafkaTemplate<>(protobufProducerFactory());
    }
}
