package coinsight.arbitrage.shared.monitoring;

import coinsight.arbitrage.shared.model.MonitorEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Deliberately self-contained, rather than reusing ingestor's KafkaProducerConfig. If any
 * module here ever gets extracted into its own deployable, it needs to be able to construct
 * this bean on its own - it can't depend on another module's config class still being around
 * in the same process. Duplicating a small amount of producer boilerplate now is the cost of
 * that independence actually being real, not just true by accident of today's monolith.
 */
@Configuration
public class MonitoringKafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.compression-type}")
    private String compressionType;

    @Bean
    public KafkaTemplate<String, MonitorEvent> monitoringTemplate() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
        // There's no Java consumer for monitor-topic - Vector reads the raw JSON - so there's
        // no reason to leak Java class names into Kafka message headers via Spring's default
        // "__TypeId__" behavior.
        config.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(config));
    }
}
