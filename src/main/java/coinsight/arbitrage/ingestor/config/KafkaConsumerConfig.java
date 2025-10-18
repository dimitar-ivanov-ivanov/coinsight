package coinsight.arbitrage.ingestor.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.binance.dlt.name}")
    private String binanceDltGroup;

    @Value("${kafka.coinbase.dlt.name}")
    private String coinbaseDltGroup;

    @Value("${kafka.binance.dlt.threads}")
    private int binanceDltThreads;

    @Value("${kafka.coinbase.dlt.threads}")
    private int coinbaseDltThreads;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> binanceDltListenerContainerFactory() {
        return createContainerFactory(binanceDltGroup, binanceDltThreads);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> coinbaseDltListenerContainerFactory() {
        return createContainerFactory(coinbaseDltGroup, coinbaseDltThreads);
    }

    private ConcurrentKafkaListenerContainerFactory<String, String> createContainerFactory(String group, int threads) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(stringConsumerFactory(group));
        factory.setConcurrency(threads); // Number of threads to process messages

        // Retry configuration
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
            new FixedBackOff(500L, 1L) // 1 retry after 500ms
        );

        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    @Bean
    public ConsumerFactory<String, String> stringConsumerFactory(String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return new DefaultKafkaConsumerFactory<>(props,
            new StringDeserializer(),
            new StringDeserializer());
    }
}
