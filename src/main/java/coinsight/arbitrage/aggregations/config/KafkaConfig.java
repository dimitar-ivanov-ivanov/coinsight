package coinsight.arbitrage.aggregations.config;

import coinbase.ticker.CoinbaseEvent;
import coinsight.arbitrage.shared.util.ProtobufDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import ticker.BinanceTickerOuterClass;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.binance.latest.group}")
    private String binanceLatestGroup;

    @Value("${kafka.binance.latest.consumers}")
    private int binanceConsumers;

    @Value("${kafka.coinbase.latest.group}")
    private String coinbaseLatestGroup;

    @Value("${kafka.coinbase.latest.consumers}")
    private int coinbaseConsumers;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, BinanceTickerOuterClass.BinanceTicker> binanceAggregationsFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ProtobufDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, binanceLatestGroup);

        var consumerFactory = new DefaultKafkaConsumerFactory<>(props,
                new StringDeserializer(),
                new ProtobufDeserializer<>(BinanceTickerOuterClass.BinanceTicker.parser()));

        ConcurrentKafkaListenerContainerFactory<String, BinanceTickerOuterClass.BinanceTicker> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(binanceConsumers); // Number of threads to process messages

        // Retry configuration
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                new FixedBackOff(500L, 1L) // 1 retry after 500ms
        );

        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, CoinbaseEvent.CoinbaseTicker> coinbaseAggregationsFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ProtobufDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, coinbaseLatestGroup);

        var consumerFactory = new DefaultKafkaConsumerFactory<>(props,
                new StringDeserializer(),
                new ProtobufDeserializer<>(CoinbaseEvent.CoinbaseTicker.parser()));

        ConcurrentKafkaListenerContainerFactory<String, CoinbaseEvent.CoinbaseTicker> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(coinbaseConsumers); // Number of threads to process messages

        // Retry configuration
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                new FixedBackOff(500L, 1L) // 1 retry after 500ms
        );

        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
}
