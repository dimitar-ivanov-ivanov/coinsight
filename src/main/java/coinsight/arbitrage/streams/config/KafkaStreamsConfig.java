package coinsight.arbitrage.streams.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.HashMap;
import java.util.Map;

@Configuration
@Profile("!test")
public class KafkaStreamsConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // Shared by the whole embedded Kafka Streams app - there's only one KafkaStreams instance
    // per JVM here, and BOTH BinanceStream and CoinbaseStream register their topologies onto
    // the same StreamsBuilder/application.id, not one each.
    @Value("${kafka.streams.applicationId}")
    private String streamsApplicationId;

    @Value("${kafka.streams.commitInterval}")
    private Integer commitInterval;

    // Total stream threads for the whole app, shared across every topology's partitions -
    // not "Binance's thread count", despite the property name.
    @Value("${kafka.streams.consumers}")
    private Integer streamThreads;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration streamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, streamsApplicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // No DEFAULT_VALUE_SERDE_CLASS_CONFIG on purpose - it's a single global setting that
        // can't correctly serve both BinanceTicker and CoinbaseTicker at once.
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, commitInterval);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, streamThreads);
        return new KafkaStreamsConfiguration(props);
    }
}
