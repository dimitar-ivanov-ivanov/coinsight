package coinsight.arbitrage.streams.config;

import coinsight.arbitrage.streams.binance.BinanceTickerSerde;
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

    @Value("${kafka.streams.binance.applicationId}")
    private String binanceApplicationId;

    @Value("${kafka.streams.binance.commitInterval}")
    private Integer commitInterval;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration binanceStreamConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, binanceApplicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, BinanceTickerSerde.class);
        // The semantics of caching is that data is flushed to the state store and forwarded to the next downstream
        // processor node whenever the earliest of commit.interval.ms or statestore.cache.max.bytes (cache pressure)
        // The idea is to buffer events per key in a intermediary topic and then after the window "commitInterval" passes
        // we output the last event in the window to the output topic
        // Doing this makes sure that the data isn't always chaging, changes every 300-400 millis to make sure the human eye can track it
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, commitInterval);
        return new KafkaStreamsConfiguration(props);
    }
}
