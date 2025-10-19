package coinsight.arbitrage.shared.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class RedisConfig {

    @Value("${spring.redis.host}")
    private String host;

    @Value("${spring.redis.port}")
    private Integer port;

    /**
     * Used for latest consumer idempotency checks
     * Check out: {@link coinsight.arbitrage.bff.consumer.BinanceLatestConsumer}
     * @param connectionFactory connection factory
     * @return integer redis template
     */
    // Used for latest consumer idempotency checks
    @Bean
    public RedisTemplate<Integer, Integer> idempotencyTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<Integer, Integer> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        template.setKeySerializer(new GenericToStringSerializer<>(Integer.class));
        template.setValueSerializer(new GenericToStringSerializer<>(Integer.class));
        template.setHashKeySerializer(new GenericToStringSerializer<>(Integer.class));
        template.setHashValueSerializer(new GenericToStringSerializer<>(Integer.class));
        template.afterPropertiesSet();
        return template;
    }

    /**
     * Used for distributed locks in LeaderElectorService
     * Check out: {@link coinsight.arbitrage.ingestor.services.LeaderElectorService}
     * @param connectionFactory connection factory
     * @return string redis template
     */
    @Bean
    public RedisTemplate<String, String> lockTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, String> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new StringRedisSerializer());
        template.afterPropertiesSet();
        return template;
    }
}

