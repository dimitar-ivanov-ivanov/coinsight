package coinsight.arbitrage.ingestor.util;

import coinsight.arbitrage.shared.model.MonitorEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static coinsight.arbitrage.ingestor.util.LeaderUtil.HOSTNAME;
import static coinsight.arbitrage.ingestor.util.LeaderUtil.INSTANCE_ID;

@Component
public class MonitoringMapper {

    @Value("${spring.application.environment}")
    private String environment;

    @Value("${spring.application.name}")
    private String serviceName;

    /**
     * Mapper which converts input message and severity level to a monitoring event.
     * Published as plain JSON - see {@link MonitorEvent} for why.
     *
     * @param message input message
     * @param level input severity level
     * @return a monitoring event
     */
    public MonitorEvent toMonitoringEvent(String message, String level) {
        return new MonitorEvent(
            message,
            Instant.now().toString(),
            UUID.randomUUID().toString(),
            level,
            serviceName,
            INSTANCE_ID,
            UUID.randomUUID().toString(),
            Map.of(),
            HOSTNAME,
            environment
        );
    }
}
