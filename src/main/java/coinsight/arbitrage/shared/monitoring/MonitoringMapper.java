package coinsight.arbitrage.shared.monitoring;

import coinsight.arbitrage.shared.model.MonitorEvent;
import coinsight.arbitrage.shared.util.InstanceIdentity;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Component
public class MonitoringMapper {

    // This is the same across every module - it's per-deployment-target, not per-module,
    // so it's fine to still read it from a shared global property.
    @Value("${spring.application.environment}")
    private String environment;

    /**
     * Mapper which converts input message, severity level, and origin to a monitoring event.
     * Published as plain JSON - see {@link MonitorEvent} for why.
     *
     * @param message input message
     * @param level input severity level
     * @param origin the module/service this event originated from (e.g. "ingestor", "bff") -
     *               passed explicitly rather than inferred from spring.application.name, since
     *               today every module runs in the same JVM/process with the same application
     *               name, which would otherwise make every event claim the same origin
     *               regardless of which module actually published it
     * @return a monitoring event
     */
    public MonitorEvent toMonitoringEvent(String message, String level, String origin) {
        return new MonitorEvent(
            message,
            Instant.now().toString(),
            UUID.randomUUID().toString(),
            level,
            origin,
            InstanceIdentity.INSTANCE_ID,
            UUID.randomUUID().toString(),
            Map.of(),
            InstanceIdentity.HOSTNAME,
            environment
        );
    }
}
