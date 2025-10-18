package coinsight.arbitrage.ingestor.util;

import monitor.MonitorEventOuterClass;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
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
     * The chosen format of the event is Protobuf.
     *
     * @param message input message
     * @param level input severity level
     * @return a monitoring event using protobuf
     */
    public MonitorEventOuterClass.MonitorEvent toMonitoringEvent(String message, String level) {
        return MonitorEventOuterClass.MonitorEvent.newBuilder()
            .setMessage(message)
            .setTimestamp(Instant.now().toString())
            .setMessageId(UUID.randomUUID().toString())
            .setLevel(level)
            .setServiceName(serviceName)
            .setInstanceId(INSTANCE_ID)
            .setTraceId(UUID.randomUUID().toString())
            .setHost(HOSTNAME)
            .setEnvironment(environment)
            .build();
    }
}
