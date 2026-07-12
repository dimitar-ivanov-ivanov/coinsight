package coinsight.arbitrage.shared.monitoring;

import ch.qos.logback.core.util.StringUtil;
import coinsight.arbitrage.shared.model.MonitorEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class MonitoringService {

    @Autowired
    private MonitoringMapper monitoringMapper;

    @Autowired
    private KafkaTemplate<String, MonitorEvent> monitoringTemplate;

    @Value("${kafka.monitoring.topic}")
    private String monitoringTopic;

    /**
     * Method responsible for taking raw message, level, and origin and publishing a Kafka
     * event in JSON format to the designated topic.
     *
     * <p>The publish itself is non-blocking - {@code send()} returns as soon as the record
     * is queued, before Kafka has actually confirmed delivery. The real success/failure
     * outcome only becomes known later, asynchronously, when the returned future completes,
     * so that's where the failure logging has to live - a try/catch around {@code send()}
     * itself only ever sees synchronous failures (e.g. serialization), never a broker being
     * unreachable or a delivery timeout.
     *
     * @param message input raw message
     * @param severityLevel input severity level
     * @param origin the module/service this event originated from (e.g. "ingestor", "bff")
     */
    public void publishEvent(String message, String severityLevel, String origin) {
        if (StringUtil.isNullOrEmpty(message) || StringUtil.isNullOrEmpty(severityLevel)) {
            return;
        }

        try {
            MonitorEvent monitoringEvent = monitoringMapper.toMonitoringEvent(message, severityLevel, origin);
            monitoringTemplate.send(monitoringTopic, monitoringEvent.messageId(), monitoringEvent)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Failed to publish monitoring event to {}: [{}] {}",
                                    monitoringTopic, severityLevel, message, ex);
                        }
                    });
        } catch (Exception ex) {
            log.error("Failed to publish monitoring event to {}: [{}] {}",
                    monitoringTopic, severityLevel, message, ex);
        }
    }
}
