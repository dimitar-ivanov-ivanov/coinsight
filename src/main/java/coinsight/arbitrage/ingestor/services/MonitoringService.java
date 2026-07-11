package coinsight.arbitrage.ingestor.services;

import ch.qos.logback.core.util.StringUtil;
import coinsight.arbitrage.ingestor.util.MonitoringMapper;
import coinsight.arbitrage.shared.model.MonitorEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class MonitoringService {

    private static final Logger log = LoggerFactory.getLogger(MonitoringService.class);

    @Autowired
    private MonitoringMapper monitoringMapper;

    @Autowired
    private KafkaTemplate<String, MonitorEvent> monitoringTemplate;

    @Value("${kafka.monitoring.topic}")
    private String monitoringTopic;

    /**
     * Method responsible for taking raw message and level and publishing a Kafka event
     * in JSON format to the designated topic.
     *
     * @param message input raw message
     * @param severityLevel input severity level
     */
    public void publishEvent(String message, String severityLevel) {
        if (StringUtil.isNullOrEmpty(message) || StringUtil.isNullOrEmpty(severityLevel)) {
            return;
        }

        try {
            MonitorEvent monitoringEvent = monitoringMapper.toMonitoringEvent(message, severityLevel);
            monitoringTemplate.send(monitoringTopic, monitoringEvent.messageId(), monitoringEvent);
        } catch (Exception ex) {
            log.error("Failed to publish monitoring event to {}: [{}] {}",
                    monitoringTopic, severityLevel, message, ex);
        }
    }
}
