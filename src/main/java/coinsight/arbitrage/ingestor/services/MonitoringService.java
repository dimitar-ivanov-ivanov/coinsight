package coinsight.arbitrage.ingestor.services;

import ch.qos.logback.core.util.StringUtil;
import coinsight.arbitrage.ingestor.util.MonitoringMapper;
import coinsight.arbitrage.shared.model.DltEvent;
import coinsight.arbitrage.shared.model.MonitorEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class MonitoringService {

    @Autowired
    private MonitoringMapper monitoringMapper;

    @Autowired
    private KafkaTemplate<String, MonitorEvent> monitoringTemplate;

    @Autowired
    private KafkaTemplate<String, DltEvent> monitoringDltTemplate;

    @Value("${kafka.monitoring.topic}")
    private String monitoringTopic;

    @Value("${kafka.monitoring.dlt.topic}")
    private String dltMonitoringTopic;

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
            // publish raw to DLT
            DltEvent dltEvent = new DltEvent(
                message,
                severityLevel,
                System.currentTimeMillis(),
                null == ex.getMessage() ? "Error" : ex.getMessage(),
                UUID.randomUUID().toString()
            );
            monitoringDltTemplate.send(dltMonitoringTopic, dltEvent);
        }
    }
}
