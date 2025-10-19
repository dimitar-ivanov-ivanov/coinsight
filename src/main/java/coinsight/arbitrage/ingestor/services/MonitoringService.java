package coinsight.arbitrage.ingestor.services;

import ch.qos.logback.core.util.StringUtil;
import coinsight.arbitrage.ingestor.util.MonitoringMapper;
import monitor.MonitorDtlEvent;
import monitor.MonitorEventOuterClass;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class MonitoringService {

    @Autowired
    private MonitoringMapper monitoringMapper;

    @Autowired
    private KafkaTemplate<String, MonitorEventOuterClass.MonitorEvent> monitoringTemplate;

    @Autowired
    private KafkaTemplate<String, MonitorDtlEvent.DltEvent> monitoringDltTemplate;

    @Value("${kafka.monitoring.topic}")
    private String monitoringTopic;

    @Value("${kafka.monitoring.dlt.topic}")
    private String dltMonitoringTopic;

    /**
     * Method responsible for taking raw message and level and publishing a Kafka event
     * in protobuf format to the designated topic.
     *
     * @param message input raw message
     * @param severityLevel input severity level
     */
    public void publishEvent(String message, String severityLevel) {
        if (StringUtil.isNullOrEmpty(message) || StringUtil.isNullOrEmpty(severityLevel)) {
            return;
        }

        try {
            MonitorEventOuterClass.MonitorEvent monitoringEvent =
                monitoringMapper.toMonitoringEvent(message, severityLevel);
            monitoringTemplate.send(monitoringTopic, monitoringEvent.getMessageId(), monitoringEvent);
        } catch (Exception ex) {
            // publish raw to DLT
            MonitorDtlEvent.DltEvent dltEvent = MonitorDtlEvent.DltEvent.newBuilder()
                .setMessage(message)
                .setSeverityLevel(severityLevel)
                .setTimestamp(System.currentTimeMillis())
                .setErrorReason(null == ex.getMessage() ? "Error" : ex.getMessage())
                .build();
            monitoringDltTemplate.send(dltMonitoringTopic, dltEvent);
        }
    }
}

