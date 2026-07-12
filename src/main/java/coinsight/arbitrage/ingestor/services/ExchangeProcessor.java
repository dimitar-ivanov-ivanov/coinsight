package coinsight.arbitrage.ingestor.services;

import coinsight.arbitrage.shared.monitoring.MonitoringService;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.UUID;

public abstract class ExchangeProcessor<T> {

    @Autowired
    private KafkaTemplate<String, String> dltTemplate;

    @Autowired
    protected MonitoringService monitoringService;

    /**
     * Try to process the input message from the exchange.
     * On failure emit a message in the monitoring topic and the dlt for the given exchange.
     *
     * @param message input message from the exchange
     */
    public void processMessage(String message) {
        try {
            process(message);
        } catch (Exception ex) {
            monitoringService.publishEvent(getExchangeName() + "Error: " + ex.getMessage(), "ERROR", "ingestor");

            // send message to DLT
            dltTemplate.send(getDltTopicName(), UUID.randomUUID().toString(), message);

            //Don't rethrow here, it will terminate the connection
        }
    }

    protected abstract String getDltTopicName();

    protected abstract String getExchangeName();

    public abstract T process(String message) throws JsonProcessingException;
}
