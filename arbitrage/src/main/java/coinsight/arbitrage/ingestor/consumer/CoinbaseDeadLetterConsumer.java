package coinsight.arbitrage.ingestor.consumer;

import coinbase.ticker.CoinbaseEvent;
import coinsight.arbitrage.ingestor.services.MonitoringService;
import coinsight.arbitrage.ingestor.services.coinbase.CoinbaseProcessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "kafka.coinbase.dlt.enabled", havingValue = "true")
public class CoinbaseDeadLetterConsumer {

    @Autowired
    private CoinbaseProcessor coinbaseProcessor;

    @Autowired
    private MonitoringService monitoringService;

    /**
     * Consumer for the coinbase dead letter topic.
     * It is enabled manually via kafka.coinbase.dlt.enabled property.
     * Its responsibility is to re-emit the event to the original topic.
     *
     * @param message message to be re-emitted
     */
    @KafkaListener(topics = "coinbase-dlt-topic", containerFactory = "coinbaseDeadLetterListenerContainerFactory")
    public void reprocessEvent(String message) throws JsonProcessingException {
        CoinbaseEvent.CoinbaseTicker producedEvent = coinbaseProcessor.process(message);
        String monitoringMessage;
        String severityLevel;

        if (producedEvent != null) {
            monitoringMessage = "[DLT] Successfully republished message with id " + producedEvent.getMessageId();
            severityLevel = "INFO";
        } else {
            // invalid input consumed from the topic
            // occurs when the message from Coinbase is null, or it doesn't contain "events"
            monitoringMessage = "[DLT] Problem with republished message: " + message;
            severityLevel = "ERROR";
        }

        monitoringService.publishEvent(monitoringMessage, severityLevel);
    }
}
