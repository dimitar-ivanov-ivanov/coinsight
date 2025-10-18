package coinsight.arbitrage.ingestor.consumer;

import coinsight.arbitrage.ingestor.services.MonitoringService;
import coinsight.arbitrage.ingestor.services.binance.BinanceProcessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import ticker.BinanceTickerOuterClass;

@Service
@ConditionalOnProperty(name = "kafka.binance.dlt.enabled", havingValue = "true")
public class BinanceDeadLetterConsumer {

    @Autowired
    private BinanceProcessor binanceProcessor;

    @Autowired
    private MonitoringService monitoringService;

    /**
     * Consumer for the binance dead letter topic.
     * It is enabled manually via kafka.binance.dlt.enabled property.
     * Its responsibility is to re-emit the event to the original topic.
     *
     * @param message message to be re-emitted
     */
    @KafkaListener(topics = "binance-dlt-topic", containerFactory = "binanceDeadLetterListenerContainerFactory")
    public void reprocessEvent(String message) throws JsonProcessingException {
        BinanceTickerOuterClass.BinanceTicker producedEvent = binanceProcessor.process(message);

        String monitoringMessage;
        String severityLevel;
        if (producedEvent != null) {
            monitoringMessage = "[DLT] Successfully republished message with id " + producedEvent.getMessageId();
            severityLevel = "INFO";
        } else {
            // invalid input consumed from the topic
            // occurs when the message from Binance is null, or it doesn't contain "e" field which refers to the event type
            monitoringMessage = "[DLT] Problem with republished message: " + message;
            severityLevel = "ERROR";
        }

        monitoringService.publishEvent(monitoringMessage, severityLevel);

    }
}
