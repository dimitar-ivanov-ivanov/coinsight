package coinsight.arbitrage.bff.consumer;

import coinbase.ticker.CoinbaseEvent;
import coinsight.arbitrage.shared.monitoring.MonitoringService;
import com.google.protobuf.util.JsonFormat;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class CoinbaseLatestConsumer {

    private final SimpMessagingTemplate messagingTemplate;

    private final MonitoringService monitoringService;

    /**
     * Consumer for the latest coinbase events.
     * Responsible for emitting the value of the event to the customer UI.
     * The emitting would be done through an open socket.
     * No idempotency check as the events come in so fast even if there is a duplicate
     * the clients won't notice.
     *
     * @param coinbaseTicker input event to process
     */
    @KafkaListener(topics = "coinbase-latest-topic",
            containerFactory = "coinbaseLatestListenerContainerFactory")
    public void processMessage(CoinbaseEvent.CoinbaseTicker coinbaseTicker) {
        try {
            // TODO: Deal with prices and their scale, then parse to json
            // Convert Protobuf -> JSON
            String json = JsonFormat.printer()
                    .alwaysPrintFieldsWithNoPresence()
                    .preservingProtoFieldNames()
                    .print(coinbaseTicker);

            // Send JSON through WebSocket
            messagingTemplate.convertAndSend("/topic/coinbase", json);
        } catch (Exception e) {
            monitoringService.publishEvent(
                    "Failed to relay Coinbase ticker to client: " + e.getMessage(), "ERROR", "bff");
        }
    }
}
