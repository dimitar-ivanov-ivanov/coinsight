package coinsight.arbitrage.bff.consumer;

import coinsight.arbitrage.shared.monitoring.MonitoringService;
import com.google.protobuf.util.JsonFormat;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;
import ticker.BinanceTickerOuterClass;

@Service
@RequiredArgsConstructor
public class BinanceLatestConsumer {

    private final SimpMessagingTemplate messagingTemplate;

    private final MonitoringService monitoringService;

    /**
     * Consumer for the latest binance events.
     * Responsible for emitting the value of the event to the customer UI.
     * The emitting would be done through an open socket.
     * No idempotency check as the events come in so fast even if there is a duplicate
     * the clients won't notice.
     *
     * @param binanceTicker input event to process
     */
    @KafkaListener(topics = "binance-latest-topic",
            containerFactory = "binanceLatestListenerContainerFactory")
    public void processMessage(BinanceTickerOuterClass.BinanceTicker binanceTicker) {
        try {
            // TODO: Deal with prices and their scale, then parse to json
            // Convert Protobuf -> JSON
            String json = JsonFormat.printer()
                    .alwaysPrintFieldsWithNoPresence()
                    .preservingProtoFieldNames()
                    .print(binanceTicker);

            // Send JSON through WebSocket
            messagingTemplate.convertAndSend("/topic/binance", json);
        } catch (Exception e) {
            monitoringService.publishEvent(
                    "Failed to relay Binance ticker to client: " + e.getMessage(), "ERROR", "bff");
        }
    }
}
