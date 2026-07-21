package coinsight.arbitrage.bff.consumer;

import coinsight.arbitrage.shared.monitoring.MonitoringService;
import com.google.protobuf.util.JsonFormat;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;
import spread.SpreadEventOuterClass;

@Service
@RequiredArgsConstructor
public class SpreadLatestConsumer {

    private final SimpMessagingTemplate messagingTemplate;

    private final MonitoringService monitoringService;

    /**
     * Consumer for the latest spread events (SpreadStream's derived output, not raw exchange
     * data).
     * Responsible for emitting the value of the event to the customer UI.
     * The emitting would be done through an open socket.
     *
     * @param spreadEvent input event to process
     */
    @KafkaListener(topics = "arbitrage-spread-topic",
            containerFactory = "spreadListenerContainerFactory")
    public void processMessage(SpreadEventOuterClass.SpreadEvent spreadEvent) {
        try {
            // Convert Protobuf -> JSON
            String json = JsonFormat.printer()
                    .alwaysPrintFieldsWithNoPresence()
                    .preservingProtoFieldNames()
                    .print(spreadEvent);

            // Send JSON through WebSocket
            messagingTemplate.convertAndSend("/topic/spread", json);
        } catch (Exception e) {
            monitoringService.publishEvent(
                    "Failed to relay spread event to client: " + e.getMessage(), "ERROR", "bff");
        }
    }
}
