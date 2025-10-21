package coinsight.arbitrage.bff.consumer;

import com.google.protobuf.util.JsonFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;
import ticker.BinanceTickerOuterClass;

import java.time.Duration;

@Service
public class BinanceLatestConsumer {

    private static final Integer KEY_EXISTS_FLAG = 1;

    @Autowired
    private RedisTemplate<Integer, Integer> idempotencyTemplate;

    @Autowired
    private SimpMessagingTemplate messagingTemplate;

    @Value("${redis.binance.idempotency.ttl}")
    private Integer idempotencyTtlInSeconds;

    private Duration duration;

    /**
     * Consumer for the latest binance events.
     * Responsible for emitting the value of the event to the customer UI.
     * The emitting would be done through an open socket.
     *
     * @param binanceTicker input event to process
     */
    @KafkaListener(topics = "binance-latest-topic",
            containerFactory = "binanceLatestListenerContainerFactory")
    public void processMessage(BinanceTickerOuterClass.BinanceTicker binanceTicker) {
        int messageId = binanceTicker.getMessageId();

        // idempotency check
        Boolean wasAbsent = idempotencyTemplate.opsForValue()
                .setIfAbsent(messageId, KEY_EXISTS_FLAG, getDuration());

        if (!wasAbsent) {
            System.out.println("Duplicate message detected, skipping: " + messageId);
            return;
        }

        try {
            // Convert Protobuf -> JSON
            String json = JsonFormat.printer()
                    .alwaysPrintFieldsWithNoPresence()
                    .preservingProtoFieldNames()
                    .print(binanceTicker);

            // Send JSON through WebSocket
            messagingTemplate.convertAndSend("/topic/binance", json);

        } catch (Exception e) {
            System.err.println("Failed to convert Protobuf to JSON: " + e.getMessage());
        }

        //System.out.println("Received event " + binanceTicker.getMessageId());
    }

    // Purposefully done in order to skip allocating/deallocating an extra object per event
    private Duration getDuration() {
        if (duration != null) {
            return duration;
        }
        duration = Duration.ofSeconds(idempotencyTtlInSeconds);
        return duration;
    }
}
