package coinsight.arbitrage.bff.consumer;

import jakarta.annotation.PostConstruct;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import ticker.BinanceTickerOuterClass;

@Service
public class BinanceLatestConsumer {
    
    @PostConstruct
    public void init() {
        System.out.println("BinanceLatestConsumer bean created and initialized");
    }

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
        // Check for idempotency in Redis - find the fastest way to do it
        //
        System.out.println("Received event " + binanceTicker.getMessageId());
    }
}
