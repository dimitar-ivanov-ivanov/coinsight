package coinsight.arbitrage.bff.consumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import ticker.BinanceTickerOuterClass;

@Service
public class BinanceLatestConsumer {

    @Autowired
    private RedisTemplate<String, String> redis;

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
