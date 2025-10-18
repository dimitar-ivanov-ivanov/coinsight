package coinsight.arbitrage.ingestor.util;

import coinsight.arbitrage.util.MessageIdGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import ticker.BinanceTickerOuterClass;

import java.math.BigDecimal;
import java.time.Instant;

import static coinsight.arbitrage.ingestor.util.PriceScaler.scaleToInt64;

@Component
public class BinanceMapper {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final int PRICE_SCALE = 8;

    private static final BigDecimal PRICE_MULTIPLY = BigDecimal.valueOf(Math.pow(10, PRICE_SCALE));

    /**
     * Mapper which takes raw message from Binance and converts it to a Protobuf representation.
     * All the numbers will be multiplied by 10^8 in order to avoid serialization and then deserialization to string
     * The consumers of this event have to know to divide by 10^8
     *
     * The key of the kafka message HAS to be the crypto pair.
     * This ensures that all events for one pair go into the same partition.
     * One partition is processed by one thread, so all events for one pair are processed sequentially.
     * If we use anything else we'll have race conditions.
     *
     * @param message input raw message from Binance
     * @return parsed protobuf class version of the message
     * @throws JsonProcessingException if there's a problem with extracting fields
     */
    public BinanceTickerOuterClass.BinanceTicker toBinanceTicker(String message) throws JsonProcessingException {
        if (message == null) {
            return null;
        }

        JsonNode root = MAPPER.readTree(message);

        // initial message after SUBSCRIBING will be response for successful subscription
        if (root == null || root.get("e") == null) {
            return null;
        }

        long eventTime = root.get("E").asLong();
        String cryptoPair = root.get("s").asText();
        long volume = scaleToInt64(root.get("v").asText(), PRICE_MULTIPLY);

        return BinanceTickerOuterClass.BinanceTicker.newBuilder()
            .setEventTime(eventTime)
            .setCryptoPair(cryptoPair)
            .setBestBidPrice(scaleToInt64(root.get("b").asText(), PRICE_MULTIPLY))
            .setBestBidQty(scaleToInt64(root.get("B").asText(), PRICE_MULTIPLY))
            .setBestAskPrice(scaleToInt64(root.get("a").asText(), PRICE_MULTIPLY))
            .setBestAskQty(scaleToInt64(root.get("A").asText(), PRICE_MULTIPLY))
            .setVolume(volume)
            .setTimestamp(Instant.now().toString())
            .setMessageId(MessageIdGenerator.generateMessageId(eventTime, cryptoPair, volume))
            .setPriceScale(PRICE_SCALE)
            .build();
    }
}
