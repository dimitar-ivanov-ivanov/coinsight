package coinsight.arbitrage.ingestor.util;

import coinbase.ticker.CoinbaseEvent;
import coinsight.arbitrage.util.MessageIdGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;

import static coinsight.arbitrage.ingestor.util.PriceScaler.scaleToInt64;

@Component
public class CoinbaseMapper {

    private static final int PRICE_SCALE = 2;

    private static final int QUANTITY_SCALE = 8;

    private static final BigDecimal PRICE_MULTIPLY = BigDecimal.valueOf(Math.pow(10, PRICE_SCALE));

    private static final BigDecimal QUANTITY_MULTIPLY = BigDecimal.valueOf(Math.pow(10, QUANTITY_SCALE));

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Mapper which takes raw message from Coinbase and converts it to a Protobuf representation.
     *
     * @param message input raw message from Coinbase
     * @return parsed protobuf class version of the message
     * @throws JsonProcessingException if there's a problem with extracting fields
     */
    public CoinbaseEvent.CoinbaseTicker toCoinbaseTicker(String message) throws JsonProcessingException {
        if (message == null) {
            return null;
        }

        JsonNode root = MAPPER.readTree(message);

        if (root == null || root.get("events") == null) {
            return null;
        }

        JsonNode tickers = root.get("events").get(0).get("tickers");

        if (tickers == null) {
            return null;
        }

        // only one ticker is expected in an event
        JsonNode ticker = tickers.get(0);
        String pair = ticker.get("product_id").asText();
        String timestamp = root.get("timestamp").asText();
        Double volume = ticker.get("volume_24_h").asDouble();

        return CoinbaseEvent.CoinbaseTicker.newBuilder()
            .setCryptoPair(ticker.get("product_id").asText())
            .setBestBid(scaleToInt64(ticker.get("best_bid").asText(), PRICE_MULTIPLY))
            .setBestAsk(scaleToInt64(ticker.get("best_ask").asText(), PRICE_MULTIPLY))
            .setPrice(scaleToInt64(ticker.get("price").asText(), PRICE_MULTIPLY))
            .setBestBidQuantity(scaleToInt64(ticker.get("best_bid_quantity").asText(), QUANTITY_MULTIPLY))
            .setBestAskQuantity(scaleToInt64(ticker.get("best_ask_quantity").asText(), QUANTITY_MULTIPLY))
            .setEventTime(root.get("timestamp").asText())
            .setTimestamp(Instant.now().toString())
            .setMessageId(MessageIdGenerator.generateMessageId(timestamp, pair, volume))
            .setPriceScale(PRICE_SCALE)
            .setQuantityScale(QUANTITY_SCALE)
            .build();
    }
}
