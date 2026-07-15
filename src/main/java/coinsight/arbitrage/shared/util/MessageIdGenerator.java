package coinsight.arbitrage.shared.util;

import lombok.experimental.UtilityClass;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

@UtilityClass
public class MessageIdGenerator {

    /**
     * Method for generating deterministic UUID for a crypto event.
     * Takes into consideration the event time, crypto pair and volume.
     *
     * @param eventTime input event time
     * @param cryptoPair the crypto for which the event happened (ex: BTC-USD, ETH-USDC)
     * @param volume the volume of pair during the sell/buy
     *
     * @return UUID that uniquely identifiers the event
     */
    public static String generateMessageId(long eventTime, String cryptoPair, long volume) {
        StringBuilder sb = new StringBuilder(64);

        sb.append(eventTime)
            .append('|')
            .append(cryptoPair)
            .append('|')
            .append(volume);

        byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
        return UUID.nameUUIDFromBytes(bytes).toString();
    }

    /**
     * Generate message id using different params
     * @param eventTime time of event
     * @param cryptoPair pair
     * @param volume current volume
     * @return UUID that uniquely identifiers the event
     */
    public static String generateMessageId(String eventTime, String cryptoPair, double volume) {
        StringBuilder sb = new StringBuilder(64);
        sb.append(eventTime)
                .append('|')
                .append(cryptoPair)
                .append('|')
                .append(volume);

        byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
        return UUID.nameUUIDFromBytes(bytes).toString();
    }
}
