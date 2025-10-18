package coinsight.arbitrage.util;

import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

import java.nio.charset.StandardCharsets;

public final class MessageIdGenerator {

    //private static final HashFunction MURMUR_HASH = Hashing.murmur3_32();

    private static final XXHash32 XX_HASH = XXHashFactory.fastestInstance().hash32();
    private static final int SEED = 0; // Consistent seed for deterministic results

    // Tried to optimize even more with adding an object pool for StringBuilder in order to skip new StringBuilder(64);
    // Turns out taking from the pool, then returning adds too much extra overhead and made it twice as slow
    //private static final SimpleStringBuilderPool POOL = new SimpleStringBuilderPool(100);

    private MessageIdGenerator() {
        // no instances
    }

    /**
     * Method for generating deterministic hash for a crypto event.
     * Takes into consideration the event time, crypto pair and volume.
     * Uses XX_HASH function, which was the most performant than other functions I tried.
     * It is also secure, so we should be fine.
     *
     * @param eventTime input event time
     * @param cryptoPair the crypto for which the event happened (ex: BTC-USD, ETH-USDC)
     * @param volume the volume of pair during the sell/buy
     *
     * @return hash for the event
     */
    public static int generateMessageId(long eventTime, String cryptoPair, long volume) {
        // Don't use String here, concatenation makes things 2-3x slower
        StringBuilder sb = new StringBuilder(64);

        sb.append(eventTime)
            .append('|')
            .append(cryptoPair)
            .append('|')
            .append(volume);

        // Old Generate deterministic hash
        //String hash = DigestUtils.sha256Hex(composite);
        //String result = hash.substring(0, 16); // Keep it reasonably short

        // Murmush hash is fast, memory efficient and the chance of collision is very low
        //int hash = MURMUR_HASH.hashString(sb.toString(), StandardCharsets.UTF_8).asInt();

        // XX_HASH is the fastest and also secure for our use case
        byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
        return XX_HASH.hash(bytes, 0, bytes.length, SEED);
    }

    /**
     * Generate message id using different params
     * @param eventTime time of event
     * @param cryptoPair pair
     * @param volume current volume
     * @return hash that uniquely identifiers the event
     */
    public static int generateMessageId(String eventTime, String cryptoPair, double volume) {
        StringBuilder sb = new StringBuilder(64);
        sb.append(eventTime)
                .append('|')
                .append(cryptoPair)
                .append('|')
                .append(volume);

        byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
        return XX_HASH.hash(bytes, 0, bytes.length, SEED);
    }
}
