package coinsight.arbitrage.ingestor.util;

import java.math.BigDecimal;

public final class PriceScaler {

    private PriceScaler() {
        // no instances
    }

    /**
     * Responsible for taking string and parsing it to long
     * with added scale to the number.
     * Used for scaling prices from Binance, Coinbase events.
     *
     * @param amountAsString input amount as string
     * @param multiplier the amount we have to multiple the number
     * @return scaled number
     */
    static long scaleToInt64(String amountAsString, BigDecimal multiplier) {
        BigDecimal amount = new BigDecimal(amountAsString);
        return amount.multiply(multiplier).longValue();
    }
}
