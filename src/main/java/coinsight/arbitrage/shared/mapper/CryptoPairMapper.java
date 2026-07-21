package coinsight.arbitrage.shared.mapper;

import coinsight.arbitrage.shared.model.Exchange;
import lombok.experimental.UtilityClass;

import java.util.Map;
import java.util.Optional;

/**
 * Maps each exchange's native pair identifier (e.g. Binance's "BTCUSDT", Coinbase's "ETH-USD")
 * to one shared canonical identifier, so pairs from different exchanges can actually be joined
 * (see SpreadStream).
 *
 * Only pairs present on every currently-supported exchange are mapped at all
 */
@UtilityClass
public class CryptoPairMapper {

    private static final Map<String, String> BINANCE_TO_CANONICAL = Map.of(
        "BTCUSDT", "BTC-USDT",
        "ETHUSDT", "ETH-USDT",
        "SOLUSDT", "SOL-USDT",
        "XRPUSDT", "XRP-USDT"
    );

    private static final Map<String, String> COINBASE_TO_CANONICAL = Map.of(
        "BTC-USDT", "BTC-USDT",
        "ETH-USD", "ETH-USDT",
        "SOL-USD", "SOL-USDT",
        "XRP-USD", "XRP-USDT"
    );

    /**
     * @param exchange   which exchange the native pair identifier came from
     * @param nativePair the exchange's own pair string (e.g. "BTCUSDT", "ETH-USD")
     * @return the shared canonical identifier, or empty if this exchange/pair combination
     *         isn't one of the currently-supported, present-on-every-exchange pairs
     */
    public static Optional<String> toCanonical(Exchange exchange, String nativePair) {
        Map<String, String> table = switch (exchange) {
            case BINANCE -> BINANCE_TO_CANONICAL;
            case COINBASE -> COINBASE_TO_CANONICAL;
        };
        return Optional.ofNullable(table.get(nativePair));
    }
}
