package coinsight.arbitrage.shared.mapper;

import coinsight.arbitrage.shared.model.Exchange;

import java.util.Map;
import java.util.Optional;

/**
 * Maps each exchange's native pair identifier (e.g. Binance's "BTCUSDT", Coinbase's "ETH-USD")
 * to one shared canonical identifier, so pairs from different exchanges can actually be joined
 * (see SpreadStream).
 *
 * <p>Deliberately hub-and-spoke - each exchange maps to ONE canonical form - rather than
 * pairwise exchange-to-exchange mappings, since more exchanges are planned. A pairwise design
 * would need a new translation table between every pair of exchanges (O(n^2) as exchanges
 * grow); this only ever needs one new mapping per new exchange (O(n)).
 *
 * <p>The canonical form always uses USDT as the quote currency label, even for pairs actually
 * quoted in USD on their native exchange (e.g. Coinbase's "ETH-USD"). USDT is treated as
 * economically equivalent to USD for this app's comparison purposes - a standard simplification
 * in cross-exchange crypto tooling, given USDT's near-1:1 USD peg - not because they're
 * literally the same asset. A genuine USDT de-peg would show up here as an apparent arbitrage
 * spread that isn't really one.
 *
 * <p>Only pairs present on every currently-supported exchange are mapped at all - see the
 * subscribed pair lists in BinanceWebSocketClient/CoinbaseWebSocketClient, which were pruned to
 * match this exact set.
 */
public final class CryptoPairMapper {

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

    private CryptoPairMapper() {
        // no instances
    }

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
