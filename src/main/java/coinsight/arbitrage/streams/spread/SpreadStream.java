package coinsight.arbitrage.streams.spread;

import coinbase.ticker.CoinbaseEvent;
import coinsight.arbitrage.shared.mapper.CryptoPairMapper;
import coinsight.arbitrage.shared.model.Exchange;
import coinsight.arbitrage.streams.binance.BinanceTickerSerde;
import coinsight.arbitrage.streams.coinbase.CoinbaseTickerSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import spread.SpreadEventOuterClass.SpreadEvent;
import ticker.BinanceTickerOuterClass.BinanceTicker;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

@Component
public class SpreadStream {

    private static final int PRICE_DECIMAL_PLACES = 8;

    @Value("${kafka.binance.latest.topic}")
    private String binanceLatestTopic;

    @Value("${kafka.coinbase.latest.topic}")
    private String coinbaseLatestTopic;

    @Value("${kafka.spread.topic}")
    private String spreadTopic;

    @Autowired
    public void spreadStream(StreamsBuilder builder) {
        Serde<BinanceTicker> binanceSerde = new BinanceTickerSerde();
        Serde<CoinbaseEvent.CoinbaseTicker> coinbaseSerde = new CoinbaseTickerSerde();
        Serde<SpreadEvent> spreadSerde = new SpreadEventSerde();

        KTable<String, BinanceTicker> binanceTable =
                builder.table(binanceLatestTopic, Consumed.with(Serdes.String(), binanceSerde));

        KTable<String, CoinbaseEvent.CoinbaseTicker> coinbaseTable =
                builder.table(coinbaseLatestTopic, Consumed.with(Serdes.String(), coinbaseSerde));

        // Rekey both sides onto the shared canonical pair identifier (CryptoPairMapper)
        KTable<String, BinanceTicker> canonicalBinance = binanceTable.toStream()
                .filter((pair, ticker) -> CryptoPairMapper.toCanonical(Exchange.BINANCE, pair).isPresent())
                .selectKey((pair, ticker) -> CryptoPairMapper.toCanonical(Exchange.BINANCE, pair).orElseThrow())
                .groupByKey(Grouped.<String, BinanceTicker>as("binance-canonical-grouped")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(binanceSerde))
                .reduce((oldVal, newVal) -> newVal,
                        Materialized.<String, BinanceTicker, KeyValueStore<Bytes, byte[]>>as("binance-canonical-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(binanceSerde));

        KTable<String, CoinbaseEvent.CoinbaseTicker> canonicalCoinbase = coinbaseTable.toStream()
                .filter((pair, ticker) -> CryptoPairMapper.toCanonical(Exchange.COINBASE, pair).isPresent())
                .selectKey((pair, ticker) -> CryptoPairMapper.toCanonical(Exchange.COINBASE, pair).orElseThrow())
                .groupByKey(Grouped.<String, CoinbaseEvent.CoinbaseTicker>as("coinbase-canonical-grouped")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(coinbaseSerde))
                .reduce((oldVal, newVal) -> newVal,
                        Materialized.<String, CoinbaseEvent.CoinbaseTicker, KeyValueStore<Bytes, byte[]>>as("coinbase-canonical-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(coinbaseSerde));

        // KTable-KTable join, not a windowed KStream join, on purpose - each side already
        // represents "the latest known price for this pair", so this re-fires the instant
        // EITHER exchange updates, always using the other side's most recent known value.
        KTable<String, SpreadEvent> spreadTable = canonicalBinance.join(canonicalCoinbase,
                SpreadStream::computeSpread,
                Materialized.<String, SpreadEvent, KeyValueStore<Bytes, byte[]>>as("spread-join-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(spreadSerde));

        spreadTable.toStream().to(spreadTopic, Produced.with(Serdes.String(), spreadSerde));
    }

    private static SpreadEvent computeSpread(BinanceTicker binanceTicker, CoinbaseEvent.CoinbaseTicker coinbaseTicker) {
        BigDecimal binanceScale = BigDecimal.TEN.pow(binanceTicker.getPriceScale());
        BigDecimal binanceBestBid = BigDecimal.valueOf(binanceTicker.getBestBidPrice())
                .divide(binanceScale, PRICE_DECIMAL_PLACES, RoundingMode.HALF_UP);
        BigDecimal binanceBestAsk = BigDecimal.valueOf(binanceTicker.getBestAskPrice())
                .divide(binanceScale, PRICE_DECIMAL_PLACES, RoundingMode.HALF_UP);

        BigDecimal coinbaseScale = BigDecimal.TEN.pow(coinbaseTicker.getPriceScale());
        BigDecimal coinbaseBestBid = BigDecimal.valueOf(coinbaseTicker.getBestBid())
                .divide(coinbaseScale, PRICE_DECIMAL_PLACES, RoundingMode.HALF_UP);
        BigDecimal coinbaseBestAsk = BigDecimal.valueOf(coinbaseTicker.getBestAsk())
                .divide(coinbaseScale, PRICE_DECIMAL_PLACES, RoundingMode.HALF_UP);

        // Both directions - buy on the exchange with the lower ask, sell on the one with the
        // higher bid. A naive midpoint diff would show a "spread" nobody could actually
        // execute at.
        BigDecimal spreadBuyBinanceSellCoinbase = coinbaseBestBid.subtract(binanceBestAsk);
        BigDecimal spreadBuyCoinbaseSellBinance = binanceBestBid.subtract(coinbaseBestAsk);

        // Deterministic, not random - derived from both source tickers' own message_ids, so
        // the exact same pair of updates always produces the same spread event id
        String combinedSourceIds = binanceTicker.getMessageId() + "|" + coinbaseTicker.getMessageId();
        String messageId = UUID.nameUUIDFromBytes(combinedSourceIds.getBytes(StandardCharsets.UTF_8)).toString();

        String canonicalPair = CryptoPairMapper.toCanonical(Exchange.BINANCE, binanceTicker.getCryptoPair())
                .orElse(binanceTicker.getCryptoPair());

        return SpreadEvent.newBuilder()
                .setCryptoPair(canonicalPair)
                .setMessageId(messageId)
                .setTimestamp(Instant.now().toString())
                .setBinanceBestBid(binanceBestBid.doubleValue())
                .setBinanceBestAsk(binanceBestAsk.doubleValue())
                .setCoinbaseBestBid(coinbaseBestBid.doubleValue())
                .setCoinbaseBestAsk(coinbaseBestAsk.doubleValue())
                .setSpreadBuyBinanceSellCoinbase(spreadBuyBinanceSellCoinbase.doubleValue())
                .setSpreadBuyCoinbaseSellBinance(spreadBuyCoinbaseSellBinance.doubleValue())
                .build();
    }
}
