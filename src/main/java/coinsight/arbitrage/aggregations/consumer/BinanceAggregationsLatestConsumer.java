package coinsight.arbitrage.aggregations.consumer;

import coinsight.arbitrage.aggregations.pojo.TickRow;
import coinsight.arbitrage.aggregations.repositories.TickerRepository;
import coinsight.arbitrage.shared.model.Exchange;
import coinsight.arbitrage.shared.monitoring.MonitoringService;
import coinsight.arbitrage.shared.util.ExceptionUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import ticker.BinanceTickerOuterClass;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class BinanceAggregationsLatestConsumer {

    private static final int PRICE_DECIMAL_PLACES = 8;

    private final MonitoringService monitoringService;

    private final TickerRepository tickerRepository;

    @KafkaListener(topics = "binance-latest-topic",
            containerFactory = "binanceAggregationsFactory")
    public void processMessages(List<BinanceTickerOuterClass.BinanceTicker> binanceTickers) {
        List<TickRow> rows = new ArrayList<>(binanceTickers.size());

        for (BinanceTickerOuterClass.BinanceTicker binanceTicker : binanceTickers) {
            try {
                rows.add(toTickRow(binanceTicker));
            } catch (Exception e) {
                monitoringService.publishEvent(
                        "Failed to map Binance ticker: " + ExceptionUtils.rootCauseMessage(e), "ERROR", "aggregations");
            }
        }

        try {
            tickerRepository.insertTicks(rows);
        } catch (Exception e) {
            monitoringService.publishEvent(
                    "Failed to persist Binance ticker batch of " + rows.size() + ": " + ExceptionUtils.rootCauseMessage(e),
                    "ERROR", "aggregations");
        }
    }

    private TickRow toTickRow(BinanceTickerOuterClass.BinanceTicker binanceTicker) {
        BigDecimal scale = BigDecimal.TEN.pow(binanceTicker.getPriceScale());

        // Binance has no single "price" field, only a two-sided bid/ask quote - unlike
        // Coinbase, which has an explicit last-traded price. Using the midpoint as a
        // stand-in for now; this is a deliberate simplification, not a hidden loss - the
        // actual bid/ask spread isn't persisted at all yet, and revisiting that is
        // exactly the spread-join work already sequenced later in the plan.
        BigDecimal price = BigDecimal.valueOf(binanceTicker.getBestBidPrice())
                .add(BigDecimal.valueOf(binanceTicker.getBestAskPrice()))
                .divide(BigDecimal.valueOf(2), PRICE_DECIMAL_PLACES, RoundingMode.HALF_UP)
                .divide(scale, PRICE_DECIMAL_PLACES, RoundingMode.HALF_UP);

        OffsetDateTime time = OffsetDateTime.parse(binanceTicker.getTimestamp());

        return new TickRow(time, Exchange.BINANCE.getValue(), binanceTicker.getCryptoPair(), price, binanceTicker.getMessageId());
    }
}
