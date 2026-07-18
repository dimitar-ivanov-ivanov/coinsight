package coinsight.arbitrage.aggregations.consumer;

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

@Service
@RequiredArgsConstructor
public class BinanceAggregationsLatestConsumer {

    private static final int PRICE_DECIMAL_PLACES = 8;

    private final MonitoringService monitoringService;

    private final TickerRepository tickerRepository;

    /**
     * Consumer for the latest binance events.
     * Responsible for aggregating the binance events.
     *
     * @param binanceTicker input event to process
     */
    @KafkaListener(topics = "binance-latest-topic",
            containerFactory = "binanceAggregationsFactory")
    public void processMessage(BinanceTickerOuterClass.BinanceTicker binanceTicker) {
        try {
            BigDecimal scale = BigDecimal.TEN.pow(binanceTicker.getPriceScale());

            // Binance has no single "price" field, only a two-sided bid/ask quote
            // Using the midpoint as a stand-in for now, this is a deliberate simplification
            BigDecimal price = BigDecimal.valueOf(binanceTicker.getBestBidPrice())
                    .add(BigDecimal.valueOf(binanceTicker.getBestAskPrice()))
                    .divide(BigDecimal.valueOf(2), PRICE_DECIMAL_PLACES, RoundingMode.HALF_UP)
                    .divide(scale, PRICE_DECIMAL_PLACES, RoundingMode.HALF_UP);

            OffsetDateTime time = OffsetDateTime.parse(binanceTicker.getTimestamp());

            tickerRepository.insertTick(
                    time, Exchange.BINANCE.getValue(), binanceTicker.getCryptoPair(), price, binanceTicker.getMessageId());
        } catch (Exception e) {
            monitoringService.publishEvent(
                    "Failed to persist Binance ticker: " + ExceptionUtils.rootCauseMessage(e), "ERROR", "aggregations");
        }
    }
}
