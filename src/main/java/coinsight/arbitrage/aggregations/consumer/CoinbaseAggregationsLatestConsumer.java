package coinsight.arbitrage.aggregations.consumer;

import coinbase.ticker.CoinbaseEvent;
import coinsight.arbitrage.aggregations.repositories.TickerRepository;
import coinsight.arbitrage.shared.monitoring.MonitoringService;
import coinsight.arbitrage.shared.util.ExceptionUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.OffsetDateTime;

@Service
@RequiredArgsConstructor
public class CoinbaseAggregationsLatestConsumer {

    private static final String EXCHANGE = "coinbase";

    private static final int PRICE_DECIMAL_PLACES = 8;

    private final MonitoringService monitoringService;

    private final TickerRepository tickerRepository;

    /**
     * Consumer for the latest coinbase events.
     * Responsible for aggregating the coinbase events.
     *
     * @param coinbaseTicker input event to process
     */
    @KafkaListener(topics = "coinbase-latest-topic",
            containerFactory = "coinbaseAggregationsFactory")
    public void processMessage(CoinbaseEvent.CoinbaseTicker coinbaseTicker) {
        try {
            BigDecimal scale = BigDecimal.TEN.pow(coinbaseTicker.getPriceScale());

            // Coinbase has an explicit last-traded price field, unlike Binance - use it
            // directly rather than deriving a midpoint.
            BigDecimal price = BigDecimal.valueOf(coinbaseTicker.getPrice())
                    .divide(scale, PRICE_DECIMAL_PLACES, RoundingMode.HALF_UP);

            OffsetDateTime time = OffsetDateTime.parse(coinbaseTicker.getTimestamp());

            tickerRepository.insertTick(
                    time, EXCHANGE, coinbaseTicker.getCryptoPair(), price, coinbaseTicker.getMessageId());
        } catch (Exception e) {
            monitoringService.publishEvent(
                    "Failed to persist Coinbase ticker: " + ExceptionUtils.rootCauseMessage(e), "ERROR", "aggregations");
        }
    }
}
