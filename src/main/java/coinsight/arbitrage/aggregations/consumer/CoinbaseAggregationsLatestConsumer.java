package coinsight.arbitrage.aggregations.consumer;

import coinbase.ticker.CoinbaseEvent;
import coinsight.arbitrage.aggregations.pojo.TickRow;
import coinsight.arbitrage.aggregations.repositories.TickerRepository;
import coinsight.arbitrage.shared.model.Exchange;
import coinsight.arbitrage.shared.monitoring.MonitoringService;
import coinsight.arbitrage.shared.util.ExceptionUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class CoinbaseAggregationsLatestConsumer {

    private static final int PRICE_DECIMAL_PLACES = 8;

    private final MonitoringService monitoringService;

    private final TickerRepository tickerRepository;

    /**
     * Batch consumer for the latest coinbase events.
     * Responsible for aggregating the coinbase events.
     *
     * <p>Mapping failures are handled per-ticker (skip + log just that one) so a single
     * malformed message doesn't poison the whole batch - the actual DB write is still one
     * batch insert for everything that mapped successfully.
     *
     * @param coinbaseTickers batch of events to process
     */
    @KafkaListener(topics = "coinbase-latest-topic",
            containerFactory = "coinbaseAggregationsFactory")
    public void processMessages(List<CoinbaseEvent.CoinbaseTicker> coinbaseTickers) {
        List<TickRow> rows = new ArrayList<>(coinbaseTickers.size());

        for (CoinbaseEvent.CoinbaseTicker coinbaseTicker : coinbaseTickers) {
            try {
                rows.add(toTickRow(coinbaseTicker));
            } catch (Exception e) {
                monitoringService.publishEvent(
                        "Failed to map Coinbase ticker: " + ExceptionUtils.rootCauseMessage(e), "ERROR", "aggregations");
            }
        }

        try {
            tickerRepository.insertTicks(rows);
        } catch (Exception e) {
            monitoringService.publishEvent(
                    "Failed to persist Coinbase ticker batch of " + rows.size() + ": " + ExceptionUtils.rootCauseMessage(e),
                    "ERROR", "aggregations");
        }
    }

    private TickRow toTickRow(CoinbaseEvent.CoinbaseTicker coinbaseTicker) {
        BigDecimal scale = BigDecimal.TEN.pow(coinbaseTicker.getPriceScale());

        // Coinbase has an explicit last-traded price field, unlike Binance - use it
        // directly rather than deriving a midpoint.
        BigDecimal price = BigDecimal.valueOf(coinbaseTicker.getPrice())
                .divide(scale, PRICE_DECIMAL_PLACES, RoundingMode.HALF_UP);

        OffsetDateTime time = OffsetDateTime.parse(coinbaseTicker.getTimestamp());

        return new TickRow(time, Exchange.COINBASE.getValue(), coinbaseTicker.getCryptoPair(), price, coinbaseTicker.getMessageId());
    }
}
