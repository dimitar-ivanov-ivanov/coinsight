package coinsight.arbitrage.aggregations.services;

import coinsight.arbitrage.aggregations.pojo.OhlcPoint;
import coinsight.arbitrage.aggregations.pojo.Tier;
import coinsight.arbitrage.aggregations.repositories.TickerRepository;
import coinsight.arbitrage.shared.model.Exchange;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;

import static org.springframework.http.HttpStatus.BAD_REQUEST;

@Service
@Slf4j
@RequiredArgsConstructor
public class AggregationsService {

    // Copied from Coinbase's real chart resolution choices, not picked arbitrarily -
    // "1 month -> hourly" on their platform works out to ~720 points. 750 is just above
    // that, so this budget reproduces their exact tier choices at every range they showed
    private static final int MAX_POINTS = 750;

    private final TickerRepository tickerRepository;

    public List<OhlcPoint> getAggregations(String cryptoPair, OffsetDateTime startDate, OffsetDateTime endDate, String exchange) {
        if (!endDate.isAfter(startDate)) {
            throw new ResponseStatusException(BAD_REQUEST, "endDate must be after startDate");
        }

        Tier tier = selectTier(startDate, endDate);
        return tickerRepository.findOhlc(tier, cryptoPair, startDate, endDate, normalizeExchange(exchange));
    }

    private String normalizeExchange(String exchange) {
        if (exchange == null) {
            return null;
        }

        try {
            return Exchange.valueOf(exchange.toUpperCase()).getValue();
        } catch (IllegalArgumentException ex) {
            throw new ResponseStatusException(BAD_REQUEST,
                    "Unknown exchange '" + exchange + "'. Valid values: " + Arrays.toString(Exchange.values()), ex);
        }
    }

    private Tier selectTier(OffsetDateTime start, OffsetDateTime end) {
        Duration range = Duration.between(start, end);

        if (range.dividedBy(Duration.ofMinutes(1)) <= MAX_POINTS) {
            return Tier.MINUTE;
        }
        if (range.dividedBy(Duration.ofMinutes(30)) <= MAX_POINTS) {
            return Tier.THIRTY_MINUTE;
        }
        if (range.dividedBy(Duration.ofHours(1)) <= MAX_POINTS) {
            return Tier.HOURLY;
        }
        return Tier.DAILY;
    }
}
