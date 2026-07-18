package coinsight.arbitrage.aggregations.pojo;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

public record OhlcPoint(
    OffsetDateTime bucket,
    String exchange,
    String cryptoPair,
    BigDecimal open,
    BigDecimal high,
    BigDecimal low,
    BigDecimal close,
    BigDecimal avgPrice,
    long tickCount
) {
}
