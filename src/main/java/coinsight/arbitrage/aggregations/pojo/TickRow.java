package coinsight.arbitrage.aggregations.pojo;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

/**
 * One tick, already mapped/descaled and ready to insert - the shape TickerRepository.insertTicks
 * batches into a single JDBC batch statement.
 */
public record TickRow(
    OffsetDateTime time,
    String exchange,
    String cryptoPair,
    BigDecimal price,
    String messageId
) {
}
