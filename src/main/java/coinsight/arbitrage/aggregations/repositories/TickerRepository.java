package coinsight.arbitrage.aggregations.repositories;

import coinsight.arbitrage.aggregations.pojo.OhlcPoint;
import coinsight.arbitrage.aggregations.pojo.Tier;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class TickerRepository {

    // Inserts a tick and handles idempontecy check with the DO NOTHING clause
    private static final String INSERT_TICK_SQL = """
            INSERT INTO ticks (time, exchange, crypto_pair, price, message_id)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (time, crypto_pair, message_id) DO NOTHING
            """;

    private static final String OHLC_SELECT = """
            SELECT bucket, exchange, crypto_pair, open, high, low, close, avg_price, tick_count
            FROM %s
            WHERE crypto_pair = ? AND bucket >= ? AND bucket <= ?
            """;

    private final JdbcTemplate jdbcTemplate;

    public void insertTick(OffsetDateTime time, String exchange, String cryptoPair, BigDecimal price, String messageId) {
        jdbcTemplate.update(INSERT_TICK_SQL, time, exchange, cryptoPair, price, messageId);
    }

    public List<OhlcPoint> findOhlc(Tier tier, String cryptoPair, OffsetDateTime start, OffsetDateTime end, String exchange) {
        String table = switch (tier) {
            case MINUTE -> "ticks_minute";
            case THIRTY_MINUTE -> "ticks_30min";
            case HOURLY -> "ticks_hourly";
            case DAILY -> "ticks_daily";
        };

        StringBuilder sql = new StringBuilder(OHLC_SELECT.formatted(table));
        List<Object> params = new ArrayList<>(List.of(cryptoPair, start, end));

        if (exchange != null) {
            sql.append("AND exchange = ?\n");
            params.add(exchange);
        }
        sql.append("ORDER BY bucket ASC");

        return jdbcTemplate.query(sql.toString(), (rs, rowNum) -> new OhlcPoint(
                rs.getObject("bucket", OffsetDateTime.class),
                rs.getString("exchange"),
                rs.getString("crypto_pair"),
                rs.getBigDecimal("open"),
                rs.getBigDecimal("high"),
                rs.getBigDecimal("low"),
                rs.getBigDecimal("close"),
                rs.getBigDecimal("avg_price"),
                rs.getLong("tick_count")
        ), params.toArray());
    }
}
