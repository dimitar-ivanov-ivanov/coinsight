package coinsight.arbitrage.aggregations.repositories;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

@Repository
@RequiredArgsConstructor
public class TickerRepository {

    // Inserts a tick and handles idempontecy cehck with the DO NOTHING clause
    private static final String INSERT_TICK_SQL = """
            INSERT INTO ticks (time, exchange, crypto_pair, price, message_id)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (time, crypto_pair, message_id) DO NOTHING
            """;

    private final JdbcTemplate jdbcTemplate;

    public void insertTick(OffsetDateTime time, String exchange, String cryptoPair, BigDecimal price, String messageId) {
        jdbcTemplate.update(INSERT_TICK_SQL, time, exchange, cryptoPair, price, messageId);
    }
}
