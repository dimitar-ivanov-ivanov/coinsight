-- 1. Raw ticks hypertable
-- One row per tick consumed
CREATE TABLE IF NOT EXISTS ticks (
    time        TIMESTAMPTZ NOT NULL,
    exchange    TEXT        NOT NULL,
    crypto_pair TEXT        NOT NULL,
    price       NUMERIC     NOT NULL
);

-- Time partitioning (required for every hypertable) - one chunk per day.
SELECT create_hypertable(
    'ticks',
    by_range('time', INTERVAL '1 day'),
    if_not_exists => TRUE
);

-- Space partitioning on crypto_pair, in addition to time - a query scoped to
-- one pair doesn't need to touch chunks holding every other pair's data.
SELECT add_dimension('ticks', by_hash('crypto_pair', 4), if_not_exists => TRUE);

-- The time column is indexed automatically by create_hypertable - this
-- covers the actual query shape (one pair/exchange, ordered by recency).
CREATE INDEX IF NOT EXISTS idx_ticks_pair_time
    ON ticks (crypto_pair, exchange, time DESC);

-- 2. Daily OHLC continuous aggregate (the "1 month view -> 1 point/day" tier)
-- OHLC instead of a flat average, on purpose: an average of a day that
-- spiked and came back down is indistinguishable from a day that never
-- moved at all
CREATE MATERIALIZED VIEW IF NOT EXISTS ticks_daily
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', time) AS bucket,
    exchange,
    crypto_pair,
    first(price, time) AS open,
    max(price)         AS high,
    min(price)         AS low,
    last(price, time)  AS close,
    avg(price)         AS avg_price, -- optional "center line", OHLC is the real signal
    count(*)           AS tick_count
FROM ticks
GROUP BY bucket, exchange, crypto_pair
WITH NO DATA;

-- Keeps ticks_daily current automatically in the background - Timescale only
-- re-touches buckets that actually received new raw data, it never
-- recomputes the whole view from scratch (unlike a plain Postgres
-- materialized view's manual, full REFRESH).
-- end_offset leaves "today" alone since it's still receiving ticks and isn't
-- done yet; start_offset bounds how far back it bothers re-checking for
-- late-arriving data; schedule_interval is how often this refresh runs.
SELECT add_continuous_aggregate_policy('ticks_daily',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- 1 month retention on raw ticks
SELECT add_retention_policy('ticks', INTERVAL '1 month', if_not_exists => TRUE);
