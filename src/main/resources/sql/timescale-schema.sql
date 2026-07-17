-- Coinsight - TimescaleDB schema (Aggregations)
--
-- Tier design (see the diagram in README.md's Aggregations section):
--   ticks (raw hypertable) -> ticks_hourly -> ticks_daily -> ticks_weekly
-- Only "ticks" is a real, manually-created hypertable. Every tier above it is a
-- continuous aggregate.
-- Each tier is built FROM the tier directly below it, not from raw ticks again each time -
-- this is TimescaleDB's supported "hierarchical continuous aggregates" feature, so daily
-- reuses hourly's work instead of re-scanning raw data, and so on up the chain.

-- 1. Raw ticks hypertable
CREATE TABLE IF NOT EXISTS ticks (
    time        TIMESTAMPTZ NOT NULL,
    exchange    TEXT        NOT NULL,
    crypto_pair TEXT        NOT NULL,
    price       NUMERIC     NOT NULL,
    message_id  TEXT        NOT NULL
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

-- Idempotency: TimescaleDB requires every unique constraint on a hypertable to
-- include ALL of its partitioning columns (time AND crypto_pair here) -
-- uniqueness is enforced per chunk, so message_id alone can't be a standalone
-- UNIQUE column on this table; it has to be composite.
ALTER TABLE ticks ADD CONSTRAINT ticks_time_pair_message_unique
    UNIQUE (time, crypto_pair, message_id);

-- The time column is indexed automatically by create_hypertable - this
-- covers the actual query shape (one pair/exchange, ordered by recency).
CREATE INDEX IF NOT EXISTS idx_ticks_pair_time
    ON ticks (crypto_pair, exchange, time DESC);

-- 2. Hourly OHLC continuous aggregate - built directly from raw ticks.
CREATE MATERIALIZED VIEW IF NOT EXISTS ticks_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    exchange,
    crypto_pair,
    first(price, time) AS open,
    max(price)         AS high,
    min(price)         AS low,
    last(price, time)  AS close,
    avg(price)         AS avg_price,
    count(*)           AS tick_count
FROM ticks
GROUP BY bucket, exchange, crypto_pair
WITH NO DATA;

-- Keeps ticks_hourly current automatically in the background - only re-touches
-- buckets that actually received new raw data.
-- the start_offset and end_offset are the limitations for the sliding window that is considered upon refresh
-- [now - 3 hours, now - 10 minutes] is what the query will consider when executed
SELECT add_continuous_aggregate_policy('ticks_hourly',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '10 minutes',
    schedule_interval => INTERVAL '10 minutes',
    if_not_exists => TRUE
);

-- 3. Daily OHLC continuous aggregate - built FROM ticks_hourly
DROP MATERIALIZED VIEW IF EXISTS ticks_daily CASCADE;

CREATE MATERIALIZED VIEW ticks_daily
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', bucket) AS bucket,
    exchange,
    crypto_pair,
    first(open, bucket) AS open,
    max(high)           AS high,
    min(low)            AS low,
    last(close, bucket) AS close,
    -- NOT avg(avg_price) - that would silently mis-weight the result unless every
    -- hourly bucket happened to have the exact same tick_count.
    -- Meaning if one hour had 10 ticks and another hour had 100 ticks they should have different weight
    sum(avg_price * tick_count) / sum(tick_count) AS avg_price,
    sum(tick_count) AS tick_count
FROM ticks_hourly
GROUP BY time_bucket('1 day', bucket), exchange, crypto_pair
WITH NO DATA;

-- end_offset (1 hour) is deliberately larger than ticks_hourly's own end_offset
-- (10 minutes) - by the time this runs for a given hour, ticks_hourly has
-- already finalized it. Getting this ordering wrong would mean aggregating
-- over an hourly bucket that isn't done updating yet.
SELECT add_continuous_aggregate_policy('ticks_daily',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- 4. Weekly OHLC continuous aggregate - built FROM ticks_daily
CREATE MATERIALIZED VIEW IF NOT EXISTS ticks_weekly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 week', bucket) AS bucket,
    exchange,
    crypto_pair,
    first(open, bucket) AS open,
    max(high)           AS high,
    min(low)            AS low,
    last(close, bucket) AS close,
    sum(avg_price * tick_count) / sum(tick_count) AS avg_price,
    sum(tick_count) AS tick_count
FROM ticks_daily
GROUP BY time_bucket('1 week', bucket), exchange, crypto_pair
WITH NO DATA;

-- end_offset (1 day) deliberately larger than ticks_daily's own end_offset (1 hour) -
-- same staggering reasoning as above, one level up.
SELECT add_continuous_aggregate_policy('ticks_weekly',
    start_offset => INTERVAL '3 weeks',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '6 hours',
    if_not_exists => TRUE
);

-- 1 month retention on raw ticks only - NOT on any rollup tier. Continuous
-- aggregates are backed by their own separate hypertable, so hourly/daily/weekly
-- can all outlive the raw data that fed them at negligible storage cost -
-- there's no reason to lose long-range history just because raw ticks expired.
SELECT add_retention_policy('ticks', INTERVAL '1 month', if_not_exists => TRUE);
