-- Coinsight - TimescaleDB schema (Aggregations)
--
-- Tier design (see the diagram in README.md's Aggregations section):
--   ticks (raw hypertable)
--     -> ticks_minute  -> ticks_30min                 (fine-grained branch: last hour/day/week)
--     -> ticks_hourly  -> ticks_daily                  (coarser branch: last month)
-- Only "ticks" is a real, manually-created hypertable. Everything above it is a continuous
-- aggregate. Two independent branches off of raw ticks, not one long chain - ticks_hourly
-- stays built directly from raw ticks (not from ticks_30min) on purpose, to avoid touching
-- the already-working hourly/daily chain when this branch was added.

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

-- 2a. Minute OHLC continuous aggregate - built directly from raw ticks.
-- This is the historical "replay the last hour minute-by-minute" view - deliberately
-- separate from the live WebSocket stream, which only ever shows the current instant.
CREATE MATERIALIZED VIEW IF NOT EXISTS ticks_minute
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', time) AS bucket,
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

-- Fixed absolute margins here, NOT "3x bucket width" like the coarser tiers below -
-- 3x a 1-minute bucket is only 3 minutes, which a single container restart during
-- development could easily exceed, permanently orphaning a bucket with no error.
-- 30 minutes of margin is cheap either way and actually survives real downtime.
-- Refreshing every minute is what makes this feel close to real-time for a "last hour"
-- view - a slower schedule would make the most recent chart bars visibly stale.
SELECT add_continuous_aggregate_policy('ticks_minute',
    start_offset => INTERVAL '30 minutes',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists => TRUE
);

-- 2b. 30-minute OHLC continuous aggregate - built FROM ticks_minute.
CREATE MATERIALIZED VIEW IF NOT EXISTS ticks_30min
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('30 minutes', bucket) AS bucket,
    exchange,
    crypto_pair,
    first(open, bucket) AS open,
    max(high)           AS high,
    min(low)            AS low,
    last(close, bucket) AS close,
    -- NOT avg(avg_price) - that would silently mis-weight the result unless every
    -- minute bucket happened to have the exact same tick_count.
    -- Meaning if one minute had 10 ticks and another minute had 100 ticks they should
    -- have different weight.
    sum(avg_price * tick_count) / sum(tick_count) AS avg_price,
    sum(tick_count) AS tick_count
FROM ticks_minute
GROUP BY time_bucket('30 minutes', bucket), exchange, crypto_pair
WITH NO DATA;

-- end_offset (5 minutes) is comfortably larger than ticks_minute's own end_offset
-- (1 minute) - by the time this runs for a given 30-minute bucket, ticks_minute has
-- already finalized every minute inside it.
SELECT add_continuous_aggregate_policy('ticks_30min',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists => TRUE
);

-- 3. Hourly OHLC continuous aggregate - built directly from raw ticks (NOT from
-- ticks_30min - separate branch, see the tier design note at the top of this file).
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

-- 1 month retention on raw ticks only - NOT on any rollup tier. Continuous
-- aggregates are backed by their own separate hypertable, so every rollup tier
-- can outlive the raw data that fed it at negligible storage cost
SELECT add_retention_policy('ticks', INTERVAL '1 month', if_not_exists => TRUE);
