-- FluxTape schema (Phase 3). Apply with:
--   docker exec -i fluxtape-timescaledb psql -U fluxtape -d fluxtape < infra/db/schema.sql

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Source of truth: raw trades (Postgres). Idempotent on trade_id.
CREATE TABLE IF NOT EXISTS trades (
    trade_id    BIGINT      PRIMARY KEY,
    symbol      TEXT        NOT NULL,
    price       DOUBLE PRECISION NOT NULL,
    quantity    DOUBLE PRECISION NOT NULL,
    side        TEXT        NOT NULL,
    event_time  TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS trades_symbol_time ON trades (symbol, event_time DESC);

-- Hot analytics: 1s OHLCV+VWAP+SMA bars (Timescale hypertable). Idempotent on (symbol, window_start).
CREATE TABLE IF NOT EXISTS bars (
    symbol       TEXT        NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    open  DOUBLE PRECISION, high DOUBLE PRECISION, low DOUBLE PRECISION, close DOUBLE PRECISION,
    volume DOUBLE PRECISION, vwap DOUBLE PRECISION, count INT,
    sma5 DOUBLE PRECISION, sma20 DOUBLE PRECISION,
    PRIMARY KEY (symbol, window_start)
);
SELECT create_hypertable('bars', 'window_start', if_not_exists => TRUE);
