CREATE TABLE IF NOT EXISTS fx_prices (
    id SERIAL PRIMARY KEY,
    timestamp_utc TIMESTAMP NOT NULL,
    pair VARCHAR(10) NOT NULL,
    timeframe VARCHAR(10),
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    source VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS economic_events (
    id SERIAL PRIMARY KEY,
    timestamp_utc TIMESTAMP NOT NULL,
    country VARCHAR(50),
    event_name VARCHAR(100),
    impact VARCHAR(20),
    actual DOUBLE PRECISION,
    forecast DOUBLE PRECISION,
    previous DOUBLE PRECISION,
    source VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS ecb_policy_rates (
    id SERIAL PRIMARY KEY,
    timestamp_utc TIMESTAMP NOT NULL,
    rate_type VARCHAR(50),
    rate DOUBLE PRECISION,
    frequency VARCHAR(20),
    unit VARCHAR(20),
    source VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS ecb_exchange_rates (
    id SERIAL PRIMARY KEY,
    timestamp_utc TIMESTAMP NOT NULL,
    currency_pair VARCHAR(20),
    rate DOUBLE PRECISION,
    frequency VARCHAR(20),
    source VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS macro_indicators (
    id SERIAL PRIMARY KEY,
    timestamp_utc TIMESTAMP NOT NULL,
    series_id VARCHAR(100),
    value DOUBLE PRECISION,
    source VARCHAR(50)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_fx_prices_time ON fx_prices(timestamp_utc);
CREATE INDEX IF NOT EXISTS idx_economic_events_time ON economic_events(timestamp_utc);
CREATE INDEX IF NOT EXISTS idx_ecb_policy_rates_time ON ecb_policy_rates(timestamp_utc);
CREATE INDEX IF NOT EXISTS idx_ecb_exchange_rates_time ON ecb_exchange_rates(timestamp_utc);
CREATE INDEX IF NOT EXISTS idx_macro_indicators_time ON macro_indicators(timestamp_utc);
