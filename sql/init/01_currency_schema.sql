CREATE TABLE IF NOT EXISTS raw_currency (
  id BIGSERIAL PRIMARY KEY,
  base_currency TEXT NOT NULL,
  symbol TEXT NOT NULL,
  rate DOUBLE PRECISION NOT NULL,
  fetched_at TIMESTAMPTZ NOT NULL,
  ingested_at TIMESTAMPTZ DEFAULT NOW(),
  kafka_offset BIGINT,
  payload JSONB
);
CREATE INDEX IF NOT EXISTS idx_raw_currency_time ON raw_currency (fetched_at);
CREATE INDEX IF NOT EXISTS idx_raw_currency_symbol_time ON raw_currency (symbol, fetched_at);

CREATE TABLE IF NOT EXISTS agg_currency_3m (
  window_start TIMESTAMPTZ,
  window_end   TIMESTAMPTZ,
  base_currency TEXT,
  symbol        TEXT,
  rate_open     DOUBLE PRECISION,
  rate_close    DOUBLE PRECISION,
  rate_min      DOUBLE PRECISION,
  rate_max      DOUBLE PRECISION,
  rate_avg      DOUBLE PRECISION,
  pct_change    DOUBLE PRECISION,
  samples       INT,
  PRIMARY KEY (window_start, base_currency, symbol)
);
