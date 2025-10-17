CREATE TABLE IF NOT EXISTS observations_raw(
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  service TEXT NOT NULL,
  endpoint TEXT NOT NULL,
  host TEXT,
  payload JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS observations_raw_idx ON observations_raw(service, endpoint, ts);
