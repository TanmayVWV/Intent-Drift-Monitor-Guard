# services/drift/db_bootstrap.py
import os
import psycopg2

SCHEMA = """
CREATE TABLE IF NOT EXISTS observations_raw (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  service TEXT NOT NULL,
  endpoint TEXT NOT NULL,
  host TEXT,
  payload JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_obs_svc_ep_ts
  ON observations_raw(service, endpoint, ts);
"""

def ensure_schema():
    url = os.getenv("DATABASE_URL")
    if not url:
        print("⚠️ DATABASE_URL not set; skipping schema ensure")
        return
    with psycopg2.connect(url) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(SCHEMA)
    print("✅ schema ensured")
