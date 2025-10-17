import os, json, statistics, datetime as dt
import psycopg2, psycopg2.extras

DB_URL = os.getenv("DATABASE_URL", "postgresql://dev:dev@localhost:5432/drift")
OUT_PATH = os.getenv("BASELINE_PATH", "db/baseline_payments_api.json")

# how far back to read (minutes)
WINDOW_MINUTES = int(os.getenv("BASELINE_WINDOW_MINUTES", "120"))

def load_rows():
    conn = psycopg2.connect(DB_URL)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT payload
                FROM observations_raw
                WHERE service = 'payments_api'
                  AND endpoint = 'POST /v2/payments'
                  AND ts >= NOW() - INTERVAL '%s minutes'
                """,
                (WINDOW_MINUTES,)
            )
            return [r["payload"] for r in cur.fetchall()]
    finally:
        conn.close()

def p95(values):
    if not values: return None
    s = sorted(values)
    k = int(0.95*(len(s)-1))
    return s[k]

def main():
    rows = load_rows()
    amounts, currencies, status_codes, durations = [], {}, {}, []

    for r in rows:
        amount = (r.get("request") or {}).get("body", {}).get("amount")
        currency = (r.get("request") or {}).get("body", {}).get("currency")
        status = (r.get("response") or {}).get("status")
        dur = (r.get("response") or {}).get("duration_ms")
        if isinstance(amount, (int,float)): amounts.append(float(amount))
        if isinstance(currency, str): currencies[currency] = currencies.get(currency,0)+1
        if isinstance(status, int):
            bucket = "2xx" if 200 <= status < 300 else ("5xx" if 500 <= status < 600 else "other")
            status_codes[bucket] = status_codes.get(bucket,0)+1
        if isinstance(dur, int): durations.append(dur)

    baseline = {
        "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "service": "payments_api",
        "endpoint": "POST /v2/payments",
        "samples": len(rows),
        "numeric": {
            "amount": {
                "n": len(amounts),
                "mean": statistics.fmean(amounts) if amounts else None,
                "std": statistics.pstdev(amounts) if len(amounts)>1 else None,
                "min": min(amounts) if amounts else None,
                "max": max(amounts) if amounts else None,
                "sample": amounts[:5000]  # small reservoir for KS
            },
            "latency_ms": {
                "n": len(durations),
                "p95": p95(durations),
                "sample": durations[:5000]
            }
        },
        "categorical": {
            "currency": currencies,
            "status_buckets": status_codes
        },
        # SLOs you can tune
        "slo": {
            "success_rate_min": 0.995,
            "latency_p95_max": 2000
        }
    }

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(baseline, f, indent=2)
    print(f"Baseline written → {OUT_PATH} with {len(rows)} rows")

if __name__ == "__main__":
    main()
