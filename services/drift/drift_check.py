import os, json, math, datetime as dt, statistics
from collections import Counter
import psycopg2, psycopg2.extras
from scipy import stats

DB_URL = os.getenv("DATABASE_URL", "postgresql://dev:dev@localhost:5432/drift")
BASELINE = os.getenv("BASELINE_PATH", "db/baseline_payments_api.json")
WINDOW_MINUTES = int(os.getenv("DRIFT_WINDOW_MINUTES", "30"))

def load_baseline():
    with open(BASELINE, "r", encoding="utf-8") as f:
        return json.load(f)

def load_live_rows():
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
    s = sorted(values)
    return s[int(0.95*(len(s)-1))] if s else None

def psi(expected_counts, actual_counts):
    cats = set(expected_counts) | set(actual_counts)
    te = sum(expected_counts.get(c,0) for c in cats) or 1
    ta = sum(actual_counts.get(c,0) for c in cats) or 1
    val = 0.0
    for c in cats:
        pe = max(expected_counts.get(c,0)/te, 1e-6)
        pa = max(actual_counts.get(c,0)/ta, 1e-6)
        val += (pa - pe) * math.log(pa/pe)
    return abs(val)

def main():
    baseline = load_baseline()
    rows = load_live_rows()

    # collect live stats
    amounts, currencies, status_bk, durations = [], Counter(), Counter(), []
    ok_count, total = 0, 0
    for r in rows:
        req = (r.get("request") or {}).get("body", {})
        resp = (r.get("response") or {})
        amt = req.get("amount")
        cur = req.get("currency")
        status = resp.get("status")
        dur = resp.get("duration_ms")
        if isinstance(amt, (int,float)): amounts.append(float(amt))
        if isinstance(cur, str): currencies[cur]+=1
        if isinstance(status, int):
            bucket = "2xx" if 200 <= status < 300 else ("5xx" if 500 <= status < 600 else "other")
            status_bk[bucket]+=1
            total += 1
            if 200 <= status < 300: ok_count += 1
        if isinstance(dur, int): durations.append(dur)

    report = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "window_minutes": WINDOW_MINUTES,
        "samples": len(rows),
        "checks": {}
    }

    # --- Numeric: amount KS test ---
    base_amount_sample = baseline["numeric"]["amount"]["sample"]
    ks_ok, ks_p = True, 1.0
    if base_amount_sample and len(amounts) >= 50 and len(base_amount_sample) >= 50:
        d, p = stats.ks_2samp(base_amount_sample, amounts)
        ks_ok, ks_p = bool(p >= 0.05), float(p)   # 👈 cast to Python bool
        report["checks"]["amount_ks"] = {"ok": ks_ok, "p_value": ks_p, "d": float(d), "live_n": len(amounts)}
    else:
        report["checks"]["amount_ks"] = {"ok": True, "reason": "insufficient_samples", "live_n": len(amounts)}

    # --- Categorical: currency PSI ---
    base_cur = baseline["categorical"]["currency"]
    psi_val = psi(base_cur, dict(currencies)) if base_cur else 0.0
    psi_ok = bool(psi_val <= 0.2)              # 👈 cast
    report["checks"]["currency_psi"] = {"ok": psi_ok, "psi": psi_val, "live_counts": dict(currencies)}

    # --- SLO: success rate ---
    base_slo = baseline.get("slo", {})
    min_success = base_slo.get("success_rate_min", 0.995)
    success_rate = (ok_count/total) if total else None
    sr_ok = bool((success_rate is None) or (success_rate >= min_success))  # 👈 cast
    report["checks"]["success_rate"] = {"ok": sr_ok, "success_rate": success_rate, "min": min_success, "total": total}

    # --- Latency p95 ---
    live_p95 = p95(durations)
    p95_max = base_slo.get("latency_p95_max", 2000)
    lat_ok = bool((live_p95 is None) or (live_p95 <= p95_max))             # 👈 cast
    report["checks"]["latency_p95"] = {"ok": lat_ok, "p95": live_p95, "max": p95_max, "live_n": len(durations)}

    # overall status
    overall_ok = all(bool(v.get("ok", True)) for v in report["checks"].values())  # 👈 cast
    report["status"] = "ok" if overall_ok else "fail"

     # --- output the report ---
    def _json_default(o):
        try:
            import numpy as np
            if isinstance(o, (np.bool_,)): return bool(o)
            if isinstance(o, (np.floating,)): return float(o)
            if isinstance(o, (np.integer,)): return int(o)
        except Exception:
            pass
        return str(o)

    print(json.dumps(report, indent=2, default=_json_default))

if __name__ == "__main__":
    main()
