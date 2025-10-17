import os, json, math, datetime as dt
from collections import Counter
from pathlib import Path
import psycopg2, psycopg2.extras
from scipy import stats
MIN_NUMERIC = 50        # need at least 50 samples to run KS
MIN_CATEGORICAL = 1     # need at least 1 categorical sample to compute PSI

DB_URL = os.getenv("DATABASE_URL", "postgresql://dev:dev@localhost:5432/drift")
REPO_ROOT = Path(__file__).resolve().parents[2]   # back to repo root
BASELINE = os.getenv("BASELINE_PATH", str(REPO_ROOT / "db" / "baseline_payments_api.json"))
WINDOW_MINUTES = int(os.getenv("DRIFT_WINDOW_MINUTES", "30"))

def _load_baseline():
    if BASELINE.startswith(("postgres://", "postgresql://")):
        raise RuntimeError(
            f"BASELINE_PATH looks like a database URL, not a file path: {BASELINE[:32]}..."
        )
    p = Path(BASELINE)
    if not p.is_file():
        # Helpful message rather than crashing with FileNotFoundError
        raise FileNotFoundError(f"Baseline JSON not found at {p}. "
                                f"Set BASELINE_PATH=/opt/render/project/src/db/baseline_payments_api.json")
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

def _load_live_rows(window_minutes: int = None):
    minutes = window_minutes or WINDOW_MINUTES
    conn = psycopg2.connect(DB_URL)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT payload
                FROM observations_raw
                WHERE service = 'payments_api'
                  AND endpoint = 'POST /v2/payments'
                  AND ts >= NOW() - INTERVAL %s
                """,
                (f"{minutes} minutes",)
            )
            return [r["payload"] for r in cur.fetchall()]
    finally:
        conn.close()

def _p95(values):
    s = sorted(values)
    return s[int(0.95*(len(s)-1))] if s else None

def _psi(expected_counts, actual_counts):
    cats = set(expected_counts) | set(actual_counts)
    te = sum(expected_counts.get(c,0) for c in cats) or 1
    ta = sum(actual_counts.get(c,0) for c in cats) or 1
    val = 0.0
    for c in cats:
        pe = max(expected_counts.get(c,0)/te, 1e-6)
        pa = max(actual_counts.get(c,0)/ta, 1e-6)
        val += (pa - pe) * math.log(pa/pe)
    return abs(val)

def generate_report(window_minutes: int = None) -> dict:
    baseline = _load_baseline()
    rows = _load_live_rows(window_minutes)

    amounts, currencies, durations = [], Counter(), []
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
            total += 1
            if 200 <= status < 300: ok_count += 1
        if isinstance(dur, int): durations.append(dur)

    report = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "window_minutes": window_minutes or WINDOW_MINUTES,
        "samples": len(rows),
        "checks": {}
    }

    # amount KS (robust to low sample sizes)
    base_amt = baseline["numeric"]["amount"]["sample"]
    if base_amt and len(base_amt) >= MIN_NUMERIC and len(amounts) >= MIN_NUMERIC:
        d, p = stats.ks_2samp(base_amt, amounts)
        report["checks"]["amount_ks"] = {
            "ok": bool(p >= 0.05),
            "p_value": float(p),
            "d": float(d),
            "live_n": len(amounts),
        }
    else:
        report["checks"]["amount_ks"] = {
            "ok": True,
            "reason": "insufficient_samples",
            "live_n": len(amounts),
        }


    # currency PSI (don’t compute when no live data)
    live_total = sum(currencies.values())
    if live_total < MIN_CATEGORICAL:
        report["checks"]["currency_psi"] = {
            "ok": True,
            "reason": "insufficient_samples",
            "psi": None,
            "live_counts": dict(currencies),
        }
    else:
        psi_val = _psi(baseline["categorical"]["currency"], dict(currencies)) if baseline["categorical"]["currency"] else 0.0
        report["checks"]["currency_psi"] = {
            "ok": bool(psi_val <= 0.2),
            "psi": psi_val,
            "live_counts": dict(currencies),
        }
    # success rate
    slo = baseline.get("slo", {})
    min_success = slo.get("success_rate_min", 0.995)
    success_rate = (ok_count/total) if total else None
    report["checks"]["success_rate"] = {
        "ok": bool((success_rate is None) or (success_rate >= min_success)),
        "success_rate": success_rate, "min": min_success, "total": total
    }

    # latency p95
    live_p95 = _p95(durations)
    p95_max = slo.get("latency_p95_max", 2000)
    report["checks"]["latency_p95"] = {"ok": bool((live_p95 is None) or (live_p95 <= p95_max)), "p95": live_p95, "max": p95_max, "live_n": len(durations)}

    overall_ok = all(bool(v.get("ok", True)) for v in report["checks"].values())
    report["status"] = "ok" if overall_ok else "fail"
    return report
