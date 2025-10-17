# Intent Drift Monitor & Guard

A small, production-style tool that **ingests live API/ML traffic**, builds a **baseline**, and detects **behavioral drift** using **KS** (numeric) and **PSI** (categorical). It ships with a FastAPI backend, Postgres (Docker), and a tiny React dashboard.

![Dashboard](docs/screenshot-dashboard.png)

## ✨ Features
- **Ingestion API**: `POST /v1/ingest` (batch JSON) → Postgres
- **Baseline & Drift**: KS for numeric, PSI for categorical, plus SLO checks (success rate, latency p95)
- **Live Report**: `GET /reports/latest?window_minutes=...`
- **Dashboard**: auto-refresh UI showing status and per-check details
- **Synthetic traffic**: baseline & drift senders to demo OK → FAIL

## 🧱 Architecture
            ┌─────────────────────────┐
            │      Test Clients        │
            │  (send_baseline.py /     │
            │   send_drift_amount.py)  │
            └───────────┬──────────────┘
                        │  HTTP (JSON batch)
                        ▼
                ┌───────────────────┐
                │   FastAPI API     │
                │ services/drift    │
                │  - /v1/ingest     │
                │  - /reports/latest│
                │  - /health        │
                └─────────┬─────────┘
                          │ psycopg2
                          ▼
                ┌───────────────────┐
                │   PostgreSQL      │
                │   (docker db)     │
                │  observations_raw │
                └─────────┬─────────┘
                          │ SQL (time-windowed read)
                          ▼
                ┌───────────────────┐
                │  Drift Engine     │
                │ drift_core.py     │
                │  - KS (numeric)   │
                │  - PSI (categor.) │
                │  - SLO checks     │
                └─────────┬─────────┘
                          │ JSON report
                          ▼
                ┌───────────────────┐
                │   React (Vite)    │
                │     /web          │
                │  auto-refresh UI  │
                └───────────────────┘

### Data model

- **Table:** `observations_raw`
  - `id BIGSERIAL PK`
  - `ts TIMESTAMPTZ NOT NULL` – event timestamp (from client)
  - `service TEXT NOT NULL` – e.g. `"payments_api"`
  - `endpoint TEXT NOT NULL` – e.g. `"POST /v2/payments"`
  - `host TEXT` – client/host identifier
  - `payload JSONB NOT NULL` – full event (request/response/body)

> Index: `(service, endpoint, ts)` to support fast time-window queries.

### Ingestion contract

`POST /v1/ingest` accepts a **JSON array** of events:
```json
[
  {
    "ts": "2025-01-01T00:00:00Z",
    "service": "payments_api",
    "endpoint": "POST /v2/payments",
    "host": "client-1",
    "request": { "body": { "amount": 123.45, "currency": "USD" } },
    "response": { "status": 200, "duration_ms": 850 }
  }
]
Report generation (server-side)

Endpoint: GET /reports/latest?window_minutes=30

Reads recent rows (default DRIFT_WINDOW_MINUTES), compares to baseline JSON (db/baseline_payments_api.json).

Produces a JSON like:
{
  "generated_at": "...",
  "window_minutes": 30,
  "samples": 700,
  "checks": {
    "amount_ks": { "ok": false, "p_value": 0.012, "d": 0.13, "live_n": 700 },
    "currency_psi": { "ok": true, "psi": 0.01, "live_counts": { "USD": 417, "EUR": 83, "CAD": 200 } },
    "success_rate": { "ok": true, "success_rate": 0.996, "min": 0.995, "total": 700 },
    "latency_p95": { "ok": true, "p95": 1309, "max": 2000, "live_n": 700 }
  },
  "status": "fail"
}
Drift/statistics logic
    KS test (numeric, e.g. amount)
    Compares live distribution vs baseline sample.
    Uses two-sample Kolmogorov–Smirnov: fail if p_value < KS_P_MIN (default 0.05).
    Guardrail: requires at least 50 samples in both sets.
PSI (categorical, e.g. currency)
    Population Stability Index on category proportions.
    Fail if PSI > PSI_MAX (default 0.2).
    Guardrail: if no live samples, mark as insufficient_samples (OK).
SLO checks
    Success rate: live % 2xx must be >= SLO_SUCCESS_MIN (default 0.995).
    Latency p95: must be <= SLO_P95_MAX (default 2000 ms).
Overall status: "ok" iff all checks' ok are true; else "fail".

Baseline

Built by services/drift/baseline.py.

Reads a historical window (default 120 min), computes:
    numeric snapshots (mean/std/min/max + sample reservoir for KS),
    categorical counts (for PSI),
    SLO targets (editable).

Saved to db/baseline_payments_api.json. Path can be overridden with BASELINE_PATH.

Config (env vars)
    DATABASE_URL (default postgresql://dev:dev@localhost:5432/drift)
    BASELINE_PATH (default db/baseline_payments_api.json)
    DRIFT_WINDOW_MINUTES (default 30)
    KS_P_MIN (default 0.05)
    PSI_MAX (default 0.2)
    SLO_SUCCESS_MIN (default 0.995)
    SLO_P95_MAX (default 2000)
    SLACK_WEBHOOK_URL (optional; for alerts if implemented)
Local run (quick reference)
# DB
docker compose up -d db adminer
# API
cd services/drift
python -m uvicorn ingest:app --reload
# Seed + baseline
python ../../tests/send_baseline.py
python baseline.py
# Dashboard
cd ../../web
npm install
npm run dev
Frontend
    Vite + React app polls /reports/latest every 5s.
    CORS must allow http://localhost:5173 in ingest.py.