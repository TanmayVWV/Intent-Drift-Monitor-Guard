import os, json
from typing import List, Any
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import psycopg2, psycopg2.extras
from drift_core import generate_report
from db_bootstrap import ensure_schema

DB_URL = os.getenv("DATABASE_URL", "postgresql://dev:dev@localhost:5432/drift")
app = FastAPI(title="Drift Ingest")

def get_conn():
    return psycopg2.connect(DB_URL)

DASH_ORIGIN = "https://intent-drift-monitor-guard.onrender.com"

app.add_middleware(
    CORSMiddleware,
    allow_origins=[DASH_ORIGIN],  
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,      
)

@app.get("/reports/latest")
def reports_latest(window_minutes: int = Query(None, ge=1, le=1440)):
    rep = generate_report(window_minutes)
    return rep

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/debug/env")
def debug_env():
    try:
        db = os.getenv("DATABASE_URL", "")
        base = os.getenv("BASELINE_PATH", "")
        exists = (Path(base).is_file() if base else None)
        return {
            "database_url_prefix": (db[:25] + "...") if db else None,
            "baseline_path": base or None,
            "baseline_exists": exists,
        }
    except Exception as e:
        # Never 500—return the error in JSON so we can see it
        return JSONResponse(status_code=200, content={"debug_error": str(e)})

@app.post("/v1/ingest")
def ingest(items: List[Any]):
    if not isinstance(items, list) or not items:
        raise HTTPException(status_code=400, detail="Expected non-empty JSON array")
    for it in items:
        if not all(k in it for k in ("ts", "service", "endpoint")):
            raise HTTPException(status_code=400, detail="Missing ts/service/endpoint in item")

    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            psycopg2.extras.register_default_jsonb(cur)
            for it in items:
                cur.execute(
                    """
                    INSERT INTO observations_raw (ts, service, endpoint, host, payload)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (it["ts"], it["service"], it["endpoint"], it.get("host"), json.dumps(it))
                )
        return {"accepted": len(items)}
    finally:
        conn.close()