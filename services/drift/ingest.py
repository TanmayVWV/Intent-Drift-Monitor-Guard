import os, json
from typing import List, Any
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import psycopg2, psycopg2.extras
from drift_core import generate_report

DB_URL = os.getenv("DATABASE_URL", "postgresql://dev:dev@localhost:5432/drift")
app = FastAPI(title="Drift Ingest")

def get_conn():
    return psycopg2.connect(DB_URL)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/reports/latest")
def reports_latest(window_minutes: int = Query(None, ge=1, le=1440)):
    rep = generate_report(window_minutes)
    return rep

@app.get("/health")
def health():
    return {"ok": True}

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