import datetime as dt, requests, random

URL = "http://127.0.0.1:8000/v1/ingest"

def sample_currency():
    r = random.random()
    if r < 0.6: return "USD"
    if r < 0.9: return "CAD"
    return "EUR"

batch = []
for _ in range(200):
    item = {
        "ts": dt.datetime.utcnow().isoformat() + "Z",
        "service": "payments_api",
        "endpoint": "POST /v2/payments",
        "host": "gen-local",
        "request": {"body": {"amount": max(0, random.gauss(100, 20)), "currency": sample_currency()}},
        "response": {"status": 200 if random.random() < 0.995 else 500,
                     "duration_ms": int(max(50, random.gauss(800, 200)))}
    }
    batch.append(item)

resp = requests.post(URL, json=batch, timeout=5)
print(resp.status_code, resp.text)
