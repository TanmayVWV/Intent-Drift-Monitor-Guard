import { useEffect, useState, useRef } from "react";

// ==== Adjust this if your API runs elsewhere ====
const API_BASE = "http://127.0.0.1:8000";

function Pill({ status }) {
  const ok = status === "ok";
  const color = ok ? { bg: "#E8F5E9", text: "#1B5E20" } : { bg: "#FFEBEE", text: "#B71C1C" };
  return (
    <span
      style={{
        padding: "6px 10px",
        borderRadius: 999,
        background: color.bg,
        color: color.text,
        fontWeight: 700,
        fontSize: 12,
      }}
    >
      {String(status).toUpperCase()}
    </span>
  );
}

function Row({ label, value }) {
  return (
    <div style={{ display: "flex", gap: 8, alignItems: "baseline" }}>
      <div style={{ minWidth: 120, color: "#555" }}>{label}</div>
      <div style={{ fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace" }}>{value}</div>
    </div>
  );
}

function CheckCard({ name, data }) {
  const hasOk = Object.prototype.hasOwnProperty.call(data, "ok");
  return (
    <div
      style={{
        border: "1px solid #eee",
        borderRadius: 12,
        padding: 16,
        background: "#fff",
        boxShadow: "0 1px 2px rgba(0,0,0,0.04)",
      }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
        <h3 style={{ margin: 0, fontSize: 16 }}>{name}</h3>
        {hasOk ? <Pill status={data.ok ? "ok" : "fail"} /> : null}
      </div>
      <pre
        style={{
          margin: 0,
          background: "#FAFAFA",
          padding: 12,
          borderRadius: 8,
          overflowX: "auto",
          fontSize: 12,
          lineHeight: 1.4,
        }}
      >
        {JSON.stringify(data, null, 2)}
      </pre>
    </div>
  );
}

export default function App() {
  const [report, setReport] = useState(null);
  const [loading, setLoading] = useState(true);
  const [mins, setMins] = useState(30);
  const [error, setError] = useState("");
  const timerRef = useRef(null);

  async function load(windowMinutes = mins) {
    try {
      setError("");
      setLoading(true);
      const res = await fetch(`${API_BASE}/reports/latest?window_minutes=${windowMinutes}`);
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(`${res.status} ${res.statusText} — ${txt}`);
      }
      const json = await res.json();
      setReport(json);
    } catch (e) {
      setError(e.message || String(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load(mins);
    timerRef.current = setInterval(() => load(mins), 5000);
    return () => clearInterval(timerRef.current);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const onApply = () => {
    clearInterval(timerRef.current);
    load(mins);
    timerRef.current = setInterval(() => load(mins), 5000);
  };

  return (
    <div style={{ fontFamily: "Inter, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif", background: "#F6F7F9", minHeight: "100vh" }}>
      <div style={{ maxWidth: 980, margin: "0 auto", padding: 24 }}>
        <header style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
          <h1 style={{ margin: 0, fontSize: 22 }}>Intent Drift Monitor & Guard</h1>
          {report ? <Pill status={report.status} /> : null}
        </header>

        <section
          style={{
            display: "grid",
            gridTemplateColumns: "1fr",
            gap: 12,
            marginBottom: 16,
            background: "#fff",
            border: "1px solid #eee",
            borderRadius: 12,
            padding: 16,
          }}
        >
          <Row label="API Base" value={API_BASE} />
          <Row label="Window minutes">
            <span />
          </Row>
          <div style={{ display: "flex", gap: 8 }}>
            <input
              type="number"
              value={mins}
              onChange={(e) => setMins(Math.max(1, Math.min(1440, Number(e.target.value || 1))))}
              min={1}
              max={1440}
              style={{ width: 100, padding: "6px 8px", borderRadius: 8, border: "1px solid #ddd" }}
            />
            <button onClick={onApply} style={{ padding: "6px 12px", borderRadius: 8, border: "1px solid #ddd", background: "#f3f4f6", cursor: "pointer" }}>
              Apply & Refresh
            </button>
          </div>
          <div style={{ color: "#555", fontSize: 13 }}>
            Auto-refresh every 5s • Adjust window and click <b>Apply</b>.
          </div>
        </section>

        {loading && <div style={{ padding: 12 }}>Loading…</div>}
        {error && (
          <div style={{ padding: 12, background: "#FFEBEE", color: "#B71C1C", borderRadius: 8, marginBottom: 12 }}>
            Error: {error} (Check that FastAPI is running and CORS is enabled)
          </div>
        )}

        {report && (
          <>
            <section
              style={{
                background: "#fff",
                border: "1px solid #eee",
                borderRadius: 12,
                padding: 16,
                marginBottom: 12,
              }}
            >
              <Row label="Generated" value={new Date(report.generated_at).toLocaleString()} />
              <Row label="Window">{report.window_minutes} min</Row>
              <Row label="Samples">{report.samples}</Row>
            </section>

            <section style={{ display: "grid", gap: 12 }}>
              {Object.entries(report.checks || {}).map(([name, data]) => (
                <CheckCard key={name} name={name} data={data} />
              ))}
            </section>
          </>
        )}
      </div>
    </div>
  );
}
