import { useEffect, useState } from "react";
import { jget, jpost, SimStatus } from "../api";

// Snap slider output to clean steps: 0.005s precision when very low so we can
// dial in the high-throughput range, but 0.1s steps above that since
// "1.0s vs 1.05s" doesn't really differ in practice.
function snapInterval(v: number): number {
  if (v < 0.1) return Math.round(v / 0.005) * 0.005;
  return Math.round(v / 0.1) * 0.1;
}

function formatInterval(v: number): string {
  if (v < 0.1) return v.toFixed(3);
  if (v >= 1) return v.toFixed(1);
  return v.toFixed(2);
}

export default function SimToggle() {
  const [status, setStatus] = useState<SimStatus | null>(null);

  const refresh = async () => {
    try {
      setStatus(await jget<SimStatus>("/simulator"));
    } catch (_) { /* ignore */ }
  };

  useEffect(() => {
    refresh();
    // 500ms so the actual-rate gauge feels live during simulator changes.
    const id = setInterval(refresh, 500);
    return () => clearInterval(id);
  }, []);

  const setEnabled = async (enabled: boolean) => {
    setStatus(await jpost<SimStatus>("/simulator", { enabled }));
  };
  const setInterval_ = async (interval_seconds: number) => {
    setStatus(await jpost<SimStatus>("/simulator", { interval_seconds }));
  };
  const setWorkers = async (workers: number) => {
    setStatus(await jpost<SimStatus>("/simulator", { workers }));
  };

  if (!status) return <div>Loading…</div>;

  // Theoretical max rate: workers / interval. The real ceiling is lower
  // (MySQL roundtrip latency caps each worker to roughly 100–300/sec).
  const targetRate = status.workers / status.interval_seconds;

  return (
    <div>
      <div className="row" style={{ alignItems: "center" }}>
        <div style={{ flex: 0 }}>
          <button
            className={status.enabled ? "" : "secondary"}
            onClick={() => setEnabled(!status.enabled)}
            style={{ width: 110 }}
          >
            {status.enabled ? "Stop" : "Start"}
          </button>
        </div>
        <div style={{ color: "var(--muted)" }}>
          {status.enabled
            ? `${status.workers}× workers @ ${formatInterval(status.interval_seconds)}s → ~${targetRate.toFixed(0)}/sec target`
            : "Simulator is off"}
        </div>
      </div>

      <label style={{ marginTop: 10 }}>Interval (seconds)</label>
      <input
        type="range"
        min={0.005}
        max={10}
        step="any"
        value={status.interval_seconds}
        onChange={(e) => setInterval_(snapInterval(parseFloat(e.target.value)))}
      />
      <div style={{ color: "var(--muted)", fontSize: 12 }}>
        {formatInterval(status.interval_seconds)}s per worker
      </div>

      <label style={{ marginTop: 10 }}>Workers ({status.workers})</label>
      <input
        type="range"
        min={1}
        max={50}
        step={1}
        value={status.workers}
        onChange={(e) => setWorkers(parseInt(e.target.value, 10))}
      />
      <div style={{ color: "var(--muted)", fontSize: 12 }}>
        target ≈ {targetRate.toFixed(0)} publishes/sec
      </div>

      {status.enabled && status.actual_rate_per_sec !== undefined && (
        <ActualRate status={status} target={targetRate} />
      )}
    </div>
  );
}

function ActualRate({ status, target }: { status: SimStatus; target: number }) {
  const actual = status.actual_rate_per_sec ?? 0;
  const queue = status.queue_depth ?? 0;
  const queueMax = status.queue_maxsize ?? 1;
  const lastSize = status.last_batch_size ?? 0;
  const lastMs = status.last_batch_ms ?? 0;
  const batchesPerSec = status.batches_per_sec ?? 0;
  const dropped = status.dropped_total ?? 0;
  const flushMs = status.flush_interval_ms ?? 0;
  const pct = target > 0 ? Math.min(100, (actual / target) * 100) : 0;
  const queuePct = Math.min(100, (queue / queueMax) * 100);
  const color = pct > 80 ? "var(--green)" : pct > 40 ? "var(--amber)" : "var(--pink)";

  return (
    <div
      style={{
        marginTop: 10,
        padding: "6px 8px",
        background: "var(--panel-2)",
        border: "1px solid var(--border)",
        borderRadius: 4,
        fontSize: 11,
        color: "var(--muted)",
      }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4 }}>
        <span>
          actual:{" "}
          <strong style={{ color }}>{actual.toFixed(0)}</strong>/sec
          {" of "}
          <strong style={{ color: "var(--text)" }}>{target.toFixed(0)}</strong> target
          {" "}({pct.toFixed(0)}%)
        </span>
        <span>
          flusher: <strong style={{ color: "var(--text)" }}>{batchesPerSec.toFixed(1)}</strong> batches/s
          {" · last "}
          <strong style={{ color: "var(--text)" }}>{lastSize}</strong> rows in{" "}
          <strong style={{ color: "var(--text)" }}>{lastMs.toFixed(1)}ms</strong>
        </span>
      </div>
      <div
        style={{
          height: 4,
          background: "var(--bg)",
          borderRadius: 2,
          overflow: "hidden",
          marginBottom: 4,
        }}
      >
        <div
          style={{
            height: "100%",
            width: `${pct}%`,
            background: color,
            transition: "width 200ms ease",
          }}
        />
      </div>
      <div style={{ display: "flex", justifyContent: "space-between" }}>
        <span>
          queue: <strong style={{ color: queuePct > 75 ? "var(--pink)" : "var(--text)" }}>{queue.toLocaleString()}</strong>
          {" / "}
          {queueMax.toLocaleString()} ({queuePct.toFixed(0)}% · flush every {flushMs}ms)
        </span>
        {dropped > 0 && (
          <span style={{ color: "var(--pink)" }}>
            <strong>{dropped.toLocaleString()}</strong> dropped (queue full)
          </span>
        )}
      </div>
    </div>
  );
}
