import { useEffect, useState } from "react";
import { jget, Stats, TopicSizes } from "../api";

export default function Analytics() {
  const [stats, setStats] = useState<Stats | null>(null);
  const [sizes, setSizes] = useState<TopicSizes | null>(null);

  useEffect(() => {
    const refresh = async () => {
      try {
        setStats(await jget<Stats>("/analytics"));
      } catch (_) { /* ignore */ }
    };
    refresh();
    // Faster polling so the events/sec rate visibly tracks throughput during
    // catch-up, the same way search-consumer's batching panel does.
    const id = setInterval(refresh, 500);
    return () => clearInterval(id);
  }, []);

  useEffect(() => {
    const refresh = async () => {
      try {
        setSizes(await jget<TopicSizes>("/analytics/topic-sizes"));
      } catch (_) { /* ignore */ }
    };
    refresh();
    // Polls less often — this endpoint counts records by reading the changelog.
    const id = setInterval(refresh, 4000);
    return () => clearInterval(id);
  }, []);

  if (!stats) return <div>Loading analytics…</div>;

  const max = Math.max(1, ...stats.top_authors.map((a) => a.count));

  return (
    <div>
      <div style={{ marginBottom: 10 }}>
        <span className="pill"><strong>{stats.total_articles}</strong> total</span>
        <span className="pill"><strong>{stats.unique_authors}</strong> authors</span>
        <span className="pill"><strong>{stats.rate_last_60s}</strong> in last 60s</span>
        <span className="pill"><strong>{stats.rate_last_300s}</strong> in last 5m</span>
      </div>

      <RebuildProgress stats={stats} />

      <BatchingInfo stats={stats} />

      {sizes && (
        <div className="section" style={{ marginBottom: 10 }}>
          <div style={{ color: "var(--muted)", fontSize: 12, marginBottom: 4 }}>
            Kafka topic sizes (changelog vs source)
          </div>
          <table style={{ width: "100%", fontSize: 12, color: "var(--muted)" }}>
            <thead>
              <tr>
                <th style={{ textAlign: "left", fontWeight: 500 }}>topic</th>
                <th style={{ textAlign: "right", fontWeight: 500 }}>written</th>
                <th style={{ textAlign: "right", fontWeight: 500 }}>kept</th>
                <th style={{ textAlign: "right", fontWeight: 500 }}>reclaimed</th>
              </tr>
            </thead>
            <tbody>
              {Object.entries(sizes).map(([topic, v]) => {
                const reclaimed = v.offsets_written - v.records_kept;
                const isChangelog = topic.includes("analytics-state");
                return (
                  <tr key={topic}>
                    <td style={{ color: isChangelog ? "var(--green)" : "var(--text)" }}>
                      {topic}
                    </td>
                    <td style={{ textAlign: "right", color: "var(--text)" }}>
                      {v.offsets_written.toLocaleString()}
                    </td>
                    <td style={{ textAlign: "right", color: "var(--text)" }}>
                      {v.records_kept.toLocaleString()}
                    </td>
                    <td
                      style={{
                        textAlign: "right",
                        color: reclaimed > 0 ? "var(--green)" : "var(--muted)",
                      }}
                    >
                      {reclaimed > 0 ? `−${reclaimed.toLocaleString()}` : "—"}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      <div className="section">
        <div style={{ color: "var(--muted)", fontSize: 12, marginBottom: 4 }}>By kind</div>
        <div className="kv">
          {Object.entries(stats.kinds).map(([k, v]) => (
            <Slot key={k} label={k} count={v as number} />
          ))}
        </div>
      </div>

      <hr className="sep" />

      <div className="section">
        <div style={{ color: "var(--muted)", fontSize: 12, marginBottom: 4 }}>
          Top authors
        </div>
        <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
          {stats.top_authors.map((a) => (
            <div key={a.author}>
              <div style={{ display: "flex", justifyContent: "space-between" }}>
                <span>{a.author}</span>
                <span style={{ color: "var(--muted)" }}>{a.count}</span>
              </div>
              <div className="bar" style={{ width: `${(a.count / max) * 100}%` }} />
            </div>
          ))}
          {stats.top_authors.length === 0 && (
            <div style={{ color: "var(--muted)" }}>No data yet — publish or start the simulator.</div>
          )}
        </div>
      </div>
    </div>
  );
}

function RebuildProgress({ stats }: { stats: Stats }) {
  // Aggregated-fraction of the source, the analytics analog of the search
  // panel's "Indexed: X / Y (Z%)" bar. Shown only while behind MySQL (during a
  // re-snapshot / catch-up); hidden once the store has caught up to truth.
  const total = stats.mysql_count ?? 0;
  const done = stats.total_articles;
  if (total === 0 || done >= total) return null;

  const pct = Math.min(100, (done / total) * 100);
  const rate = stats.events_per_sec ?? 0;
  const remaining = total - done;
  const etaSec = rate > 0 ? Math.ceil(remaining / rate) : null;

  return (
    <div style={{ marginBottom: 10 }}>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          fontSize: 11,
          marginBottom: 4,
          color: "var(--muted)",
        }}
      >
        <span>
          Rebuilt:{" "}
          <strong style={{ color: "var(--text)" }}>{done.toLocaleString()}</strong>
          {" / "}
          <strong style={{ color: "var(--text)" }}>{total.toLocaleString()}</strong>
          {" "}
          ({pct.toFixed(1)}%)
        </span>
        {etaSec !== null && (
          <span>
            ~{etaSec}s left @{" "}
            <strong style={{ color: "var(--text)" }}>{Math.round(rate).toLocaleString()}</strong>/sec
          </span>
        )}
      </div>
      <div
        style={{
          height: 10,
          background: "var(--panel-2)",
          borderRadius: 4,
          overflow: "hidden",
          border: "1px solid var(--border)",
        }}
      >
        <div
          style={{
            width: `${pct}%`,
            height: "100%",
            background: "var(--purple)",
            transition: "width 200ms ease",
          }}
        />
      </div>
    </div>
  );
}

function Slot({ label, count }: { label: string; count: number }) {
  return (
    <>
      <div>{label}</div>
      <div>{count}</div>
    </>
  );
}

function BatchingInfo({ stats }: { stats: Stats }) {
  if (stats.events_per_sec === undefined) return null;
  const events = stats.events_per_sec ?? 0;
  const writes = stats.state_writes_per_sec ?? 0;
  const failures = stats.state_write_failures ?? 0;
  const traceBatch = stats.trace_batch_size ?? 0;
  const traceAge = stats.trace_batch_age_ms ?? 0;
  const flushMs = stats.state_flush_interval_ms ?? 0;
  return (
    <div
      style={{
        marginBottom: 10,
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
          Pipelined writes (linger {flushMs}ms · trace batch ≤{traceBatch}/{traceAge}ms)
        </span>
        {failures > 0 && (
          <span style={{ color: "var(--pink)" }}>
            <strong>{failures}</strong> write failures
          </span>
        )}
      </div>
      <div style={{ display: "flex", justifyContent: "space-between" }}>
        <span>
          → <strong style={{ color: "var(--green)" }}>{events.toFixed(0)}</strong> events/sec processed
        </span>
        <span>
          → <strong style={{ color: "var(--text)" }}>{writes.toFixed(0)}</strong> changelog writes/sec
        </span>
      </div>
    </div>
  );
}
