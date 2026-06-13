import { useEffect, useRef, useState } from "react";
import { Article, jget, SearchResp, SearchStats } from "../api";

export default function Search() {
  const [q, setQ] = useState("");
  const [res, setRes] = useState<SearchResp | null>(null);
  const [busy, setBusy] = useState(false);
  const [stats, setStats] = useState<SearchStats | null>(null);
  // Which hit is expanded, plus a small cache of full articles fetched on
  // demand. Search hits carry no body (Meili omits it), so opening one lazily
  // pulls the full text from MySQL via /articles/{id} and we keep it around so
  // re-opening is instant.
  const [openId, setOpenId] = useState<number | null>(null);
  const [details, setDetails] = useState<Record<number, Article>>({});
  const [loadingId, setLoadingId] = useState<number | null>(null);
  const [detailErr, setDetailErr] = useState<number | null>(null);
  const timer = useRef<number | null>(null);

  async function toggleHit(id: number) {
    if (openId === id) {
      setOpenId(null);
      return;
    }
    setOpenId(id);
    if (details[id]) return; // already cached
    setLoadingId(id);
    setDetailErr(null);
    try {
      const a = await jget<Article>(`/articles/${id}`);
      setDetails((d) => ({ ...d, [id]: a }));
    } catch (_) {
      setDetailErr(id);
    } finally {
      setLoadingId(null);
    }
  }

  // A fresh query invalidates which result is open.
  useEffect(() => {
    setOpenId(null);
  }, [res]);

  useEffect(() => {
    if (timer.current) window.clearTimeout(timer.current);
    if (!q.trim()) {
      setRes(null);
      return;
    }
    timer.current = window.setTimeout(async () => {
      setBusy(true);
      try {
        setRes(await jget<SearchResp>(`/search?q=${encodeURIComponent(q)}`));
      } catch (_) {
        setRes(null);
      } finally {
        setBusy(false);
      }
    }, 200);
  }, [q]);

  useEffect(() => {
    const refresh = async () => {
      try {
        // Single endpoint owns the snapshot. The pill, catch-up bar, and
        // stage-breakdown bar all derive from the same `stats` object so
        // they can never disagree across UI elements.
        setStats(await jget<SearchStats>("/search/stats"));
      } catch (_) { /* ignore */ }
    };
    refresh();
    const id = setInterval(refresh, 400);
    return () => clearInterval(id);
  }, []);

  return (
    <div>
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6 }}>
        <IndexingBadge stats={stats} />
      </div>
      <CatchUpProgress stats={stats} />
      <LagBreakdownBar stats={stats} />
      <BatchingInfo stats={stats} />
      <input
        value={q}
        onChange={(e) => setQ(e.target.value)}
        placeholder="Search articles by any word..."
      />
      <div style={{ color: "var(--muted)", fontSize: 12, margin: "4px 0 8px" }}>
        {busy ? "Searching…" : res ? `${res.hits.length} hits in ${res.processing_ms ?? "?"}ms` : "Try: 'kettle', 'bread', 'maps'"}
      </div>
      <div className="scroll" style={{ maxHeight: 260 }}>
        {res?.hits.map((h) => {
          const open = openId === h.id;
          const detail = details[h.id];
          return (
            <div key={h.id} className="article">
              <div
                onClick={() => toggleHit(h.id)}
                style={{ cursor: "pointer" }}
                title={open ? "Click to collapse" : "Click to read"}
              >
                <div className="title">
                  <span
                    style={{
                      display: "inline-block",
                      width: 12,
                      color: "var(--muted)",
                      transition: "transform 150ms",
                      transform: open ? "rotate(90deg)" : "none",
                    }}
                  >
                    ▸
                  </span>{" "}
                  {h.title}
                </div>
                <div className="meta">
                  <span className="tag tag-search">{h.kind}</span>{" "}
                  by <strong>{h.author}</strong> · id #{h.id}
                </div>
              </div>
              {open && (
                <div
                  style={{
                    marginTop: 8,
                    paddingTop: 8,
                    borderTop: "1px solid var(--border)",
                  }}
                >
                  {loadingId === h.id ? (
                    <div style={{ color: "var(--muted)", fontSize: 12 }}>Loading…</div>
                  ) : detailErr === h.id ? (
                    <div style={{ color: "var(--pink)", fontSize: 12 }}>
                      Could not load article #{h.id}.
                    </div>
                  ) : detail ? (
                    <>
                      <div
                        style={{
                          color: "var(--muted)",
                          fontSize: 11,
                          marginBottom: 6,
                        }}
                      >
                        {new Date(detail.published_at).toLocaleString()}
                      </div>
                      <div
                        style={{
                          whiteSpace: "pre-wrap",
                          fontSize: 13,
                          lineHeight: 1.5,
                          maxHeight: 220,
                          overflowY: "auto",
                          fontFamily:
                            h.kind === "poem"
                              ? "ui-monospace, SFMono-Regular, monospace"
                              : "inherit",
                        }}
                      >
                        {detail.body}
                      </div>
                    </>
                  ) : null}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

function CatchUpProgress({ stats }: { stats: SearchStats | null }) {
  // Indexed-fraction of the source. Simpler and more honest than the
  // previous "lag-recovered-from-peak" model: that one showed 0% whenever
  // the page loaded mid-catchup, because the peak was just current lag.
  // This bar always answers "how close is the index to the source of truth?"
  if (!stats || stats.lag === 0 || stats.mysql_articles_count === 0) return null;

  const total = stats.mysql_articles_count;
  const indexed = stats.documents_count;
  const lag = stats.lag;
  const pct = Math.min(100, (indexed / total) * 100);
  const rate = stats.docs_indexed_per_sec ?? 0;
  const etaSec = rate > 0 && lag > 0 ? Math.ceil(lag / rate) : null;

  return (
    <div style={{ marginBottom: 8 }}>
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
          Indexed:{" "}
          <strong style={{ color: "var(--text)" }}>{indexed.toLocaleString()}</strong>
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

function BatchingInfo({ stats }: { stats: SearchStats | null }) {
  if (!stats || stats.max_batch_size === undefined) return null;
  const pending = stats.pending_docs ?? 0;
  const lastSize = stats.last_batch_size ?? 0;
  const lastMs = stats.last_batch_ms ?? 0;
  const batchesPerSec = stats.batches_per_sec ?? 0;
  const docsPerSec = stats.docs_indexed_per_sec ?? 0;
  const avgSize = stats.avg_batch_size ?? 0;
  const maxSize = stats.max_batch_size ?? 500;
  const maxAge = stats.max_batch_age_ms ?? 100;

  const bufferPct = Math.min(100, (pending / maxSize) * 100);
  return (
    <div
      style={{
        marginBottom: 8,
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
          Batch buffer (≤{maxSize} docs / {maxAge}ms):{" "}
          <strong style={{ color: "var(--text)" }}>{pending}</strong>
        </span>
        <span>
          last batch:{" "}
          <strong style={{ color: "var(--text)" }}>{lastSize}</strong> docs in{" "}
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
            width: `${bufferPct}%`,
            background: "var(--purple)",
            transition: "width 200ms ease",
          }}
        />
      </div>
      <div style={{ display: "flex", justifyContent: "space-between" }}>
        <span>
          <strong style={{ color: "var(--text)" }}>{batchesPerSec.toFixed(1)}</strong> batches/sec
          {" · "}
          avg <strong style={{ color: "var(--text)" }}>{avgSize.toFixed(0)}</strong> docs/batch
        </span>
        <span>
          → <strong style={{ color: "var(--green)" }}>{docsPerSec.toFixed(0)}</strong> docs/sec indexed
        </span>
      </div>
    </div>
  );
}

function LagBreakdownBar({ stats }: { stats: SearchStats | null }) {
  if (!stats || stats.total_lag === undefined) return null;
  const debezium_lag = stats.debezium_lag ?? 0;
  const consumer_lag = stats.consumer_lag ?? 0;
  const meili_lag = stats.meili_lag ?? 0;
  const total_lag = stats.total_lag ?? 0;
  if (total_lag === 0) {
    return (
      <div style={{ color: "var(--muted)", fontSize: 11, marginBottom: 8 }}>
        Pipeline caught up across all stages.
      </div>
    );
  }
  const segments = [
    { key: "debezium", label: "Debezium (MySQL→Kafka)", value: debezium_lag, color: "var(--amber)" },
    { key: "consumer", label: "Consumer (Kafka→read)",  value: consumer_lag, color: "var(--purple)" },
    { key: "meili",    label: "Meilisearch (queued)",    value: meili_lag,    color: "var(--pink)" },
  ];
  return (
    <div style={{ marginBottom: 8 }}>
      <div
        style={{
          display: "flex",
          width: "100%",
          height: 8,
          background: "var(--panel-2)",
          border: "1px solid var(--border)",
          borderRadius: 4,
          overflow: "hidden",
        }}
      >
        {segments.map((s) => (
          <div
            key={s.key}
            title={`${s.label}: ${s.value}`}
            style={{
              width: `${(s.value / total_lag) * 100}%`,
              background: s.color,
              transition: "width 200ms ease",
            }}
          />
        ))}
      </div>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          fontSize: 11,
          color: "var(--muted)",
          marginTop: 4,
          gap: 8,
        }}
      >
        {segments.map((s) => (
          <span key={s.key}>
            <span
              style={{
                display: "inline-block",
                width: 6,
                height: 6,
                borderRadius: 1,
                background: s.color,
                marginRight: 4,
              }}
            />
            {s.label.split(" ")[0]}: <strong style={{ color: "var(--text)" }}>{s.value}</strong>
          </span>
        ))}
      </div>
    </div>
  );
}

function IndexingBadge({ stats }: { stats: SearchStats | null }) {
  if (!stats) return null;
  const offline = !stats.search_reachable;
  const behind = stats.lag > 0;
  // mr:0 overrides .pill's default 6px margin-right so the parent flex
  // container's `gap` controls all spacing consistently.
  const base: React.CSSProperties = { marginRight: 0 };
  const alert = offline || behind;
  return (
    <>
      <span
        className={`pill ${behind && !offline ? "pulse" : ""}`}
        style={
          alert
            ? { ...base, borderColor: "var(--pink)", color: "var(--pink)" }
            : base
        }
      >
        {offline ? (
          "offline"
        ) : (
          <>
            lag: <strong style={behind ? { color: "var(--pink)" } : undefined}>{stats.lag}</strong>
          </>
        )}
      </span>
      <span className="pill" style={base}>
        <strong>{stats.documents_count}</strong> / {stats.mysql_articles_count} docs
      </span>
    </>
  );
}
