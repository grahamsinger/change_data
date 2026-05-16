import { memo, useEffect, useMemo, useRef, useState } from "react";
import { eventsWebSocketURL, TraceEvent } from "../api";

const SERVICES = ["api", "mysql", "search", "analytics"] as const;
const MAX_EVENTS = 200;
const FLUSH_INTERVAL_MS = 100;

// Events carry a client-assigned id so React.memo can use a stable key
// (otherwise prepending makes every row's index shift and every memo bust).
type DecoratedEvent = TraceEvent & { _id: number; _payloadStr: string | null };

export default function EventFeed() {
  const [events, setEvents] = useState<DecoratedEvent[]>([]);
  const [paused, setPaused] = useState(false);
  const [filter, setFilter] = useState<Set<string>>(new Set(SERVICES));
  const [connected, setConnected] = useState(false);

  // Buffer messages between flushes so we don't re-render on every WS frame.
  // At 600 events/sec we render 10 times/sec with batches of ~60, not 600.
  const buffer = useRef<DecoratedEvent[]>([]);
  const idCounter = useRef(0);
  const pauseRef = useRef(paused);
  pauseRef.current = paused;

  // Single timer that drains the buffer into state.
  useEffect(() => {
    const flush = () => {
      if (buffer.current.length === 0) return;
      const incoming = buffer.current;
      buffer.current = [];
      setEvents((prev) => {
        // Buffer is in arrival order (oldest first). Newest should be at
        // the top of the list, so reverse the batch before prepending.
        incoming.reverse();
        const next = incoming.concat(prev);
        return next.length > MAX_EVENTS ? next.slice(0, MAX_EVENTS) : next;
      });
    };
    const id = window.setInterval(flush, FLUSH_INTERVAL_MS);
    return () => window.clearInterval(id);
  }, []);

  useEffect(() => {
    let ws: WebSocket | null = null;
    let cancelled = false;

    const connect = () => {
      ws = new WebSocket(eventsWebSocketURL());
      ws.onopen = () => setConnected(true);
      ws.onclose = () => {
        setConnected(false);
        if (!cancelled) setTimeout(connect, 1500);
      };
      ws.onmessage = (m) => {
        if (pauseRef.current) return;
        try {
          const parsed = JSON.parse(m.data);
          // Server now sends `{batch: [...]}`; tolerate single-event form too.
          const incoming: TraceEvent[] = Array.isArray(parsed?.batch)
            ? parsed.batch
            : [parsed as TraceEvent];
          for (const ev of incoming) {
            const payloadStr =
              ev.payload && Object.keys(ev.payload).length > 0
                ? JSON.stringify(ev.payload, null, 1)
                : null;
            buffer.current.push({
              ...ev,
              _id: ++idCounter.current,
              _payloadStr: payloadStr,
            });
          }
          // Soft cap on the staging buffer. At very high throughput the
          // flusher will keep it shorter, but don't grow without bound.
          if (buffer.current.length > MAX_EVENTS * 4) {
            buffer.current = buffer.current.slice(-MAX_EVENTS * 2);
          }
        } catch (_) { /* ignore */ }
      };
    };
    connect();

    return () => {
      cancelled = true;
      ws?.close();
    };
  }, []);

  const toggle = (s: string) => {
    setFilter((prev) => {
      const next = new Set(prev);
      if (next.has(s)) next.delete(s);
      else next.add(s);
      return next;
    });
  };

  // Filter at render time. With memo'd rows the cost is just one shallow
  // pass per flush; React skips re-rendering rows whose props didn't change.
  const shown = useMemo(
    () => events.filter((e) => filter.has(e.service)),
    [events, filter]
  );

  return (
    <>
      <div style={{ display: "flex", gap: 6, alignItems: "center", marginBottom: 8, flexWrap: "wrap" }}>
        <span
          className="pill"
          style={{ background: connected ? "rgba(101,211,148,.15)" : "rgba(240,138,161,.15)" }}
        >
          {connected ? "● connected" : "○ reconnecting"}
        </span>
        <button className="secondary" style={{ width: 80 }} onClick={() => setPaused((p) => !p)}>
          {paused ? "Resume" : "Pause"}
        </button>
        <button className="secondary" style={{ width: 80 }} onClick={() => setEvents([])}>Clear</button>
        {SERVICES.map((s) => (
          <button
            key={s}
            className={filter.has(s) ? "" : "secondary"}
            style={{ width: "auto", padding: "4px 10px" }}
            onClick={() => toggle(s)}
          >
            <span className={`tag tag-${s}`}>{s}</span>
          </button>
        ))}
      </div>

      <div className="scroll">
        {shown.length === 0 && (
          <div style={{ color: "var(--muted)" }}>
            No events yet. Publish an article or start the simulator.
          </div>
        )}
        {shown.map((e) => (
          <EventRow key={e._id} ev={e} />
        ))}
      </div>
    </>
  );
}

const EventRow = memo(function EventRow({ ev }: { ev: DecoratedEvent }) {
  // `time` is derived from ev.ts; ev is stable per _id so we can compute once.
  const t = ev.ts ? new Date(ev.ts * 1000) : new Date();
  const time =
    t.toLocaleTimeString("en-US", { hour12: false }) +
    "." +
    String(t.getMilliseconds()).padStart(3, "0");
  return (
    <div className="event">
      <div className="event-head">
        <span>
          <span className={`tag tag-${ev.service}`}>{ev.service}</span>{" "}
          <span className="event-type">{ev.type}</span>
        </span>
        <span className="event-time">{time}</span>
      </div>
      {ev._payloadStr && <div className="event-body">{ev._payloadStr}</div>}
    </div>
  );
});
