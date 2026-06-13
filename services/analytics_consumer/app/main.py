"""Analytics consumer.

This implementation uses the Kafka Streams "state store + changelog topic"
pattern. All aggregates (counters, top-authors map, author-name lookup)
live in memory, but every mutation is also written to a compacted Kafka
topic (`cdc.analytics-state`). On restart we restore state from that
changelog rather than replaying the full source CDC topics — restart time
becomes proportional to the number of *unique keys*, not the total event
count.

Why this matters: as the source `cdc.demo.articles` topic grows, full
replay gets linearly slower. The changelog, thanks to compaction, stays
bounded by:
  - one entry per author (author_name:<id>, author_count:<id>)
  - one entry per kind (kind:<k>)
  - one entry total (total)
  - one entry per article (article_author:<id>)  -- the only piece that
    still grows linearly with articles; the rest stays small forever.

State keys written to the changelog
-----------------------------------
  total                  -> int   (total article count)
  kind:<kind>            -> int   (per-kind count)
  author_name:<id>       -> str   (author_id -> name)
  author_count:<id>      -> int   (articles_by_author[id])
  article_author:<id>    -> int   (article_id -> author_id; doubles as
                                   idempotency marker for de-dupe)

A null value at any key is a tombstone — compaction will delete the key
on its next pass."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import Counter, deque
from contextlib import asynccontextmanager
from typing import Any

import httpx
import uvicorn
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRebalanceListener, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError
from fastapi import FastAPI

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("analytics-consumer")

KAFKA = os.environ["KAFKA_BOOTSTRAP"]
EVENT_URL = os.environ["API_EVENT_URL"]
BATCH_EVENT_URL = EVENT_URL + "-batch" if EVENT_URL else ""
SOURCE_TOPICS = ["cdc.demo.articles", "cdc.demo.authors"]
CHANGELOG_TOPIC = "cdc.analytics-state"

# How often we drain the coalescing buffer and force the producer to flush.
# Sets an upper bound on changelog write latency during quiet periods; under
# load, the size trigger (COALESCE_MAX_KEYS) fires well before this does.
STATE_FLUSH_INTERVAL_MS = 100

# Write coalescing. Every value we write to the changelog is the *absolute*
# current state (a count, a name, an author id — never a delta), so repeated
# writes to the same key within a flush window are redundant: only the last
# one matters to a compacted-changelog consumer. We buffer mutations keyed by
# changelog key and emit one send per *distinct* key per flush, instead of one
# per logical mutation. This is the analog of the search-consumer's batched
# add_documents (which also dedupes by primary key): it collapses the
# 4-writes-per-article firehose — especially the hot `total` key, written on
# every single insert — down to at most one write per key per window.
COALESCE_MAX_KEYS = 5000

# Trace event batching. Same pattern as search-consumer: enqueue in memory,
# flush in batches via /events/log-batch instead of one HTTP POST per event.
TRACE_BATCH_SIZE = 500
TRACE_BATCH_AGE_MS = 100


class Stats:
    def __init__(self) -> None:
        self.author_names: dict[int, str] = {}
        self.articles_by_author: Counter[int] = Counter()
        self.kinds: Counter[str] = Counter()
        self.total_articles = 0
        self.article_author: dict[int, int] = {}
        self.recent_ts: deque[float] = deque(maxlen=20000)

    def snapshot(self) -> dict[str, Any]:
        top = self.articles_by_author.most_common(10)
        top_named = [
            {"author": self.author_names.get(aid, f"author#{aid}"), "count": c}
            for aid, c in top
        ]
        now = time.time()
        return {
            "total_articles": self.total_articles,
            "unique_authors": len({a for a, c in self.articles_by_author.items() if c > 0}),
            "kinds": dict(self.kinds),
            "top_authors": top_named,
            "rate_last_60s": sum(1 for t in self.recent_ts if now - t <= 60),
            "rate_last_300s": sum(1 for t in self.recent_ts if now - t <= 300),
        }


stats = Stats()
producer: AIOKafkaProducer | None = None

# Counters surfaced via /stats so the UI can show throughput climbing.
_state_writes_total = 0
_state_write_failures = 0
_events_processed_total = 0
# Periodic snapshots of (timestamp, events_total, state_writes_total). The
# sampler task appends one entry per second; rate is computed as the slope
# between the oldest and newest sample in the window. This replaces a
# per-event timestamp deque, which silently caps the displayed rate when
# throughput exceeds maxlen/window — at >4000 ev/s the deque overflowed
# in <5s and we under-counted.
_throughput_samples: deque[tuple[float, int, int]] = deque(maxlen=15)

# Coalescing buffer: changelog key -> latest absolute value (None == tombstone).
# write_state records into this map (no Kafka I/O); the flusher drains it,
# emitting one send per distinct key. The swap in _flush_dirty is done without
# an intervening await, so it's atomic against the single-task consume loop —
# no lock needed. _logical_writes_total counts pre-coalesce mutations so /stats
# can report the coalescing ratio (logical writes collapsed per actual send).
_dirty: dict[str, Any] = {}
_logical_writes_total = 0


# ---------------------------------------------------------------------------
# Changelog write helpers
# ---------------------------------------------------------------------------
def _make_producer() -> AIOKafkaProducer:
    """Producer tuned for high-throughput pipelined writes.

    - linger_ms: wait briefly so multiple sends batch into one broker request.
      At low rates this adds ~20ms latency; at high rates it pays for itself
      many times over by collapsing N tiny writes into one network roundtrip.
    - enable_idempotence: dedupes producer retries broker-side, which lets us
      keep multiple in-flight requests per partition without risking reorders.
      Per-key order is what the changelog needs to stay correct.
    """
    return AIOKafkaProducer(
        bootstrap_servers=KAFKA,
        compression_type="gzip",
        linger_ms=20,
        max_batch_size=65536,
        enable_idempotence=True,
    )


def _send_callback(fut: asyncio.Future) -> None:
    """Track state-write success/failure without awaiting each Future.
    Called when aiokafka resolves the per-send Future."""
    global _state_writes_total, _state_write_failures
    exc = fut.exception() if not fut.cancelled() else None
    if exc is not None:
        _state_write_failures += 1
        log.debug("state write failed: %s", exc)
    else:
        _state_writes_total += 1


async def write_state(key: str, value: Any) -> None:
    """Record one state mutation into the coalescing buffer (no Kafka I/O).

    We do *not* send to Kafka here. Instead we stash key -> latest value in
    the in-memory dirty map. Because every value is the absolute current
    state (never a delta), repeated writes to the same key within a flush
    window are redundant — only the last one matters to a compacted-changelog
    consumer — so overwriting in a dict is exactly the right semantics. The
    state_flusher (or the size trigger below) drains the map, emitting one
    send per distinct key.

    Pass value=None to record a tombstone (key gets removed at compaction);
    dict stores None as a value, so `key in _dirty` still distinguishes it
    from an untouched key.
    """
    global _logical_writes_total
    _logical_writes_total += 1
    _dirty[key] = value
    # Size trigger: bound memory and write latency under heavy load, mirroring
    # the search-consumer's MAX_BATCH_SIZE. Distinct keys per window stay small
    # even at high event rates (bounded by authors/articles actually touched).
    if len(_dirty) >= COALESCE_MAX_KEYS:
        await _flush_dirty()


async def _flush_dirty() -> None:
    """Drain the coalescing buffer, emitting one changelog send per distinct
    key. The producer's own linger_ms then batches these sends into broker
    requests — so we get two layers of reduction: coalesce-by-key here, then
    batch-on-the-wire in aiokafka.

    The `batch = _dirty; _dirty = {}` swap has no await between its two
    statements, so it's atomic against the single-task consume loop: any
    write_state that runs while we're awaiting sends below lands in the fresh
    map and is picked up by the next flush."""
    global _dirty
    if not _dirty or producer is None:
        return
    batch = _dirty
    _dirty = {}
    for key, value in batch.items():
        payload = None if value is None else json.dumps({"v": value}).encode("utf-8")
        fut = await producer.send(CHANGELOG_TOPIC, key=key.encode("utf-8"), value=payload)
        fut.add_done_callback(_send_callback)


async def state_flusher() -> None:
    """Drain the coalescing buffer and force a producer flush every
    STATE_FLUSH_INTERVAL_MS. Bounds worst-case changelog staleness during
    quiet periods; under load the size trigger in write_state fires first."""
    interval = STATE_FLUSH_INTERVAL_MS / 1000
    while True:
        await asyncio.sleep(interval)
        if producer is None:
            continue
        try:
            await _flush_dirty()
            await producer.flush()
        except Exception as e:  # noqa: BLE001
            log.exception("periodic state flush failed: %s", e)


async def throughput_sampler() -> None:
    """Snapshot running totals once per second. /stats computes rate as the
    slope between the oldest and newest sample in the ring buffer — no
    per-event work, no maxlen-induced cap."""
    while True:
        await asyncio.sleep(1)
        _throughput_samples.append(
            (time.time(), _events_processed_total, _state_writes_total)
        )


def apply_state_entry(key: str, value: dict | None) -> None:
    """Apply one changelog entry to the in-memory Stats during restore.
    Tombstones (value=None) remove the key."""
    if value is None:
        if key.startswith("author_name:"):
            stats.author_names.pop(int(key.split(":", 1)[1]), None)
        elif key.startswith("author_count:"):
            stats.articles_by_author.pop(int(key.split(":", 1)[1]), None)
        elif key.startswith("kind:"):
            stats.kinds.pop(key.split(":", 1)[1], None)
        elif key.startswith("article_author:"):
            stats.article_author.pop(int(key.split(":", 1)[1]), None)
        return
    v = value["v"]
    if key.startswith("author_name:"):
        stats.author_names[int(key.split(":", 1)[1])] = v
    elif key.startswith("author_count:"):
        stats.articles_by_author[int(key.split(":", 1)[1])] = int(v)
    elif key.startswith("kind:"):
        stats.kinds[key.split(":", 1)[1]] = int(v)
    elif key.startswith("article_author:"):
        stats.article_author[int(key.split(":", 1)[1])] = int(v)
    elif key == "total":
        stats.total_articles = int(v)


# ---------------------------------------------------------------------------
# Topic management
# ---------------------------------------------------------------------------
async def ensure_changelog_topic() -> None:
    """Create the compacted changelog topic if it doesn't exist. Aggressive
    compaction settings make the behavior visible in a demo (otherwise
    compaction can take hours)."""
    admin = AIOKafkaAdminClient(bootstrap_servers=KAFKA)
    await admin.start()
    try:
        topic = NewTopic(
            name=CHANGELOG_TOPIC,
            num_partitions=1,
            replication_factor=1,
            topic_configs={
                "cleanup.policy": "compact",
                "segment.ms": "20000",          # roll segments every 20s
                "min.cleanable.dirty.ratio": "0.01",  # compact at any dirtiness
                "min.compaction.lag.ms": "0",   # no waiting period
                "delete.retention.ms": "1000",  # tombstones drop fast
            },
        )
        try:
            await admin.create_topics([topic])
            log.info("created changelog topic %s with compaction", CHANGELOG_TOPIC)
        except TopicAlreadyExistsError:
            log.info("changelog topic %s already exists", CHANGELOG_TOPIC)
    finally:
        await admin.close()


# ---------------------------------------------------------------------------
# Restore from changelog
# ---------------------------------------------------------------------------
async def restore_from_changelog() -> int:
    """Replay the entire compacted changelog topic to rebuild in-memory
    Stats. Returns the number of entries applied (i.e. surviving after
    compaction at the time of read)."""
    consumer = AIOKafkaConsumer(
        bootstrap_servers=KAFKA,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )
    await consumer.start()
    try:
        all_topics = await consumer.topics()
        if CHANGELOG_TOPIC not in all_topics:
            log.info("changelog topic missing during restore; starting from empty state")
            return 0
        # Explicit assign loads partition metadata immediately.
        tp = TopicPartition(CHANGELOG_TOPIC, 0)
        consumer.assign([tp])
        await consumer.seek_to_beginning(tp)
        end_offset = (await consumer.end_offsets([tp]))[tp]
        if end_offset == 0:
            log.info("changelog topic empty; starting from empty state")
            return 0
        # Read the whole changelog up to end_offset. The per-article
        # `article_author:<id>` key means the surviving (post-compaction)
        # record count grows linearly with articles — at multi-million scale
        # an arbitrary iteration cap silently truncates the restore, leaving
        # high-offset keys (notably `total`, rewritten every event so its only
        # survivor lives near the end) unapplied and the in-memory totals
        # wrong. Loop until we actually reach end_offset, with a no-progress
        # guard so a stalled fetch can't spin forever.
        n = 0
        empty_polls = 0
        while (await consumer.position(tp)) < end_offset:
            batches = await consumer.getmany(timeout_ms=1000, max_records=2000)
            if not batches:
                empty_polls += 1
                if empty_polls >= 3:
                    log.warning(
                        "restore stopped early at offset %d/%d (no more records arriving)",
                        await consumer.position(tp), end_offset,
                    )
                    break
                continue
            empty_polls = 0
            for _tp, msgs in batches.items():
                for msg in msgs:
                    apply_state_entry(msg.key, msg.value)
                    n += 1
        log.info("restored stats from %d changelog entries (read to offset %d/%d)",
                 n, await consumer.position(tp), end_offset)
        return n
    finally:
        await consumer.stop()


# ---------------------------------------------------------------------------
# Trace helpers (batched HTTP POSTs, mirrors search-consumer)
# ---------------------------------------------------------------------------
pending_traces: list[dict[str, Any]] = []
trace_lock = asyncio.Lock()
_trace_http: httpx.AsyncClient | None = None


async def trace(_client: httpx.AsyncClient | None, type_: str, payload: dict, *, correlation_id: Any = None) -> None:
    """Enqueue a trace event for batched POST to the api's event log.

    Non-blocking apart from a tiny lock + append. The first arg is kept for
    backward compatibility with the previous signature but is unused — the
    flusher uses the module-level _trace_http client."""
    async with trace_lock:
        pending_traces.append({
            "service": "analytics",
            "type": type_,
            "payload": payload,
            "correlation_id": correlation_id,
        })
        should_flush = len(pending_traces) >= TRACE_BATCH_SIZE
    if should_flush:
        await flush_traces()


async def flush_traces() -> None:
    async with trace_lock:
        if not pending_traces:
            return
        batch = pending_traces[:]
        pending_traces.clear()
    if _trace_http is None:
        return
    try:
        await _trace_http.post(BATCH_EVENT_URL, json=batch, timeout=5)
    except Exception as e:  # noqa: BLE001
        log.debug("trace batch flush failed (size=%d): %s", len(batch), e)


async def trace_flusher() -> None:
    interval = TRACE_BATCH_AGE_MS / 1000
    while True:
        await asyncio.sleep(interval)
        try:
            await flush_traces()
        except Exception as e:  # noqa: BLE001
            log.exception("periodic trace flush failed: %s", e)


# ---------------------------------------------------------------------------
# Main consume loop
# ---------------------------------------------------------------------------
class FreshBootstrapRewinder(ConsumerRebalanceListener):
    """First-deploy-only rewinder. If the changelog had nothing to restore
    from, we treat the consumer as never-before-run and seek the source
    topics to offset 0 so we can populate the changelog from history.

    Subsequent restarts will find a non-empty changelog and skip this —
    they restore from the (compact, small) changelog and resume the
    source consumer from its committed offset."""

    def __init__(self) -> None:
        self.consumer: AIOKafkaConsumer | None = None
        self._done = False

    def bind(self, consumer: AIOKafkaConsumer) -> None:
        self.consumer = consumer

    async def on_partitions_revoked(self, revoked) -> None:  # noqa: ARG002, ANN001
        return

    async def on_partitions_assigned(self, assigned) -> None:  # noqa: ANN001
        if self._done or self.consumer is None or not assigned:
            return
        await self.consumer.seek_to_beginning(*assigned)
        log.info("first-deploy bootstrap: rewound %d partition(s) to 0", len(assigned))
        self._done = True


async def consume() -> None:
    await ensure_changelog_topic()
    global producer, _trace_http
    producer = _make_producer()
    await producer.start()

    restored = await restore_from_changelog()
    fresh_deploy = restored == 0
    log.info("post-restore stats: total=%d unique_authors=%d (fresh_deploy=%s)",
             stats.total_articles, stats.snapshot()["unique_authors"], fresh_deploy)

    consumer = AIOKafkaConsumer(
        bootstrap_servers=KAFKA,
        group_id="analytics-consumer",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
    )
    if fresh_deploy:
        rewinder = FreshBootstrapRewinder()
        rewinder.bind(consumer)
        consumer.subscribe(topics=SOURCE_TOPICS, listener=rewinder)
    else:
        consumer.subscribe(topics=SOURCE_TOPICS)
    await consumer.start()
    log.info("kafka consumer started, topics=%s (restored=%d, fresh_deploy=%s)",
             SOURCE_TOPICS, restored, fresh_deploy)
    async with httpx.AsyncClient() as http:
        _trace_http = http
        flusher = asyncio.create_task(state_flusher())
        traces = asyncio.create_task(trace_flusher())
        sampler = asyncio.create_task(throughput_sampler())
        try:
            async for msg in consumer:
                await handle(msg.topic, msg.value, http)
        finally:
            flusher.cancel()
            traces.cancel()
            sampler.cancel()
            # Drain in-flight work on shutdown so we don't lose trace events
            # or queued state writes. Flush the coalescing buffer first, then
            # the producer's own send buffer.
            try:
                await _flush_dirty()
                await producer.flush()
                await flush_traces()
            except Exception:  # noqa: BLE001
                pass
            await consumer.stop()
            await producer.stop()
            _trace_http = None


async def handle(topic: str, value: dict | None, http: httpx.AsyncClient) -> None:
    global _events_processed_total
    _events_processed_total += 1
    op = (value or {}).get("__op", "?")
    table = (value or {}).get("__source_table") or topic.split(".")[-1]
    deleted = str((value or {}).get("__deleted", "false")).lower() == "true"

    await trace(http, "kafka_received", {
        "topic": topic, "op": op, "deleted": deleted, "row_id": (value or {}).get("id"),
    }, correlation_id=(value or {}).get("id"))

    if value is None:
        return

    if table == "authors":
        author_id = int(value["id"])
        if deleted:
            stats.author_names.pop(author_id, None)
            await write_state(f"author_name:{author_id}", None)
        else:
            stats.author_names[author_id] = value["name"]
            await write_state(f"author_name:{author_id}", value["name"])
            await trace(http, "author_registered", {
                "author_id": author_id, "name": value["name"],
            }, correlation_id=author_id)
        return

    if table == "articles":
        article_id = int(value["id"])
        if deleted:
            prev_author = stats.article_author.pop(article_id, None)
            await write_state(f"article_author:{article_id}", None)
            if prev_author is not None:
                stats.articles_by_author[prev_author] -= 1
                await write_state(f"author_count:{prev_author}",
                                  int(stats.articles_by_author[prev_author]))
                stats.total_articles = max(0, stats.total_articles - 1)
                await write_state("total", stats.total_articles)
            await trace(http, "article_removed", {"id": article_id}, correlation_id=article_id)
            return

        author_id = int(value["author_id"])
        prev_author = stats.article_author.get(article_id)
        if prev_author is None:
            # New article — count it. (If we're re-processing an event we
            # already counted, this `prev_author is None` check skips it,
            # giving us at-least-once -> exactly-once de-dupe.)
            stats.article_author[article_id] = author_id
            stats.articles_by_author[author_id] += 1
            kind = value.get("kind", "article")
            stats.kinds[kind] += 1
            stats.total_articles += 1
            stats.recent_ts.append(time.time())
            await write_state(f"article_author:{article_id}", author_id)
            await write_state(f"author_count:{author_id}", int(stats.articles_by_author[author_id]))
            await write_state(f"kind:{kind}", int(stats.kinds[kind]))
            await write_state("total", stats.total_articles)
        elif prev_author != author_id:
            # Update changing author. Move count from old to new.
            stats.articles_by_author[prev_author] -= 1
            stats.articles_by_author[author_id] += 1
            stats.article_author[article_id] = author_id
            await write_state(f"article_author:{article_id}", author_id)
            await write_state(f"author_count:{prev_author}",
                              int(stats.articles_by_author[prev_author]))
            await write_state(f"author_count:{author_id}", int(stats.articles_by_author[author_id]))

        await trace(http, "analytics_updated", {
            "total_articles": stats.total_articles,
            "id": article_id,
            "author_id": author_id,
        }, correlation_id=article_id)


# ---------------------------------------------------------------------------
# HTTP front
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.task = asyncio.create_task(consume())
    yield
    app.state.task.cancel()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health() -> dict:
    return {"ok": True}


def _batch_metrics() -> dict:
    """Compute rates as the slope between the oldest and newest samples in
    the throughput buffer. Window grows from 1s up to ~14s as the sampler
    fills the deque, then stays at the buffer width — accurate at any
    real-world throughput."""
    events_per_sec = 0.0
    writes_per_sec = 0.0
    if len(_throughput_samples) >= 2:
        first = _throughput_samples[0]
        last = _throughput_samples[-1]
        dt = max(0.001, last[0] - first[0])
        events_per_sec = (last[1] - first[1]) / dt
        writes_per_sec = (last[2] - first[2]) / dt
    # Coalescing ratio: logical mutations collapsed per actual changelog send.
    # 1.0 = no coalescing; 4.0 = we cut changelog writes to a quarter. Climbs
    # during replay when hot keys (esp. `total`) get hit repeatedly per window.
    coalesce_ratio = (
        _logical_writes_total / _state_writes_total
        if _state_writes_total > 0 else 0.0
    )
    return {
        "events_processed_total": _events_processed_total,
        "logical_writes_total": _logical_writes_total,
        "state_writes_total": _state_writes_total,
        "state_write_failures": _state_write_failures,
        "coalesce_ratio": round(coalesce_ratio, 2),
        "events_per_sec": round(events_per_sec, 1),
        "state_writes_per_sec": round(writes_per_sec, 1),
        "trace_batch_size": TRACE_BATCH_SIZE,
        "trace_batch_age_ms": TRACE_BATCH_AGE_MS,
        "state_flush_interval_ms": STATE_FLUSH_INTERVAL_MS,
    }


@app.get("/stats")
def get_stats() -> dict:
    return {**stats.snapshot(), **_batch_metrics()}


@app.get("/admin/lag-breakdown")
async def lag_breakdown() -> dict:
    """Return the Kafka-side numbers needed to compute per-stage lag for
    cdc.demo.articles:
      - kafka_end_offset: high water mark (= number of events Debezium has
        emitted to this topic since start, assuming no compaction)
      - search_consumer_committed_offset: where the `search-consumer`
        group has committed up to (= articles the consumer has processed)
    Lag math is done in the api so it can join these with MySQL truth and
    Meilisearch doc count."""
    consumer = AIOKafkaConsumer(bootstrap_servers=KAFKA, enable_auto_commit=False)
    admin = AIOKafkaAdminClient(bootstrap_servers=KAFKA)
    await consumer.start()
    await admin.start()
    try:
        tp = TopicPartition("cdc.demo.articles", 0)
        consumer.assign([tp])
        ends = await consumer.end_offsets([tp])
        end_offset = int(ends[tp])

        committed = 0
        try:
            offsets = await admin.list_consumer_group_offsets("search-consumer")
            entry = offsets.get(tp)
            if entry is not None:
                committed = int(entry.offset)
        except Exception as e:  # noqa: BLE001
            log.debug("could not read search-consumer committed offset: %s", e)

        return {
            "kafka_end_offset": end_offset,
            "search_consumer_committed_offset": committed,
        }
    finally:
        await consumer.stop()
        await admin.close()


@app.get("/admin/topic-sizes")
async def topic_sizes() -> dict:
    """Compare source topic sizes vs the compacted changelog.

    For source topics: `offsets_written = records_kept` (no compaction).
    For the changelog: `offsets_written` is the total number of state
    mutations ever produced, while `records_kept` counts the records still
    on disk after compaction has run — the gap between them is what
    compaction has reclaimed."""
    out: dict[str, dict[str, int]] = {}
    consumer = AIOKafkaConsumer(bootstrap_servers=KAFKA, enable_auto_commit=False)
    await consumer.start()
    try:
        all_topics = await consumer.topics()
        for topic in [*SOURCE_TOPICS, CHANGELOG_TOPIC]:
            if topic not in all_topics:
                out[topic] = {"offsets_written": 0, "records_kept": 0}
                continue
            tp = TopicPartition(topic, 0)
            consumer.assign([tp])
            beginnings = await consumer.beginning_offsets([tp])
            ends = await consumer.end_offsets([tp])
            offsets_written = ends[tp] - beginnings[tp]
            if topic != CHANGELOG_TOPIC:
                # Source topics aren't compacted; offset count == record count.
                out[topic] = {"offsets_written": offsets_written, "records_kept": offsets_written}
                continue
            # For the changelog, actually count surviving records.
            await consumer.seek_to_beginning(tp)
            records = 0
            for _ in range(200):
                if (await consumer.position(tp)) >= ends[tp]:
                    break
                batches = await consumer.getmany(timeout_ms=500, max_records=2000)
                if not batches:
                    break
                for _tp, msgs in batches.items():
                    records += len(msgs)
            out[topic] = {"offsets_written": offsets_written, "records_kept": records}
    finally:
        await consumer.stop()
    return out


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8002)
