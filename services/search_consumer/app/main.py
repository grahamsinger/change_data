"""Search consumer.

Subscribes to the `cdc.demo.articles` and `cdc.demo.authors` Kafka topics
produced by Debezium, maintains a small (author_id -> name) cache so it can
denormalize articles, and **batches** them into Meilisearch.

Batching: instead of one `add_documents([doc])` per CDC event (each costing
3–10ms of HTTP roundtrip plus a new Meilisearch task), we buffer documents
and flush either when the buffer hits `MAX_BATCH_SIZE` or after
`MAX_BATCH_AGE_MS`, whichever comes first. One HTTP call indexes hundreds
of docs in roughly the same wall time as one — net throughput goes up
roughly 10–50× at high rates.

Also runs a FastAPI server on :8001 with /search and /stats routes."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import deque
from contextlib import asynccontextmanager
from typing import Any

import httpx
import uvicorn
from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener
from fastapi import FastAPI
from meilisearch import Client as MeiliClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("search-consumer")

KAFKA = os.environ["KAFKA_BOOTSTRAP"]
MEILI_URL = os.environ["MEILI_URL"]
MEILI_KEY = os.environ["MEILI_KEY"]
EVENT_URL = os.environ["API_EVENT_URL"]

TOPICS = ["cdc.demo.articles", "cdc.demo.authors"]
INDEX = "articles"

# Batching knobs. Sized to give a visible "indexing-in-progress" feel:
# smaller batches => more frequent flushes => docs_count climbs in many
# small steps rather than one massive jump every few seconds.
MAX_BATCH_SIZE = 250
MAX_BATCH_AGE_MS = 100
TRACE_BATCH_SIZE = 500
TRACE_BATCH_AGE_MS = 100

# author_id -> name, populated from the authors topic
authors_cache: dict[int, str] = {}
# article_id -> document, in case authors arrive after articles
pending_articles: dict[int, dict[str, Any]] = {}

# Batching state -----------------------------------------------------------
pending_docs: list[dict[str, Any]] = []
batch_lock = asyncio.Lock()
# Recent batches: (timestamp, size, elapsed_ms). Bounded ring so /stats can
# report rolling windows without unbounded memory.
batch_history: deque[tuple[float, int, float]] = deque(maxlen=400)

# Trace batching state. Previously every CDC event triggered a synchronous
# HTTP POST to the api for the live timeline — at thousands of events/sec
# that's the dominant cost in the consumer loop. Now we enqueue and flush.
pending_traces: list[dict[str, Any]] = []
trace_lock = asyncio.Lock()

# Set in consume() so the periodic flushers and handle() can both reach them.
_meili: MeiliClient | None = None
_trace_http: httpx.AsyncClient | None = None
BATCH_EVENT_URL = EVENT_URL + "-batch" if EVENT_URL else ""


async def trace(type_: str, payload: dict, *, correlation_id: Any = None) -> None:
    """Enqueue a trace event for batched POST to the api's event log.

    Non-blocking apart from a tiny lock + append — does NOT do an HTTP
    call per event. The periodic flusher (or the size-trigger inside this
    function) ships them in batches via /events/log-batch."""
    async with trace_lock:
        pending_traces.append({
            "service": "search",
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


def setup_meili() -> MeiliClient:
    client = MeiliClient(MEILI_URL, MEILI_KEY)
    # Create index if missing. Meilisearch is idempotent here.
    try:
        client.get_index(INDEX)
    except Exception:  # noqa: BLE001
        client.create_index(INDEX, {"primaryKey": "id"})
    client.index(INDEX).update_settings({
        "searchableAttributes": ["title", "body", "author"],
        "displayedAttributes": ["id", "title", "author", "kind", "published_at"],
    })
    return client


# --- Batching ------------------------------------------------------------
async def queue_doc(doc: dict) -> None:
    """Append a document to the batch buffer. If we hit the size threshold
    we flush immediately so we never lag on the size-trigger path."""
    async with batch_lock:
        pending_docs.append(doc)
        should_flush = len(pending_docs) >= MAX_BATCH_SIZE
    if should_flush:
        await flush_batch(reason="size")


async def flush_batch(*, reason: str = "timer") -> None:
    """Send the current buffer to Meilisearch as a single add_documents call."""
    async with batch_lock:
        if not pending_docs:
            return
        batch = pending_docs[:]
        pending_docs.clear()
    # Outside the lock — don't block other queue_doc calls during HTTP.
    started = time.perf_counter()
    try:
        # meilisearch-py is sync. Run it in a thread so we don't block the
        # event loop during the HTTP call.
        await asyncio.to_thread(_meili.index(INDEX).add_documents, batch)
    except Exception as e:  # noqa: BLE001
        log.exception("batch flush failed (size=%d): %s", len(batch), e)
        return
    elapsed_ms = (time.perf_counter() - started) * 1000
    batch_history.append((time.time(), len(batch), elapsed_ms))
    await trace("batch_indexed", {
        "size": len(batch),
        "elapsed_ms": round(elapsed_ms, 1),
        "reason": reason,
    })


async def periodic_flusher() -> None:
    """Flush the buffer on a timer even if it hasn't hit MAX_BATCH_SIZE,
    so docs don't sit unindexed during quiet periods."""
    interval = MAX_BATCH_AGE_MS / 1000
    while True:
        await asyncio.sleep(interval)
        try:
            await flush_batch(reason="timer")
        except Exception as e:  # noqa: BLE001
            log.exception("periodic flush failed: %s", e)


# --- Consumer loop --------------------------------------------------------
class AuthorsRewinder(ConsumerRebalanceListener):
    """On every partition assignment, seek the `cdc.demo.authors` partitions
    back to offset 0 so the consumer re-streams all author events and
    rebuilds the in-memory `authors_cache` from scratch."""

    def __init__(self) -> None:
        self.consumer: AIOKafkaConsumer | None = None
        self._done = False

    def bind(self, consumer: AIOKafkaConsumer) -> None:
        self.consumer = consumer

    async def on_partitions_revoked(self, revoked) -> None:  # noqa: ARG002, ANN001
        return

    async def on_partitions_assigned(self, assigned) -> None:  # noqa: ANN001
        if self._done or self.consumer is None:
            return
        authors_tps = [tp for tp in assigned if tp.topic == "cdc.demo.authors"]
        if not authors_tps:
            return
        await self.consumer.seek_to_beginning(*authors_tps)
        log.info("rewound %d authors partition(s) to offset 0 for cache bootstrap",
                 len(authors_tps))
        self._done = True


async def consume(meili: MeiliClient) -> None:
    global _meili, _trace_http
    _meili = meili
    consumer = AIOKafkaConsumer(
        bootstrap_servers=KAFKA,
        group_id="search-consumer",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
        key_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
    )
    rewinder = AuthorsRewinder()
    consumer.subscribe(topics=TOPICS, listener=rewinder)
    rewinder.bind(consumer)
    await consumer.start()
    log.info("kafka consumer started, topics=%s", TOPICS)
    async with httpx.AsyncClient() as http:
        _trace_http = http
        flusher = asyncio.create_task(periodic_flusher())
        traces = asyncio.create_task(trace_flusher())
        try:
            async for msg in consumer:
                await handle(msg.topic, msg.value)
        finally:
            flusher.cancel()
            traces.cancel()
            # Drain the last batches on shutdown so we don't lose data.
            try:
                await flush_batch(reason="shutdown")
                await flush_traces()
            except Exception:  # noqa: BLE001
                pass
            await consumer.stop()
            _trace_http = None


async def handle(topic: str, value: dict | None) -> None:
    """Process one Debezium event."""
    op = (value or {}).get("__op", "?")
    table = (value or {}).get("__source_table") or topic.split(".")[-1]
    deleted = str((value or {}).get("__deleted", "false")).lower() == "true"

    await trace("kafka_received", {
        "topic": topic,
        "op": op,
        "deleted": deleted,
        "id": (value or {}).get("id"),
        "title": (value or {}).get("title"),
    }, correlation_id=(value or {}).get("id"))

    if value is None or deleted:
        if table == "articles" and value is not None and _meili is not None:
            # Deletes still go direct; they're rare in this demo and don't
            # belong on the batch path with adds.
            try:
                await asyncio.to_thread(_meili.index(INDEX).delete_document, value["id"])
                await trace("indexed", {"action": "delete", "id": value["id"]}, correlation_id=value["id"])
            except Exception as e:  # noqa: BLE001
                log.warning("meili delete failed: %s", e)
        return

    if table == "authors":
        authors_cache[int(value["id"])] = value["name"]
        for aid, doc in list(pending_articles.items()):
            if doc["author_id"] == int(value["id"]):
                doc["author"] = value["name"]
                await queue_doc(doc)
                pending_articles.pop(aid, None)
        return

    if table == "articles":
        author_id = int(value["author_id"])
        author_name = authors_cache.get(author_id)
        doc = {
            "id": int(value["id"]),
            "author_id": author_id,
            "author": author_name or f"author#{author_id}",
            "title": value["title"],
            "body": value["body"],
            "kind": value.get("kind", "article"),
            "published_at": value.get("published_at"),
        }
        if author_name is None:
            pending_articles[doc["id"]] = doc
            await trace("deferred", {"id": doc["id"], "reason": "author_unknown"}, correlation_id=doc["id"])
            return
        await queue_doc(doc)


# --- HTTP front -----------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.meili = setup_meili()
    app.state.task = asyncio.create_task(consume(app.state.meili))
    yield
    app.state.task.cancel()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health() -> dict:
    return {"ok": True}


@app.get("/search")
async def search(q: str) -> dict:
    if not q.strip():
        return {"hits": []}
    res = app.state.meili.index(INDEX).search(q, {"limit": 20})
    await trace("search_query", {"q": q, "n_hits": len(res.get("hits", []))})
    return {"hits": res.get("hits", []), "query": q, "processing_ms": res.get("processingTimeMs")}


def _batch_metrics() -> dict:
    """Roll up batch_history into UI-friendly numbers over a 5s window."""
    now = time.time()
    recent = [(t, s, ms) for t, s, ms in batch_history if now - t <= 5]
    last = batch_history[-1] if batch_history else None
    n = len(recent)
    docs = sum(s for _, s, _ in recent)
    return {
        "pending_docs": len(pending_docs),
        "last_batch_size": last[1] if last else 0,
        "last_batch_ms": round(last[2], 1) if last else 0.0,
        "batches_per_sec": round(n / 5, 1) if n else 0.0,
        "docs_indexed_per_sec": round(docs / 5, 1) if docs else 0.0,
        "avg_batch_size": round(docs / n, 1) if n else 0.0,
        "max_batch_size": MAX_BATCH_SIZE,
        "max_batch_age_ms": MAX_BATCH_AGE_MS,
    }


@app.get("/stats")
async def stats() -> dict:
    headers = {"Authorization": f"Bearer {MEILI_KEY}"}
    async with httpx.AsyncClient(timeout=2) as http:
        s = await http.get(f"{MEILI_URL}/indexes/{INDEX}/stats", headers=headers)
        t = await http.get(
            f"{MEILI_URL}/tasks",
            params={"statuses": "enqueued,processing", "limit": 0, "indexUids": INDEX},
            headers=headers,
        )
    s_json = s.json() if s.status_code == 200 else {}
    t_json = t.json() if t.status_code == 200 else {}
    return {
        "is_indexing": bool(s_json.get("isIndexing", False)),
        "documents_count": int(s_json.get("numberOfDocuments", 0)),
        "pending_tasks": int(t_json.get("total", 0)),
        **_batch_metrics(),
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
