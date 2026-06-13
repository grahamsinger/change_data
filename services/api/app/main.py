"""FastAPI service: article CRUD, event hub, search/analytics proxy, simulator."""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .db import get_conn, wait_for_mysql
from .events import hub
from .simulator import simulator

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("api")

SEARCH_URL = os.environ.get("SEARCH_SERVICE_URL", "http://search-consumer:8001")
ANALYTICS_URL = os.environ.get("ANALYTICS_SERVICE_URL", "http://analytics-consumer:8002")

app = FastAPI(title="CDC Demo API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------
class PublishRequest(BaseModel):
    author: str = Field(..., min_length=1, max_length=120)
    title: str = Field(..., min_length=1, max_length=255)
    body: str = Field(..., min_length=1)
    kind: str = Field("article", pattern="^(poem|article)$")


class SimConfig(BaseModel):
    enabled: bool | None = None
    interval_seconds: float | None = Field(None, ge=0.001, le=60)
    workers: int | None = Field(None, ge=1, le=50)


class TraceEvent(BaseModel):
    service: str
    type: str
    payload: dict[str, Any] = {}
    correlation_id: str | int | None = None


# ---------------------------------------------------------------------------
# Core publish path (used by both the HTTP route and the simulator)
# ---------------------------------------------------------------------------
async def _publish(author: str, title: str, body: str, kind: str, *, source: str) -> dict:
    await hub.publish({
        "service": "api",
        "type": "publish_request",
        "payload": {"author": author, "title": title, "kind": kind, "source": source},
    })

    # Upsert author, insert article. Both INSERTs will surface as CDC events.
    def _do_insert() -> dict:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO authors (name) VALUES (%s) "
                    "ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)",
                    (author,),
                )
                author_id = cur.lastrowid
                cur.execute(
                    "INSERT INTO articles (author_id, title, body, kind) VALUES (%s, %s, %s, %s)",
                    (author_id, title, body, kind),
                )
                article_id = cur.lastrowid
                return {"author_id": author_id, "article_id": article_id}

    ids = await asyncio.to_thread(_do_insert)

    await hub.publish({
        "service": "mysql",
        "type": "row_inserted",
        "payload": {
            "table": "articles",
            "id": ids["article_id"],
            "author_id": ids["author_id"],
            "title": title,
        },
        "correlation_id": ids["article_id"],
    })
    return ids


async def _publish_bulk(items: list[tuple[str, str, str, str]]) -> int:
    """Bulk publish path used by the simulator. One pooled connection,
    one multi-row author upsert, one multi-row article INSERT — instead
    of len(items) handshakes plus 2*len(items) round trips. Skips
    per-item trace events; the flusher emits one summary instead."""
    if not items:
        return 0

    def _do_bulk_insert() -> int:
        unique_authors = list({a for a, _, _, _ in items})
        with get_conn() as conn, conn.cursor() as cur:
            # Multi-row author upsert. INSERT IGNORE skips dup-key conflicts;
            # we then SELECT all needed ids in one shot.
            placeholders = ",".join(["(%s)"] * len(unique_authors))
            cur.execute(
                f"INSERT IGNORE INTO authors (name) VALUES {placeholders}",
                unique_authors,
            )
            in_clause = ",".join(["%s"] * len(unique_authors))
            cur.execute(
                f"SELECT id, name FROM authors WHERE name IN ({in_clause})",
                unique_authors,
            )
            author_id = {r["name"]: r["id"] for r in cur.fetchall()}

            # Multi-row article INSERT.
            row_placeholders = ",".join(["(%s, %s, %s, %s)"] * len(items))
            flat: list = []
            for a, t, b, k in items:
                flat.extend([author_id[a], t, b, k])
            cur.execute(
                "INSERT INTO articles (author_id, title, body, kind) "
                f"VALUES {row_placeholders}",
                flat,
            )
        return len(items)

    return await asyncio.to_thread(_do_bulk_insert)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.on_event("startup")
async def on_startup() -> None:
    wait_for_mysql()
    simulator.configure(_publish, _publish_bulk, hub.publish)
    hub.ensure_started()
    log.info("API ready")


@app.get("/health")
def health() -> dict:
    return {"ok": True}


@app.post("/articles")
async def publish_article(req: PublishRequest) -> dict:
    return await _publish(req.author, req.title, req.body, req.kind, source="user")


@app.get("/articles")
def list_articles(limit: int = 25) -> list[dict]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.id, a.title, a.body, a.kind, a.published_at, au.name AS author
            FROM articles a JOIN authors au ON a.author_id = au.id
            ORDER BY a.id DESC LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    for r in rows:
        r["published_at"] = r["published_at"].isoformat()
    return rows


@app.get("/articles/{article_id}")
def get_article(article_id: int) -> dict:
    """Full article incl. body, by id. Search hits omit the body to stay
    lean (Meilisearch's displayedAttributes excludes it), so the UI fetches
    the full text from MySQL — the source of truth — on demand when a result
    is opened."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.id, a.title, a.body, a.kind, a.published_at, au.name AS author
            FROM articles a JOIN authors au ON a.author_id = au.id
            WHERE a.id = %s
            """,
            (article_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise HTTPException(404, f"article {article_id} not found")
    row["published_at"] = row["published_at"].isoformat()
    return row


# --- Simulator control ----------------------------------------------------
@app.get("/simulator")
async def get_sim() -> dict:
    return simulator.status


@app.post("/simulator")
async def set_sim(cfg: SimConfig) -> dict:
    if cfg.interval_seconds is not None:
        simulator.set_interval(cfg.interval_seconds)
    if cfg.workers is not None:
        simulator.set_workers(cfg.workers)
    if cfg.enabled is not None:
        await simulator.set_enabled(cfg.enabled)
    return simulator.status


# --- Event hub ------------------------------------------------------------
@app.post("/events/log")
async def log_event(ev: TraceEvent) -> dict:
    await hub.publish(ev.model_dump())
    return {"ok": True}


@app.post("/events/log-batch")
async def log_events_batch(events: list[TraceEvent]) -> dict:
    """Batched variant of /events/log. Used by services that emit many
    traces per second — saves an HTTP roundtrip per event."""
    for ev in events:
        await hub.publish(ev.model_dump())
    return {"ok": True, "count": len(events)}


@app.websocket("/ws/events")
async def ws_events(ws: WebSocket) -> None:
    await hub.connect(ws)
    try:
        while True:
            # We don't expect inbound messages; just keep the connection open.
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        await hub.disconnect(ws)


# --- Proxies to downstream services (so the UI only talks to one host) ----
@app.get("/search")
async def proxy_search(q: str) -> Any:
    async with httpx.AsyncClient(timeout=5) as client:
        r = await client.get(f"{SEARCH_URL}/search", params={"q": q})
        if r.status_code >= 400:
            raise HTTPException(r.status_code, r.text)
        return r.json()


@app.get("/analytics")
async def proxy_analytics() -> Any:
    """Proxy the analytics consumer's /stats, enriched with the MySQL
    source-of-truth article count. That lets the UI show rebuild progress
    (total_articles / mysql_count) — the analytics analog of the search
    panel's indexed-fraction bar. Mirrors how /search/stats is enriched."""

    def _mysql_count() -> int:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS c FROM articles")
            return int(cur.fetchone()["c"])

    mysql_count_task = asyncio.to_thread(_mysql_count)
    async with httpx.AsyncClient(timeout=5) as client:
        r = await client.get(f"{ANALYTICS_URL}/stats")
        if r.status_code >= 400:
            raise HTTPException(r.status_code, r.text)
        stats = r.json()
    try:
        stats["mysql_count"] = await mysql_count_task
    except Exception:
        # Don't fail the whole panel if the source count is briefly unavailable;
        # the UI hides the progress bar when mysql_count is missing/0.
        stats.setdefault("mysql_count", 0)
    return stats


@app.get("/lag-breakdown")
async def proxy_lag_breakdown() -> Any:
    """Per-stage lag for cdc.demo.articles.

    Pipeline stages we measure:
      mysql_count        : SELECT COUNT(*) from MySQL (source of truth)
      kafka_end_offset   : Debezium-emitted event count on cdc.demo.articles
      committed_offset   : how far search-consumer has read
      meili_docs_count   : documents currently indexed in Meilisearch

    Lag components (each non-negative):
      debezium_lag       = mysql_count - kafka_end_offset
      consumer_lag       = kafka_end_offset - committed_offset
      meili_lag          = committed_offset - meili_docs_count
      total_lag          = mysql_count - meili_docs_count
                         = debezium_lag + consumer_lag + meili_lag
    """
    def _mysql_count() -> int:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS c FROM articles")
            return int(cur.fetchone()["c"])

    mysql_count_task = asyncio.to_thread(_mysql_count)

    async with httpx.AsyncClient(timeout=5) as client:
        async def _kafka() -> dict:
            try:
                r = await client.get(f"{ANALYTICS_URL}/admin/lag-breakdown")
                return r.json() if r.status_code < 400 else {}
            except Exception:  # noqa: BLE001
                return {}

        async def _search() -> dict:
            try:
                r = await client.get(f"{SEARCH_URL}/stats")
                return r.json() if r.status_code < 400 else {}
            except Exception:  # noqa: BLE001
                return {}

        mysql_count, kafka_info, search_info = await asyncio.gather(
            mysql_count_task, _kafka(), _search()
        )

    kafka_end = int(kafka_info.get("kafka_end_offset", 0))
    committed = int(kafka_info.get("search_consumer_committed_offset", 0))
    meili_docs = int(search_info.get("documents_count", 0))

    # Clamp each component to ≥0 to avoid scary negative numbers when
    # measurements are slightly out of order (e.g. Meilisearch finished
    # indexing between our two HTTP calls).
    debezium_lag = max(0, mysql_count - kafka_end)
    consumer_lag = max(0, kafka_end - committed)
    meili_lag = max(0, committed - meili_docs)
    total_lag = max(0, mysql_count - meili_docs)

    return {
        "mysql_count": mysql_count,
        "kafka_end_offset": kafka_end,
        "consumer_committed_offset": committed,
        "meili_docs_count": meili_docs,
        "debezium_lag": debezium_lag,
        "consumer_lag": consumer_lag,
        "meili_lag": meili_lag,
        "total_lag": total_lag,
    }


@app.get("/analytics/topic-sizes")
async def proxy_analytics_topic_sizes() -> Any:
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(f"{ANALYTICS_URL}/admin/topic-sizes")
        if r.status_code >= 400:
            raise HTTPException(r.status_code, r.text)
        return r.json()


@app.get("/search/stats")
async def proxy_search_stats() -> Any:
    """Fetch search-consumer state, MySQL truth, and Kafka offsets in a
    single concurrent snapshot, then derive the topline lag *and* the
    per-stage breakdown from that one snapshot.

    Why one endpoint instead of two: the previous /search/stats and
    /lag-breakdown polled independently every 250ms / 800ms, each
    re-querying MySQL and Meilisearch at slightly different moments. With
    a fast-moving Meili count, the topline pill and the stage bar would
    routinely disagree by hundreds of docs. Folding the breakdown into
    /search/stats guarantees they're internally consistent."""
    def _count() -> int:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS c FROM articles")
            return int(cur.fetchone()["c"])

    mysql_count_task = asyncio.to_thread(_count)

    async with httpx.AsyncClient(timeout=5) as client:
        async def _search() -> tuple[dict, bool]:
            try:
                r = await client.get(f"{SEARCH_URL}/stats")
                return (r.json() if r.status_code < 400 else {}, r.status_code < 400)
            except Exception:  # noqa: BLE001
                return ({}, False)

        async def _kafka() -> dict:
            try:
                r = await client.get(f"{ANALYTICS_URL}/admin/lag-breakdown")
                return r.json() if r.status_code < 400 else {}
            except Exception:  # noqa: BLE001
                return {}

        mysql_count, (search_stats, search_reachable), kafka_info = await asyncio.gather(
            mysql_count_task, _search(), _kafka(),
        )

    meili_count = int(search_stats.get("documents_count", 0))
    kafka_end = int(kafka_info.get("kafka_end_offset", 0))
    committed = int(kafka_info.get("search_consumer_committed_offset", 0))

    debezium_lag = max(0, mysql_count - kafka_end)
    consumer_lag = max(0, kafka_end - committed)
    meili_lag = max(0, committed - meili_count)
    total_lag = max(0, mysql_count - meili_count)

    passthrough_keys = (
        "pending_docs", "last_batch_size", "last_batch_ms",
        "batches_per_sec", "docs_indexed_per_sec", "avg_batch_size",
        "max_batch_size", "max_batch_age_ms",
    )
    return {
        "is_indexing": bool(search_stats.get("is_indexing", False)),
        "documents_count": meili_count,
        "pending_tasks": int(search_stats.get("pending_tasks", 0)),
        "mysql_articles_count": mysql_count,
        "lag": total_lag,
        "search_reachable": search_reachable,
        # Per-stage breakdown derived from the same snapshot:
        "kafka_end_offset": kafka_end,
        "consumer_committed_offset": committed,
        "debezium_lag": debezium_lag,
        "consumer_lag": consumer_lag,
        "meili_lag": meili_lag,
        "total_lag": total_lag,
        **{k: search_stats.get(k) for k in passthrough_keys if k in search_stats},
    }
