"""Background workload generator that publishes fake articles.

Two-stage architecture:

  worker tasks  ──put──>  asyncio.Queue  ──drain──>  flusher task  ──>  bulk INSERT

The N worker tasks each generate publications at a configurable interval
and `put_nowait()` them on a shared queue — they do no MySQL work. A single
flusher task drains the queue every FLUSH_INTERVAL_MS and issues one bulk
INSERT per batch via `_publish_bulk`.

Why the indirection? The previous architecture had each worker do its own
INSERT inline, which meant every publish paid a MySQL handshake (now fixed
by the connection pool) and one row at a time over the wire. With queue +
flusher, a 22-worker / 4400-target-rate config produces ~220 rows per 50ms
batch — one network roundtrip and one transaction per batch instead of 220.

The slider semantics are preserved: target_rate ≈ workers / interval.
What changes is whether that target rate is *achievable*."""
from __future__ import annotations

import asyncio
import logging
import random
import time
from collections import deque
from typing import Any, Awaitable, Callable

from .content import random_publication

log = logging.getLogger("simulator")

# Size of the precomputed publication pool. wonderwords sentence generation
# is CPU-heavy; doing it inline on the asyncio event loop bottlenecked the
# workers themselves. Pre-generating once at startup and recycling lets the
# simulator actually act as a load generator instead of a CPU benchmark.
# 500 is plenty for varied search results and generates in ~1s; 5000 was
# making Start take 30+s on first click.
POOL_SIZE = 500

# Single-publish path (used by /articles POST so manual publishes still feel
# synchronous and return the inserted id).
SinglePublishFn = Callable[..., Awaitable[dict]]
# Bulk-publish path (used by the flusher). Returns row count inserted.
BulkPublishFn = Callable[[list[tuple[str, str, str, str]]], Awaitable[int]]
# Trace event sink — the api hub.publish; the flusher emits one summary
# event per batch so the EventFeed isn't flooded with per-article noise.
TraceFn = Callable[[dict[str, Any]], Awaitable[None]]

FLUSH_INTERVAL_MS = 50
QUEUE_MAXSIZE = 50_000


class Simulator:
    MAX_WORKERS = 50

    def __init__(self) -> None:
        self._enabled = False
        self._interval = 3.0
        self._workers = 1
        self._tasks: list[asyncio.Task] = []
        self._flusher_task: asyncio.Task | None = None
        self._publish: SinglePublishFn | None = None
        self._publish_bulk: BulkPublishFn | None = None
        self._trace: TraceFn | None = None
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
        # Precomputed pool — populated lazily on first start so we don't pay
        # the generation cost when the api boots without the simulator.
        self._pool: list[tuple[str, str, str, str]] = []
        # Guards set_enabled so concurrent clicks don't kick off multiple
        # parallel pool generations (which thrash the threadpool and stall
        # the api for tens of seconds).
        self._enable_lock = asyncio.Lock()

        # Throughput tracking. We sample the running insert total once a
        # second and compute slope, the same approach we use in the
        # analytics consumer — accurate at any rate, no maxlen cap.
        self._inserts_total = 0
        self._batches_total = 0
        self._dropped_total = 0  # queue-full backpressure events
        self._last_batch_size = 0
        self._last_batch_ms = 0.0
        self._samples: deque[tuple[float, int, int]] = deque(maxlen=10)
        self._sampler_task: asyncio.Task | None = None

    @property
    def status(self) -> dict:
        rate = 0.0
        batches_per_sec = 0.0
        if len(self._samples) >= 2:
            first = self._samples[0]
            last = self._samples[-1]
            dt = max(0.001, last[0] - first[0])
            rate = (last[1] - first[1]) / dt
            batches_per_sec = (last[2] - first[2]) / dt
        target = self._workers / self._interval if self._interval > 0 else 0
        return {
            "enabled": self._enabled,
            "interval_seconds": self._interval,
            "workers": self._workers,
            # Diagnostics (UI can compare actual vs target):
            "target_rate_per_sec": round(target, 1),
            "actual_rate_per_sec": round(rate, 1),
            "batches_per_sec": round(batches_per_sec, 2),
            "queue_depth": self._queue.qsize(),
            "queue_maxsize": QUEUE_MAXSIZE,
            "last_batch_size": self._last_batch_size,
            "last_batch_ms": round(self._last_batch_ms, 1),
            "inserts_total": self._inserts_total,
            "batches_total": self._batches_total,
            "dropped_total": self._dropped_total,
            "flush_interval_ms": FLUSH_INTERVAL_MS,
        }

    def configure(self, publish: SinglePublishFn, publish_bulk: BulkPublishFn, trace: TraceFn) -> None:
        self._publish = publish
        self._publish_bulk = publish_bulk
        self._trace = trace
        # Build the publication pool eagerly in the background so the user's
        # first Start click doesn't have to wait for wonderwords. By the
        # time anyone hits Start, the pool is almost certainly ready.
        if not self._pool:
            asyncio.create_task(self._prebuild_pool())

    async def _prebuild_pool(self) -> None:
        async with self._enable_lock:
            if self._pool:
                return
            log.info("pre-generating publication pool (size=%d) in background…", POOL_SIZE)
            t0 = time.perf_counter()
            self._pool = await asyncio.to_thread(
                lambda: [random_publication() for _ in range(POOL_SIZE)]
            )
            log.info("pool ready in %.1fs", time.perf_counter() - t0)

    async def set_enabled(self, enabled: bool) -> None:
        async with self._enable_lock:
            if enabled == self._enabled:
                return
            if enabled and not self._pool:
                # Generate in a thread so we don't freeze the asyncio loop
                # for the ~5–10s wonderwords takes. Other api requests keep
                # flowing. The lock above prevents concurrent gens.
                log.info("pre-generating publication pool (size=%d)…", POOL_SIZE)
                t0 = time.perf_counter()
                self._pool = await asyncio.to_thread(
                    lambda: [random_publication() for _ in range(POOL_SIZE)]
                )
                log.info("pool ready in %.1fs", time.perf_counter() - t0)
            self._enabled = enabled
            if enabled:
                self._spawn_workers()
                self._ensure_flusher()
                self._ensure_sampler()
            else:
                for t in self._tasks:
                    t.cancel()
                self._tasks = []
                if self._flusher_task is not None:
                    self._flusher_task.cancel()
                    self._flusher_task = None
                if self._sampler_task is not None:
                    self._sampler_task.cancel()
                    self._sampler_task = None

    def set_interval(self, seconds: float) -> None:
        self._interval = max(0.001, seconds)

    def set_workers(self, n: int) -> None:
        n = max(1, min(self.MAX_WORKERS, int(n)))
        if n == self._workers:
            return
        self._workers = n
        if self._enabled:
            self._spawn_workers()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _spawn_workers(self) -> None:
        self._tasks = [t for t in self._tasks if not t.done()]
        while len(self._tasks) > self._workers:
            t = self._tasks.pop()
            t.cancel()
        while len(self._tasks) < self._workers:
            self._tasks.append(asyncio.create_task(self._worker_loop(len(self._tasks))))

    def _ensure_flusher(self) -> None:
        if self._flusher_task is None or self._flusher_task.done():
            self._flusher_task = asyncio.create_task(self._flusher_loop())

    def _ensure_sampler(self) -> None:
        if self._sampler_task is None or self._sampler_task.done():
            self._sampler_task = asyncio.create_task(self._sampler_loop())

    async def _worker_loop(self, worker_id: int) -> None:
        log.info("simulator worker %d started", worker_id)
        try:
            while self._enabled:
                # O(1) — just an index into the precomputed pool.
                item = self._pool[random.randrange(len(self._pool))]
                try:
                    self._queue.put_nowait(item)
                except asyncio.QueueFull:
                    # Backpressure: flusher can't keep up. Drop this
                    # publication rather than back up worker tasks (which
                    # would hide the real bottleneck downstream).
                    self._dropped_total += 1
                try:
                    await asyncio.sleep(self._interval)
                except asyncio.CancelledError:
                    return
        finally:
            log.info("simulator worker %d stopped", worker_id)

    async def _flusher_loop(self) -> None:
        log.info("simulator flusher started (every %dms)", FLUSH_INTERVAL_MS)
        interval = FLUSH_INTERVAL_MS / 1000
        try:
            while self._enabled:
                await asyncio.sleep(interval)
                await self._drain_and_insert()
        except asyncio.CancelledError:
            # Drain whatever is queued before exiting so we don't lose data.
            await self._drain_and_insert()
            raise
        finally:
            log.info("simulator flusher stopped")

    async def _drain_and_insert(self) -> None:
        if self._publish_bulk is None:
            return
        items: list[tuple[str, str, str, str]] = []
        while True:
            try:
                items.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        if not items:
            return
        started = time.perf_counter()
        try:
            n = await self._publish_bulk(items)
        except Exception as e:  # noqa: BLE001
            log.exception("bulk insert failed (size=%d): %s", len(items), e)
            return
        elapsed_ms = (time.perf_counter() - started) * 1000
        self._inserts_total += n
        self._batches_total += 1
        self._last_batch_size = n
        self._last_batch_ms = elapsed_ms
        if self._trace is not None:
            try:
                await self._trace({
                    "service": "simulator",
                    "type": "batch_published",
                    "payload": {
                        "rows": n,
                        "elapsed_ms": round(elapsed_ms, 1),
                    },
                })
            except Exception:  # noqa: BLE001
                pass

    async def _sampler_loop(self) -> None:
        try:
            while self._enabled:
                await asyncio.sleep(1)
                self._samples.append((time.time(), self._inserts_total, self._batches_total))
        except asyncio.CancelledError:
            return


simulator = Simulator()
