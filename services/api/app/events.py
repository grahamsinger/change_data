"""In-process WebSocket event hub.

Every service POSTs trace events to `/events/log`; this hub broadcasts
them to connected UI clients so the user can watch the CDC pipeline flow
in real time.

Events are batched on the server before being sent to clients — at high
throughput (thousands of events/sec) the overhead of one WebSocket frame
per event was the dominant cost on both server and browser. Now we
accumulate for `flush_interval_ms` (or until the batch fills) and ship a
single `{"batch": [...]}` frame.

Clients can still handle either form: a bare event or a `{batch: [...]}`."""
from __future__ import annotations

import asyncio
import time
from collections import deque
from typing import Any

from fastapi import WebSocket


class EventHub:
    def __init__(
        self,
        history: int = 200,
        flush_interval_ms: int = 50,
        max_batch: int = 500,
    ) -> None:
        self._clients: set[WebSocket] = set()
        self._history: deque[dict[str, Any]] = deque(maxlen=history)
        self._lock = asyncio.Lock()
        self._buffer: list[dict[str, Any]] = []
        self._flush_interval = flush_interval_ms / 1000
        self._max_batch = max_batch
        self._flush_task: asyncio.Task | None = None

    def ensure_started(self) -> None:
        """Idempotent — kicks off the background flusher on first call."""
        if self._flush_task is None or self._flush_task.done():
            self._flush_task = asyncio.create_task(self._flusher())

    async def _flusher(self) -> None:
        while True:
            await asyncio.sleep(self._flush_interval)
            if self._buffer:
                await self._flush_now()

    async def _flush_now(self) -> None:
        if not self._buffer:
            return
        batch = self._buffer
        self._buffer = []
        await self._broadcast({"batch": batch})

    async def _broadcast(self, msg: dict) -> None:
        dead: list[WebSocket] = []
        for ws in list(self._clients):
            try:
                await ws.send_json(msg)
            except Exception:  # noqa: BLE001
                dead.append(ws)
        if dead:
            async with self._lock:
                for ws in dead:
                    self._clients.discard(ws)

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        async with self._lock:
            self._clients.add(ws)
            backlog = list(self._history)
        # Ship the history as a single batch so the new client paints quickly.
        if backlog:
            try:
                await ws.send_json({"batch": backlog})
            except Exception:  # noqa: BLE001
                pass

    async def disconnect(self, ws: WebSocket) -> None:
        async with self._lock:
            self._clients.discard(ws)

    async def publish(self, event: dict[str, Any]) -> None:
        event.setdefault("ts", time.time())
        self._history.append(event)
        self._buffer.append(event)
        # If the buffer fills before the periodic flush, ship it immediately
        # so we don't grow without bound under sustained heavy load.
        if len(self._buffer) >= self._max_batch:
            await self._flush_now()


hub = EventHub()
