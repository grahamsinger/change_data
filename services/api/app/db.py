"""Synchronous MySQL access with a connection pool.

Connections used to be opened per-request; on the publish hot path that
~5–15ms TCP+auth handshake dominated the actual ~1–2ms INSERT cost.
We now keep a small pool of long-lived connections and hand them out via
get_conn() — borrows are O(1), and the handshake only happens when the
pool is empty.

The pool is `queue.Queue`, which is thread-safe — important because
FastAPI's sync routes and `asyncio.to_thread` callers borrow from
multiple threads."""
from __future__ import annotations

import os
import queue
import time
from contextlib import contextmanager

import pymysql
from pymysql.cursors import DictCursor

MYSQL = dict(
    host=os.environ.get("MYSQL_HOST", "mysql"),
    port=int(os.environ.get("MYSQL_PORT", "3306")),
    user=os.environ.get("MYSQL_USER", "demo"),
    password=os.environ.get("MYSQL_PASSWORD", "demopw"),
    database=os.environ.get("MYSQL_DB", "demo"),
    cursorclass=DictCursor,
    autocommit=True,
)

POOL_SIZE = int(os.environ.get("MYSQL_POOL_SIZE", "20"))
_pool: queue.Queue = queue.Queue(maxsize=POOL_SIZE)


def wait_for_mysql(max_seconds: int = 60) -> None:
    deadline = time.time() + max_seconds
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            conn = pymysql.connect(**MYSQL)
            conn.close()
            return
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(1)
    raise RuntimeError(f"MySQL not reachable after {max_seconds}s: {last_err}")


def _new_conn() -> pymysql.Connection:
    return pymysql.connect(**MYSQL)


@contextmanager
def get_conn():
    """Borrow a pooled connection. If the pool is empty we open a fresh one
    on demand so we never block the publish path; the connection still
    returns to the pool on release (up to POOL_SIZE)."""
    try:
        conn = _pool.get_nowait()
    except queue.Empty:
        conn = _new_conn()
    healthy = True
    try:
        yield conn
    except Exception:
        # If the caller errored we can't trust the connection state.
        healthy = False
        raise
    finally:
        if healthy:
            try:
                _pool.put_nowait(conn)
            except queue.Full:
                conn.close()
        else:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
