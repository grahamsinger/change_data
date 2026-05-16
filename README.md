# Change Data Capture demo

A working CDC pipeline you can poke at: MySQL → Debezium → Kafka → two
independent consumers (a Meilisearch full-text indexer and a Kafka
Streams-style analytics service), with a React UI that lets you publish
articles, run a load simulator, and watch lag drain across each stage in
real time.

The whole thing runs from `docker compose`. The point is to see the
mechanics of CDC up close — binlog rows turning into Kafka topics, two
downstream services rebuilding their state from those topics on demand,
catch-up after a service restart, and what each layer's bottleneck
actually is.

## Quick start

```bash
./scripts/start.sh   # bring everything up + register Debezium connector
./scripts/stop.sh    # stop containers (preserve data)
```

To wipe all state and start over:

```bash
docker compose down -v && ./scripts/start.sh
```

UI: <http://localhost:5173>

## Architecture

```
                                                    ┌──────────────────┐
                                                    │ search-consumer  │
                                                    │  + Meilisearch   │
                                                    │  /search?q=...   │
                                                    └────────▲─────────┘
                                                             │ Kafka
 ┌──────────┐   binlog    ┌──────────┐   produces   ┌────────┴────────┐
 │  MySQL   │ ──────────► │ Debezium │ ────────────►│      Kafka      │
 │  demo db │  (ROW+GTID) │  (Kafka  │              │  cdc.demo.*     │
 └────▲─────┘             │  Connect)│              │  cdc.analytics-state
      │ INSERT/UPDATE     └──────────┘              └────────┬────────┘
      │ (pooled, batched)                                    │
 ┌────┴─────────┐                                   ┌────────▼─────────┐
 │   api        │                                   │ analytics-       │
 │ FastAPI      │                                   │ consumer         │
 │ + simulator  │                                   │ (changelog +     │
 │ (queue+flush)│                                   │  in-memory state)│
 └────▲─────────┘                                   └──────────────────┘
      │ HTTP
 ┌────┴─────┐
 │ frontend │
 │ React UI │
 └──────────┘
```

## Services

| Service             | Port  | Role                                                       |
| ------------------- | ----- | ---------------------------------------------------------- |
| frontend            | 5173  | React UI (Simulator, Publish, Search, Analytics, Articles) |
| api                 | 8000  | FastAPI: article CRUD, simulator control, lag/stats proxies |
| search-consumer     | 8001  | Reads CDC topics, batches writes to Meilisearch            |
| analytics-consumer  | 8002  | Reads CDC topics, maintains compacted changelog state      |
| meilisearch         | 7700  | Full-text search engine                                    |
| connect             | 8083  | Kafka Connect running the Debezium MySQL connector         |
| kafka               | 9092  | Single-broker Kafka                                        |
| zookeeper           | 2181  | Coordination for Kafka                                     |
| mysql               | 3306  | Source of truth (`demo` / `demopw`)                        |

## What's in the UI

Two columns: load generation/inspection on the left, downstream
consumers on the right.

**Simulator** — workers × interval sliders set a target publish rate. The
panel shows actual rate vs target (green/amber/pink), queue depth, last
batch size/ms, batches/sec, and dropped count. The slider math
(`workers / interval`) gives the *target*; what gets achieved depends on
MySQL throughput downstream.

**Publish** — manual single-publish form. Goes through the synchronous
single-row INSERT path so the inserted id comes back immediately.

**Recent articles (from MySQL)** — last 25 rows from MySQL.

**Full-text search (Meilisearch)** — search box, plus:
- `lag` pill (= `mysql_articles_count - documents_count`)
- `Indexed: X / Y (Z%)` progress bar
- Stage breakdown: Debezium (MySQL → Kafka), Consumer (Kafka → consumer
  buffer), Meilisearch (waiting in Meili's task queue)
- Batching info: pending docs, last batch size/ms, batches/sec, docs/sec
  shipped to Meili

The pill and breakdown are derived from a single `/search/stats`
snapshot, so they're guaranteed to agree at any moment.

**Analytics** — totals (articles, unique authors, kinds, top authors),
rate over the last 60s/5m, batching info (events/sec processed, changelog
writes/sec), and the changelog vs source topic sizes (showing how much
compaction has reclaimed).

## How CDC works here

1. **MySQL** is configured with `binlog_format=ROW` and GTID enabled
   (`mysql/conf.d/binlog.cnf`). Every committed row change is appended to
   the binlog as a structured event.
2. **Debezium** runs as a Kafka Connect source connector. It reads MySQL's
   binlog from a tracked GTID position and produces one Kafka topic per
   table: `cdc.demo.authors`, `cdc.demo.articles`. The `unwrap` SMT
   flattens the Debezium envelope so consumers see the row directly with
   `__op`, `__deleted`, etc. metadata fields.
3. **Consumers** subscribe to those topics independently with their own
   consumer groups. Each tracks its own committed offset, so they can
   restart and resume without coordination, or be reset to offset 0 to
   replay from history.

## Key patterns implemented

These are the things worth understanding if you're cribbing from this
repo:

### 1. Compacted changelog for analytics state

Analytics state (per-author counters, kind counters, etc.) lives in
memory but every mutation is also written to a *compacted* Kafka topic
`cdc.analytics-state`. On restart, the consumer replays that changelog
instead of the full source CDC topic — restart time is proportional to
the number of unique keys (a few hundred) rather than the total event
count (millions).

`min.cleanable.dirty.ratio=0.01` and `segment.ms=20000` make compaction
visible within seconds for the demo. In production you'd let it tick
along normally.

### 2. AuthorsRewinder for in-memory cache rebuild

The search consumer keeps an `author_id → name` cache so it can
denormalize articles before sending to Meilisearch. On every partition
assignment, a `ConsumerRebalanceListener` seeks the `cdc.demo.authors`
partition back to offset 0 — the authors topic is small enough that
re-reading it is cheap, and it guarantees the cache is fully populated
before processing articles.

### 3. Multi-stage batching (the most important pattern)

Per-event sync I/O on a hot path is almost always the bottleneck. The
fix is the same pattern at every layer:

- **Search → Meilisearch**: buffer documents, flush on size (250) or age
  (100ms), one HTTP call per batch instead of per doc.
- **Analytics state writes**: `producer.send()` (fire-and-forget) +
  `linger_ms=20` + `enable_idempotence=True`. Multiple changelog writes
  to the same key collapse into one broker request.
- **Trace events** (consumers → api `/events/log`): same pattern —
  enqueue + periodic flush via `/events/log-batch`.
- **Simulator → MySQL**: workers `put_nowait` items on an `asyncio.Queue`;
  a single flusher drains every 50ms and issues *one* multi-row
  `INSERT ... VALUES (...), (...), ...` per batch.

### 4. MySQL connection pool

`pymysql` opens a fresh TCP connection per call (5–15ms handshake). On
the publish path that handshake dominated the actual ~1–2ms INSERT. A
20-conn `queue.Queue` pool reuses long-lived connections — borrows are
O(1).

### 5. Pre-generated publication pool

The simulator was originally calling `wonderwords.RandomSentence` per
publish on the asyncio event loop. With many workers, that CPU work
serialized through the loop and capped throughput around 30/sec. Fix:
generate 500 publications once at api startup (in `to_thread`, in the
background) and have workers pick from the pool. Workers do near-zero
CPU work on the hot path; the simulator becomes a real load generator.

### 6. Unified snapshot for the search panel

`/search/stats` does MySQL count + search-consumer stats + Kafka offset
queries *concurrently*, then derives both the topline lag and the
per-stage breakdown from those numbers. Previously the pill and the
stage bar were two separate endpoints polled at different intervals; at
high indexing rates they'd disagree by hundreds of docs.

### 7. Sample-based throughput meter

Don't compute rates by maintaining a per-event timestamp deque — the
deque silently caps the displayed rate at `maxlen / window_seconds`. The
analytics consumer instead snapshots the running totals once per second
and computes the rate as the slope between the oldest and newest
samples in the buffer. Accurate at any throughput.

### 8. Catch-up = `indexed / total`, not "lag recovered from peak"

Earlier the catch-up bar tracked the highest lag observed since page
load. If you opened the UI mid-catchup, the "peak" was just the current
lag, so progress always read 0%. Replaced with the simpler
`documents_count / mysql_articles_count` — always meaningful, regardless
of when you start watching.

## Common operations

**Stop and resume preserving state**:

```bash
./scripts/stop.sh
./scripts/start.sh
```

State (committed offsets, MySQL data, Meilisearch index, analytics
changelog) survives restart, so the consumers pick up where they left off.

**Wipe everything and start fresh**:

```bash
docker compose down -v && ./scripts/start.sh
```

The `start.sh` script will detect the missing connector and re-register
it.

**Replay a single consumer from offset 0**:

```bash
./scripts/replay.sh search       # wipes Meili index, resets search-consumer offsets
./scripts/replay.sh analytics    # wipes changelog, resets analytics-consumer offsets
./scripts/replay.sh both
```

What `replay.sh` actually does:
1. Stop the container
2. Wait for the consumer group to go inactive in Kafka
3. Reset its offsets to earliest on the source topics
4. Wipe the consumer's downstream state (Meili index for search; changelog
   topic for analytics)
5. Restart the container — it bootstraps from scratch

**Watch raw CDC events**:

```bash
docker exec -it cdc-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cdc.demo.articles \
  --from-beginning \
  --property print.key=true
```

A typical INSERT after the `unwrap` SMT looks like:

```json
{
  "id": 42,
  "author_id": 3,
  "title": "Notes on Bread, Patience, and Salt",
  "body": "Good bread is mostly waiting...",
  "kind": "article",
  "published_at": 1715520000000,
  "__op": "c",
  "__source_ts_ms": 1715520000123,
  "__source_db": "demo",
  "__source_table": "articles",
  "__deleted": "false"
}
```

`__op`: `c` create, `u` update, `d` delete, `r` snapshot read.

**Inspect consumer-group lag directly**:

```bash
docker exec cdc-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group analytics-consumer
```

## File map

```
docker-compose.yml             # full stack: mysql, kafka, connect, meili,
                               #   api, search-consumer, analytics-consumer,
                               #   frontend
mysql/
  init.sql                     # authors + articles schema, debezium user
  conf.d/binlog.cnf            # ROW format, GTID, server-id

debezium/
  mysql-connector.json         # connector config + ExtractNewRecordState SMT
  register-connector.sh        # POST the config to Kafka Connect

services/api/
  app/main.py                  # FastAPI: CRUD, simulator control, proxies,
                               #   /search/stats (unified snapshot)
  app/db.py                    # pooled pymysql connections (POOL_SIZE=20)
  app/simulator.py             # queue + flusher + pre-generated pool
  app/content.py               # wonderwords-backed random publication
  app/events.py                # WebSocket trace hub (server-side batched)

services/search_consumer/
  app/main.py                  # CDC → Meilisearch with multi-stage batching:
                               #   AuthorsRewinder, batched docs, batched traces

services/analytics_consumer/
  app/main.py                  # CDC → in-memory state + compacted changelog:
                               #   pipelined producer.send, batched traces,
                               #   sample-based throughput

frontend/src/
  App.tsx                      # 2-column layout
  components/Search.tsx        # IndexingBadge, CatchUpProgress (Indexed/Total),
                               #   LagBreakdownBar, BatchingInfo
  components/Analytics.tsx     # totals, BatchingInfo, topic-sizes table
  components/SimToggle.tsx     # workers/interval sliders + ActualRate panel
  components/Publish.tsx       # manual single publish
  components/Articles.tsx      # recent rows from MySQL

scripts/
  start.sh                     # docker compose up + ensure connector
  stop.sh                      # docker compose stop (volumes preserved)
  replay.sh                    # full-replay one consumer from offset 0
```

`CHANGE_LOGS.md` covers exactly where each durable log lives (MySQL
binlog file paths, Kafka segment locations) and how to inspect them.

## Configuration knobs worth knowing

| Knob                              | Where                              | Why                                              |
| --------------------------------- | ---------------------------------- | ------------------------------------------------ |
| `POOL_SIZE` (sim publication pool)| `services/api/app/simulator.py`    | 500 generates in ~1s of CPU; bigger ≠ better     |
| `FLUSH_INTERVAL_MS` (sim queue)   | `services/api/app/simulator.py`    | Trade between batch size and end-to-end latency  |
| `QUEUE_MAXSIZE` (sim queue)       | `services/api/app/simulator.py`    | Backpressure threshold; over this we drop        |
| `MAX_BATCH_SIZE` (search → meili) | `services/search_consumer/app/main.py` | 250 keeps the demo visible; 2000+ is faster at scale |
| `MAX_BATCH_AGE_MS`                | same                               | Latency upper bound during quiet periods         |
| `linger_ms` (analytics producer)  | `services/analytics_consumer/app/main.py` | Batch window for changelog writes         |
| `MYSQL_POOL_SIZE`                 | `services/api/app/db.py` (env)     | Cap on pooled connections                        |
| `min.cleanable.dirty.ratio`       | analytics consumer changelog topic | How aggressively compaction runs (0.01 = always) |

## Bugs & lessons from building this

| Symptom                                          | Root cause                                                                 | Fix                                                            |
| ------------------------------------------------ | -------------------------------------------------------------------------- | -------------------------------------------------------------- |
| Search lag pill and stage bar disagreed by 100s  | Two separate endpoints polled independently; each re-queried Meili         | Folded breakdown into `/search/stats` (one snapshot)           |
| Catch-up bar always at 0% mid-catchup            | Tracked "lag recovered from peak", peak was just current lag at page load  | Switched to `indexed / total` percentage                       |
| Analytics replay was way slower than search      | Per-event `await producer.send_and_wait()` + per-event trace HTTP POST     | Pipelined `send()` + linger_ms + idempotence; batched traces   |
| `events/sec` capped at 4000 in UI                | Per-event timestamp `deque(maxlen=20000)` overflowed in <5s                | Sample running counters once per second; rate = slope          |
| Articles deferred forever after restart          | In-memory authors_cache lost; committed offset on authors topic was past   | AuthorsRewinder: seek authors partition to 0 on every assign  |
| Analytics showed `author#N` placeholders         | Same in-memory cache + committed offset bug                                | Compacted changelog with full state restore on boot            |
| Simulator stuck at ~30 publishes/sec at any rate | `wonderwords.sentence()` blocked the asyncio event loop                    | Pre-generate 500-item pool at startup, recycle                 |
| Publish path did 5–15ms TCP handshake per insert | `pymysql.connect()` per call                                               | 20-conn pool, borrows are O(1)                                 |
| Bulk insert took 2.3s for 88 rows                | Same event-loop starvation; the to_thread couldn't schedule its result    | Same fix as above (pool the work off the loop)                 |
| `replay.sh` appeared to hang                     | Awk parsed wrong column when consumer group was inactive                   | grep for known state names instead of column-index parsing     |
| `consumer.seek_to_beginning()` was a no-op       | Coroutine wasn't awaited (silent runtime warning only)                     | Add `await`                                                    |
| Click Start → 30s timeout                        | Race: every concurrent click started its own pool generation               | `asyncio.Lock` + pre-generate at startup                       |
| api hung for ~30s after first Start              | Pool gen ran on the asyncio loop synchronously                             | `await asyncio.to_thread(...)`                                 |

## Open improvement opportunities

If you keep working on this, the highest-value remaining wins:

1. **Bigger Meilisearch batches** during catch-up. `MAX_BATCH_SIZE` is
   small (250) for visibility — at large volumes that creates thousands
   of tiny Meili tasks. A 5-line bump to 2000+ approximately halves
   Meili-side replay time.
2. **Bootstrap-mode batched state writes for analytics**. During a fresh
   replay, instead of writing every per-event mutation to the changelog
   (12M writes for 3M articles), accumulate in memory and emit only the
   final compacted state at the end (~140 writes). Minutes → seconds.
3. **Pipelined Meilisearch sends** — keep N batches in flight via
   `asyncio.Semaphore` instead of awaiting each one before the next.
4. **More Kafka partitions** for `cdc.demo.articles` so multiple consumer
   instances can split the load horizontally.
5. **Auto-pause the (now removed) EventFeed** when lag is high — the
   trace endpoints `/events/log`, `/events/log-batch`, `/ws/events` still
   exist on the api but no UI consumer right now. Could be brought back
   if useful.
6. **Surface analytics-consumer catch-up state in the UI** the same way
   search has it (currently you have to use `kafka-consumer-groups
   --describe` to see it).

## Schema

```sql
CREATE TABLE authors (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  name        VARCHAR(120) NOT NULL UNIQUE,
  created_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE articles (
  id            INT AUTO_INCREMENT PRIMARY KEY,
  author_id     INT          NOT NULL,
  title         VARCHAR(255) NOT NULL,
  body          TEXT         NOT NULL,
  kind          ENUM('poem','article') NOT NULL DEFAULT 'article',
  published_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (author_id) REFERENCES authors(id) ON DELETE CASCADE
);
```

The bulk publish path uses `INSERT IGNORE` for authors then
`SELECT id FROM authors WHERE name IN (...)` to get back ids — a
two-roundtrip approach that's still vastly faster than per-row inserts.

## Endpoints summary

| Endpoint                                | Returns                                               |
| --------------------------------------- | ----------------------------------------------------- |
| `GET  /search/stats`                    | Unified search panel snapshot (lag + stage breakdown) |
| `GET  /lag-breakdown`                   | Same data, kept for admin/debug                       |
| `GET  /analytics`                       | Totals + batch metrics (proxied from analytics-consumer) |
| `GET  /analytics/topic-sizes`           | Source vs changelog sizes (compaction visibility)     |
| `GET  /simulator`                       | Sim status + actual/target rate, queue depth, etc.    |
| `POST /simulator`                       | Toggle / reconfigure                                  |
| `GET  /articles?limit=N`                | Recent rows from MySQL                                |
| `POST /articles`                        | Manual single publish                                 |
| `POST /events/log` / `/events/log-batch`| Trace events from consumers (no UI consumer right now) |
| `GET  /ws/events`                       | WebSocket for trace stream (no UI consumer right now)  |
