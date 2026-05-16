# Session resume notes

A working CDC (change-data-capture) demo and what's been built / decided / learned across the design session. A future agent should be able to read this and pick up cleanly.

## What this project is

A live demo of MySQL → Debezium → Kafka → downstream services (full-text search + analytics), with a React UI that surfaces every stage of the pipeline. The user's goal was to **see how CDC actually works** — not just a black-box pipeline but the raw events, the consumer mechanics, the failure modes, and the optimization patterns.

The project lives at `/Users/grahamsinger/dev/claude_code/change_data`.

## Architecture

```
                                                ┌──────────────────┐
                                                │ search-consumer  │
                                                │  FastAPI :8001   │
                                                │  ─► Meilisearch  │
                                                │  /search, /stats │
                                                └────────▲─────────┘
                                                         │ Kafka
 ┌──────────┐  binlog   ┌──────────┐   produces   ┌──────┴────────┐
 │  MySQL   │ ────────► │ Debezium │ ────────────►│     Kafka     │
 │  demo db │           │  (Kafka  │              │ cdc.demo.*    │
 │  :3306   │           │  Connect)│              │ cdc.analytics-│
 └────▲─────┘           │  :8083   │              │   state       │
      │ INSERTs         └──────────┘              └──────┬────────┘
      │                                                  │ Kafka
 ┌────┴─────┐                                  ┌─────────▼─────────┐
 │   api    │◄──── /events/log{,-batch} POSTs ─│ analytics-        │
 │ FastAPI  │      from each downstream svc    │ consumer :8002    │
 │  :8000   │                                  │ /stats, /admin/*  │
 │ + sim    │                                  └───────────────────┘
 │ + WS hub │
 └────▲─────┘
      │ WebSocket /ws/events (batched trace timeline)
 ┌────┴─────┐
 │ frontend │  Vite + React, http://localhost:5173
 └──────────┘
```

**Service inventory (`docker-compose.yml`):**

| Service | Port | Role |
| --- | --- | --- |
| `mysql` | 3306 | Source DB. Row-based binlog enabled via `mysql/conf.d/binlog.cnf`. Schema in `mysql/init.sql`. |
| `zookeeper` | — | Required by `confluentinc/cp-kafka:7.5.0`. |
| `kafka` | 9092 / 29092 | Single-broker, single-partition. Volume `kafka-data` persists topic log. 7-day retention. |
| `connect` | 8083 | Debezium 2.5. Stores its config + binlog position in Kafka topics (`connect_configs`, `connect_offsets`, `connect_statuses`). |
| `meilisearch` | 7700 | FTS. Volume `meili-data` persists index. Master key from `.env`. |
| `api` | 8000 | FastAPI. Article CRUD, simulator, event hub, lag breakdown computation. |
| `search-consumer` | 8001 | Kafka → Meilisearch indexer. Batches both trace events and `add_documents` calls. |
| `analytics-consumer` | 8002 | Kafka → in-memory counters, backed by a compacted Kafka changelog topic. |
| `frontend` | 5173 | Vite dev server, React UI. |

## Key files and their purpose

```
change_data/
├── docker-compose.yml         # Full stack, volumes, env vars
├── .env                       # MYSQL_*, MEILI_MASTER_KEY, KAFKA_BOOTSTRAP
├── README.md                  # User-facing run instructions
├── CHANGE_LOGS.md             # Where binlog vs Kafka topic log live, and why
├── RESUME.md                  # ← this file
│
├── mysql/
│   ├── init.sql               # Schema: authors, articles. Creates `debezium` user.
│   └── conf.d/binlog.cnf      # log_bin=mysql-bin, ROW format, GTID on, 1-day expire
│
├── debezium/
│   ├── mysql-connector.json   # Connector config. Uses ExtractNewRecordState SMT
│   │                          #   so events have flat fields + __op, __deleted, etc.
│   │                          #   Drop the `transforms.*` block to see raw envelope.
│   └── register-connector.sh  # Idempotent: deletes existing + reposts JSON.
│
├── services/
│   ├── api/app/
│   │   ├── main.py            # Routes, simulator, event hub, lag breakdown
│   │   ├── events.py          # WebSocket EventHub with server-side batching (50ms)
│   │   ├── simulator.py       # Multi-worker simulator with shared interval
│   │   ├── content.py         # Weighted author pool + wonderwords article gen
│   │   └── db.py              # pymysql sync, get_conn() context manager
│   │
│   ├── search_consumer/app/
│   │   └── main.py            # Trace batching, doc batching, AuthorsRewinder
│   │
│   └── analytics_consumer/app/
│       └── main.py            # Compacted changelog topic, FreshBootstrapRewinder
│
├── frontend/src/
│   ├── App.tsx
│   ├── api.ts                 # Types: SearchStats, LagBreakdown, SimStatus, etc.
│   └── components/
│       ├── Publish.tsx
│       ├── SimToggle.tsx      # Workers + interval sliders (snap-to-tenth)
│       ├── Articles.tsx
│       ├── Search.tsx         # Contains IndexingBadge, LagBreakdownBar,
│       │                      #   BatchingInfo, CatchUpProgress
│       ├── Analytics.tsx      # Stats + top-authors bars + topic-sizes table
│       └── EventFeed.tsx      # Batched WebSocket frames, memoized rows,
│                              #   pre-cached payload strings
│
└── scripts/
    ├── start.sh               # docker compose up -d + connector check
    ├── stop.sh                # docker compose stop (preserves volumes)
    └── replay.sh              # "from-beginning" rebuild for search/analytics/both
```

## Patterns implemented (the educational core of this project)

### 1. Compacted changelog topic for analytics state (Kafka Streams pattern)

`services/analytics_consumer/app/main.py`. Every state mutation writes to the **compacted** `cdc.analytics-state` topic, keyed by:

```
total                  -> int     (one entry per topic, heavy compaction)
kind:<kind>            -> int     (2 entries total, heavy compaction)
author_name:<id>       -> str     (~hundreds of entries, no duplicates)
author_count:<id>      -> int     (~hundreds of entries, heavy compaction)
article_author:<id>    -> int     (one entry per article ever — UNIQUE key, no
                                   compaction benefit; doubles as exactly-once
                                   de-dupe marker)
```

On startup, `restore_from_changelog()` replays the compacted topic to rebuild state in memory. If the changelog is empty (`fresh_deploy=True`), the `FreshBootstrapRewinder` listener seeks the source topics to offset 0 instead.

The "compaction win" is visible in `/admin/topic-sizes`: thousands of state writes survive as a few hundred records.

### 2. AuthorsRewinder pattern (in-memory cache rebuild on restart)

`services/search_consumer/app/main.py`. The search-consumer keeps `authors_cache: dict[int, str]` in RAM but Meilisearch holds the indexed docs. On restart:

- Indexed docs survive in Meilisearch's volume.
- `authors_cache` is empty.
- The consumer's committed offset on `cdc.demo.articles` resumes correctly.
- But author events for actively-publishing authors are **past** the committed offset of `cdc.demo.authors` and won't be re-read.

Fix: `AuthorsRewinder` is a `ConsumerRebalanceListener` that **awaits** `seek_to_beginning` on authors partitions during the very first partition assignment. The articles topic resumes from committed offset; only authors gets re-read (cheap, ~hundreds of events).

### 3. Multi-stage batching for high throughput

Three independent batching layers in the search-consumer (`services/search_consumer/app/main.py`):

| Buffer | Trigger | Destination |
| --- | --- | --- |
| `pending_docs` | size ≥ 250 docs **or** every 100ms | Meilisearch `add_documents([batch])` |
| `pending_traces` | size ≥ 500 events **or** every 100ms | api `/events/log-batch` |
| EventHub `_buffer` (server-side, in api) | size ≥ 500 events **or** every 50ms | WebSocket clients as `{batch: [...]}` |

This turned ~1000 events/sec into a few large HTTP calls per second, lowering both server CPU and browser render cost.

**Why batch size is 250 (not 2000):** the user wanted to *see* progress visually. Smaller batches → more frequent `documentsCount` ticks → "indexing in progress" feel.

### 4. Drive-by author generation

`services/api/app/content.py`. Authors are weighted: 5 core contributors (weights 6–9), 4 regulars (weight 3), 5 occasionals (weight 1). On top of that, **~15% of publishes use a randomly-generated "drive-by" name** (random first + last from small pools) who publishes 1–5 times then never again. Net effect: leaderboard is stable, but `unique_authors` keeps climbing.

### 5. Multi-worker simulator

`services/api/app/simulator.py`. Each "worker" is an independent async loop with its own `asyncio.sleep(interval)`. Total publish rate ≈ `workers / interval`. Slider in UI ranges 1–50 workers, 0.005–10s interval (snap-to-tenth above 0.1s). Capable of ~10K/sec at peak.

### 6. Lag breakdown computation

`/lag-breakdown` in `services/api/app/main.py` joins four numbers to compute per-stage lag:

```
mysql_count           = SELECT COUNT(*) FROM articles
kafka_end_offset      = end offset of cdc.demo.articles
committed_offset     = search-consumer's committed offset on cdc.demo.articles
meili_docs_count      = Meilisearch numberOfDocuments

debezium_lag = mysql_count - kafka_end_offset       (MySQL → Kafka stage)
consumer_lag = kafka_end_offset - committed_offset  (Kafka → consumer read)
meili_lag    = committed_offset - meili_docs_count  (consumer → Meilisearch index)
total_lag    = mysql_count - meili_docs_count
             = debezium_lag + consumer_lag + meili_lag
```

The UI renders this as a stacked horizontal bar (amber/purple/pink). Kafka offsets come from `analytics-consumer`'s `/admin/lag-breakdown` (since it already has aiokafka admin client setup).

### 7. Catch-up progress bar

`CatchUpProgress` in `frontend/src/components/Search.tsx`. Tracks the high-water mark of lag across an episode (anytime lag > 0). Bar fills as the high-water shrinks back to 0. Resets 2s after lag stays at 0. Shows ETA based on `docs_indexed_per_sec`.

## Bugs we found and lessons learned

These came up during the session and are worth knowing about (some are documented inline in the code with `# Why this is needed:` comments):

| Bug | Lesson |
| --- | --- |
| Articles deferred to `pending_articles` forever after restart | In-memory state alongside CDC consumers needs a bootstrap strategy. See [pattern 2](#2-authorsrewinder-pattern-in-memory-cache-rebuild-on-restart). |
| `consumer.seek_to_beginning()` not awaited → silent no-op | aiokafka APIs are async; missing await = coroutine garbage-collected. The warning "coroutine was never awaited" is your friend. |
| `kafka-consumer-groups --reset-offsets` failed with "group is Stable" | Resetting offsets requires the group to be inactive (`Empty` or `Dead`). Stop all consumers and wait ~12s before resetting. |
| `consumer.partitions_for_topic(t)` returns None right after `start()` | Metadata for arbitrary topics isn't fetched eagerly. Use `await consumer.topics()` to force a metadata refresh, or directly `assign([TopicPartition(t, 0)])`. |
| Lag staying constant when simulator rate is halved | Adaptive batching at every stage (Debezium polls, Kafka `getmany`, Meilisearch task batching) → effective throughput scales with input. Little's Law in action. |
| Awk parsing the wrong column from `--state` output | `ASSIGNMENT-STRATEGY` is blank when group is inactive, shifting columns. Use `grep -E "(Empty|Dead)"` instead of `awk '$N'`. |
| Trace `kafka_received` was shipping `value["body"]` (KBs/sec) | Don't include large fields in trace payloads. At 1000+/sec it floods the WebSocket and tanks the browser. |
| Search ~5× slower than analytics initially | Every CDC event triggered a synchronous HTTP POST for tracing — the observability layer was the bottleneck. **Fix: batch the traces too.** Same pattern as Meilisearch batching. |
| Compaction reclaims `total`/`kind`/`author_count` but not `article_author` | Counter-style keys with shared keys compact dramatically; identity-style keys with unique per-entity values do not. Both have their place. |

## Common commands

```bash
# Lifecycle
./scripts/start.sh                  # bring everything up, register connector if needed
./scripts/stop.sh                   # halt, keep volumes
docker compose down -v              # nuclear: wipe ALL data

# Replay (simulate a brand-new downstream consumer)
./scripts/replay.sh search          # wipe Meili index + reset offsets, rebuild
./scripts/replay.sh analytics       # delete changelog topic + reset offsets, rebuild
./scripts/replay.sh both

# Inspect Kafka directly
docker exec -it cdc-kafka kafka-topics --bootstrap-server localhost:9092 --list
docker exec -it cdc-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cdc.demo.articles --from-beginning --property print.key=true
docker exec cdc-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --describe --group search-consumer

# Inspect Meilisearch
curl -H "Authorization: Bearer devkey" http://localhost:7700/indexes/articles/stats
curl -H "Authorization: Bearer devkey" "http://localhost:7700/tasks?statuses=enqueued,processing"

# Force rebuild a service after code changes
docker compose up -d --build <service-name>
```

## Endpoints summary

| URL | Purpose |
| --- | --- |
| http://localhost:5173 | UI |
| http://localhost:8000/docs | api FastAPI docs |
| http://localhost:8000/lag-breakdown | computed per-stage lag |
| http://localhost:8000/search/stats | search consumer stats + lag + batching |
| http://localhost:8000/analytics | analytics aggregates |
| http://localhost:8000/analytics/topic-sizes | source vs changelog topic sizes |
| http://localhost:8001/stats | search consumer raw stats (proxied above) |
| http://localhost:8001/search?q=... | direct search |
| http://localhost:8002/stats | analytics raw stats (proxied above) |
| http://localhost:8002/admin/topic-sizes | raw topic offsets + compacted counts |
| http://localhost:8002/admin/lag-breakdown | Kafka offsets for lag computation |
| http://localhost:8083/connectors | Kafka Connect (Debezium) |
| http://localhost:7700 | Meilisearch |

## Open improvement opportunities (suggested by the user)

These were discussed but not implemented. Ranked roughly by impact:

1. **Apply trace + state batching to analytics-consumer.** Currently each article event triggers 1 trace POST + 4 `producer.send_and_wait()` calls = 5+ network roundtrips. Same patterns as search-consumer would bring its throughput up to par. Likely 30 lines of code.

2. **Pipeline in-flight Meilisearch batches** in `search_consumer/main.py`. Currently `flush_batch` is sequential — we await Meilisearch's ack before sending the next. An `asyncio.Semaphore(N)` would allow N concurrent flushes, overlapping Meilisearch processing with batch-building. Could 2–3× throughput before hitting Meilisearch's internal indexing ceiling.

3. **Async HTTP client for Meilisearch.** Currently using sync `meilisearch` package wrapped in `asyncio.to_thread`. Direct `httpx.AsyncClient` calls to Meilisearch's REST API would skip the thread-pool overhead. Maybe 10–15% improvement.

4. **More Kafka partitions on `cdc.demo.articles`.** Currently 1 partition → 1 consumer can read at a time. Going to 4 partitions + 4 consumer instances would parallelize reads. Real production scaling pattern, but requires recreating the topic and updating the Debezium connector config.

5. **EMA-smoothed throughput display.** The `docs_indexed_per_sec` value can jitter due to bursty batching. A simple exponentially-weighted moving average would steady the displayed number.

6. **Sparkline of recent throughput** in the Search panel. Visual rate-over-time chart instead of just the current number.

## Important configuration knobs

| File | Setting | Current | Why |
| --- | --- | --- | --- |
| `services/search_consumer/app/main.py` | `MAX_BATCH_SIZE` | 250 | Visible progress; bumped down from 2000 |
| `services/search_consumer/app/main.py` | `MAX_BATCH_AGE_MS` | 100 | Timer flush cadence |
| `services/search_consumer/app/main.py` | `TRACE_BATCH_SIZE` | 500 | Trace flush threshold |
| `services/search_consumer/app/main.py` | `TRACE_BATCH_AGE_MS` | 100 | Trace timer cadence |
| `services/api/app/events.py` | `flush_interval_ms` | 50 | Server-side WS batch cadence |
| `services/api/app/events.py` | `max_batch` | 500 | Server-side WS size flush |
| `services/api/app/simulator.py` | `MAX_WORKERS` | 50 | Hard cap on parallel publishers |
| `services/analytics_consumer/app/main.py` | `recent_ts maxlen` | 20000 | Rate window (5min @ 66/sec sustained) |
| `docker-compose.yml` | `KAFKA_LOG_RETENTION_HOURS` | 168 | Topic retention; raise for longer "replay history" demos |
| `mysql/conf.d/binlog.cnf` | `expire_logs_days` | 1 | Binlog retention on MySQL side |

## Things to verify before making large changes

- **Code lives in `services/<svc>/app/`** and is **COPYed** into the image at build time (no bind mount in prod-style compose). Always rebuild the relevant service after editing:
  ```bash
  docker compose up -d --build <service>
  ```
- **Frontend has the same issue** — Vite dev server runs inside the container against COPYed source. Edits on the host don't hot-reload unless you bind-mount `./frontend/src:/app/src` (see "Optional dev-mode tweak" in the README).
- **Schema changes** to MySQL require either nuking the volume (`docker compose down -v` then re-up) or running migrations manually. The `mysql/init.sql` only runs on first creation of the data volume.
- **Connector config changes** in `debezium/mysql-connector.json` aren't applied automatically — re-run `./debezium/register-connector.sh` to repost (it deletes then recreates).
- **The user prefers `uv` for Python operations** (from their global CLAUDE.md). Inside Docker we use `uv pip install --system` in the Dockerfiles.

## Recent session state

Last successfully verified state:
- All containers up via `./scripts/start.sh`
- Simulator works at 50 workers × 0.005s = ~10K/sec target
- All three batching layers in place (traces, docs, WebSocket)
- Lag breakdown bar shows correct per-stage values
- Catch-up progress bar appears during episodes (e.g. after `replay.sh search`)
- Compacted changelog reaches ~30 records after compaction settles vs source's 8K+ events
