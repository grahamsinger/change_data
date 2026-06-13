# Re-snapshotting from MySQL (full rebuild from source of truth)

## When you need this

`scripts/replay.sh` rewinds a consumer to offset 0 and replays the **Kafka
topic**. That only works while the topic still holds the full history. Our
source topic `cdc.demo.articles` has `cleanup.policy=delete` with
`retention.ms=604800000` (7 days), so once records age past the window Kafka
deletes those segments. After that, "rewind to 0" lands on the *earliest
surviving* offset — not true zero — and a replay can only rebuild from the
recent tail.

That's exactly what bit us: the topic's earliest offset had advanced to
~3.21M, so only ~60k records remained, and `replay.sh both` rebuilt to 60k
instead of the full 3.27M (it looked "stuck" but was actually caught up to all
*available* data).

A **re-snapshot** sidesteps the trimmed log entirely: it tells Debezium to
forget it ever snapshotted, then re-read **every row from MySQL** (the real
source of truth — `SELECT COUNT(*)` still showed all 3,272,279 rows) and
re-stream them into fresh Kafka topics. Downstream consumers rebuild from
those.

Prerequisites in this stack (both satisfied):
- Kafka **Connect 3.6+** — for the connector offsets REST API.
- **Debezium 2.5+** with `snapshot.mode: initial` in `debezium/mysql-connector.json`.

---

## The procedure (exact steps run)

All commands assume the compose stack is up. `MK` is the Meilisearch key:

```bash
MK=$(grep -E '^MEILI_MASTER_KEY=' .env | cut -d= -f2); MK=${MK:-devkey}
CONN=mysql-demo-connector
```

**0. Stop the simulator** (UI toggle or API). Keep it off for the whole
rebuild so `mysql_count` — the dashboard's denominator — stays fixed at 3.27M
instead of drifting while you watch progress.

**1. Stop the Debezium connector.** A connector must be `STOPPED` (not just
paused) before its offsets can be reset.

```bash
curl -sS -X PUT http://localhost:8083/connectors/$CONN/stop
# poll until state == STOPPED:
curl -sS http://localhost:8083/connectors/$CONN/status
```

**2. Stop the consumers** so they release the topics and we can delete +
reset them without a live reader fighting us.

```bash
docker compose stop search-consumer analytics-consumer
```

**3. Reset Debezium's stored offset** — this is what makes it snapshot again.
`snapshot.mode: initial` only snapshots when *no* offset exists; Debezium's
binlog/snapshot position lives in Connect's internal `connect-offsets` topic,
so deleting+re-adding the connector alone would NOT re-snapshot. The REST call
clears it:

```bash
curl -sS -X DELETE http://localhost:8083/connectors/$CONN/offsets
# verify: should return {"offsets":[]}
curl -sS http://localhost:8083/connectors/$CONN/offsets
```

**4. Wipe the data topics + downstream state** so everything rebuilds from
offset 0. (Why delete the source topics instead of letting the snapshot append
to them? See "Why reset the topic" below — it's load-bearing for both the
dashboard visual and correctness.)

```bash
# source topics (Debezium recreates them; broker auto.create.topics.enable=true)
docker exec cdc-kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic cdc.demo.articles
docker exec cdc-kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic cdc.demo.authors
# analytics changelog so the analytics consumer bootstraps fresh
docker exec cdc-kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic cdc.analytics-state
# Meilisearch index so meili_docs starts at 0
curl -sS -X DELETE "http://localhost:7700/indexes/articles" -H "Authorization: Bearer $MK"
```

> Do **NOT** delete `schema-changes.demo` — that's Debezium's schema history.
> Deleting the data topics also drops the consumer groups' committed offsets
> (you may see `GroupIdNotFoundException` when trying to delete the groups
> explicitly — that's fine, it means they're already gone). With
> `auto_offset_reset="earliest"` on both consumers, they start from 0 on
> restart regardless.

**5. Resume the connector** → with offsets gone and `snapshot.mode: initial`,
Debezium performs a full snapshot of `demo.authors` then `demo.articles`,
emitting one `op=r` ("read") event per row into the freshly recreated topics.

```bash
curl -sS -X PUT http://localhost:8083/connectors/$CONN/resume
# watch the topic refill from 0:
docker exec cdc-kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 --topic cdc.demo.articles --time -1
```

**6. Restart the consumers** so they reindex live as snapshot records land.

```bash
docker compose start search-consumer analytics-consumer
```

That's it. The snapshot of 3.27M rows completes in seconds (a single streaming
`SELECT`); the consumers are then the long pole, draining the backlog at tens
of thousands of records/sec.

---

## How it works (why each piece matters)

- **`snapshot.mode: initial` + offset reset = re-snapshot.** Debezium decides
  whether to snapshot by checking for a stored offset. Reset it and it behaves
  like a brand-new connector: snapshot every table, *then* resume binlog
  streaming from the current position. We never touch MySQL — it already holds
  the full truth.

- **Stop → reset offsets → resume**, not delete/recreate the connector. On
  Connect 3.6+, `DELETE /connectors/{name}/offsets` is the supported way to
  reset a connector's position, and it requires the `STOPPED` lifecycle state.
  This keeps the same connector name and config.

- **Why reset the topic (delete, don't append).** The dashboard computes
  `debezium_lag = mysql_count - kafka_end_offset`, where `kafka_end_offset` is
  the high-water mark of `cdc.demo.articles`. If the snapshot *appended* to the
  existing topic, `kafka_end` would climb from 3.27M toward 6.5M, overshoot
  `mysql_count`, and `debezium_lag` (clamped at `max(0, …)`) would read 0 the
  whole time — no visible progress — while analytics double-counted old + new
  rows. Deleting the topic first makes `kafka_end` grow cleanly 0 → 3.27M, so
  the lag bar drains and counts stay correct.

- **Keep the schema history topic.** `schema-changes.demo` lets Debezium
  interpret column layouts. The snapshot re-reads/append schema there; wiping it
  is unnecessary since the schema hasn't changed.

---

## Will I see progress in the dashboard?

**Yes — as lag bars draining, not a dedicated "snapshot %" widget.** The
`/lag-breakdown` endpoint decomposes the pipeline into three stages, and a
re-snapshot lights them up in sequence:

```
debezium_lag = mysql_count        - kafka_end_offset    # snapshot writing to Kafka
consumer_lag = kafka_end_offset   - committed_offset     # consumer reading from Kafka
meili_lag    = committed_offset   - meili_docs_count      # Meilisearch indexing
total_lag    = mysql_count        - meili_docs_count      # = sum of the three
```

What you'll watch, in order:

1. **`debezium_lag` drains 3.27M → 0** as the snapshot emits to the empty
   topic. Because `kafka_end_offset` *is* the snapshot's write cursor (with the
   topic reset to 0), this segment effectively *is* a snapshot progress bar.
   It's fast — often a few seconds.
2. **`consumer_lag` spikes then drains** as search/analytics replay the backlog
   from offset 0.
3. **`meili_lag` drains** as Meilisearch works through its async bulk-index
   queue (`meili_docs_count` steps up in batches rather than smoothly — that's
   the internal task queue, not a stall).
4. **Analytics panel** counters climb 0 → 3.27M (`total_articles`,
   `unique_authors`, per-kind), with live `events/s` showing the rebuild rate.

The one caveat baked into the procedure: this clean visual depends on **step 4
deleting the source topic first**. Skip it and the snapshot appends, the
`debezium_lag` bar pins at 0, and you'd only see movement downstream.

---

## Observed run (2026-06-13)

- Baseline: `mysql=3,272,279`, topic earlier trimmed to ~60k usable records.
- Snapshot filled `cdc.demo.articles` 0 → 3.27M in seconds (`debezium_lag → 0`).
- Consumers then drained: analytics climbing at ~32k events/s (coalesce ratio
  ~3.8), search reindexing at ~55k/s into Meilisearch.
