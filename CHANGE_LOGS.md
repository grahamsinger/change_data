# Where the change logs live

CDC keeps **two** durable change logs, in two different places. Both record
every row-level mutation, but they serve different roles.

```
   write happens here                replayed/consumed from here
        │                                       │
        ▼                                       ▼
 ┌──────────────────┐    Debezium reads   ┌─────────────────────┐
 │  MySQL binlog    │ ──────────────────► │  Kafka topic log    │
 │  (binary, on the │                     │  (one append-only   │
 │   MySQL server)  │                     │   log per topic)    │
 └──────────────────┘                     └─────────────────────┘
   short-ish retention,                    days–forever retention,
   for replication                         for downstream consumers
```

## 1. MySQL binary log (binlog)

The original record of every change. MySQL writes to this **before** any
external system sees the change — it exists so MySQL replicas can stay in
sync with the primary. Debezium just pretends to be a replica.

### Where it's stored

Inside the `cdc-mysql` container, in `/var/lib/mysql/`, as files named
`mysql-bin.000001`, `mysql-bin.000002`, etc. plus an index file `mysql-bin.index`.

The data directory is mounted on the named Docker volume `mysql-data`, so
the binlog survives `docker compose down` (but not `down -v`).

### Configuration

Set in [`mysql/conf.d/binlog.cnf`](mysql/conf.d/binlog.cnf):

| Setting | Value | Why |
| ------- | ----- | --- |
| `log_bin = mysql-bin`         | enables binlog     | required for any CDC |
| `binlog_format = ROW`         | row-level events   | Debezium needs row diffs, not SQL statements |
| `binlog_row_image = FULL`     | include all columns| so consumers see unchanged fields too |
| `expire_logs_days = 1`        | 1-day retention    | MySQL auto-deletes binlog files older than this |
| `gtid_mode = ON`              | global transaction IDs | clean failover/resume semantics |

### How to view it

```bash
# List the binlog files currently on disk
docker exec -it cdc-mysql ls -lh /var/lib/mysql | grep mysql-bin

# Decode one to human-readable form (shows every row change as SQL-like text)
docker exec -it cdc-mysql sh -c \
  "mysqlbinlog --base64-output=DECODE-ROWS --verbose /var/lib/mysql/mysql-bin.000001" | less

# See where Debezium currently is in the binlog stream
docker exec -it cdc-mysql mysql -uroot -prootpw -e "SHOW MASTER STATUS\G"
```

### Retention behavior

Files are deleted by MySQL when they pass `expire_logs_days`. **Once a file
is gone, Debezium cannot replay events from it.** This is why we don't rely
on the binlog as long-term history — that job goes to Kafka.

## 2. Kafka topic log

The replayable, long-retention version. Each Debezium event becomes a
message in a Kafka topic, and Kafka stores messages on disk as append-only
log segments. New downstream consumers can read from offset 0 to rebuild
their own state without touching MySQL.

### Where it's stored

Inside the `cdc-kafka` container, in `/var/lib/kafka/data/`, in directories
named `<topic>-<partition>`:

```
/var/lib/kafka/data/
├── cdc.demo.articles-0/      ← one partition of the articles topic
│   ├── 00000000000000000000.log         ← actual messages, append-only
│   ├── 00000000000000000000.index       ← offset → byte lookup
│   ├── 00000000000000000000.timeindex   ← timestamp → offset lookup
│   └── leader-epoch-checkpoint
├── cdc.demo.authors-0/
├── schema-changes.demo-0/    ← Debezium's DDL history (internal)
├── connect_configs-0/        ← Kafka Connect's own state topics
├── connect_offsets-0/
├── connect_statuses-0/
└── __consumer_offsets-<N>/   ← where each consumer group left off
```

The data directory is mounted on the named Docker volume `kafka-data`, so
**the full change log persists across `docker compose down/up`** (but not
across `down -v`).

### Configuration

Set in [`docker-compose.yml`](docker-compose.yml) on the `kafka` service:

| Setting | Value | Why |
| ------- | ----- | --- |
| `KAFKA_LOG_RETENTION_HOURS`          | `168` (7 days) | how long messages stay |
| `KAFKA_AUTO_CREATE_TOPICS_ENABLE`    | `true`         | Debezium creates topics on first produce |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` | `1`        | single-broker dev cluster |

Bump `KAFKA_LOG_RETENTION_HOURS` higher (or set it to `-1` for unlimited)
if you want to demo "spin up a brand-new consumer weeks later and replay
the whole history."

### How to view it

```bash
# Stream every message from the very start (Debezium events + new ones live)
docker exec -it cdc-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cdc.demo.articles \
  --from-beginning \
  --property print.key=true

# What --from-beginning does:
#   sets the consumer's starting offset to the earliest available in the
#   partition (offset 0 if no segments have been deleted), then reads
#   forward through every retained message before tailing for new ones.

# List every topic Kafka knows about
docker exec -it cdc-kafka kafka-topics \
  --bootstrap-server localhost:9092 --list

# Inspect a raw segment file (forensics view: shows offsets, timestamps,
# producer IDs, message size, etc.)
docker exec -it cdc-kafka kafka-dump-log \
  --files /var/lib/kafka/data/cdc.demo.articles-0/00000000000000000000.log \
  --print-data-log | less

# How many messages currently in each topic
docker exec -it cdc-kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 --topic cdc.demo.articles
```

### Retention behavior

Kafka deletes whole **log segments** (not individual messages) once every
message in that segment is older than the retention. A segment rolls over
when it hits ~1 GB or 7 days by default. Inside the retention window,
every message is replayable as many times as you want, by any number of
consumers.

## The relationship between the two

The flow for one INSERT:

1. App runs `INSERT INTO articles ...`
2. MySQL commits the row to InnoDB **and** appends a row-event to
   `mysql-bin.NNNN` on disk.
3. Debezium (running as a fake replica inside Kafka Connect) reads the new
   binlog event, transforms it into a JSON message, and produces to
   `cdc.demo.articles` in Kafka.
4. Kafka appends the message to
   `/var/lib/kafka/data/cdc.demo.articles-0/00000000000000000000.log`.
5. Both the search consumer and the analytics consumer read that message
   from Kafka and update their own state.

Debezium records its position in the binlog (file + offset, or GTID) in
the `schema-changes.demo` and `connect_offsets` Kafka topics. If it
crashes and restarts, it resumes from exactly where it left off — that's
why all three of `connect_*` topics are themselves stored on the Kafka
log.

## What gets lost when

| Command | MySQL data | Binlog | Kafka log | Meilisearch | Analytics state |
| ------- | ---------- | ------ | --------- | ----------- | --------------- |
| `docker compose restart`        | kept | kept | kept | kept | **lost** (in-memory) |
| `docker compose down`           | kept | kept | kept | kept | lost |
| `docker compose down -v`        | wiped | wiped | wiped | wiped | lost |

Analytics is the only piece that has no durable store of its own — it's
in-memory by design, to show how Kafka makes that safe: after wiping it,
restart and it rebuilds from the topic log via `auto_offset_reset=earliest`.

## Applying the Kafka volume

The `kafka-data` volume was added after the initial setup. To get the
benefit, recreate the kafka container:

```bash
docker compose up -d kafka
# Confirm the volume is mounted
docker inspect cdc-kafka --format '{{ range .Mounts }}{{ .Name }} → {{ .Destination }}{{"\n"}}{{ end }}'
```

> ⚠️ This recreate **wipes the current in-container Kafka data** because
> it never had a volume before. Easiest path: after recreating kafka, also
> re-register the Debezium connector (`./debezium/register-connector.sh`)
> so it does a fresh snapshot from MySQL. From then on, the Kafka log is
> persisted across container restarts.
