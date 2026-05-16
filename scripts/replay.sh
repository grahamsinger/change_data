#!/usr/bin/env bash
# Simulate a "from-beginning" load: wipe a consumer's durable state and
# reset its Kafka consumer-group offsets to 0, then let it rebuild from
# the source CDC topics by replaying everything.
#
# Usage:
#   scripts/replay.sh search       # wipe Meilisearch index, replay
#   scripts/replay.sh analytics    # wipe changelog topic, replay
#   scripts/replay.sh both         # both, in parallel pipelines

set -euo pipefail

SERVICE="${1:-}"
if [[ -z "$SERVICE" ]]; then
  echo "Usage: $0 <search|analytics|both>"
  exit 1
fi

# Read MEILI key from .env if present, fall back to default.
ENV_FILE="$(dirname "$0")/../.env"
MEILI_KEY="$(grep -E '^MEILI_MASTER_KEY=' "$ENV_FILE" 2>/dev/null | cut -d= -f2 || true)"
MEILI_KEY="${MEILI_KEY:-devkey}"

wait_for_group_empty() {
  # The --describe --state output has a variable-column-count problem: the
  # ASSIGNMENT-STRATEGY column is blank when the group is inactive, which
  # shifts STATE to a different field index. Don't parse by column — just
  # grep the known state names out of the line.
  local group="$1"
  echo -n "  waiting for consumer group '$group' to go inactive"
  for _ in $(seq 1 30); do
    local output
    output=$(docker exec cdc-kafka kafka-consumer-groups \
      --bootstrap-server localhost:9092 --describe --group "$group" --state 2>/dev/null || true)
    if echo "$output" | grep -qE "(Empty|Dead)"; then
      echo " ✓"
      return
    fi
    echo -n "."
    sleep 1
  done
  echo " (timed out, proceeding anyway)"
}

replay_search() {
  echo "=== Replaying search-consumer from beginning ==="
  echo "  stopping container..."
  docker compose stop search-consumer >/dev/null

  wait_for_group_empty search-consumer

  echo "  resetting Kafka offsets to earliest on cdc.demo.articles..."
  docker exec cdc-kafka kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --group search-consumer \
    --reset-offsets --to-earliest --all-topics --execute 2>&1 \
    | awk 'NR==1 || /cdc\.demo\./'

  echo "  wiping Meilisearch 'articles' index..."
  curl -fsS -X DELETE "http://localhost:7700/indexes/articles" \
    -H "Authorization: Bearer ${MEILI_KEY}" || true
  echo

  echo "  starting container — it will rebuild the index from the topic log."
  docker compose start search-consumer >/dev/null
  echo "  done. Watch the lag-breakdown bar in the UI: consumer-lag (purple) will spike, then drain into meili-lag (pink) as it indexes."
}

replay_analytics() {
  echo "=== Replaying analytics-consumer from beginning ==="
  echo "  stopping container..."
  docker compose stop analytics-consumer >/dev/null

  wait_for_group_empty analytics-consumer

  echo "  resetting Kafka offsets to earliest on source topics..."
  docker exec cdc-kafka kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --group analytics-consumer \
    --reset-offsets --to-earliest --all-topics --execute 2>&1 \
    | awk 'NR==1 || /cdc\.demo\./'

  echo "  deleting changelog topic cdc.analytics-state..."
  docker exec cdc-kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --delete --topic cdc.analytics-state 2>&1 | sed 's/^/    /' || true

  echo "  starting container — it will detect empty changelog and bootstrap from source."
  docker compose start analytics-consumer >/dev/null
  echo "  done. Watch the analytics panel — counters will start at 0 and climb back to MySQL truth."
}

case "$SERVICE" in
  search)    replay_search ;;
  analytics) replay_analytics ;;
  both)      replay_search; echo; replay_analytics ;;
  *)         echo "Unknown service: $SERVICE"; echo "Usage: $0 <search|analytics|both>"; exit 1 ;;
esac
