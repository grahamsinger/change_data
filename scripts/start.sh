#!/usr/bin/env bash
# Bring everything back up after ./scripts/stop.sh. Containers,
# volumes, Kafka topic offsets, and Debezium's bookkeeping all persist
# across shutdown/start, so this should just resume.

set -euo pipefail
cd "$(dirname "$0")/.."

echo "Starting containers..."
docker compose up -d

# Kafka Connect takes a few seconds longer than the broker to be ready.
echo -n "Waiting for Kafka Connect"
for _ in $(seq 1 60); do
  if curl -fs http://localhost:8083/ >/dev/null 2>&1; then
    echo " ✓"
    break
  fi
  echo -n "."
  sleep 1
done

# Debezium's connector definition and its committed binlog position both
# live in Kafka, so it normally comes back on its own. Verify, and
# re-register only if something is off.
echo -n "Checking Debezium connector..."
status=$(curl -fs http://localhost:8083/connectors/mysql-demo-connector/status 2>/dev/null \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('connector',{}).get('state','UNKNOWN'))" 2>/dev/null \
  || echo "MISSING")

case "$status" in
  RUNNING)
    echo " RUNNING ✓"
    ;;
  MISSING)
    echo " not registered, registering now..."
    ./debezium/register-connector.sh
    ;;
  *)
    echo " state=$status (re-registering to be safe)"
    ./debezium/register-connector.sh
    ;;
esac

echo
echo "Done. UI: http://localhost:5173"
