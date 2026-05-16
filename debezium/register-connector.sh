#!/usr/bin/env bash
# Register the Debezium MySQL connector with Kafka Connect.
# Idempotent: deletes existing connector with the same name first.
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
NAME="mysql-demo-connector"

echo "Waiting for Kafka Connect at $CONNECT_URL..."
until curl -fs "$CONNECT_URL/" >/dev/null; do
  sleep 2
done
echo "Kafka Connect is up."

if curl -fs "$CONNECT_URL/connectors/$NAME" >/dev/null 2>&1; then
  echo "Deleting existing connector $NAME..."
  curl -fsS -X DELETE "$CONNECT_URL/connectors/$NAME"
  sleep 1
fi

echo "Registering connector..."
curl -fsS -X POST -H "Content-Type: application/json" \
  --data @"$(dirname "$0")/mysql-connector.json" \
  "$CONNECT_URL/connectors" | sed 's/.*/&\n/'

echo "Done. Connector status:"
sleep 2
curl -fsS "$CONNECT_URL/connectors/$NAME/status"
echo
