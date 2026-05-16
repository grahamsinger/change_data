#!/usr/bin/env bash
# Stop all containers but preserve volumes (MySQL data, Kafka topic log,
# Meilisearch index, analytics changelog). Resume later with
# ./scripts/start.sh — your state will be exactly where you left it.

set -euo pipefail
cd "$(dirname "$0")/.."

echo "Stopping all containers (volumes preserved)..."
docker compose stop

echo
echo "Stopped. To resume: ./scripts/start.sh"
echo "To wipe ALL data instead, run: docker compose down -v"
