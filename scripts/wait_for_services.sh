#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

wait_for_http() {
  local url="$1"
  local name="$2"
  local attempt
  for attempt in $(seq 1 90); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      echo "$name is ready"
      return 0
    fi
    sleep 2
  done
  echo "$name is not ready: $url" >&2
  return 1
}

wait_for_kafka() {
  local broker="$1"
  local name="$2"
  local attempt
  for attempt in $(seq 1 90); do
    if docker compose exec -T "$name" kafka-topics --bootstrap-server "$broker" --list >/dev/null 2>&1; then
      echo "$name is ready"
      return 0
    fi
    sleep 2
  done
  echo "$name is not ready: $broker" >&2
  return 1
}

wait_for_kafka "kafka-1:29092" "kafka-1"
wait_for_kafka "kafka-2:29092" "kafka-2"
wait_for_kafka "kafka-3:29092" "kafka-3"
wait_for_http "http://localhost:8081/subjects" "schema-registry"
wait_for_http "http://localhost:8080/nifi-api/flow/status" "nifi"

