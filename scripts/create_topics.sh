#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p artifacts/task1 artifacts/task2

docker compose exec -T kafka-1 kafka-topics \
  --bootstrap-server kafka-1:29092 \
  --create \
  --if-not-exists \
  --topic orders \
  --partitions 3 \
  --replication-factor 3 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000 \
  --config segment.bytes=268435456 \
  --config min.insync.replicas=2

docker compose exec -T kafka-1 kafka-topics \
  --bootstrap-server kafka-1:29092 \
  --create \
  --if-not-exists \
  --topic nifi-events \
  --partitions 3 \
  --replication-factor 3 \
  --config cleanup.policy=delete \
  --config retention.ms=86400000 \
  --config segment.bytes=134217728 \
  --config min.insync.replicas=2

docker compose exec -T kafka-1 kafka-topics \
  --bootstrap-server kafka-1:29092 \
  --describe \
  --topic orders > artifacts/task1/kafka-topics-describe.txt

