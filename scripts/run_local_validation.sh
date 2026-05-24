#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p artifacts/task1 artifacts/task2 artifacts/screenshots

go build ./...

docker compose down >/dev/null 2>&1 || true
docker compose up -d
./scripts/wait_for_services.sh
./scripts/create_topics.sh
./scripts/register_schema.sh
./scripts/configure_nifi_flow.sh

go run ./cmd/producer > artifacts/task1/producer.log
go run ./cmd/consumer --expected-count 3 --timeout-seconds 20 > artifacts/task1/consumer.log

docker compose ps > artifacts/task2/services.txt
sleep 15
docker compose exec -T nifi sh -lc "grep -E 'inventory_sync|PublishKafka_2_6|GenerateFlowFile|LogAttribute' /opt/nifi/nifi-current/logs/nifi-app.log | tail -n 120" > artifacts/task2/nifi-flow.log
docker compose exec -T kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:29092 \
  --topic nifi-events \
  --from-beginning \
  --max-messages 3 \
  --timeout-ms 15000 > artifacts/task2/kafka-console-consumer.log

./scripts/generate_screenshots.sh

echo "Artifacts saved under artifacts/"
