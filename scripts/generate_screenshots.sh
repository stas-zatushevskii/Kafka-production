#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

go run ./cmd/rendercapture artifacts/task1/curl-subjects.txt artifacts/screenshots/task1-curl-subjects.png "curl http://localhost:8081/subjects"
go run ./cmd/rendercapture artifacts/task1/curl-subject-versions.txt artifacts/screenshots/task1-curl-subject-versions.png "curl http://localhost:8081/subjects/orders-value/versions"
go run ./cmd/rendercapture artifacts/task1/kafka-topics-describe.txt artifacts/screenshots/task1-kafka-topics-describe.png "kafka-topics --describe --topic orders"
go run ./cmd/rendercapture artifacts/task1/producer.log artifacts/screenshots/task1-producer-log.png "Producer log"
go run ./cmd/rendercapture artifacts/task1/consumer.log artifacts/screenshots/task1-consumer-log.png "Consumer log"
go run ./cmd/rendercapture artifacts/task2/services.txt artifacts/screenshots/task2-services.png "docker compose ps"
go run ./cmd/rendercapture artifacts/task2/nifi-flow.log artifacts/screenshots/task2-nifi-log.png "NiFi flow log"
go run ./cmd/rendercapture artifacts/task2/kafka-console-consumer.log artifacts/screenshots/task2-kafka-console-consumer.png "kafka-console-consumer"
