#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p artifacts/task1

SCHEMA_FILE="${1:-schemas/order-value.avsc}"
SUBJECT="${2:-orders-value}"
BASE_URL="${3:-http://localhost:8081}"

if ! curl -fsS "$BASE_URL/subjects/$SUBJECT/versions/latest" >/dev/null 2>&1; then
  jq -Rs '{schemaType:"AVRO", schema:.}' < "$SCHEMA_FILE" \
    | curl -fsS \
        --request POST \
        --url "$BASE_URL/subjects/$SUBJECT/versions" \
        --header 'Content-Type: application/vnd.schemaregistry.v1+json' \
        --data @- >/dev/null
fi

curl -fsS "$BASE_URL/subjects" > artifacts/task1/curl-subjects.txt
curl -fsS "$BASE_URL/subjects/$SUBJECT/versions" > artifacts/task1/curl-subject-versions.txt

