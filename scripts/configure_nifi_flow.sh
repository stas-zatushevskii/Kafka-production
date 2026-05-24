#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

NIFI_URL="${NIFI_URL:-http://localhost:8080/nifi-api}"
FLOW_NAME="KafkaIntegrationFlow"

api_get() {
  curl -fsS "$NIFI_URL/$1"
}

api_post() {
  local path="$1"
  local payload="$2"
  curl -fsS \
    --request POST \
    --url "$NIFI_URL/$path" \
    --header 'Content-Type: application/json' \
    --data "$payload"
}

api_put() {
  local path="$1"
  local payload="$2"
  curl -fsS \
    --request PUT \
    --url "$NIFI_URL/$path" \
    --header 'Content-Type: application/json' \
    --data "$payload"
}

existing_group_id="$(
  api_get "flow/process-groups/root" \
    | jq -r --arg name "$FLOW_NAME" '
        .processGroupFlow.flow.processGroups[]
        | select(.component.name == $name)
        | .component.id
      ' \
    | head -n 1
)"

if [[ -n "$existing_group_id" ]]; then
  echo "NiFi flow already exists: $existing_group_id"
  exit 0
fi

group_payload='{
  "revision": {"version": 0},
  "component": {
    "name": "'"$FLOW_NAME"'",
    "position": {"x": 120.0, "y": 120.0}
  }
}'
group_response="$(api_post "process-groups/root/process-groups" "$group_payload")"
group_id="$(jq -r '.id' <<<"$group_response")"

create_processor() {
  local name="$1"
  local type="$2"
  local x="$3"
  local y="$4"
  local config_json="$5"

  local payload
  payload="$(
    jq -n \
      --arg name "$name" \
      --arg type "$type" \
      --argjson x "$x" \
      --argjson y "$y" \
      --argjson config "$config_json" '
        {
          revision: {version: 0},
          component: {
            name: $name,
            type: $type,
            position: {x: $x, y: $y},
            config: $config
          }
        }
      '
  )"
  api_post "process-groups/$group_id/processors" "$payload"
}

generate_config='{
  "schedulingPeriod": "5 sec",
  "schedulingStrategy": "TIMER_DRIVEN",
  "autoTerminatedRelationships": [],
  "properties": {
    "Batch Size": "1",
    "Data Format": "Text",
    "generate-ff-custom-text": "{\"source\":\"nifi\",\"event\":\"inventory_sync\",\"status\":\"ok\"}",
    "Unique FlowFiles": "false"
  }
}'
generate_response="$(create_processor "Generate Messages" "org.apache.nifi.processors.standard.GenerateFlowFile" 80 160 "$generate_config")"
generate_id="$(jq -r '.id' <<<"$generate_response")"

log_config='{
  "schedulingPeriod": "0 sec",
  "schedulingStrategy": "TIMER_DRIVEN",
  "autoTerminatedRelationships": [],
  "properties": {
    "Log Payload": "true",
    "Log Level": "info",
    "Output Format": "Single Line"
  }
}'
log_response="$(create_processor "Log Message" "org.apache.nifi.processors.standard.LogAttribute" 420 160 "$log_config")"
log_id="$(jq -r '.id' <<<"$log_response")"

publish_config='{
  "schedulingPeriod": "0 sec",
  "schedulingStrategy": "TIMER_DRIVEN",
  "autoTerminatedRelationships": ["success", "failure"],
  "properties": {
    "bootstrap.servers": "kafka-1:29092,kafka-2:29092,kafka-3:29092",
    "topic": "nifi-events",
    "security.protocol": "PLAINTEXT",
    "use-transactions": "false"
  }
}'
publish_response="$(create_processor "Publish To Kafka" "org.apache.nifi.processors.kafka.pubsub.PublishKafka_2_6" 760 160 "$publish_config")"
publish_id="$(jq -r '.id' <<<"$publish_response")"

create_connection() {
  local source_id="$1"
  local destination_id="$2"
  local payload
  payload="$(
    jq -n \
      --arg source_id "$source_id" \
      --arg destination_id "$destination_id" '
        {
          revision: {version: 0},
          component: {
            source: {
              id: $source_id,
              type: "PROCESSOR",
              groupId: "'"$group_id"'"
            },
            destination: {
              id: $destination_id,
              type: "PROCESSOR",
              groupId: "'"$group_id"'"
            },
            selectedRelationships: ["success"],
            backPressureObjectThreshold: 10000,
            backPressureDataSizeThreshold: "1 GB",
            flowFileExpiration: "0 sec"
          }
        }
      '
  )"
  api_post "process-groups/$group_id/connections" "$payload" >/dev/null
}

create_connection "$generate_id" "$log_id"
create_connection "$log_id" "$publish_id"

start_processor() {
  local processor_id="$1"
  local current
  current="$(api_get "processors/$processor_id")"
  local version
  version="$(jq -r '.revision.version' <<<"$current")"
  local client_id
  client_id="$(jq -r '.revision.clientId // "script-client"' <<<"$current")"
  local payload
  payload="$(
    jq -n \
      --arg version "$version" \
      --arg client_id "$client_id" \
      --arg processor_id "$processor_id" '
        {
          revision: {
            version: ($version | tonumber),
            clientId: $client_id
          },
          state: "RUNNING",
          disconnectedNodeAcknowledged: false
        }
      '
  )"
  api_put "processors/$processor_id/run-status" "$payload" >/dev/null
}

start_processor "$publish_id"
start_processor "$log_id"
start_processor "$generate_id"

echo "NiFi flow configured: $group_id"
