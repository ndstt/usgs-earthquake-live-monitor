#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
else
  echo ".env not found, using current shell environment and defaults."
fi

uv sync

./scripts/create_topics.sh

if [[ "${USE_LOCAL_FS_FOR_STORAGE:-false}" == "true" ]]; then
  mkdir -p \
    "${LOCAL_STORAGE_PATH:-./data/earthquakes}/events" \
    "${LOCAL_STORAGE_PATH:-./data/earthquakes}/daily_summary" \
    "${LOCAL_STORAGE_PATH:-./data/earthquakes}/checkpoints"
else
  if command -v hdfs >/dev/null 2>&1; then
    hdfs dfs -mkdir -p \
      "${HDFS_BASE_PATH:-/data/earthquakes}/events" \
      "${HDFS_BASE_PATH:-/data/earthquakes}/daily_summary" \
      "${HDFS_BASE_PATH:-/data/earthquakes}/checkpoints"
  else
    echo "hdfs command not found. Set USE_LOCAL_FS_FOR_STORAGE=true for local filesystem mode." >&2
  fi
fi

AUTH_ARGS=()
if [[ -n "${ELASTICSEARCH_USERNAME:-}" && -n "${ELASTICSEARCH_PASSWORD:-}" ]]; then
  AUTH_ARGS=(-u "${ELASTICSEARCH_USERNAME}:${ELASTICSEARCH_PASSWORD}")
fi

curl -sS "${AUTH_ARGS[@]}" \
  -X PUT "${ELASTICSEARCH_URL:-http://localhost:9200}/_index_template/earthquake-events-template" \
  -H "Content-Type: application/json" \
  --data-binary @infra/elasticsearch/index-template.json

curl -sS "${AUTH_ARGS[@]}" \
  -X PUT "${ELASTICSEARCH_URL:-http://localhost:9200}/_index_template/earthquake-daily-summary-template" \
  -H "Content-Type: application/json" \
  --data-binary @infra/elasticsearch/daily-summary-template.json

echo "Bootstrap complete."

