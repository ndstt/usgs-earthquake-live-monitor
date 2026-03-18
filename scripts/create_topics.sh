#!/usr/bin/env bash
set -euo pipefail

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

TOPICS_CMD="${KAFKA_TOPICS_CMD:-kafka-topics.sh}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"

"${TOPICS_CMD}" --bootstrap-server "${BOOTSTRAP}" --create --if-not-exists --topic "${KAFKA_RAW_TOPIC:-earthquake.raw.v1}" --partitions 3 --replication-factor 1
"${TOPICS_CMD}" --bootstrap-server "${BOOTSTRAP}" --create --if-not-exists --topic "${KAFKA_BACKFILL_TOPIC:-earthquake.backfill.v1}" --partitions 3 --replication-factor 1
"${TOPICS_CMD}" --bootstrap-server "${BOOTSTRAP}" --create --if-not-exists --topic "${KAFKA_NORMALIZED_TOPIC:-earthquake.normalized.v1}" --partitions 3 --replication-factor 1
"${TOPICS_CMD}" --bootstrap-server "${BOOTSTRAP}" --create --if-not-exists --topic "${KAFKA_DAILY_SUMMARY_TOPIC:-earthquake.summary.daily.v1}" --partitions 1 --replication-factor 1

