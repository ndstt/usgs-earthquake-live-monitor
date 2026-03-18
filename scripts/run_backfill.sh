#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <start-date> <end-date> [window-days]" >&2
  exit 1
fi

START_DATE="$1"
END_DATE="$2"
WINDOW_DAYS="${3:-30}"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

uv run python -m app.cli backfill \
  --start-date "${START_DATE}" \
  --end-date "${END_DATE}" \
  --window-days "${WINDOW_DAYS}"

