UV ?= uv

.PHONY: sync api realtime backfill spark test lint bootstrap topics

sync:
	$(UV) sync

api:
	$(UV) run uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

realtime:
	$(UV) run python -m app.cli realtime

backfill:
	$(UV) run python -m app.cli backfill --start-date $(START) --end-date $(END) --window-days $(or $(WINDOW_DAYS),30)

spark:
	$(UV) run python spark_jobs/earthquake_stream_to_hdfs_es.py

test:
	$(UV) run pytest

lint:
	$(UV) run ruff check .
	$(UV) run mypy app spark_jobs

bootstrap:
	./scripts/bootstrap_local.sh

topics:
	./scripts/create_topics.sh

