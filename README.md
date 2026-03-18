# USGS Earthquake Live Monitor

USGS Earthquake Live Monitor is a production-style but demo-friendly data engineering project that ingests USGS earthquake events, publishes raw JSON to Kafka, normalizes the stream with Spark Structured Streaming, stores long-term history in HDFS Parquet, and indexes recent serving data into Elasticsearch for Kibana dashboards.

## Stack

- FastAPI
- Python 3.11+
- `uv` with a project-local `.venv`
- Kafka
- Spark Structured Streaming
- HDFS or local filesystem fallback
- Parquet
- Elasticsearch
- Kibana

## Why HDFS For History And Elasticsearch For Serving

HDFS is the long-term system of record because Parquet on HDFS is better for cheap multi-year retention, replay, and batch analytics. Elasticsearch is the serving layer because Kibana needs fast filters, text search, aggregations, and geo visualizations, but Elasticsearch is not the right place to keep a full three-year archive.

## Repository Layout

```text
.
├── app/                      FastAPI app, configuration, models, API routers, services
├── spark_jobs/               Structured Streaming job and normalization helpers
├── infra/                    Docker Compose and Elasticsearch/Kibana/Spark notes
├── docs/                     Architecture, topic/index, and dashboard documentation
├── scripts/                  Bootstrap, topic creation, and backfill helper scripts
├── tests/                    Lightweight unit tests
├── .env.example              Environment template
├── Makefile                  Common developer commands
└── pyproject.toml            uv-managed project metadata
```

## Local Setup

1. Copy the environment template.

```bash
cp .env.example .env
```

2. Start local infrastructure.

```bash
docker compose -f infra/docker-compose.yml up -d
```

3. Install Python dependencies into the project-local `.venv`.

```bash
uv sync
```

4. Bootstrap Kafka topics, storage paths, and Elasticsearch index templates.

```bash
./scripts/bootstrap_local.sh
```

If you do not want to use HDFS locally, set `USE_LOCAL_FS_FOR_STORAGE=true` in `.env` and the Spark job will write to `LOCAL_STORAGE_PATH` instead.

## Run The API

```bash
uv run uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

Optional automatic realtime polling:

```bash
ENABLE_REALTIME_POLLER=true POLL_INTERVAL_SECONDS=60 uv run uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## Run The Spark Job

The project expects the Kafka connector package to be available. The default environment uses:

```bash
SPARK_KAFKA_PACKAGE=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
```

Run the stream processor from the project root:

```bash
uv run python spark_jobs/earthquake_stream_to_hdfs_es.py
```

## Example API Calls

Health:

```bash
curl http://localhost:8000/health
```

Realtime ingest:

```bash
curl -X POST http://localhost:8000/ingest/realtime
```

Backfill ingest:

```bash
curl -X POST http://localhost:8000/ingest/backfill \
  -H "Content-Type: application/json" \
  -d '{
    "start_date": "2026-01-01",
    "end_date": "2026-01-31",
    "window_days": 10
  }'
```

Last run metrics:

```bash
curl http://localhost:8000/metrics/last-run
```

Standalone backfill CLI:

```bash
uv run python -m app.cli backfill --start-date 2026-01-01 --end-date 2026-01-31 --window-days 10
```

## Kafka Topics

- `earthquake.raw.v1`
- `earthquake.backfill.v1`
- `earthquake.normalized.v1` reserved for optional downstream fan-out
- `earthquake.summary.daily.v1` reserved for optional downstream fan-out

The active implementation reads `earthquake.raw.v1` and `earthquake.backfill.v1` in Spark.

## Spark Outputs

Detailed normalized events are written to:

- `/data/earthquakes/events/year=YYYY/month=MM/day=DD/`

Daily summaries are written to:

- `/data/earthquakes/daily_summary/year=YYYY/month=MM/day=DD/`

Recent detailed events are indexed into Elasticsearch indices like:

- `earthquake-events-2026.03`

Daily summary documents are written to:

- `earthquake-daily-summary-v1`

## Commands

```bash
make sync
make api
make spark
make realtime
make test
```

## Testing

```bash
uv run pytest
```

## Development Notes

- The app uses the USGS event ID as the stable identifier.
- Kafka producer idempotence is enabled.
- Elasticsearch uses `_id = event_id`, which makes serving writes naturally overwrite duplicate events.
- HDFS remains append-only for simplicity, which keeps replay easy but does not provide strict exactly-once archival writes without a separate compaction step.

## Useful Docs

- [`docs/architecture.md`](docs/architecture.md)
- [`docs/topics-and-indexes.md`](docs/topics-and-indexes.md)
- [`docs/dashboard-spec.md`](docs/dashboard-spec.md)

