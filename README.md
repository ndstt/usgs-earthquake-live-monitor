# USGS Earthquake Live Monitor

USGS Earthquake Live Monitor is a demo-friendly data engineering project that ingests earthquake events from the USGS public feeds, stores analytical tables in Delta Lake on HDFS, and serves search and dashboard data through Elasticsearch and Kibana.

The project is split into two ingestion lines:

- `Realtime ingestion`: `USGS realtime feed -> FastAPI -> Kafka -> Python consumer -> flat_event Delta -> latest_event Delta`
- `Batch ingestion`: `USGS catalog API -> Spark batch -> flat_event Delta -> latest_event Delta -> region_hourly_stats Delta`

FastAPI also orchestrates periodic serving jobs:

- `latest_event Delta -> Elasticsearch` every 60 seconds
- `latest_event Delta -> region_hourly_stats Delta -> Elasticsearch` every 60 minutes

## Architecture And System Overview

<p align="center">
  <img src="docs/flowchart.svg" alt="USGS Earthquake Live Monitor architecture and system overview" width="1100" />
</p>

## Stack

- FastAPI
- Python 3.11+
- `uv`
- Kafka
- PySpark
- HDFS
- Delta Lake
- Elasticsearch
- Kibana
- JupyterLab

## What The Data Contains

The source data comes from USGS GeoJSON feeds and the USGS event catalog API. Each earthquake event contains fields such as:

- `event_id`
- `event_time_utc`
- `updated_time_utc`
- `place`
- `mag`
- `mag_type`
- `longitude`
- `latitude`
- `depth_km`
- `tsunami`
- `status`
- quality metrics such as `nst`, `dmin`, `rms`, and `gap`

The project flattens the nested GeoJSON payload into analytics-ready tables.

## Data Products

### `flat_event`

Append-only Delta table used as the base analytical fact table.

Core columns:

- `event_id`
- `event_time_utc`
- `updated_time_utc`
- `place`
- `mag`
- `mag_type`
- `longitude`
- `latitude`
- `depth_km`
- `tz`
- `sig`
- `felt`
- `cdi`
- `mmi`
- `alert`
- `tsunami`
- `status`
- `nst`
- `dmin`
- `rms`
- `gap`
- `event_date`
- `year`
- `month`
- `day`

### `latest_event`

Delta snapshot table with one latest row per `event_id`.

It is built from `flat_event` by using a Spark window over `event_id` ordered by:

1. `updated_time_utc DESC`
2. `event_time_utc DESC`

### `region_hourly_stats`

Hourly aggregate Delta table derived from `latest_event`.

Dimensions:

- `bucket_start_utc`
- `region_text`
- `city`
- `state`

Metrics:

- `event_count`
- `tsunami_count`
- `mag_count`
- `mag_sum`
- `max_mag`
- `avg_mag`
- `unknown_count`
- `small_count`
- `moderate_count`
- `strong_count`
- `severe_count`
- `extreme_count`
- `avg_longitude`
- `avg_latitude`

## Storage Strategy

### Delta Lake On HDFS

Delta tables are used for the analytical storage layer:

- `/data/earthquakes/flat_event`
- `/data/earthquakes/latest_event`
- `/data/earthquakes/region_hourly_stats`

Each table is partitioned by:

- `year`
- `month`
- `day`

### Elasticsearch

Elasticsearch is the serving layer for Kibana.

Current indices:

- `earthquake-latest-event-v1`
- `earthquake-region-hourly-v1`

`earthquake-latest-event-v1` is used for maps of real earthquake points.

`earthquake-region-hourly-v1` is used for hourly regional dashboards and aggregate maps.

## Repository Layout

```text
.
|-- app/
|   |-- api/                FastAPI routers
|   |-- consumers/          Kafka consumers
|   |-- loaders/            Delta-to-Elasticsearch loaders
|   |-- services/           Core ingestion, transform, and orchestration logic
|   |-- config.py           Settings model
|   `-- main.py             FastAPI app and lifespan orchestration
|-- spark_jobs/
|   |-- backfill_api_to_flat_event.py
|   |-- flat_event_to_latest_event.py
|   |-- latest_event_to_region_hourly_stats.py
|   |-- flat_event_transform.py
|   `-- region_hourly_stats.py
|-- docs/
|   `-- flowchart.svg         Architecture and system overview diagram
|-- tests/                  Unit tests and notebook exploration
|-- .env.example            Environment template
|-- pyproject.toml          Project metadata and dependencies
`-- README.md
```

## Runtime Assumptions

This repository assumes the external services below are already running:

- Kafka
- HDFS
- Spark master and worker
- Elasticsearch
- Kibana

The project is typically run in a native WSL Ubuntu environment where those services are managed with `systemd`.

## Local Setup

1. Copy the environment template.

```bash
cp .env.example .env
```

2. Install Python dependencies.

```bash
uv sync --dev
```

3. Review the key settings in `.env`.

Important defaults:

- `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`
- `SPARK_MASTER_URL=spark://localhost:7077`
- `HDFS_DEFAULT_FS=hdfs://localhost:9000`
- `HDFS_BASE_PATH=/data/earthquakes`
- `ELASTICSEARCH_URL=http://localhost:9200`
- `ENABLE_REALTIME_POLLER=true`
- `ENABLE_LATEST_EVENT_LOADER=true`
- `ENABLE_REGION_HOURLY_PIPELINE=true`

If you want to run without HDFS, set:

```env
USE_LOCAL_FS_FOR_STORAGE=true
LOCAL_STORAGE_PATH=./data/earthquakes
```

## Run The API

Run FastAPI from the project root:

```bash
uv run uvicorn app.main:app --host 0.0.0.0 --port 8000
```

FastAPI responsibilities:

- expose health and ingest endpoints
- poll the USGS realtime feed if enabled
- publish raw events to Kafka
- orchestrate periodic Elasticsearch sync and hourly analytics refresh

## Run The Realtime Consumer

The realtime consumer reads the Kafka raw topic, flattens the payload, appends `flat_event`, and upserts `latest_event`.

```bash
uv run python -m app.consumers.kafka_to_flat_event
```

Useful options:

```bash
uv run python -m app.consumers.kafka_to_flat_event --auto-offset-reset earliest
uv run python -m app.consumers.kafka_to_flat_event --max-batch-size 100 --max-batch-wait-seconds 60
```

## Run The Batch Backfill

The direct historical backfill path bypasses Kafka and uses Spark to fetch the USGS catalog API and write `flat_event` directly.

Example:

```bash
uv run python -m spark_jobs.backfill_api_to_flat_event \
  --start-date 2025-03-01 \
  --end-date 2026-03-20 \
  --window-days 30 \
  --write-mode overwrite
```

`overwrite` clears the output once at the start of the job, then appends later chunks.

## Rebuild `latest_event`

Use Spark to rebuild the latest snapshot from `flat_event`.

```bash
uv run python -m spark_jobs.flat_event_to_latest_event --write-mode overwrite
```

## Rebuild `region_hourly_stats`

Use Spark to aggregate hourly regional metrics from `latest_event`.

```bash
uv run python -m spark_jobs.latest_event_to_region_hourly_stats --write-mode overwrite
```

## Load Delta Tables Into Elasticsearch

Load `latest_event` into Elasticsearch:

```bash
uv run python -m app.loaders.latest_event_to_elasticsearch --refresh-index
```

Load `region_hourly_stats` into Elasticsearch:

```bash
uv run python -m app.loaders.region_hourly_stats_to_elasticsearch --refresh-index
```

## API Endpoints

Health:

```bash
curl http://localhost:8000/health
```

Manual realtime ingest:

```bash
curl -X POST http://localhost:8000/ingest/realtime
```

Manual Kafka backfill ingest:

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

## Kafka Topics

- `earthquake.raw.v1`
- `earthquake.backfill.v1`
- `earthquake.normalized.v1` reserved
- `earthquake.summary.daily.v1` reserved

The active realtime Delta path consumes `earthquake.raw.v1`.

## Realtime And Batch Flow

### Realtime Ingestion

1. USGS realtime feed updates every minute.
2. FastAPI polls the feed every 60 seconds.
3. New raw events are published to Kafka.
4. The Python consumer reads the Kafka raw topic.
5. The consumer flattens the GeoJSON payload into `flat_event`.
6. The consumer appends `flat_event` to Delta Lake.
7. The consumer upserts `latest_event` to Delta Lake.
8. FastAPI periodically syncs `latest_event` into Elasticsearch.
9. Every 60 minutes, FastAPI runs the Spark hourly pipeline:
   `latest_event -> region_hourly_stats -> Elasticsearch`.

### Batch Ingestion

1. A Spark backfill job fetches historical data from the USGS catalog API.
2. The job flattens the payload into `flat_event`.
3. `flat_event` is written to Delta Lake on HDFS.
4. A Spark rebuild job derives `latest_event` from `flat_event`.
5. A Spark aggregate job derives `region_hourly_stats` from `latest_event`.
6. Loaders push `latest_event` and `region_hourly_stats` into Elasticsearch.

## Kibana Usage

Recommended data views:

- `earthquake-latest-event-v1`
- `earthquake-region-hourly-v1`

Recommended visualizations:

- event count over time
- severity distribution over time
- max and average magnitude over time
- top regions by event count
- tsunami count by region
- maps from `earthquake-latest-event-v1`

Important metric guidance for `earthquake-region-hourly-v1`:

- use `sum(event_count)` for totals
- use `max(max_mag)` for peak magnitude
- use `sum(mag_sum) / sum(mag_count)` for rolled-up average magnitude
- use `sum(small_count)`, `sum(moderate_count)`, and so on for severity charts

## Testing

Run the test suite:

```bash
uv run pytest
```

Targeted examples:

```bash
uv run pytest tests/test_flat_event.py -q
uv run pytest tests/test_latest_event.py -q
uv run pytest tests/test_region_hourly_stats.py -q
```

## Notes

- `latest_event` selection uses a Spark window for deterministic latest-row selection.
- `region_hourly_stats` uses `groupBy` because it is an aggregate table.
- Realtime `latest_event` updates are handled by the Python Kafka consumer, not by Spark.
- Batch rebuilds remain available to repair or fully regenerate downstream tables.
