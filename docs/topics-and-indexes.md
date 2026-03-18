# Topics And Indexes

## Kafka Topics

### `earthquake.raw.v1`

- Producer: FastAPI realtime ingestion endpoint
- Payload: one USGS earthquake event envelope per message
- Key: `event_id`

### `earthquake.backfill.v1`

- Producer: FastAPI backfill endpoint or CLI
- Payload: same envelope shape as realtime, but `source_mode=backfill`
- Key: `event_id`

### Optional future topics

- `earthquake.normalized.v1`
- `earthquake.summary.daily.v1`

The current implementation writes normalized and summary outputs directly from Spark to HDFS and Elasticsearch.

## HDFS Layout

### Event truth dataset

- Path: `/data/earthquakes/events/year=YYYY/month=MM/day=DD/`
- Format: Parquet
- Retention target: 3 years
- Purpose: historical truth, replay, batch analytics

### Daily summary dataset

- Path: `/data/earthquakes/daily_summary/year=YYYY/month=MM/day=DD/`
- Format: Parquet
- Purpose: compact daily rollups for long-range trend views

## Elasticsearch

### Detailed event indices

- Pattern: `earthquake-events-YYYY.MM`
- Template file: `infra/elasticsearch/index-template.json`
- Important fields:
  - `event_time_utc` as the primary time field
  - `magnitude`, `depth_km`, `significance` as numerics
  - `event_id`, `network`, `mag_type`, `region_text` as keywords
  - `location` as `geo_point`

### Daily summary index

- Index: `earthquake-daily-summary-v1`
- Template file: `infra/elasticsearch/daily-summary-template.json`
- Purpose: dashboard-level rollups without scanning raw event documents

