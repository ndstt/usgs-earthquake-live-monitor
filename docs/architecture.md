# Architecture

## Pipeline

`USGS realtime feed -> FastAPI ingestion service -> Kafka raw topic`

`USGS catalog backfill -> FastAPI endpoint or CLI -> Kafka backfill topic`

`Kafka raw/backfill topics -> Spark Structured Streaming -> normalized event stream`

`Normalized stream -> HDFS Parquet for historical truth`

`Normalized stream -> Elasticsearch for recent search and Kibana dashboards`

## Components

- `FastAPI` owns the external control surface and the demo-friendly operational endpoints.
- `USGSClient` calls the realtime GeoJSON feed and the catalog API with retries and timeouts.
- `KafkaEventProducer` publishes one event per message using the USGS event ID as the key.
- `Spark Structured Streaming` parses the raw envelopes, derives dashboard-ready columns, and writes to both sinks.
- `HDFS Parquet` is the append-only historical store used for replay, retention, and batch analytics.
- `Elasticsearch` is the serving tier used by Kibana for fast filters, aggregations, tables, and maps.

## Realtime Flow

1. `POST /ingest/realtime` or the optional background poller requests `all_hour.geojson`.
2. The service validates the payload, deduplicates by `event_id` within the run, and publishes events to `earthquake.raw.v1`.
3. Spark consumes the Kafka topic, normalizes the records, and writes partitioned Parquet to `/data/earthquakes/events/...`.
4. The same normalized rows are bulk-indexed into monthly Elasticsearch indices such as `earthquake-events-2026.03`.

## Backfill Flow

1. `POST /ingest/backfill` or `uv run python -m app.cli backfill ...` accepts a start and end date.
2. The service slices the date range into configurable windows, defaulting to 30 days.
3. Each chunk is fetched from the USGS catalog API and published to `earthquake.backfill.v1`.
4. Spark processes the chunked events with the same normalization path used for realtime events.

## Deduplication and Correctness

- The stable key is the USGS `event_id`.
- The ingestion service removes duplicates within each fetch and keeps an in-memory recent-ID cache to reduce obvious replays.
- Kafka producer idempotence is enabled where supported by the broker.
- Elasticsearch uses `_id = event_id`, so repeated events overwrite the same serving record instead of creating duplicate dashboard rows.
- HDFS remains append-only, so repeated replay runs can still create duplicate history rows unless a downstream compaction job is added. That tradeoff is acceptable for a local demo and is documented explicitly.

## Why HDFS For History And Elasticsearch For Serving

- `HDFS + Parquet` is cheap, replayable, and appropriate for multi-year retention. It is the system of record.
- `Elasticsearch` is optimized for low-latency search, maps, and aggregations that Kibana needs. It is the serving layer, not the archive.

