# Kibana Dashboard Spec

## Dashboard Name

`USGS Earthquake Live Monitor`

## Data Views

- `earthquake-events-*` using `event_time_utc`
- `earthquake-daily-summary-v1` using `event_date`

## Filters

- Time picker on `event_time_utc`
- Magnitude bucket
- Region
- Tsunami flag
- Source mode (`realtime` or `backfill`)

## Required Panels

### Total earthquakes

- Visualization: metric
- Field: document count from `earthquake-events-*`

### Earthquakes by hour or day

- Visualization: date histogram
- X-axis: `event_time_utc`
- Y-axis: count

### Magnitude distribution

- Visualization: bar chart
- Buckets: `magnitude_bucket`

### Top regions

- Visualization: horizontal bar chart
- Field: `region_text.keyword`

### Tsunami event count

- Visualization: metric
- Filter: `tsunami_flag:true`

### Average depth

- Visualization: metric
- Field: average of `depth_km`

### Latest events table

- Visualization: table
- Columns:
  - `event_time_utc`
  - `event_id`
  - `magnitude`
  - `place`
  - `depth_km`
  - `tsunami_flag`
  - `status`

### Earthquake map

- Visualization: map
- Geo field: `location`
- Tooltip:
  - `event_id`
  - `magnitude`
  - `place`
  - `event_time_utc`

## Demo Narrative

1. Trigger `POST /ingest/realtime`.
2. Show Kafka topic growth.
3. Show Spark writing Parquet partitions.
4. Refresh Kibana and show the latest events table and map.
5. Run a short backfill and show longer time-range trends.

