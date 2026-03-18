# Kafka Notes

The project uses these topics by default:

- `earthquake.raw.v1`
- `earthquake.backfill.v1`
- `earthquake.normalized.v1` for optional future fan-out
- `earthquake.summary.daily.v1` for optional downstream summaries

The ingestion service publishes one JSON message per USGS event and uses the USGS event ID as the Kafka message key.

