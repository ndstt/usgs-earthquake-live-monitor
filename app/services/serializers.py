from __future__ import annotations

import json
from datetime import UTC, date, datetime
from typing import Literal, cast

from app.models import KafkaEarthquakeEnvelope, USGSFeature


def build_event_envelope(
    *,
    feature: USGSFeature,
    source_feed: str,
    source_mode: Literal["realtime", "backfill"],
    topic: str,
    ingest_time_utc: datetime | None = None,
    request_window_start: date | None = None,
    request_window_end: date | None = None,
) -> KafkaEarthquakeEnvelope:
    timestamp = ingest_time_utc or datetime.now(tz=UTC)
    return KafkaEarthquakeEnvelope(
        event_id=feature.event_id,
        source_feed=source_feed,
        source_mode=source_mode,
        ingest_time_utc=timestamp,
        published_topic=topic,
        feature=feature.model_dump(mode="json", by_alias=True),
        request_window_start=request_window_start.isoformat() if request_window_start else None,
        request_window_end=request_window_end.isoformat() if request_window_end else None,
    )


def serialize_envelope(envelope: KafkaEarthquakeEnvelope) -> bytes:
    return envelope.model_dump_json(by_alias=True).encode("utf-8")


def deserialize_envelope(payload: bytes) -> dict[str, object]:
    return cast(dict[str, object], json.loads(payload.decode("utf-8")))
