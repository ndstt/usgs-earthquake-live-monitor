from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Iterable, Sequence

from app.services.flat_event import delta_table_uri


def latest_event_index_body() -> dict[str, object]:
    return {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
        },
        "mappings": {
            "properties": {
                "event_id": {"type": "keyword"},
                "event_time_utc": {"type": "date"},
                "updated_time_utc": {"type": "date"},
                "place": {"type": "keyword"},
                "mag": {"type": "double"},
                "mag_type": {"type": "keyword"},
                "longitude": {"type": "double"},
                "latitude": {"type": "double"},
                "depth_km": {"type": "double"},
                "location": {"type": "geo_point"},
                "tz": {"type": "integer"},
                "sig": {"type": "integer"},
                "felt": {"type": "integer"},
                "cdi": {"type": "double"},
                "mmi": {"type": "double"},
                "alert": {"type": "keyword"},
                "tsunami": {"type": "integer"},
                "status": {"type": "keyword"},
                "nst": {"type": "integer"},
                "dmin": {"type": "double"},
                "rms": {"type": "double"},
                "gap": {"type": "double"},
                "event_date": {"type": "date"},
                "year": {"type": "integer"},
                "month": {"type": "integer"},
                "day": {"type": "integer"},
            }
        },
    }


def _normalize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat() + ("Z" if value.tzinfo is None else "")
    if isinstance(value, date):
        return value.isoformat()
    return value


def normalize_latest_event_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized = {key: _normalize_value(value) for key, value in row.items()}
    event_id = normalized.get("event_id")
    if not isinstance(event_id, str) or not event_id:
        raise ValueError("latest_event row is missing a valid event_id")

    latitude = normalized.get("latitude")
    longitude = normalized.get("longitude")
    if latitude is not None and longitude is not None:
        normalized["location"] = {"lat": latitude, "lon": longitude}

    return normalized


def build_latest_event_bulk_index_payload(
    rows: Iterable[dict[str, Any]],
    *,
    index_name: str,
) -> str:
    lines: list[str] = []
    for row in rows:
        normalized = normalize_latest_event_row(row)
        event_id = normalized["event_id"]
        lines.append(
            json.dumps(
                {"index": {"_index": index_name, "_id": event_id}},
                separators=(",", ":"),
            )
        )
        lines.append(json.dumps(normalized, separators=(",", ":"), ensure_ascii=False))
    return "".join(f"{line}\n" for line in lines)


def iter_latest_event_rows(
    *,
    source_path: str,
    use_local_fs: bool,
    hdfs_default_fs: str,
    batch_size: int,
    event_ids: Sequence[str] | None = None,
) -> Iterable[list[dict[str, Any]]]:
    try:
        from deltalake import DeltaTable
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "deltalake is required to read latest_event Delta tables."
        ) from exc

    table_uri = delta_table_uri(
        output_path=source_path,
        use_local_fs=use_local_fs,
        hdfs_default_fs=hdfs_default_fs,
    )
    delta_table = DeltaTable(table_uri)

    filters: list[tuple[str, str, Any]] | None = None
    if event_ids:
        filters = [("event_id", "in", list(event_ids))]

    arrow_table = delta_table.to_pyarrow_table(filters=filters)

    for record_batch in arrow_table.to_batches(max_chunksize=batch_size):
        yield record_batch.to_pylist()
