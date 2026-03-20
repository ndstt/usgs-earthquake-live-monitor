from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterable

from app.services.flat_event import delta_table_uri


def region_hourly_stats_index_body() -> dict[str, object]:
    return {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
        },
        "mappings": {
            "properties": {
                "document_id": {"type": "keyword"},
                "bucket_start_utc": {"type": "date"},
                "region_text": {"type": "keyword"},
                "city": {"type": "keyword"},
                "state": {"type": "keyword"},
                "event_count": {"type": "long"},
                "tsunami_count": {"type": "long"},
                "mag_count": {"type": "long"},
                "mag_sum": {"type": "double"},
                "max_mag": {"type": "double"},
                "avg_mag": {"type": "double"},
                "unknown_count": {"type": "long"},
                "small_count": {"type": "long"},
                "moderate_count": {"type": "long"},
                "strong_count": {"type": "long"},
                "severe_count": {"type": "long"},
                "extreme_count": {"type": "long"},
                "avg_longitude": {"type": "double"},
                "avg_latitude": {"type": "double"},
                "location": {"type": "geo_point"},
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
    if isinstance(value, Decimal):
        return float(value)
    return value


def normalize_region_hourly_stats_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized = {key: _normalize_value(value) for key, value in row.items()}
    document_id = normalized.get("document_id")
    if not isinstance(document_id, str) or not document_id:
        raise ValueError("region_hourly_stats row is missing a valid document_id")

    latitude = normalized.get("avg_latitude")
    longitude = normalized.get("avg_longitude")
    if latitude is not None and longitude is not None:
        normalized["location"] = {"lat": latitude, "lon": longitude}

    return normalized


def build_bulk_index_payload(
    rows: Iterable[dict[str, Any]],
    *,
    index_name: str,
) -> str:
    lines: list[str] = []
    for row in rows:
        normalized = normalize_region_hourly_stats_row(row)
        lines.append(
            json.dumps(
                {"index": {"_index": index_name, "_id": normalized["document_id"]}},
                separators=(",", ":"),
            )
        )
        lines.append(json.dumps(normalized, separators=(",", ":"), ensure_ascii=False))
    return "".join(f"{line}\n" for line in lines)


def iter_region_hourly_stats_rows(
    *,
    source_path: str,
    use_local_fs: bool,
    hdfs_default_fs: str,
    batch_size: int,
) -> Iterable[list[dict[str, Any]]]:
    try:
        from deltalake import DeltaTable
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "deltalake is required to read region_hourly_stats Delta tables."
        ) from exc

    table_uri = delta_table_uri(
        output_path=source_path,
        use_local_fs=use_local_fs,
        hdfs_default_fs=hdfs_default_fs,
    )
    delta_table = DeltaTable(table_uri)
    arrow_table = delta_table.to_pyarrow_table()

    for record_batch in arrow_table.to_batches(max_chunksize=batch_size):
        yield record_batch.to_pylist()
