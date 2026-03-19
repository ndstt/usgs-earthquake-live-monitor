from __future__ import annotations

import json
from datetime import UTC, datetime
from json import JSONDecodeError
from pathlib import Path
from typing import Any


def build_flat_event_arrow_schema() -> Any:
    try:
        import pyarrow as pa
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "pyarrow is required to write flat_event Delta tables."
        ) from exc

    return pa.schema(
        [
            pa.field("event_id", pa.string()),
            pa.field("event_time_utc", pa.timestamp("us")),
            pa.field("updated_time_utc", pa.timestamp("us")),
            pa.field("place", pa.string()),
            pa.field("mag", pa.float64()),
            pa.field("mag_type", pa.string()),
            pa.field("longitude", pa.float64()),
            pa.field("latitude", pa.float64()),
            pa.field("depth_km", pa.float64()),
            pa.field("tz", pa.int32()),
            pa.field("sig", pa.int32()),
            pa.field("felt", pa.int32()),
            pa.field("cdi", pa.float64()),
            pa.field("mmi", pa.float64()),
            pa.field("alert", pa.string()),
            pa.field("tsunami", pa.int32()),
            pa.field("status", pa.string()),
            pa.field("nst", pa.int32()),
            pa.field("dmin", pa.float64()),
            pa.field("rms", pa.float64()),
            pa.field("gap", pa.float64()),
            pa.field("event_date", pa.date32()),
            pa.field("year", pa.int32()),
            pa.field("month", pa.int32()),
            pa.field("day", pa.int32()),
        ]
    )


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_utc_naive(value_ms: Any) -> datetime | None:
    if value_ms is None:
        return None
    try:
        return datetime.fromtimestamp(float(value_ms) / 1000, tz=UTC).replace(tzinfo=None)
    except (TypeError, ValueError, OSError, OverflowError):
        return None


def delta_table_uri(*, output_path: str, use_local_fs: bool, hdfs_default_fs: str) -> str:
    if use_local_fs:
        return str(Path(output_path).resolve())
    if output_path.startswith("hdfs://"):
        return output_path
    normalized_path = output_path if output_path.startswith("/") else f"/{output_path}"
    return f"{hdfs_default_fs.rstrip('/')}{normalized_path}"


def flatten_payload_json(payload_json: str) -> dict[str, object] | None:
    try:
        envelope = json.loads(payload_json)
    except JSONDecodeError:
        return None
    feature = envelope.get("feature")
    if not isinstance(feature, dict):
        return None

    properties = feature.get("properties")
    geometry = feature.get("geometry")
    if not isinstance(properties, dict) or not isinstance(geometry, dict):
        return None

    if properties.get("type") != "earthquake":
        return None
    if geometry.get("type") != "Point":
        return None

    event_id = envelope.get("event_id")
    event_time_utc = _to_utc_naive(properties.get("time"))
    if not isinstance(event_id, str) or not event_id or event_time_utc is None:
        return None

    coordinates = geometry.get("coordinates")
    if not isinstance(coordinates, list):
        coordinates = []

    longitude = _as_float(coordinates[0]) if len(coordinates) > 0 else None
    latitude = _as_float(coordinates[1]) if len(coordinates) > 1 else None
    depth_km = _as_float(coordinates[2]) if len(coordinates) > 2 else None

    return {
        "event_id": event_id,
        "event_time_utc": event_time_utc,
        "updated_time_utc": _to_utc_naive(properties.get("updated")),
        "place": properties.get("place"),
        "mag": _as_float(properties.get("mag")),
        "mag_type": properties.get("magType"),
        "longitude": longitude,
        "latitude": latitude,
        "depth_km": depth_km,
        "tz": _as_int(properties.get("tz")),
        "sig": _as_int(properties.get("sig")),
        "felt": _as_int(properties.get("felt")),
        "cdi": _as_float(properties.get("cdi")),
        "mmi": _as_float(properties.get("mmi")),
        "alert": properties.get("alert"),
        "tsunami": _as_int(properties.get("tsunami")),
        "status": properties.get("status"),
        "nst": _as_int(properties.get("nst")),
        "dmin": _as_float(properties.get("dmin")),
        "rms": _as_float(properties.get("rms")),
        "gap": _as_float(properties.get("gap")),
        "event_date": event_time_utc.date(),
        "year": event_time_utc.year,
        "month": event_time_utc.month,
        "day": event_time_utc.day,
    }


def flatten_payload_batch(payload_json_rows: list[str]) -> list[dict[str, object]]:
    flattened_rows: list[dict[str, object]] = []
    for payload_json in payload_json_rows:
        flat_row = flatten_payload_json(payload_json)
        if flat_row is not None:
            flattened_rows.append(flat_row)
    return flattened_rows


def append_flat_event_rows(
    *,
    output_path: str,
    flat_rows: list[dict[str, object]],
    use_local_fs: bool,
    hdfs_default_fs: str,
) -> int:
    if not flat_rows:
        return 0

    try:
        import pyarrow as pa
        from deltalake import DeltaTable, write_deltalake
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "deltalake and pyarrow are required to write flat_event Delta tables."
        ) from exc

    table_uri = delta_table_uri(
        output_path=output_path,
        use_local_fs=use_local_fs,
        hdfs_default_fs=hdfs_default_fs,
    )
    arrow_table = pa.Table.from_pylist(flat_rows, schema=build_flat_event_arrow_schema())
    table_exists = DeltaTable.is_deltatable(table_uri)

    write_kwargs: dict[str, object] = {"mode": "append"}
    if not table_exists:
        write_kwargs["partition_by"] = ["year", "month", "day"]

    write_deltalake(table_uri, arrow_table, **write_kwargs)
    return len(flat_rows)
