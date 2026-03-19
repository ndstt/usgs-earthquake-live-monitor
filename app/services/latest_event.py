from __future__ import annotations

from datetime import datetime
from typing import Any

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from app.services.flat_event import build_flat_event_arrow_schema, delta_table_uri


def _is_newer_event(candidate: dict[str, object], current: dict[str, object]) -> bool:
    candidate_updated = candidate.get("updated_time_utc")
    current_updated = current.get("updated_time_utc")

    if isinstance(candidate_updated, datetime) and isinstance(current_updated, datetime):
        if candidate_updated != current_updated:
            return candidate_updated > current_updated
    elif isinstance(candidate_updated, datetime):
        return True
    elif isinstance(current_updated, datetime):
        return False

    candidate_event_time = candidate.get("event_time_utc")
    current_event_time = current.get("event_time_utc")
    if isinstance(candidate_event_time, datetime) and isinstance(current_event_time, datetime):
        return candidate_event_time > current_event_time
    return False


def dedupe_latest_rows(flat_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    latest_by_event_id: dict[str, dict[str, object]] = {}

    for row in flat_rows:
        event_id = row.get("event_id")
        if not isinstance(event_id, str) or not event_id:
            continue

        current = latest_by_event_id.get(event_id)
        if current is None or _is_newer_event(row, current):
            latest_by_event_id[event_id] = row

    return list(latest_by_event_id.values())


def upsert_latest_event_rows(
    *,
    output_path: str,
    flat_rows: list[dict[str, object]],
    use_local_fs: bool,
    hdfs_default_fs: str,
) -> int:
    deduped_rows = dedupe_latest_rows(flat_rows)
    if not deduped_rows:
        return 0

    table_uri = delta_table_uri(
        output_path=output_path,
        use_local_fs=use_local_fs,
        hdfs_default_fs=hdfs_default_fs,
    )
    source_table = pa.Table.from_pylist(deduped_rows, schema=build_flat_event_arrow_schema())

    if not DeltaTable.is_deltatable(table_uri):
        write_deltalake(
            table_uri,
            source_table,
            mode="append",
            partition_by=["year", "month", "day"],
        )
        return len(deduped_rows)

    target_table = DeltaTable(table_uri)
    merge_result: dict[str, Any] = (
        target_table.merge(
            source=source_table,
            predicate="target.event_id = source.event_id",
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update_all(
            predicate=(
                "target.updated_time_utc IS NULL "
                "OR source.updated_time_utc > target.updated_time_utc "
                "OR (source.updated_time_utc = target.updated_time_utc "
                "AND source.event_time_utc > target.event_time_utc)"
            )
        )
        .when_not_matched_insert_all()
        .execute()
    )

    return int(merge_result.get("num_target_rows_inserted", 0)) + int(
        merge_result.get("num_target_rows_updated", 0)
    )
