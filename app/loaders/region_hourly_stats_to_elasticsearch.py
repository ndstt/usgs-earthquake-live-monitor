from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from typing import Any

import httpx

from app.config import get_settings
from app.services.region_hourly_stats_loader import (
    build_bulk_index_payload,
    iter_region_hourly_stats_rows,
    region_hourly_stats_index_body,
)

logger = logging.getLogger("region_hourly_stats_to_elasticsearch")


@dataclass(frozen=True, slots=True)
class RegionHourlyStatsLoaderSettings:
    source_path: str
    elasticsearch_url: str
    index_name: str
    username: str | None
    password: str | None
    batch_size: int
    request_timeout_seconds: float
    hdfs_default_fs: str
    use_local_fs_for_storage: bool
    refresh_index: bool

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> RegionHourlyStatsLoaderSettings:
        settings = get_settings()
        return cls(
            source_path=args.source_path or settings.region_hourly_stats_output_path,
            elasticsearch_url=settings.elasticsearch_url.rstrip("/"),
            index_name=args.index_name or settings.elasticsearch_region_hourly_stats_index,
            username=settings.elasticsearch_username,
            password=settings.elasticsearch_password,
            batch_size=args.batch_size or settings.elasticsearch_bulk_batch_size,
            request_timeout_seconds=settings.elasticsearch_request_timeout_seconds,
            hdfs_default_fs=settings.hdfs_default_fs,
            use_local_fs_for_storage=settings.use_local_fs_for_storage,
            refresh_index=args.refresh_index,
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Load the region_hourly_stats Delta table into Elasticsearch.",
    )
    parser.add_argument(
        "--source-path",
        default=None,
        help=(
            "Source region_hourly_stats Delta table path. "
            "Defaults to <storage_base>/region_hourly_stats."
        ),
    )
    parser.add_argument(
        "--index-name",
        default=None,
        help="Elasticsearch index name. Defaults to ELASTICSEARCH_REGION_HOURLY_STATS_INDEX.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Bulk index batch size. Defaults to ELASTICSEARCH_BULK_BATCH_SIZE.",
    )
    parser.add_argument(
        "--refresh-index",
        action="store_true",
        help="Refresh the Elasticsearch index after the bulk load completes.",
    )
    return parser


def create_http_client(settings: RegionHourlyStatsLoaderSettings) -> httpx.Client:
    auth: tuple[str, str] | None = None
    if settings.username and settings.password:
        auth = (settings.username, settings.password)

    return httpx.Client(
        base_url=settings.elasticsearch_url,
        auth=auth,
        timeout=settings.request_timeout_seconds,
    )


def ensure_index(client: httpx.Client, index_name: str) -> None:
    response = client.head(f"/{index_name}")
    if response.status_code == 200:
        return
    if response.status_code != 404:
        response.raise_for_status()

    create_response = client.put(f"/{index_name}", json=region_hourly_stats_index_body())
    if create_response.status_code in (200, 201):
        return

    payload = create_response.json()
    error_type = payload.get("error", {}).get("type")
    if error_type == "resource_already_exists_exception":
        return
    create_response.raise_for_status()


def _raise_on_bulk_errors(response_payload: dict[str, Any]) -> None:
    if not response_payload.get("errors"):
        return

    failed_items = [
        item["index"]
        for item in response_payload.get("items", [])
        if item.get("index", {}).get("error") is not None
    ]
    first_error = failed_items[0] if failed_items else {}
    raise RuntimeError(
        "Elasticsearch bulk load failed for one or more region_hourly_stats documents: "
        f"{first_error}"
    )


def run_load(settings: RegionHourlyStatsLoaderSettings) -> int:
    total_loaded = 0

    with create_http_client(settings) as client:
        ensure_index(client, settings.index_name)

        for rows in iter_region_hourly_stats_rows(
            source_path=settings.source_path,
            use_local_fs=settings.use_local_fs_for_storage,
            hdfs_default_fs=settings.hdfs_default_fs,
            batch_size=settings.batch_size,
        ):
            if not rows:
                continue

            payload = build_bulk_index_payload(rows, index_name=settings.index_name)
            response = client.post(
                "/_bulk",
                content=payload,
                headers={"content-type": "application/x-ndjson"},
            )
            response.raise_for_status()
            response_payload = response.json()
            _raise_on_bulk_errors(response_payload)

            total_loaded += len(rows)

        if settings.refresh_index:
            refresh_response = client.post(f"/{settings.index_name}/_refresh")
            refresh_response.raise_for_status()

    logger.info(
        "region_hourly_stats load completed",
        extra={
            "source_path": settings.source_path,
            "index_name": settings.index_name,
            "loaded_count": total_loaded,
        },
    )
    return total_loaded


def main() -> None:
    logging.basicConfig(level=get_settings().log_level)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    parser = build_parser()
    args = parser.parse_args()
    settings = RegionHourlyStatsLoaderSettings.from_args(args)
    run_load(settings)


if __name__ == "__main__":
    main()
