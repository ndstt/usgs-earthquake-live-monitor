from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from typing import Any, Sequence

import httpx

from app.config import get_settings
from app.services.latest_event_loader import (
    build_latest_event_bulk_index_payload,
    iter_latest_event_rows,
    latest_event_index_body,
)

logger = logging.getLogger("latest_event_to_elasticsearch")


@dataclass(frozen=True, slots=True)
class LatestEventLoaderSettings:
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
    def from_args(cls, args: argparse.Namespace) -> LatestEventLoaderSettings:
        settings = get_settings()
        return cls(
            source_path=args.source_path or settings.latest_event_output_path,
            elasticsearch_url=settings.elasticsearch_url.rstrip("/"),
            index_name=args.index_name or settings.elasticsearch_latest_event_index,
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
        description="Load the latest_event Delta table into Elasticsearch.",
    )
    parser.add_argument(
        "--source-path",
        default=None,
        help="Source latest_event Delta table path. Defaults to <storage_base>/latest_event.",
    )
    parser.add_argument(
        "--index-name",
        default=None,
        help="Elasticsearch index name. Defaults to ELASTICSEARCH_LATEST_EVENT_INDEX.",
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


def create_http_client(settings: LatestEventLoaderSettings) -> httpx.Client:
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

    create_response = client.put(f"/{index_name}", json=latest_event_index_body())
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
        "Elasticsearch bulk load failed for one or more latest_event documents: "
        f"{first_error}"
    )


def run_load(
    settings: LatestEventLoaderSettings,
    *,
    event_ids: Sequence[str] | None = None,
) -> int:
    total_loaded = 0

    with create_http_client(settings) as client:
        ensure_index(client, settings.index_name)

        for rows in iter_latest_event_rows(
            source_path=settings.source_path,
            use_local_fs=settings.use_local_fs_for_storage,
            hdfs_default_fs=settings.hdfs_default_fs,
            batch_size=settings.batch_size,
            event_ids=event_ids,
        ):
            if not rows:
                continue

            payload = build_latest_event_bulk_index_payload(rows, index_name=settings.index_name)
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
        "latest_event load completed",
        extra={
            "source_path": settings.source_path,
            "index_name": settings.index_name,
            "loaded_count": total_loaded,
            "filtered_event_ids": len(event_ids) if event_ids is not None else None,
        },
    )
    return total_loaded


def main() -> None:
    logging.basicConfig(level=get_settings().log_level)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    parser = build_parser()
    args = parser.parse_args()
    settings = LatestEventLoaderSettings.from_args(args)
    run_load(settings)


if __name__ == "__main__":
    main()
