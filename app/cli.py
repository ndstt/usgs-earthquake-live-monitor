from __future__ import annotations

import argparse
import asyncio
import json
from datetime import date

from app.config import get_settings
from app.logging import configure_logging
from app.models import BackfillRequest, OperationalSnapshot
from app.services.ingestion_service import EarthquakeIngestionService, IngestionResources
from app.services.kafka_producer import KafkaEventProducer, RecentEventCache
from app.services.usgs_client import USGSClient


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="USGS Earthquake Live Monitor CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    realtime_parser = subparsers.add_parser("realtime", help="Run a single realtime ingest")
    realtime_parser.set_defaults(command="realtime")

    backfill_parser = subparsers.add_parser("backfill", help="Run a backfill ingest")
    backfill_parser.add_argument("--start-date", required=True)
    backfill_parser.add_argument("--end-date", required=True)
    backfill_parser.add_argument("--window-days", type=int, default=None)
    return parser


async def run_cli(args: argparse.Namespace) -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    usgs_client = USGSClient(
        realtime_feed_url=settings.usgs_realtime_feed_url,
        catalog_api_url=settings.usgs_catalog_api_url,
        timeout_seconds=settings.usgs_timeout_seconds,
        max_retries=settings.usgs_max_retries,
    )
    producer = KafkaEventProducer(settings)
    deduper = RecentEventCache(
        ttl_seconds=settings.dedupe_ttl_seconds,
        max_ids=settings.dedupe_max_ids,
    )
    resources = IngestionResources(
        settings=settings,
        usgs_client=usgs_client,
        kafka_producer=producer,
        deduper=deduper,
        snapshot=OperationalSnapshot(),
    )
    service = EarthquakeIngestionService(resources)

    await producer.start()
    try:
        if args.command == "realtime":
            result = await service.ingest_realtime()
        else:
            request = BackfillRequest(
                start_date=date.fromisoformat(args.start_date),
                end_date=date.fromisoformat(args.end_date),
                window_days=args.window_days,
            )
            result = await service.ingest_backfill(request)
        print(json.dumps(result.model_dump(mode="json"), indent=2))
    finally:
        await producer.stop()
        await usgs_client.close()


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    asyncio.run(run_cli(args))


if __name__ == "__main__":
    main()
