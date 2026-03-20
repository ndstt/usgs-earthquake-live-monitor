from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.backfill import router as backfill_router
from app.api.health import router as health_router
from app.api.ingest import router as ingest_router
from app.config import Settings, get_settings
from app.logging import configure_logging
from app.models import OperationalSnapshot
from app.services.ingestion_service import EarthquakeIngestionService, IngestionResources
from app.services.kafka_producer import KafkaEventProducer, RecentEventCache
from app.services.pipeline_orchestrator import (
    PipelineOrchestrator,
    cancel_task,
    periodic_loop,
)
from app.services.usgs_client import USGSClient

logger = logging.getLogger(__name__)


async def realtime_poll_loop(
    service: EarthquakeIngestionService,
    settings: Settings,
    stop_event: asyncio.Event,
) -> None:
    while not stop_event.is_set():
        try:
            await service.ingest_realtime()
        except Exception:
            logger.exception("Realtime polling cycle failed")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=settings.poll_interval_seconds)
        except TimeoutError:
            continue


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    configure_logging(settings.log_level)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    usgs_client = USGSClient(
        realtime_feed_url=settings.usgs_realtime_feed_url,
        catalog_api_url=settings.usgs_catalog_api_url,
        timeout_seconds=settings.usgs_timeout_seconds,
        max_retries=settings.usgs_max_retries,
    )
    kafka_producer = KafkaEventProducer(settings)
    deduper = RecentEventCache(
        ttl_seconds=settings.dedupe_ttl_seconds,
        max_ids=settings.dedupe_max_ids,
    )
    snapshot = OperationalSnapshot()
    resources = IngestionResources(
        settings=settings,
        usgs_client=usgs_client,
        kafka_producer=kafka_producer,
        deduper=deduper,
        snapshot=snapshot,
    )
    service = EarthquakeIngestionService(resources)

    await kafka_producer.start()
    app.state.settings = settings
    app.state.ingestion_service = service
    app.state.poller_stop_event = asyncio.Event()
    app.state.poller_task = None
    app.state.orchestrator = PipelineOrchestrator(
        settings=settings,
        latest_event_lock=asyncio.Lock(),
        region_hourly_lock=asyncio.Lock(),
    )
    app.state.latest_event_loader_task = None
    app.state.region_hourly_pipeline_task = None

    if settings.enable_realtime_poller:
        app.state.poller_task = asyncio.create_task(
            realtime_poll_loop(
                service,
                settings,
                app.state.poller_stop_event,
            )
        )
        logger.info(
            "Realtime poller enabled",
            extra={"poll_interval_seconds": settings.poll_interval_seconds},
        )

    if settings.enable_latest_event_loader:
        app.state.latest_event_loader_task = asyncio.create_task(
            periodic_loop(
                name="latest_event_loader",
                interval_seconds=settings.latest_event_loader_interval_seconds,
                stop_event=app.state.poller_stop_event,
                operation=app.state.orchestrator.sync_latest_event,
            )
        )
        logger.info(
            "latest_event loader enabled",
            extra={"interval_seconds": settings.latest_event_loader_interval_seconds},
        )

    if settings.enable_region_hourly_pipeline:
        app.state.region_hourly_pipeline_task = asyncio.create_task(
            periodic_loop(
                name="region_hourly_pipeline",
                interval_seconds=settings.region_hourly_pipeline_interval_seconds,
                stop_event=app.state.poller_stop_event,
                operation=app.state.orchestrator.refresh_region_hourly_pipeline,
            )
        )
        logger.info(
            "region_hourly pipeline enabled",
            extra={"interval_seconds": settings.region_hourly_pipeline_interval_seconds},
        )

    try:
        yield
    finally:
        app.state.poller_stop_event.set()
        await cancel_task(app.state.region_hourly_pipeline_task)
        await cancel_task(app.state.latest_event_loader_task)
        await cancel_task(app.state.poller_task)

        await kafka_producer.stop()
        await usgs_client.close()


def create_app() -> FastAPI:
    app = FastAPI(
        title="USGS Earthquake Live Monitor",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.include_router(health_router)
    app.include_router(ingest_router)
    app.include_router(backfill_router)
    return app


app = create_app()

