from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from app.config import Settings
from app.models import (
    BackfillRequest,
    ChunkRunSummary,
    IngestionRunSummary,
    OperationalSnapshot,
    USGSFeature,
)
from app.services.kafka_producer import KafkaEventProducer, RecentEventCache
from app.services.serializers import build_event_envelope
from app.services.usgs_client import USGSClient, chunk_date_range

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class IngestionResources:
    settings: Settings
    usgs_client: USGSClient
    kafka_producer: KafkaEventProducer
    deduper: RecentEventCache
    snapshot: OperationalSnapshot


class EarthquakeIngestionService:
    def __init__(self, resources: IngestionResources) -> None:
        self._resources = resources
        self._run_lock = asyncio.Lock()

    @property
    def snapshot(self) -> OperationalSnapshot:
        return self._resources.snapshot

    @property
    def is_busy(self) -> bool:
        return self._run_lock.locked()

    async def ingest_realtime(self) -> IngestionRunSummary:
        async with self._run_lock:
            topic = self._resources.settings.kafka_raw_topic
            started_at = datetime.now(tz=UTC)
            collection = await self._resources.usgs_client.fetch_realtime_feed()
            unique_features = self._unique_features(collection.features)
            envelopes = [
                build_event_envelope(
                    feature=feature,
                    source_feed=collection.metadata.url
                    or self._resources.settings.usgs_realtime_feed_url,
                    source_mode="realtime",
                    topic=topic,
                    ingest_time_utc=started_at,
                )
                for feature in unique_features
            ]
            publish_counts = await self._resources.kafka_producer.publish_envelopes(
                topic=topic,
                envelopes=envelopes,
                deduper=self._resources.deduper,
            )
            finished_at = datetime.now(tz=UTC)
            summary = IngestionRunSummary(
                mode="realtime",
                topic=topic,
                source_feed=collection.metadata.url
                or self._resources.settings.usgs_realtime_feed_url,
                started_at_utc=started_at,
                finished_at_utc=finished_at,
                fetched_count=len(collection.features),
                unique_count=len(unique_features),
                published_count=publish_counts.published_count,
                duplicate_count=publish_counts.duplicate_count
                + max(0, len(collection.features) - len(unique_features)),
                metadata={
                    "feed_title": collection.metadata.title,
                    "generated_time_utc": (
                        collection.metadata.generated_time_utc.isoformat()
                        if collection.metadata.generated_time_utc
                        else None
                    ),
                    "status_code": collection.metadata.status,
                },
            )
            self._resources.snapshot.last_run = summary
            logger.info(
                "Realtime ingest completed",
                extra={
                    "mode": summary.mode,
                    "fetched_count": summary.fetched_count,
                    "published_count": summary.published_count,
                    "duplicate_count": summary.duplicate_count,
                    "topic": summary.topic,
                },
            )
            return summary

    async def ingest_backfill(self, request: BackfillRequest) -> IngestionRunSummary:
        async with self._run_lock:
            settings = self._resources.settings
            topic = settings.kafka_backfill_topic
            started_at = datetime.now(tz=UTC)
            window_days = request.window_days or settings.backfill_window_days
            chunk_summaries: list[ChunkRunSummary] = []
            fetched_total = 0
            unique_total = 0
            published_total = 0
            duplicate_total = 0
            request_seen_ids: set[str] = set()

            for chunk_start, chunk_end in chunk_date_range(
                request.start_date,
                request.end_date,
                window_days,
            ):
                collection = await self._resources.usgs_client.fetch_catalog_range(
                    start_date=chunk_start,
                    end_date=chunk_end,
                )
                unique_features = self._unique_features(collection.features)
                chunk_envelopes = []
                cross_chunk_duplicates = 0

                for feature in unique_features:
                    if feature.event_id in request_seen_ids:
                        cross_chunk_duplicates += 1
                        continue
                    request_seen_ids.add(feature.event_id)
                    chunk_envelopes.append(
                        build_event_envelope(
                            feature=feature,
                            source_feed=collection.metadata.url
                            or settings.usgs_catalog_api_url,
                            source_mode="backfill",
                            topic=topic,
                            ingest_time_utc=datetime.now(tz=UTC),
                            request_window_start=chunk_start,
                            request_window_end=chunk_end,
                        )
                    )

                publish_counts = await self._resources.kafka_producer.publish_envelopes(
                    topic=topic,
                    envelopes=chunk_envelopes,
                    deduper=self._resources.deduper,
                )

                chunk_duplicate_count = (
                    cross_chunk_duplicates
                    + max(0, len(collection.features) - len(unique_features))
                    + publish_counts.duplicate_count
                )
                chunk_summary = ChunkRunSummary(
                    chunk_start=chunk_start,
                    chunk_end=chunk_end,
                    fetched_count=len(collection.features),
                    unique_count=len(unique_features),
                    published_count=publish_counts.published_count,
                    duplicate_count=chunk_duplicate_count,
                )
                chunk_summaries.append(chunk_summary)
                fetched_total += len(collection.features)
                unique_total += len(unique_features)
                published_total += publish_counts.published_count
                duplicate_total += chunk_duplicate_count

                logger.info(
                    "Backfill chunk completed",
                    extra={
                        "chunk_start": chunk_start.isoformat(),
                        "chunk_end": chunk_end.isoformat(),
                        "fetched_count": chunk_summary.fetched_count,
                        "published_count": chunk_summary.published_count,
                        "duplicate_count": chunk_summary.duplicate_count,
                        "topic": topic,
                    },
                )

            finished_at = datetime.now(tz=UTC)
            summary = IngestionRunSummary(
                mode="backfill",
                topic=topic,
                source_feed=settings.usgs_catalog_api_url,
                started_at_utc=started_at,
                finished_at_utc=finished_at,
                fetched_count=fetched_total,
                unique_count=unique_total,
                published_count=published_total,
                duplicate_count=duplicate_total,
                chunk_count=len(chunk_summaries),
                chunks=chunk_summaries,
                metadata={
                    "request_start_date": request.start_date.isoformat(),
                    "request_end_date": request.end_date.isoformat(),
                    "window_days": window_days,
                },
            )
            self._resources.snapshot.last_run = summary
            logger.info(
                "Backfill ingest completed",
                extra={
                    "mode": summary.mode,
                    "fetched_count": summary.fetched_count,
                    "published_count": summary.published_count,
                    "duplicate_count": summary.duplicate_count,
                    "chunk_count": summary.chunk_count,
                    "topic": summary.topic,
                },
            )
            return summary

    @staticmethod
    def _unique_features(features: list[USGSFeature]) -> list[USGSFeature]:
        unique: dict[str, USGSFeature] = {}
        for feature in features:
            unique[feature.event_id] = feature
        return list(unique.values())
