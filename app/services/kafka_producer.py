from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from kafka import KafkaProducer

from app.config import Settings
from app.models import KafkaEarthquakeEnvelope
from app.services.serializers import serialize_envelope


@dataclass(slots=True)
class PublishCounts:
    published_count: int = 0
    duplicate_count: int = 0


class RecentEventCache:
    def __init__(self, ttl_seconds: int, max_ids: int) -> None:
        self.ttl = timedelta(seconds=ttl_seconds)
        self.max_ids = max_ids
        self._seen: dict[str, datetime] = {}
        self._order: deque[tuple[str, datetime]] = deque()

    def _evict(self, now: datetime) -> None:
        cutoff = now - self.ttl
        while self._order:
            event_id, seen_at = self._order[0]
            if seen_at >= cutoff and len(self._seen) <= self.max_ids:
                break
            self._order.popleft()
            latest = self._seen.get(event_id)
            if latest == seen_at:
                self._seen.pop(event_id, None)

    def should_publish(self, event_id: str, now: datetime | None = None) -> bool:
        observed_at = now or datetime.now(tz=UTC)
        self._evict(observed_at)
        last_seen = self._seen.get(event_id)
        if last_seen and observed_at - last_seen <= self.ttl:
            return False

        self._seen[event_id] = observed_at
        self._order.append((event_id, observed_at))
        self._evict(observed_at)
        return True


class KafkaEventProducer:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._producer: KafkaProducer | None = None
        self._started = False

    @property
    def started(self) -> bool:
        return self._started

    async def start(self) -> None:
        if self._started:
            return
        self._producer = await asyncio.to_thread(self._build_producer)
        self._started = True

    async def stop(self) -> None:
        if not self._started or self._producer is None:
            return
        producer = self._producer
        await asyncio.to_thread(producer.flush)
        await asyncio.to_thread(producer.close)
        self._producer = None
        self._started = False

    def _build_producer(self) -> KafkaProducer:
        bootstrap_servers = [
            server.strip()
            for server in self._settings.kafka_bootstrap_servers.split(",")
            if server.strip()
        ]
        return KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            client_id=self._settings.kafka_client_id,
            acks=self._settings.kafka_acks,
            request_timeout_ms=self._settings.kafka_request_timeout_ms,
            enable_idempotence=self._settings.kafka_enable_idempotence,
        )

    def _send_and_wait(self, topic: str, event_id: str, payload: bytes) -> None:
        producer = self._require_producer()
        future = producer.send(
            topic,
            key=event_id.encode("utf-8"),
            value=payload,
        )
        future.get(timeout=self._settings.kafka_request_timeout_ms / 1000)

    def _require_producer(self) -> KafkaProducer:
        if self._producer is None:
            raise RuntimeError("Kafka producer has not been started")
        return self._producer

    async def publish_envelopes(
        self,
        *,
        topic: str,
        envelopes: list[KafkaEarthquakeEnvelope],
        deduper: RecentEventCache,
    ) -> PublishCounts:
        counts = PublishCounts()
        for envelope in envelopes:
            if not deduper.should_publish(envelope.event_id):
                counts.duplicate_count += 1
                continue

            await asyncio.to_thread(
                self._send_and_wait,
                topic,
                envelope.event_id,
                serialize_envelope(envelope),
            )
            counts.published_count += 1
        return counts

