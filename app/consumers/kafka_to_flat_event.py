from __future__ import annotations

import argparse
import logging
import signal
from dataclasses import dataclass
from time import monotonic

from kafka import KafkaConsumer

from app.config import get_settings
from app.services.flat_event import append_flat_event_rows, flatten_payload_batch
from app.services.latest_event import upsert_latest_event_rows

logger = logging.getLogger("kafka_to_flat_event_consumer")


@dataclass(frozen=True, slots=True)
class KafkaToFlatEventSettings:
    topic: str
    flat_event_output_path: str
    latest_event_output_path: str
    bootstrap_servers: list[str]
    group_id: str
    client_id: str
    auto_offset_reset: str
    poll_timeout_ms: int
    max_batch_size: int
    max_batch_wait_seconds: float
    hdfs_default_fs: str
    use_local_fs_for_storage: bool

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> KafkaToFlatEventSettings:
        settings = get_settings()
        base_path = settings.effective_storage_base_path
        bootstrap_servers = [
            server.strip()
            for server in settings.kafka_bootstrap_servers.split(",")
            if server.strip()
        ]
        group_id = args.group_id or f"{settings.kafka_client_id}-flat-event-consumer"

        return cls(
            topic=args.topic or settings.kafka_raw_topic,
            flat_event_output_path=args.output_path or f"{base_path}/flat_event",
            latest_event_output_path=args.latest_event_output_path or settings.latest_event_output_path,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            client_id=f"{settings.kafka_client_id}-flat-event-consumer",
            auto_offset_reset=args.auto_offset_reset,
            poll_timeout_ms=args.poll_timeout_ms,
            max_batch_size=args.max_batch_size,
            max_batch_wait_seconds=args.max_batch_wait_seconds,
            hdfs_default_fs=settings.hdfs_default_fs,
            use_local_fs_for_storage=settings.use_local_fs_for_storage,
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Consume Kafka raw earthquake events, flatten them, and append flat_event Delta Lake."
    )
    parser.add_argument(
        "--topic",
        default=None,
        help="Kafka topic to consume. Defaults to KAFKA_RAW_TOPIC.",
    )
    parser.add_argument(
        "--output-path",
        default=None,
        help="Flat event Delta table output path. Defaults to <storage_base>/flat_event.",
    )
    parser.add_argument(
        "--latest-event-output-path",
        default=None,
        help="Latest event Delta table output path. Defaults to <storage_base>/latest_event.",
    )
    parser.add_argument(
        "--group-id",
        default=None,
        help="Kafka consumer group id. Defaults to <client_id>-flat-event-consumer.",
    )
    parser.add_argument(
        "--auto-offset-reset",
        choices=("earliest", "latest"),
        default="latest",
        help="Kafka offset reset policy when no committed offset exists.",
    )
    parser.add_argument(
        "--poll-timeout-ms",
        type=int,
        default=5000,
        help="Kafka poll timeout in milliseconds.",
    )
    parser.add_argument(
        "--max-batch-size",
        type=int,
        default=100,
        help="Maximum Kafka messages to write in a single Delta append batch.",
    )
    parser.add_argument(
        "--max-batch-wait-seconds",
        type=float,
        default=60.0,
        help="Flush a partial batch after this many seconds even if batch size is not reached.",
    )
    return parser


def create_consumer(settings: KafkaToFlatEventSettings) -> KafkaConsumer:
    return KafkaConsumer(
        settings.topic,
        bootstrap_servers=settings.bootstrap_servers,
        client_id=settings.client_id,
        group_id=settings.group_id,
        enable_auto_commit=False,
        auto_offset_reset=settings.auto_offset_reset,
    )


def normalize_payload(record_value: bytes | str) -> str:
    if isinstance(record_value, bytes):
        return record_value.decode("utf-8")
    return record_value


class StopRequested:
    def __init__(self) -> None:
        self.requested = False

    def request(self, signum: int, _frame: object) -> None:
        self.requested = True
        logger.info("Stop requested", extra={"signal": signum})


def run_consumer(settings: KafkaToFlatEventSettings) -> None:
    consumer = create_consumer(settings)
    stopper = StopRequested()

    signal.signal(signal.SIGINT, stopper.request)
    signal.signal(signal.SIGTERM, stopper.request)

    pending_rows: list[str] = []
    pending_message_count = 0
    last_flush_at = monotonic()

    try:
        while not stopper.requested:
            polled_records = consumer.poll(
                timeout_ms=settings.poll_timeout_ms,
                max_records=settings.max_batch_size,
            )

            fetched_count = 0
            for records in polled_records.values():
                for record in records:
                    pending_rows.append(normalize_payload(record.value))
                    fetched_count += 1

            if fetched_count:
                pending_message_count += fetched_count

            now = monotonic()
            should_flush = bool(pending_rows) and (
                len(pending_rows) >= settings.max_batch_size
                or now - last_flush_at >= settings.max_batch_wait_seconds
            )

            if not should_flush:
                continue

            flat_rows = flatten_payload_batch(pending_rows)
            written_count = append_flat_event_rows(
                output_path=settings.flat_event_output_path,
                flat_rows=flat_rows,
                hdfs_default_fs=settings.hdfs_default_fs,
                use_local_fs=settings.use_local_fs_for_storage,
            )
            latest_count = upsert_latest_event_rows(
                output_path=settings.latest_event_output_path,
                flat_rows=flat_rows,
                hdfs_default_fs=settings.hdfs_default_fs,
                use_local_fs=settings.use_local_fs_for_storage,
            )
            consumer.commit()
            logger.info(
                "Kafka batch appended to flat_event and merged into latest_event",
                extra={
                    "topic": settings.topic,
                    "consumed_count": pending_message_count,
                    "flat_event_written_count": written_count,
                    "latest_event_affected_count": latest_count,
                    "flat_event_output_path": settings.flat_event_output_path,
                    "latest_event_output_path": settings.latest_event_output_path,
                },
            )
            pending_rows.clear()
            pending_message_count = 0
            last_flush_at = monotonic()
    finally:
        if pending_rows:
            flat_rows = flatten_payload_batch(pending_rows)
            written_count = append_flat_event_rows(
                output_path=settings.flat_event_output_path,
                flat_rows=flat_rows,
                hdfs_default_fs=settings.hdfs_default_fs,
                use_local_fs=settings.use_local_fs_for_storage,
            )
            latest_count = upsert_latest_event_rows(
                output_path=settings.latest_event_output_path,
                flat_rows=flat_rows,
                hdfs_default_fs=settings.hdfs_default_fs,
                use_local_fs=settings.use_local_fs_for_storage,
            )
            consumer.commit()
            logger.info(
                "Kafka final batch appended to flat_event and merged into latest_event",
                extra={
                    "topic": settings.topic,
                    "consumed_count": pending_message_count,
                    "flat_event_written_count": written_count,
                    "latest_event_affected_count": latest_count,
                    "flat_event_output_path": settings.flat_event_output_path,
                    "latest_event_output_path": settings.latest_event_output_path,
                },
            )

        consumer.close()


def main() -> None:
    logging.basicConfig(level=get_settings().log_level)
    parser = build_parser()
    args = parser.parse_args()
    settings = KafkaToFlatEventSettings.from_args(args)
    run_consumer(settings)


if __name__ == "__main__":
    main()
