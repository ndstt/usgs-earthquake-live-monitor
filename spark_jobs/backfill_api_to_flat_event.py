from __future__ import annotations

import argparse
import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime

from pyspark.sql import SparkSession

from app.config import get_settings
from app.services.serializers import build_event_envelope
from app.services.usgs_client import USGSClient, chunk_date_range
from spark_jobs.flat_event_transform import flatten_payload_json_rows

logger = logging.getLogger("backfill_api_to_flat_event")


@dataclass(frozen=True, slots=True)
class BackfillFlatEventSettings:
    start_date: date
    end_date: date
    window_days: int
    output_path: str
    initial_write_mode: str
    hdfs_default_fs: str
    spark_master_url: str
    spark_app_name: str
    spark_shuffle_partitions: int
    spark_delta_package: str
    spark_driver_memory: str
    spark_executor_memory: str
    spark_executor_cores: int
    spark_cores_max: int
    usgs_realtime_feed_url: str
    usgs_catalog_api_url: str
    usgs_timeout_seconds: float
    usgs_max_retries: int

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> BackfillFlatEventSettings:
        settings = get_settings()
        base_path = settings.effective_storage_base_path
        return cls(
            start_date=date.fromisoformat(args.start_date),
            end_date=date.fromisoformat(args.end_date),
            window_days=args.window_days or settings.backfill_window_days,
            output_path=args.output_path or f"{base_path}/flat_event",
            initial_write_mode=args.write_mode,
            hdfs_default_fs=settings.hdfs_default_fs,
            spark_master_url=settings.spark_master_url,
            spark_app_name="usgs-earthquake-backfill-flat-event",
            spark_shuffle_partitions=settings.spark_shuffle_partitions,
            spark_delta_package=settings.spark_delta_package,
            spark_driver_memory="1g",
            spark_executor_memory="512m",
            spark_executor_cores=1,
            spark_cores_max=1,
            usgs_realtime_feed_url=settings.usgs_realtime_feed_url,
            usgs_catalog_api_url=settings.usgs_catalog_api_url,
            usgs_timeout_seconds=settings.usgs_timeout_seconds,
            usgs_max_retries=settings.usgs_max_retries,
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch historical earthquakes from the USGS API and write flat_event Delta Lake."
    )
    parser.add_argument("--start-date", required=True, help="Inclusive start date in YYYY-MM-DD.")
    parser.add_argument("--end-date", required=True, help="Inclusive end date in YYYY-MM-DD.")
    parser.add_argument(
        "--window-days",
        type=int,
        default=None,
        help="USGS fetch window size in days. Defaults to BACKFILL_WINDOW_DAYS.",
    )
    parser.add_argument(
        "--output-path",
        default=None,
        help="Flat event Delta table output path. Defaults to <storage_base>/flat_event.",
    )
    parser.add_argument(
        "--write-mode",
        choices=("append", "overwrite"),
        default="append",
        help=(
            "Initial write mode. If overwrite is chosen, the first non-empty chunk overwrites "
            "the target path and later chunks append."
        ),
    )
    return parser


def create_spark_session(settings: BackfillFlatEventSettings) -> SparkSession:
    return (
        SparkSession.builder.appName(settings.spark_app_name)
        .master(settings.spark_master_url)
        .config("spark.jars.packages", settings.spark_delta_package)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.defaultFS", settings.hdfs_default_fs)
        .config("spark.sql.shuffle.partitions", settings.spark_shuffle_partitions)
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.executor.memory", settings.spark_executor_memory)
        .config("spark.executor.cores", settings.spark_executor_cores)
        .config("spark.cores.max", settings.spark_cores_max)
        .getOrCreate()
    )


def clear_output_path(spark: SparkSession, output_path: str) -> None:
    jvm = spark._jvm
    if jvm is None:
        raise RuntimeError("Spark JVM is unavailable")

    hadoop_conf = spark._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(output_path)
    filesystem = path.getFileSystem(hadoop_conf)

    if filesystem.exists(path):
        filesystem.delete(path, True)


async def fetch_chunk_rows(
    client: USGSClient,
    *,
    chunk_start: date,
    chunk_end: date,
    seen_ids: set[str],
    source_feed_fallback: str,
) -> tuple[list[dict[str, str]], int, int]:
    collection = await client.fetch_catalog_range(start_date=chunk_start, end_date=chunk_end)
    fetched_count = len(collection.features)
    chunk_rows: list[dict[str, str]] = []
    skipped_duplicates = 0

    for feature in collection.features:
        if feature.event_id in seen_ids:
            skipped_duplicates += 1
            continue
        seen_ids.add(feature.event_id)
        envelope = build_event_envelope(
            feature=feature,
            source_feed=collection.metadata.url or source_feed_fallback,
            source_mode="backfill",
            topic="direct.api.backfill",
            ingest_time_utc=datetime.now(tz=UTC),
            request_window_start=chunk_start,
            request_window_end=chunk_end,
        )
        chunk_rows.append({"payload_json": envelope.model_dump_json(by_alias=True)})

    return chunk_rows, fetched_count, skipped_duplicates


async def run_backfill(settings: BackfillFlatEventSettings) -> None:
    if settings.end_date < settings.start_date:
        raise ValueError("end-date must be on or after start-date")

    spark = create_spark_session(settings)
    spark.sparkContext.setLogLevel("WARN")

    client = USGSClient(
        realtime_feed_url=settings.usgs_realtime_feed_url,
        catalog_api_url=settings.usgs_catalog_api_url,
        timeout_seconds=settings.usgs_timeout_seconds,
        max_retries=settings.usgs_max_retries,
    )

    seen_ids: set[str] = set()
    current_write_mode = settings.initial_write_mode

    if current_write_mode == "overwrite":
        clear_output_path(spark, settings.output_path)
        current_write_mode = "append"

    try:
        for chunk_start, chunk_end in chunk_date_range(
            settings.start_date,
            settings.end_date,
            settings.window_days,
        ):
            chunk_rows, fetched_count, skipped_duplicates = await fetch_chunk_rows(
                client,
                chunk_start=chunk_start,
                chunk_end=chunk_end,
                seen_ids=seen_ids,
                source_feed_fallback=settings.usgs_catalog_api_url,
            )

            if not chunk_rows:
                logger.info(
                    "Backfill chunk fetched no new rows",
                    extra={
                        "chunk_start": chunk_start.isoformat(),
                        "chunk_end": chunk_end.isoformat(),
                        "fetched_count": fetched_count,
                        "skipped_duplicates": skipped_duplicates,
                    },
                )
                continue

            flat_chunk_df = flatten_payload_json_rows(spark, chunk_rows)
            row_count = flat_chunk_df.count()
            if row_count == 0:
                logger.info(
                    "Backfill chunk flattened to zero rows",
                    extra={
                        "chunk_start": chunk_start.isoformat(),
                        "chunk_end": chunk_end.isoformat(),
                        "fetched_count": fetched_count,
                        "skipped_duplicates": skipped_duplicates,
                    },
                )
                continue

            (
                flat_chunk_df.write.format("delta").mode(current_write_mode)
                .partitionBy("year", "month", "day")
                .save(settings.output_path)
            )

            logger.info(
                "Backfill chunk written",
                extra={
                    "chunk_start": chunk_start.isoformat(),
                    "chunk_end": chunk_end.isoformat(),
                    "fetched_count": fetched_count,
                    "flattened_count": row_count,
                    "skipped_duplicates": skipped_duplicates,
                    "write_mode": current_write_mode,
                    "output_path": settings.output_path,
                },
            )

    finally:
        await client.close()
        spark.stop()


def main() -> None:
    logging.basicConfig(level=get_settings().log_level)
    parser = build_parser()
    args = parser.parse_args()
    settings = BackfillFlatEventSettings.from_args(args)
    asyncio.run(run_backfill(settings))


if __name__ == "__main__":
    main()
