from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass

from pyspark.sql import SparkSession

from app.config import get_settings
from spark_jobs.region_hourly_stats import build_region_hourly_stats_df

logger = logging.getLogger("latest_event_to_region_hourly_stats")


@dataclass(frozen=True, slots=True)
class RegionHourlyStatsBuildSettings:
    source_path: str
    output_path: str
    write_mode: str
    spark_master_url: str
    spark_app_name: str
    spark_shuffle_partitions: int
    spark_delta_package: str
    spark_driver_memory: str
    spark_executor_memory: str
    spark_executor_cores: int
    spark_cores_max: int

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> RegionHourlyStatsBuildSettings:
        settings = get_settings()
        return cls(
            source_path=args.source_path or settings.latest_event_output_path,
            output_path=args.output_path or settings.region_hourly_stats_output_path,
            write_mode=args.write_mode,
            spark_master_url=settings.spark_master_url,
            spark_app_name="usgs-earthquake-latest-event-to-region-hourly-stats",
            spark_shuffle_partitions=settings.spark_shuffle_partitions,
            spark_delta_package=settings.spark_delta_package,
            spark_driver_memory="1g",
            spark_executor_memory="512m",
            spark_executor_cores=1,
            spark_cores_max=1,
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build region_hourly_stats from the latest_event Delta table.",
    )
    parser.add_argument(
        "--source-path",
        default=None,
        help="Source latest_event Delta table path. Defaults to <storage_base>/latest_event.",
    )
    parser.add_argument(
        "--output-path",
        default=None,
        help=(
            "Target region_hourly_stats Delta table path. "
            "Defaults to <storage_base>/region_hourly_stats."
        ),
    )
    parser.add_argument(
        "--write-mode",
        choices=("overwrite", "append"),
        default="overwrite",
        help="Target Delta write mode. Defaults to overwrite.",
    )
    return parser


def create_spark_session(settings: RegionHourlyStatsBuildSettings) -> SparkSession:
    return (
        SparkSession.builder.appName(settings.spark_app_name)
        .master(settings.spark_master_url)
        .config("spark.jars.packages", settings.spark_delta_package)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", settings.spark_shuffle_partitions)
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.executor.memory", settings.spark_executor_memory)
        .config("spark.executor.cores", settings.spark_executor_cores)
        .config("spark.cores.max", settings.spark_cores_max)
        .getOrCreate()
    )


def run_build(settings: RegionHourlyStatsBuildSettings) -> None:
    spark = create_spark_session(settings)
    spark.sparkContext.setLogLevel("WARN")

    try:
        latest_event_df = spark.read.format("delta").load(settings.source_path)
        region_hourly_stats_df = build_region_hourly_stats_df(latest_event_df)
        stats_count = region_hourly_stats_df.count()

        (
            region_hourly_stats_df.write.format("delta")
            .mode(settings.write_mode)
            .partitionBy("year", "month", "day")
            .save(settings.output_path)
        )

        logger.info(
            "region_hourly_stats rebuild completed",
            extra={
                "source_path": settings.source_path,
                "output_path": settings.output_path,
                "stats_count": stats_count,
                "write_mode": settings.write_mode,
            },
        )
    finally:
        spark.stop()


def main() -> None:
    logging.basicConfig(level=get_settings().log_level)
    parser = build_parser()
    args = parser.parse_args()
    settings = RegionHourlyStatsBuildSettings.from_args(args)
    run_build(settings)


if __name__ == "__main__":
    main()
