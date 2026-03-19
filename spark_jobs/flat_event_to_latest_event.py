from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from app.config import get_settings

logger = logging.getLogger("flat_event_to_latest_event")


@dataclass(frozen=True, slots=True)
class LatestEventBuildSettings:
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
    def from_args(cls, args: argparse.Namespace) -> LatestEventBuildSettings:
        settings = get_settings()
        base_path = settings.effective_storage_base_path
        return cls(
            source_path=args.source_path or f"{base_path}/flat_event",
            output_path=args.output_path or settings.latest_event_output_path,
            write_mode=args.write_mode,
            spark_master_url=settings.spark_master_url,
            spark_app_name="usgs-earthquake-flat-event-to-latest-event",
            spark_shuffle_partitions=settings.spark_shuffle_partitions,
            spark_delta_package=settings.spark_delta_package,
            spark_driver_memory="1g",
            spark_executor_memory="512m",
            spark_executor_cores=1,
            spark_cores_max=1,
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Rebuild latest_event from the flat_event Delta table.",
    )
    parser.add_argument(
        "--source-path",
        default=None,
        help="Source flat_event Delta table path. Defaults to <storage_base>/flat_event.",
    )
    parser.add_argument(
        "--output-path",
        default=None,
        help="Target latest_event Delta table path. Defaults to <storage_base>/latest_event.",
    )
    parser.add_argument(
        "--write-mode",
        choices=("overwrite", "append"),
        default="overwrite",
        help="Target Delta write mode. Defaults to overwrite.",
    )
    return parser


def create_spark_session(settings: LatestEventBuildSettings) -> SparkSession:
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


def build_latest_event_df(flat_event_df):
    latest_window = Window.partitionBy("event_id").orderBy(
        F.col("updated_time_utc").desc_nulls_last(),
        F.col("event_time_utc").desc_nulls_last(),
    )

    return (
        flat_event_df.withColumn("rn", F.row_number().over(latest_window))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .withColumn("event_date", F.to_date("event_time_utc"))
        .withColumn("year", F.year("event_time_utc"))
        .withColumn("month", F.month("event_time_utc"))
        .withColumn("day", F.dayofmonth("event_time_utc"))
    )


def run_build(settings: LatestEventBuildSettings) -> None:
    spark = create_spark_session(settings)
    spark.sparkContext.setLogLevel("WARN")

    try:
        flat_event_df = spark.read.format("delta").load(settings.source_path)
        latest_event_df = build_latest_event_df(flat_event_df)
        latest_count = latest_event_df.count()

        (
            latest_event_df.write.format("delta")
            .mode(settings.write_mode)
            .partitionBy("year", "month", "day")
            .save(settings.output_path)
        )

        logger.info(
            "latest_event rebuild completed",
            extra={
                "source_path": settings.source_path,
                "output_path": settings.output_path,
                "latest_count": latest_count,
                "write_mode": settings.write_mode,
            },
        )
    finally:
        spark.stop()


def main() -> None:
    logging.basicConfig(level=get_settings().log_level)
    parser = build_parser()
    args = parser.parse_args()
    settings = LatestEventBuildSettings.from_args(args)
    run_build(settings)


if __name__ == "__main__":
    main()
