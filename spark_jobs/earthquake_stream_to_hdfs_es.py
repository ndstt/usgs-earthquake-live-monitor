from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime

import httpx
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark_jobs.schemas import RAW_EVENT_SCHEMA
from spark_jobs.transforms import normalize_stream

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("earthquake_stream")


@dataclass(frozen=True, slots=True)
class StreamJobSettings:
    kafka_bootstrap_servers: str
    raw_topic: str
    backfill_topic: str
    spark_master_url: str
    spark_app_name: str
    spark_trigger_interval: str
    spark_starting_offsets: str
    spark_shuffle_partitions: int
    spark_kafka_package: str | None
    storage_base_path: str
    elasticsearch_url: str
    elasticsearch_username: str | None
    elasticsearch_password: str | None
    elasticsearch_events_index_prefix: str
    elasticsearch_daily_summary_index: str
    elasticsearch_request_timeout_seconds: float
    elasticsearch_bulk_batch_size: int

    @property
    def events_output_path(self) -> str:
        return f"{self.storage_base_path}/events"

    @property
    def summary_output_path(self) -> str:
        return f"{self.storage_base_path}/daily_summary"

    @property
    def checkpoint_base_path(self) -> str:
        return f"{self.storage_base_path}/checkpoints"

    @classmethod
    def from_env(cls) -> "StreamJobSettings":
        use_local_fs = os.getenv("USE_LOCAL_FS_FOR_STORAGE", "false").lower() == "true"
        storage_base_path = (
            os.getenv("LOCAL_STORAGE_PATH", "./data/earthquakes")
            if use_local_fs
            else os.getenv("HDFS_BASE_PATH", "/data/earthquakes")
        )
        return cls(
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            raw_topic=os.getenv("KAFKA_RAW_TOPIC", "earthquake.raw.v1"),
            backfill_topic=os.getenv("KAFKA_BACKFILL_TOPIC", "earthquake.backfill.v1"),
            spark_master_url=os.getenv("SPARK_MASTER_URL", "spark://localhost:7077"),
            spark_app_name=os.getenv(
                "SPARK_APP_NAME",
                "usgs-earthquake-live-monitor-stream",
            ),
            spark_trigger_interval=os.getenv("SPARK_TRIGGER_INTERVAL", "30 seconds"),
            spark_starting_offsets=os.getenv("SPARK_STARTING_OFFSETS", "latest"),
            spark_shuffle_partitions=int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "4")),
            spark_kafka_package=os.getenv(
                "SPARK_KAFKA_PACKAGE",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
            ),
            storage_base_path=storage_base_path,
            elasticsearch_url=os.getenv("ELASTICSEARCH_URL", "http://localhost:9200"),
            elasticsearch_username=os.getenv("ELASTICSEARCH_USERNAME"),
            elasticsearch_password=os.getenv("ELASTICSEARCH_PASSWORD"),
            elasticsearch_events_index_prefix=os.getenv(
                "ELASTICSEARCH_EVENTS_INDEX_PREFIX",
                "earthquake-events",
            ),
            elasticsearch_daily_summary_index=os.getenv(
                "ELASTICSEARCH_DAILY_SUMMARY_INDEX",
                "earthquake-daily-summary-v1",
            ),
            elasticsearch_request_timeout_seconds=float(
                os.getenv("ELASTICSEARCH_REQUEST_TIMEOUT_SECONDS", "10.0")
            ),
            elasticsearch_bulk_batch_size=int(
                os.getenv("ELASTICSEARCH_BULK_BATCH_SIZE", "500")
            ),
        )


def create_spark_session(settings: StreamJobSettings) -> SparkSession:
    builder = (
        SparkSession.builder.appName(settings.spark_app_name)
        .master(settings.spark_master_url)
        .config("spark.sql.shuffle.partitions", settings.spark_shuffle_partitions)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    if settings.spark_kafka_package:
        builder = builder.config("spark.jars.packages", settings.spark_kafka_package)
    return builder.getOrCreate()


def read_normalized_stream(
    spark: SparkSession,
    settings: StreamJobSettings,
) -> DataFrame:
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", f"{settings.raw_topic},{settings.backfill_topic}")
        .option("startingOffsets", settings.spark_starting_offsets)
        .load()
    )
    parsed_stream = (
        raw_stream.selectExpr(
            "CAST(value AS STRING) AS value",
            "topic AS kafka_topic",
            "timestamp AS kafka_received_at",
        )
        .select(
            F.from_json(F.col("value"), RAW_EVENT_SCHEMA).alias("data"),
            "kafka_topic",
            "kafka_received_at",
        )
        .filter(F.col("data").isNotNull())
    )
    return normalize_stream(parsed_stream)


def write_events_to_elasticsearch(
    batch_df: DataFrame,
    batch_id: int,
    settings: StreamJobSettings,
) -> None:
    if batch_df.rdd.isEmpty():
        logger.info("Elasticsearch sink skipped empty batch", extra={"batch_id": batch_id})
        return

    selected = batch_df.select(
        "event_id",
        "source_feed",
        "source_mode",
        "event_time_utc",
        "event_date",
        "event_hour",
        "magnitude",
        "magnitude_bucket",
        "place",
        "region_text",
        "latitude",
        "longitude",
        "depth_km",
        "significance",
        "tsunami_flag",
        "status",
        "mag_type",
        "network",
        "updated_time_utc",
        "ingest_time_utc",
        "location",
        "event_url",
        "title",
    )
    total_rows = selected.count()
    auth = None
    if settings.elasticsearch_username and settings.elasticsearch_password:
        auth = (settings.elasticsearch_username, settings.elasticsearch_password)

    with httpx.Client(
        timeout=settings.elasticsearch_request_timeout_seconds,
        auth=auth,
    ) as client:
        buffer: list[str] = []
        sent_rows = 0

        for row in selected.toLocalIterator():
            document = row.asDict(recursive=True)
            event_time = document["event_time_utc"]
            if isinstance(event_time, datetime):
                document["event_time_utc"] = event_time.isoformat()
                monthly_suffix = event_time.strftime("%Y.%m")
            else:
                monthly_suffix = "unknown"

            for field_name in ("updated_time_utc", "ingest_time_utc"):
                field_value = document.get(field_name)
                if isinstance(field_value, datetime):
                    document[field_name] = field_value.isoformat()

            action = {
                "index": {
                    "_index": f"{settings.elasticsearch_events_index_prefix}-{monthly_suffix}",
                    "_id": document["event_id"],
                }
            }
            buffer.append(json.dumps(action))
            buffer.append(json.dumps(document, default=str))

            if len(buffer) // 2 >= settings.elasticsearch_bulk_batch_size:
                _post_bulk_payload(client, settings.elasticsearch_url, buffer)
                sent_rows += len(buffer) // 2
                buffer.clear()

        if buffer:
            _post_bulk_payload(client, settings.elasticsearch_url, buffer)
            sent_rows += len(buffer) // 2

    logger.info(
        "Elasticsearch events batch written",
        extra={
            "batch_id": batch_id,
            "row_count": total_rows,
            "sent_rows": sent_rows,
            "elasticsearch_url": settings.elasticsearch_url,
        },
    )


def write_daily_summary(
    batch_df: DataFrame,
    batch_id: int,
    settings: StreamJobSettings,
) -> None:
    if batch_df.rdd.isEmpty():
        logger.info("Daily summary sink skipped empty batch", extra={"batch_id": batch_id})
        return

    summary_df = batch_df.withColumn("summary_generated_at_utc", F.current_timestamp())

    summary_df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(
        settings.summary_output_path
    )

    auth = None
    if settings.elasticsearch_username and settings.elasticsearch_password:
        auth = (settings.elasticsearch_username, settings.elasticsearch_password)

    with httpx.Client(
        timeout=settings.elasticsearch_request_timeout_seconds,
        auth=auth,
    ) as client:
        lines: list[str] = []
        written = 0
        for row in summary_df.toLocalIterator():
            document = row.asDict(recursive=True)
            for field_name in ("latest_update_utc", "summary_generated_at_utc"):
                field_value = document.get(field_name)
                if isinstance(field_value, datetime):
                    document[field_name] = field_value.isoformat()
            action = {
                "index": {
                    "_index": settings.elasticsearch_daily_summary_index,
                    "_id": str(document["event_date"]),
                }
            }
            lines.append(json.dumps(action))
            lines.append(json.dumps(document, default=str))

        if lines:
            _post_bulk_payload(client, settings.elasticsearch_url, lines)
            written = len(lines) // 2

    logger.info(
        "Daily summary batch written",
        extra={
            "batch_id": batch_id,
            "summary_rows": written,
            "summary_output_path": settings.summary_output_path,
        },
    )


def _post_bulk_payload(client: httpx.Client, elasticsearch_url: str, lines: list[str]) -> None:
    payload = "\n".join(lines) + "\n"
    response = client.post(
        f"{elasticsearch_url}/_bulk",
        content=payload,
        headers={"Content-Type": "application/x-ndjson"},
    )
    response.raise_for_status()
    response_body = response.json()
    if response_body.get("errors"):
        raise RuntimeError(f"Elasticsearch bulk indexing reported errors: {response_body}")


def main() -> None:
    settings = StreamJobSettings.from_env()
    spark = create_spark_session(settings)
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    normalized_stream = read_normalized_stream(spark, settings)
    summary_updates = (
        normalized_stream.withWatermark("event_time_utc", "3650 days")
        .groupBy("event_date", "year", "month", "day")
        .agg(
            F.count("*").alias("earthquake_count"),
            F.avg("magnitude").alias("avg_magnitude"),
            F.avg("depth_km").alias("avg_depth_km"),
            F.max("magnitude").alias("max_magnitude"),
            F.sum(F.when(F.col("tsunami_flag"), F.lit(1)).otherwise(F.lit(0))).alias(
                "tsunami_event_count"
            ),
            F.max("updated_time_utc").alias("latest_update_utc"),
        )
    )

    _events_query = (
        normalized_stream.writeStream.format("parquet")
        .outputMode("append")
        .option("path", settings.events_output_path)
        .option("checkpointLocation", f"{settings.checkpoint_base_path}/events")
        .partitionBy("year", "month", "day")
        .trigger(processingTime=settings.spark_trigger_interval)
        .start()
    )

    _elasticsearch_query = (
        normalized_stream.writeStream.outputMode("append")
        .option(
            "checkpointLocation",
            f"{settings.checkpoint_base_path}/elasticsearch_events",
        )
        .trigger(processingTime=settings.spark_trigger_interval)
        .foreachBatch(lambda df, batch_id: write_events_to_elasticsearch(df, batch_id, settings))
        .start()
    )

    _summary_query = (
        summary_updates.writeStream.outputMode("update")
        .option(
            "checkpointLocation",
            f"{settings.checkpoint_base_path}/daily_summary",
        )
        .trigger(processingTime=settings.spark_trigger_interval)
        .foreachBatch(lambda df, batch_id: write_daily_summary(df, batch_id, settings))
        .start()
    )

    logger.info(
        "Spark Structured Streaming job started",
        extra={
            "topics": [settings.raw_topic, settings.backfill_topic],
            "events_output_path": settings.events_output_path,
            "summary_output_path": settings.summary_output_path,
            "elasticsearch_url": settings.elasticsearch_url,
        },
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
