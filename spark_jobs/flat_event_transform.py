from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from spark_jobs.schemas import RAW_EVENT_SCHEMA, raw_event_flattened_columns

PAYLOAD_JSON_SCHEMA = StructType([StructField("payload_json", StringType(), nullable=False)])


def flatten_payload_json_rows(
    spark: SparkSession,
    payload_rows: list[dict[str, str]],
) -> DataFrame:
    source_df = spark.createDataFrame(payload_rows, schema=PAYLOAD_JSON_SCHEMA)
    return (
        source_df.select(
            F.from_json(F.col("payload_json"), RAW_EVENT_SCHEMA).alias("data"),
        )
        .filter(F.col("data").isNotNull())
        .filter(F.col("data.event_id").isNotNull())
        .filter(F.col("data.feature.properties.time").isNotNull())
        .filter(F.col("data.feature.properties.type") == F.lit("earthquake"))
        .filter(F.col("data.feature.geometry.type") == F.lit("Point"))
        .select(*raw_event_flattened_columns("data"))
        .withColumn("event_date", F.to_date("event_time_utc"))
        .withColumn("year", F.year("event_time_utc"))
        .withColumn("month", F.month("event_time_utc"))
        .withColumn("day", F.dayofmonth("event_time_utc"))
    )
