from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F
from pyspark.sql import types as T

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame


def severity_value(magnitude: float | None) -> str:
    if magnitude is None:
        return "unknown"
    if magnitude < 4.0:
        return "small"
    if magnitude < 5.0:
        return "moderate"
    if magnitude < 6.0:
        return "strong"
    if magnitude < 7.0:
        return "severe"
    return "extreme"


def extract_region_fields_value(place: str | None) -> dict[str, str | None]:
    if place is None:
        return {"region_text": None, "city": None, "state": None}

    normalized_place = place.strip()
    if not normalized_place:
        return {"region_text": None, "city": None, "state": None}

    region_text = normalized_place.rsplit(" of ", 1)[-1].strip()
    if not region_text:
        return {"region_text": None, "city": None, "state": None}

    if "," in region_text:
        city = region_text.split(",", 1)[0].strip() or None
        state = region_text.rsplit(",", 1)[-1].strip() or None
    else:
        city = region_text
        state = None

    return {"region_text": region_text, "city": city, "state": state}


def region_text_expr(place_column: str | Column) -> Column:
    place_col = F.col(place_column) if isinstance(place_column, str) else place_column
    trimmed_place = F.trim(place_col)
    return (
        F.when(place_col.isNull(), F.lit(None).cast("string"))
        .when(trimmed_place == "", F.lit(None).cast("string"))
        .when(F.instr(trimmed_place, " of ") > 0, F.trim(F.substring_index(trimmed_place, " of ", -1)))
        .otherwise(trimmed_place)
    )


def city_expr(region_text_column: str | Column) -> Column:
    region_col = F.col(region_text_column) if isinstance(region_text_column, str) else region_text_column
    trimmed_region = F.trim(region_col)
    return (
        F.when(region_col.isNull(), F.lit(None).cast("string"))
        .when(trimmed_region == "", F.lit(None).cast("string"))
        .when(F.instr(trimmed_region, ",") > 0, F.trim(F.substring_index(trimmed_region, ",", 1)))
        .otherwise(trimmed_region)
    )


def state_expr(region_text_column: str | Column) -> Column:
    region_col = F.col(region_text_column) if isinstance(region_text_column, str) else region_text_column
    trimmed_region = F.trim(region_col)
    return (
        F.when(region_col.isNull(), F.lit(None).cast("string"))
        .when(trimmed_region == "", F.lit(None).cast("string"))
        .when(F.instr(trimmed_region, ",") > 0, F.trim(F.substring_index(trimmed_region, ",", -1)))
        .otherwise(F.lit(None).cast("string"))
    )


def severity_expr(magnitude_column: str | Column) -> Column:
    mag_col = F.col(magnitude_column) if isinstance(magnitude_column, str) else magnitude_column
    return (
        F.when(mag_col.isNull(), F.lit("unknown"))
        .when(mag_col < F.lit(4.0), F.lit("small"))
        .when(mag_col < F.lit(5.0), F.lit("moderate"))
        .when(mag_col < F.lit(6.0), F.lit("strong"))
        .when(mag_col < F.lit(7.0), F.lit("severe"))
        .otherwise(F.lit("extreme"))
    )


def build_region_hourly_stats_df(latest_event_df: DataFrame) -> DataFrame:
    enriched_df = (
        latest_event_df.filter(F.col("event_time_utc").isNotNull())
        .withColumn("bucket_start_utc", F.date_trunc("hour", F.col("event_time_utc")))
        .withColumn("region_text", region_text_expr("place"))
        .withColumn("city", city_expr("region_text"))
        .withColumn("state", state_expr("region_text"))
        .withColumn("severity", severity_expr("mag"))
        .withColumn("mag_decimal", F.col("mag").cast(T.DecimalType(10, 4)))
    )

    stats_df = (
        enriched_df.groupBy("bucket_start_utc", "region_text", "city", "state")
        .agg(
            F.count(F.lit(1)).cast("long").alias("event_count"),
            F.sum(F.when(F.col("tsunami") == 1, F.lit(1)).otherwise(F.lit(0)))
            .cast("long")
            .alias("tsunami_count"),
            F.count("mag_decimal").cast("long").alias("mag_count"),
            F.sum("mag_decimal").cast("double").alias("mag_sum"),
            F.max("mag").alias("max_mag"),
            F.avg("mag_decimal").cast("double").alias("avg_mag"),
            F.sum(F.when(F.col("severity") == "unknown", F.lit(1)).otherwise(F.lit(0)))
            .cast("long")
            .alias("unknown_count"),
            F.sum(F.when(F.col("severity") == "small", F.lit(1)).otherwise(F.lit(0)))
            .cast("long")
            .alias("small_count"),
            F.sum(F.when(F.col("severity") == "moderate", F.lit(1)).otherwise(F.lit(0)))
            .cast("long")
            .alias("moderate_count"),
            F.sum(F.when(F.col("severity") == "strong", F.lit(1)).otherwise(F.lit(0)))
            .cast("long")
            .alias("strong_count"),
            F.sum(F.when(F.col("severity") == "severe", F.lit(1)).otherwise(F.lit(0)))
            .cast("long")
            .alias("severe_count"),
            F.sum(F.when(F.col("severity") == "extreme", F.lit(1)).otherwise(F.lit(0)))
            .cast("long")
            .alias("extreme_count"),
            F.avg(F.col("longitude").cast(T.DecimalType(12, 6))).cast("double").alias("avg_longitude"),
            F.avg(F.col("latitude").cast(T.DecimalType(12, 6))).cast("double").alias("avg_latitude"),
        )
        .withColumn("year", F.year("bucket_start_utc"))
        .withColumn("month", F.month("bucket_start_utc"))
        .withColumn("day", F.dayofmonth("bucket_start_utc"))
        .withColumn(
            "document_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.date_format("bucket_start_utc", "yyyy-MM-dd'T'HH:mm:ss"),
                    F.coalesce(F.col("region_text"), F.lit("__null__")),
                    F.coalesce(F.col("city"), F.lit("__null__")),
                    F.coalesce(F.col("state"), F.lit("__null__")),
                ),
                256,
            ),
        )
    )

    return stats_df.select(
        "document_id",
        "bucket_start_utc",
        "region_text",
        "city",
        "state",
        "event_count",
        "tsunami_count",
        "mag_count",
        "mag_sum",
        "max_mag",
        "avg_mag",
        "unknown_count",
        "small_count",
        "moderate_count",
        "strong_count",
        "severe_count",
        "extreme_count",
        "avg_longitude",
        "avg_latitude",
        "year",
        "month",
        "day",
    )
