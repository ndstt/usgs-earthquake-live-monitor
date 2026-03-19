from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from pyspark.sql import functions as F

RAW_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("source_feed", StringType(), nullable=False),
        StructField("source_mode", StringType(), nullable=False),
        StructField("ingest_time_utc", StringType(), nullable=False),
        StructField("published_topic", StringType(), nullable=False),
        StructField("request_window_start", StringType(), nullable=True),
        StructField("request_window_end", StringType(), nullable=True),
        StructField(
            "feature",
            StructType(
                [
                    StructField("type", StringType(), nullable=True),
                    StructField("id", StringType(), nullable=True),
                    StructField(
                        "properties",
                        StructType(
                            [
                                StructField("mag", DoubleType(), nullable=True),
                                StructField("place", StringType(), nullable=True),
                                StructField("time", LongType(), nullable=True),
                                StructField("updated", LongType(), nullable=True),
                                StructField("tz", IntegerType(), nullable=True),
                                StructField("sig", IntegerType(), nullable=True),
                                StructField("felt", IntegerType(), nullable=True),
                                StructField("cdi", DoubleType(), nullable=True),
                                StructField("mmi", DoubleType(), nullable=True),
                                StructField("alert", StringType(), nullable=True),
                                StructField("tsunami", IntegerType(), nullable=True),
                                StructField("status", StringType(), nullable=True),
                                StructField("magType", StringType(), nullable=True),
                                StructField("net", StringType(), nullable=True),
                                StructField("code", StringType(), nullable=True),
                                StructField("ids", StringType(), nullable=True),
                                StructField("sources", StringType(), nullable=True),
                                StructField("types", StringType(), nullable=True),
                                StructField("nst", IntegerType(), nullable=True),
                                StructField("dmin", DoubleType(), nullable=True),
                                StructField("rms", DoubleType(), nullable=True),
                                StructField("gap", DoubleType(), nullable=True),
                                StructField("type", StringType(), nullable=True),
                                StructField("url", StringType(), nullable=True),
                                StructField("detail", StringType(), nullable=True),
                                StructField("title", StringType(), nullable=True),
                            ]
                        ),
                        nullable=True,
                    ),
                    StructField(
                        "geometry",
                        StructType(
                            [
                                StructField("type", StringType(), nullable=True),
                                StructField(
                                    "coordinates",
                                    ArrayType(DoubleType(), containsNull=True),
                                    nullable=True,
                                )
                            ]
                        ),
                        nullable=True,
                    ),
                ]
            ),
            nullable=False,
        ),
    ]
)

def raw_event_flattened_columns(root: str = "data") -> list[Column]:
    coordinates = F.col(f"{root}.feature.geometry.coordinates")

    return [
        F.col(f"{root}.event_id").alias("event_id"),
        F.to_timestamp(
            (F.col(f"{root}.feature.properties.time") / F.lit(1000)).cast("double")
        ).alias("event_time_utc"),
        F.to_timestamp(
            (F.col(f"{root}.feature.properties.updated") / F.lit(1000)).cast("double")
        ).alias("updated_time_utc"),
        F.col(f"{root}.feature.properties.place").alias("place"),
        F.col(f"{root}.feature.properties.mag").cast("double").alias("mag"),
        F.col(f"{root}.feature.properties.magType").alias("mag_type"),
        coordinates.getItem(0).cast("double").alias("longitude"),
        coordinates.getItem(1).cast("double").alias("latitude"),
        coordinates.getItem(2).cast("double").alias("depth_km"),
        F.col(f"{root}.feature.properties.tz").cast("int").alias("tz"),
        F.col(f"{root}.feature.properties.sig").cast("int").alias("sig"),
        F.col(f"{root}.feature.properties.felt").cast("int").alias("felt"),
        F.col(f"{root}.feature.properties.cdi").cast("double").alias("cdi"),
        F.col(f"{root}.feature.properties.mmi").cast("double").alias("mmi"),
        F.col(f"{root}.feature.properties.alert").alias("alert"),
        F.col(f"{root}.feature.properties.tsunami").cast("int").alias("tsunami"),
        F.col(f"{root}.feature.properties.status").alias("status"),
        F.col(f"{root}.feature.properties.nst").cast("int").alias("nst"),
        F.col(f"{root}.feature.properties.dmin").cast("double").alias("dmin"),
        F.col(f"{root}.feature.properties.rms").cast("double").alias("rms"),
        F.col(f"{root}.feature.properties.gap").cast("double").alias("gap"),
    ]


def comma_delimited_to_array_expr(column_name: str) -> Column:
    cleaned = F.regexp_replace(
        F.coalesce(F.col(column_name), F.lit("")),
        r"^,+|,+$",
        "",
    )
    empty_string_array = F.expr("CAST(array() AS array<string>)")
    return F.when(cleaned == "", empty_string_array).otherwise(F.split(cleaned, ","))
