from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

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
                                StructField("sig", IntegerType(), nullable=True),
                                StructField("tsunami", IntegerType(), nullable=True),
                                StructField("status", StringType(), nullable=True),
                                StructField("magType", StringType(), nullable=True),
                                StructField("net", StringType(), nullable=True),
                                StructField("title", StringType(), nullable=True),
                                StructField("url", StringType(), nullable=True),
                            ]
                        ),
                        nullable=True,
                    ),
                    StructField(
                        "geometry",
                        StructType(
                            [
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
