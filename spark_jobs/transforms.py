from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def magnitude_bucket_value(magnitude: float | None) -> str:
    if magnitude is None:
        return "unknown"
    if magnitude < 2.0:
        return "< 2"
    if magnitude < 4.0:
        return "2-3.9"
    if magnitude < 5.0:
        return "4-4.9"
    if magnitude < 6.0:
        return "5-5.9"
    return "6+"


def extract_region_text_value(place: str | None) -> str | None:
    if place is None:
        return None
    if " of " in place:
        return place.split(" of ", maxsplit=1)[1].strip()
    return place.strip()


def magnitude_bucket_expr(magnitude_column: Column) -> Column:
    return (
        F.when(magnitude_column.isNull(), F.lit("unknown"))
        .when(magnitude_column < 2.0, F.lit("< 2"))
        .when(magnitude_column < 4.0, F.lit("2-3.9"))
        .when(magnitude_column < 5.0, F.lit("4-4.9"))
        .when(magnitude_column < 6.0, F.lit("5-5.9"))
        .otherwise(F.lit("6+"))
    )


def region_text_expr(place_column: Column) -> Column:
    return F.when(
        place_column.contains(" of "),
        F.trim(F.substring_index(place_column, " of ", -1)),
    ).otherwise(F.trim(place_column))


def build_dashboard_record(payload: Mapping[str, Any]) -> dict[str, Any]:
    feature = payload["feature"]
    properties = feature.get("properties", {})
    geometry = feature.get("geometry", {})
    coordinates = geometry.get("coordinates", [])
    longitude = coordinates[0] if len(coordinates) > 0 else None
    latitude = coordinates[1] if len(coordinates) > 1 else None
    depth_km = coordinates[2] if len(coordinates) > 2 else None

    event_time = _from_epoch_millis(properties.get("time"))
    updated_time = _from_epoch_millis(properties.get("updated"))
    ingest_time = _from_iso_timestamp(payload.get("ingest_time_utc"))
    event_date = event_time.date().isoformat() if event_time else None
    event_hour = event_time.hour if event_time else None

    return {
        "event_id": payload.get("event_id"),
        "source_feed": payload.get("source_feed"),
        "source_mode": payload.get("source_mode"),
        "event_time_utc": event_time.isoformat() if event_time else None,
        "event_date": event_date,
        "event_hour": event_hour,
        "magnitude": properties.get("mag"),
        "magnitude_bucket": magnitude_bucket_value(properties.get("mag")),
        "place": properties.get("place"),
        "region_text": extract_region_text_value(properties.get("place")),
        "latitude": latitude,
        "longitude": longitude,
        "depth_km": depth_km,
        "significance": properties.get("sig"),
        "tsunami_flag": bool(properties.get("tsunami")),
        "status": properties.get("status"),
        "mag_type": properties.get("magType"),
        "network": properties.get("net"),
        "updated_time_utc": updated_time.isoformat() if updated_time else None,
        "ingest_time_utc": ingest_time.isoformat() if ingest_time else None,
        "location": (
            {"lat": latitude, "lon": longitude}
            if latitude is not None and longitude is not None
            else None
        ),
    }


def normalize_stream(parsed_df: DataFrame) -> DataFrame:
    feature = F.col("data.feature")
    properties = feature["properties"]
    geometry = feature["geometry"]
    coordinates = geometry["coordinates"]
    event_time_utc = F.to_timestamp((properties["time"] / F.lit(1000)).cast("double"))
    updated_time_utc = F.to_timestamp((properties["updated"] / F.lit(1000)).cast("double"))
    ingest_time_utc = F.to_timestamp(F.col("data.ingest_time_utc"))
    latitude = coordinates.getItem(1)
    longitude = coordinates.getItem(0)
    depth_km = coordinates.getItem(2)

    return (
        parsed_df.select(
            F.col("data.event_id").alias("event_id"),
            F.col("data.source_feed").alias("source_feed"),
            F.col("data.source_mode").alias("source_mode"),
            F.col("data.request_window_start").alias("request_window_start"),
            F.col("data.request_window_end").alias("request_window_end"),
            F.col("kafka_topic"),
            event_time_utc.alias("event_time_utc"),
            F.to_date(event_time_utc).alias("event_date"),
            F.hour(event_time_utc).alias("event_hour"),
            properties["mag"].cast("double").alias("magnitude"),
            magnitude_bucket_expr(properties["mag"].cast("double")).alias(
                "magnitude_bucket"
            ),
            properties["place"].alias("place"),
            region_text_expr(properties["place"]).alias("region_text"),
            latitude.cast("double").alias("latitude"),
            longitude.cast("double").alias("longitude"),
            depth_km.cast("double").alias("depth_km"),
            properties["sig"].cast("int").alias("significance"),
            (properties["tsunami"] == F.lit(1)).alias("tsunami_flag"),
            properties["status"].alias("status"),
            properties["magType"].alias("mag_type"),
            properties["net"].alias("network"),
            properties["title"].alias("title"),
            properties["url"].alias("event_url"),
            updated_time_utc.alias("updated_time_utc"),
            ingest_time_utc.alias("ingest_time_utc"),
            F.when(
                latitude.isNotNull() & longitude.isNotNull(),
                F.struct(
                    latitude.cast("double").alias("lat"),
                    longitude.cast("double").alias("lon"),
                ),
            ).alias("location"),
            F.date_format(event_time_utc, "yyyy").alias("year"),
            F.date_format(event_time_utc, "MM").alias("month"),
            F.date_format(event_time_utc, "dd").alias("day"),
        )
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("event_time_utc").isNotNull())
    )


def _from_epoch_millis(value: int | float | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromtimestamp(float(value) / 1000, tz=UTC)


def _from_iso_timestamp(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
