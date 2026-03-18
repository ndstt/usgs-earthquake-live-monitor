from __future__ import annotations

from spark_jobs.transforms import (
    build_dashboard_record,
    extract_region_text_value,
    magnitude_bucket_value,
)


def test_magnitude_bucket_value() -> None:
    assert magnitude_bucket_value(None) == "unknown"
    assert magnitude_bucket_value(1.9) == "< 2"
    assert magnitude_bucket_value(3.4) == "2-3.9"
    assert magnitude_bucket_value(4.8) == "4-4.9"
    assert magnitude_bucket_value(5.6) == "5-5.9"
    assert magnitude_bucket_value(6.2) == "6+"


def test_extract_region_text_value() -> None:
    assert extract_region_text_value("10 km S of Indios, Puerto Rico") == "Indios, Puerto Rico"
    assert extract_region_text_value("Southern Alaska") == "Southern Alaska"
    assert extract_region_text_value(None) is None


def test_build_dashboard_record() -> None:
    payload = {
        "event_id": "us7000abcd",
        "source_feed": "https://earthquake.usgs.gov/example",
        "source_mode": "realtime",
        "ingest_time_utc": "2026-03-18T02:20:00+00:00",
        "feature": {
            "properties": {
                "mag": 4.6,
                "place": "10 km S of Indios, Puerto Rico",
                "time": 1710000000000,
                "updated": 1710003600000,
                "sig": 326,
                "tsunami": 1,
                "status": "reviewed",
                "magType": "mb",
                "net": "us",
            },
            "geometry": {"coordinates": [-66.81, 17.97, 12.3]},
        },
    }
    record = build_dashboard_record(payload)

    assert record["event_id"] == "us7000abcd"
    assert record["magnitude_bucket"] == "4-4.9"
    assert record["region_text"] == "Indios, Puerto Rico"
    assert record["latitude"] == 17.97
    assert record["longitude"] == -66.81
    assert record["depth_km"] == 12.3
    assert record["tsunami_flag"] is True
    assert record["location"] == {"lat": 17.97, "lon": -66.81}

