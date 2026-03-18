from __future__ import annotations

from datetime import date

from app.services.usgs_client import chunk_date_range, parse_feature_collection


def sample_payload() -> dict[str, object]:
    return {
        "type": "FeatureCollection",
        "metadata": {
            "generated": 1710000000000,
            "title": "USGS All Earthquakes, Past Hour",
            "status": 200,
            "url": "https://earthquake.usgs.gov/example",
            "count": 1,
        },
        "features": [
            {
                "type": "Feature",
                "id": "us7000abcd",
                "properties": {
                    "mag": 4.6,
                    "place": "10 km S of Indios, Puerto Rico",
                    "time": 1710000000000,
                    "updated": 1710003600000,
                    "sig": 326,
                    "tsunami": 0,
                    "status": "reviewed",
                    "magType": "mb",
                    "net": "us",
                    "title": "M 4.6 - 10 km S of Indios, Puerto Rico",
                    "url": "https://earthquake.usgs.gov/earthquakes/eventpage/us7000abcd"
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [-66.81, 17.97, 12.3]
                }
            }
        ],
    }


def test_parse_feature_collection() -> None:
    collection = parse_feature_collection(sample_payload())
    assert collection.metadata.title == "USGS All Earthquakes, Past Hour"
    assert len(collection.features) == 1
    assert collection.features[0].event_id == "us7000abcd"
    assert collection.features[0].properties.magnitude == 4.6


def test_chunk_date_range_splits_windows() -> None:
    chunks = list(chunk_date_range(date(2025, 1, 1), date(2025, 2, 10), 15))
    assert chunks == [
        (date(2025, 1, 1), date(2025, 1, 15)),
        (date(2025, 1, 16), date(2025, 1, 30)),
        (date(2025, 1, 31), date(2025, 2, 10)),
    ]

