from __future__ import annotations

from app.config import Settings


def test_settings_local_storage_override() -> None:
    settings = Settings(
        _env_file=None,
        use_local_fs_for_storage=True,
        local_storage_path="./tmp/earthquakes",
    )
    assert settings.effective_storage_base_path == "./tmp/earthquakes"


def test_settings_hdfs_default() -> None:
    settings = Settings(_env_file=None)
    assert settings.effective_storage_base_path == "/data/earthquakes"
    assert settings.events_output_path == "/data/earthquakes/events"
    assert settings.region_hourly_stats_output_path == "/data/earthquakes/region_hourly_stats"
    assert settings.elasticsearch_region_hourly_stats_index == "earthquake-region-hourly-v1"

