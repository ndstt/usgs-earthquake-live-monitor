from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "USGS Earthquake Live Monitor"
    environment: str = "local"
    log_level: str = "INFO"

    usgs_realtime_feed_url: str = Field(
        default=(
            "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/"
            "all_hour.geojson"
        )
    )
    usgs_catalog_api_url: str = Field(
        default="https://earthquake.usgs.gov/fdsnws/event/1/query"
    )
    usgs_timeout_seconds: float = 10.0
    usgs_max_retries: int = 3

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_client_id: str = "usgs-earthquake-live-monitor"
    kafka_raw_topic: str = "earthquake.raw.v1"
    kafka_backfill_topic: str = "earthquake.backfill.v1"
    kafka_normalized_topic: str = "earthquake.normalized.v1"
    kafka_daily_summary_topic: str = "earthquake.summary.daily.v1"
    kafka_acks: str = "all"
    kafka_request_timeout_ms: int = 15000
    kafka_enable_idempotence: bool = True

    spark_master_url: str = "spark://localhost:7077"
    spark_app_name: str = "usgs-earthquake-live-monitor-stream"
    spark_trigger_interval: str = "30 seconds"
    spark_starting_offsets: str = "latest"
    spark_shuffle_partitions: int = 4
    spark_kafka_package: str = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

    hdfs_base_path: str = "/data/earthquakes"
    local_storage_path: str = "./data/earthquakes"
    use_local_fs_for_storage: bool = False

    elasticsearch_url: str = "http://localhost:9200"
    elasticsearch_username: str | None = None
    elasticsearch_password: str | None = None
    elasticsearch_events_index_prefix: str = "earthquake-events"
    elasticsearch_daily_summary_index: str = "earthquake-daily-summary-v1"
    elasticsearch_request_timeout_seconds: float = 10.0
    elasticsearch_bulk_batch_size: int = 500

    poll_interval_seconds: int = 60
    enable_realtime_poller: bool = False
    backfill_window_days: int = 30
    dedupe_ttl_seconds: int = 3600
    dedupe_max_ids: int = 20000

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def effective_storage_base_path(self) -> str:
        return self.local_storage_path if self.use_local_fs_for_storage else self.hdfs_base_path

    @property
    def events_output_path(self) -> str:
        return f"{self.effective_storage_base_path}/events"

    @property
    def daily_summary_output_path(self) -> str:
        return f"{self.effective_storage_base_path}/daily_summary"

    @property
    def checkpoint_base_path(self) -> str:
        return f"{self.effective_storage_base_path}/checkpoints"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
