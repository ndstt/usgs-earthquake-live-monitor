from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class USGSGeometry(BaseModel):
    coordinates: list[float] = Field(default_factory=list)
    geometry_type: str | None = Field(default=None, alias="type")

    model_config = ConfigDict(populate_by_name=True, extra="allow")


class USGSProperties(BaseModel):
    magnitude: float | None = Field(default=None, alias="mag")
    place: str | None = None
    event_time_ms: int | None = Field(default=None, alias="time")
    updated_time_ms: int | None = Field(default=None, alias="updated")
    significance: int | None = Field(default=None, alias="sig")
    tsunami: int | None = None
    status: str | None = None
    mag_type: str | None = Field(default=None, alias="magType")
    network: str | None = Field(default=None, alias="net")
    title: str | None = None
    url: str | None = None
    detail: str | None = None

    model_config = ConfigDict(populate_by_name=True, extra="allow")


class USGSFeature(BaseModel):
    feature_type: str = Field(alias="type")
    feature_id: str = Field(alias="id")
    properties: USGSProperties
    geometry: USGSGeometry | None = None

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    @property
    def event_id(self) -> str:
        return self.feature_id


class USGSMetadata(BaseModel):
    generated: int | None = None
    title: str | None = None
    status: int | None = None
    url: str | None = None
    count: int | None = None

    model_config = ConfigDict(extra="allow")

    @property
    def generated_time_utc(self) -> datetime | None:
        if self.generated is None:
            return None
        return datetime.fromtimestamp(self.generated / 1000, tz=UTC)


class USGSFeatureCollection(BaseModel):
    collection_type: str = Field(alias="type")
    metadata: USGSMetadata = Field(default_factory=USGSMetadata)
    features: list[USGSFeature] = Field(default_factory=list)

    model_config = ConfigDict(populate_by_name=True, extra="allow")


class KafkaEarthquakeEnvelope(BaseModel):
    event_id: str
    source_feed: str
    source_mode: Literal["realtime", "backfill"]
    ingest_time_utc: datetime
    published_topic: str
    feature: dict[str, Any]
    request_window_start: str | None = None
    request_window_end: str | None = None


class BackfillRequest(BaseModel):
    start_date: date
    end_date: date
    window_days: int | None = Field(default=None, ge=1, le=366)

    @model_validator(mode="after")
    def validate_date_order(self) -> "BackfillRequest":
        if self.end_date < self.start_date:
            raise ValueError("end_date must be on or after start_date")
        return self


class ChunkRunSummary(BaseModel):
    chunk_start: date
    chunk_end: date
    fetched_count: int
    unique_count: int
    published_count: int
    duplicate_count: int


class IngestionRunSummary(BaseModel):
    mode: Literal["realtime", "backfill"]
    topic: str
    source_feed: str
    started_at_utc: datetime
    finished_at_utc: datetime
    fetched_count: int
    unique_count: int
    published_count: int
    duplicate_count: int
    chunk_count: int = 0
    chunks: list[ChunkRunSummary] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class LastRunResponse(BaseModel):
    last_run: IngestionRunSummary | None = None


class HealthResponse(BaseModel):
    status: Literal["ok"] = "ok"
    app_name: str
    environment: str
    kafka_bootstrap_servers: str
    spark_master_url: str
    storage_base_path: str
    elasticsearch_url: str
    realtime_poller_enabled: bool


class OperationalSnapshot(BaseModel):
    last_run: IngestionRunSummary | None = None

    @field_validator("last_run")
    @classmethod
    def normalize_last_run(cls, value: IngestionRunSummary | None) -> IngestionRunSummary | None:
        return value

