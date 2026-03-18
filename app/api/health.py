from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from app.api.deps import get_settings
from app.config import Settings
from app.models import HealthResponse

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
async def health(
    settings: Annotated[Settings, Depends(get_settings)],
) -> HealthResponse:
    return HealthResponse(
        app_name=settings.app_name,
        environment=settings.environment,
        kafka_bootstrap_servers=settings.kafka_bootstrap_servers,
        spark_master_url=settings.spark_master_url,
        storage_base_path=settings.effective_storage_base_path,
        elasticsearch_url=settings.elasticsearch_url,
        realtime_poller_enabled=settings.enable_realtime_poller,
    )
