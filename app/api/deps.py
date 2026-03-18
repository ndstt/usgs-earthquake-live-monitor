from __future__ import annotations

from typing import cast

from fastapi import HTTPException, Request, status

from app.config import Settings
from app.services.ingestion_service import EarthquakeIngestionService


def get_settings(request: Request) -> Settings:
    return cast(Settings, request.app.state.settings)


def get_ingestion_service(request: Request) -> EarthquakeIngestionService:
    service = getattr(request.app.state, "ingestion_service", None)
    if service is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Ingestion service is not ready",
        )
    return cast(EarthquakeIngestionService, service)
