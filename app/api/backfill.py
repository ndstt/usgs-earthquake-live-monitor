from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status

from app.api.deps import get_ingestion_service
from app.models import BackfillRequest, IngestionRunSummary
from app.services.ingestion_service import EarthquakeIngestionService

router = APIRouter(tags=["backfill"])


@router.post("/ingest/backfill", response_model=IngestionRunSummary)
async def ingest_backfill(
    request: BackfillRequest,
    service: Annotated[EarthquakeIngestionService, Depends(get_ingestion_service)],
) -> IngestionRunSummary:
    if service.is_busy:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Another ingestion run is already in progress",
        )
    return await service.ingest_backfill(request)
