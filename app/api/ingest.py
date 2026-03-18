from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status

from app.api.deps import get_ingestion_service
from app.models import IngestionRunSummary, LastRunResponse
from app.services.ingestion_service import EarthquakeIngestionService

router = APIRouter(tags=["ingest"])


@router.post("/ingest/realtime", response_model=IngestionRunSummary)
async def ingest_realtime(
    service: Annotated[EarthquakeIngestionService, Depends(get_ingestion_service)],
) -> IngestionRunSummary:
    if service.is_busy:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Another ingestion run is already in progress",
        )
    return await service.ingest_realtime()


@router.get("/metrics/last-run", response_model=LastRunResponse, tags=["metrics"])
async def last_run_metrics(
    service: Annotated[EarthquakeIngestionService, Depends(get_ingestion_service)],
) -> LastRunResponse:
    return LastRunResponse(last_run=service.snapshot.last_run)
