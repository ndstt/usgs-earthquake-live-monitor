from __future__ import annotations

import asyncio
from collections.abc import Iterator, Mapping
from datetime import date, timedelta
from typing import Any, cast

import httpx

from app.models import USGSFeatureCollection


class USGSClient:
    def __init__(
        self,
        *,
        realtime_feed_url: str,
        catalog_api_url: str,
        timeout_seconds: float,
        max_retries: int,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._realtime_feed_url = realtime_feed_url
        self._catalog_api_url = catalog_api_url
        self._max_retries = max_retries
        self._owns_http_client = http_client is None
        self._http_client = http_client or httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_seconds)
        )

    async def close(self) -> None:
        if self._owns_http_client:
            await self._http_client.aclose()

    async def fetch_realtime_feed(self) -> USGSFeatureCollection:
        payload = await self._request_json(self._realtime_feed_url)
        return parse_feature_collection(payload)

    async def fetch_catalog_range(
        self,
        *,
        start_date: date,
        end_date: date,
    ) -> USGSFeatureCollection:
        payload = await self._request_json(
            self._catalog_api_url,
            params={
                "format": "geojson",
                "starttime": start_date.isoformat(),
                "endtime": end_date.isoformat(),
                "orderby": "time-asc",
                "limit": 20000,
            },
        )
        return parse_feature_collection(payload)

    async def _request_json(
        self,
        url: str,
        params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        for attempt in range(1, self._max_retries + 1):
            try:
                response = await self._http_client.get(url, params=params)
                response.raise_for_status()
                return cast(dict[str, Any], response.json())
            except httpx.HTTPError:
                if attempt >= self._max_retries:
                    raise
                await asyncio.sleep(min(attempt, 5))
        raise RuntimeError("USGS request retry loop exhausted")


def parse_feature_collection(payload: Mapping[str, Any]) -> USGSFeatureCollection:
    return USGSFeatureCollection.model_validate(payload)


def chunk_date_range(
    start_date: date,
    end_date: date,
    window_days: int,
) -> Iterator[tuple[date, date]]:
    cursor = start_date
    while cursor <= end_date:
        chunk_end = min(cursor + timedelta(days=window_days - 1), end_date)
        yield cursor, chunk_end
        cursor = chunk_end + timedelta(days=1)
