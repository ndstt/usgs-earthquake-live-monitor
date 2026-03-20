from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from dataclasses import dataclass

from app.services.flat_event import delta_table_uri
from app.config import Settings
from app.loaders.latest_event_to_elasticsearch import LatestEventLoaderSettings, run_load as run_latest_event_load
from app.loaders.region_hourly_stats_to_elasticsearch import (
    RegionHourlyStatsLoaderSettings,
    run_load as run_region_hourly_stats_load,
)
from spark_jobs.latest_event_to_region_hourly_stats import (
    RegionHourlyStatsBuildSettings,
    run_build as run_region_hourly_stats_build,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class PipelineOrchestrator:
    settings: Settings
    latest_event_lock: asyncio.Lock
    region_hourly_lock: asyncio.Lock

    def _delta_table_exists(self, output_path: str) -> bool:
        try:
            from deltalake import DeltaTable
        except ModuleNotFoundError:
            logger.warning("deltalake is unavailable; cannot check table existence", extra={"path": output_path})
            return False

        table_uri = delta_table_uri(
            output_path=output_path,
            use_local_fs=self.settings.use_local_fs_for_storage,
            hdfs_default_fs=self.settings.hdfs_default_fs,
        )
        return DeltaTable.is_deltatable(table_uri)

    def latest_event_loader_settings(self) -> LatestEventLoaderSettings:
        return LatestEventLoaderSettings(
            source_path=self.settings.latest_event_output_path,
            elasticsearch_url=self.settings.elasticsearch_url.rstrip("/"),
            index_name=self.settings.elasticsearch_latest_event_index,
            username=self.settings.elasticsearch_username,
            password=self.settings.elasticsearch_password,
            batch_size=self.settings.elasticsearch_bulk_batch_size,
            request_timeout_seconds=self.settings.elasticsearch_request_timeout_seconds,
            hdfs_default_fs=self.settings.hdfs_default_fs,
            use_local_fs_for_storage=self.settings.use_local_fs_for_storage,
            refresh_index=True,
        )

    def region_hourly_build_settings(self) -> RegionHourlyStatsBuildSettings:
        return RegionHourlyStatsBuildSettings(
            source_path=self.settings.latest_event_output_path,
            output_path=self.settings.region_hourly_stats_output_path,
            write_mode="overwrite",
            hdfs_default_fs=self.settings.hdfs_default_fs,
            spark_master_url=self.settings.spark_master_url,
            spark_app_name="usgs-earthquake-latest-event-to-region-hourly-stats",
            spark_shuffle_partitions=self.settings.spark_shuffle_partitions,
            spark_delta_package=self.settings.spark_delta_package,
            spark_driver_memory="1g",
            spark_executor_memory="512m",
            spark_executor_cores=1,
            spark_cores_max=1,
        )

    def region_hourly_loader_settings(self) -> RegionHourlyStatsLoaderSettings:
        return RegionHourlyStatsLoaderSettings(
            source_path=self.settings.region_hourly_stats_output_path,
            elasticsearch_url=self.settings.elasticsearch_url.rstrip("/"),
            index_name=self.settings.elasticsearch_region_hourly_stats_index,
            username=self.settings.elasticsearch_username,
            password=self.settings.elasticsearch_password,
            batch_size=self.settings.elasticsearch_bulk_batch_size,
            request_timeout_seconds=self.settings.elasticsearch_request_timeout_seconds,
            hdfs_default_fs=self.settings.hdfs_default_fs,
            use_local_fs_for_storage=self.settings.use_local_fs_for_storage,
            refresh_index=True,
        )

    async def sync_latest_event(self) -> int:
        async with self.latest_event_lock:
            if not self._delta_table_exists(self.settings.latest_event_output_path):
                logger.warning(
                    "latest_event Delta table does not exist yet; skipping Elasticsearch sync",
                    extra={"source_path": self.settings.latest_event_output_path},
                )
                return 0
            loaded_count = await asyncio.to_thread(
                run_latest_event_load,
                self.latest_event_loader_settings(),
            )
            logger.info(
                "latest_event Elasticsearch sync completed",
                extra={
                    "loaded_count": loaded_count,
                    "index_name": self.settings.elasticsearch_latest_event_index,
                },
            )
            return loaded_count

    async def refresh_region_hourly_pipeline(self) -> None:
        async with self.region_hourly_lock:
            if not self._delta_table_exists(self.settings.latest_event_output_path):
                logger.warning(
                    "latest_event Delta table does not exist yet; skipping region_hourly pipeline",
                    extra={"source_path": self.settings.latest_event_output_path},
                )
                return
            await asyncio.to_thread(
                run_region_hourly_stats_build,
                self.region_hourly_build_settings(),
            )
            loaded_count = await asyncio.to_thread(
                run_region_hourly_stats_load,
                self.region_hourly_loader_settings(),
            )
            logger.info(
                "region_hourly_stats pipeline refresh completed",
                extra={
                    "loaded_count": loaded_count,
                    "index_name": self.settings.elasticsearch_region_hourly_stats_index,
                },
            )


async def periodic_loop(
    *,
    name: str,
    interval_seconds: int,
    stop_event: asyncio.Event,
    operation,
) -> None:
    while not stop_event.is_set():
        try:
            await operation()
        except Exception:
            logger.exception("%s cycle failed", name)

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
        except TimeoutError:
            continue


async def cancel_task(task: asyncio.Task[None] | None) -> None:
    if task is None:
        return
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task
