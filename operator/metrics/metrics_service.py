from __future__ import annotations

import logging

from aioprometheus.service import Service

from .app_metrics import AppMetrics

logger = logging.getLogger(__name__)


class MetricsService:
    service = Service(registry=AppMetrics.registry)

    @classmethod
    async def start(cls, addr="0.0.0.0", port=9091) -> None:
        logging.getLogger("aiohttp").setLevel(logging.ERROR)
        await cls.service.start(addr=addr, port=port, metrics_url="/metrics")
        logger.info(f"Serving metrics on: {cls.service.metrics_url}")

    @classmethod
    async def stop(cls) -> None:
        logger.info("Stopping metrics service")
        await cls.service.stop()
