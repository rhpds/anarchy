from __future__ import annotations

from aioprometheus import REGISTRY, Histogram


class AppMetrics:
    registry = REGISTRY

    process_time = Histogram(
        "anarchy_process_time_seconds",
        "Execution time of processes in the app",
        {
            "method": "The method name",
            "status": "The status of the request",
            "app": "The application name",
            "cluster_domain": "The cluster name",
        },
        registry=registry,
    )
