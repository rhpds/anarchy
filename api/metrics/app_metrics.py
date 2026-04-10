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
            "component": "The component (operator or api)",
            "cluster_domain": "The cluster name",
        },
        registry=registry,
    )

    http_request_duration = Histogram(
        "anarchy_api_http_request_duration_seconds",
        "HTTP request duration for Anarchy API endpoints",
        {
            "method": "The HTTP method (GET, POST, PATCH)",
            "route": "The handler function name (e.g. get_run, post_action)",
            "status": "The HTTP status code",
            "cluster_domain": "The cluster name",
        },
        registry=registry,
    )
