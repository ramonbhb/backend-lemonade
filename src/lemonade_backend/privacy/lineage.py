from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import httpx

from lemonade_backend.config import get_settings


class LineageEmitter:
    def __init__(self) -> None:
        self.settings = get_settings()

    def emit_job_event(
        self,
        job_id: str,
        tenant_id: str,
        operation_name: str,
        input_ref: str | None,
        output_ref: str | None,
        parameters: dict[str, Any] | None = None,
    ) -> str:
        run_id = str(uuid4())
        event = {
            "eventType": "COMPLETE",
            "eventTime": datetime.now(timezone.utc).isoformat(),
            "run": {
                "runId": run_id,
                "facets": {
                    "nominalTime": {"nominalStartTime": datetime.now(timezone.utc).isoformat()},
                    "processing_engine": {"name": "pyspark", "version": "3.5"},
                },
            },
            "job": {
                "namespace": tenant_id,
                "name": operation_name,
                "facets": {
                    "jobType": {"jobType": "BATCH", "integration": "SPARK"},
                },
            },
            "inputs": [{"namespace": tenant_id, "name": input_ref}] if input_ref else [],
            "outputs": [{"namespace": tenant_id, "name": output_ref}] if output_ref else [],
            "producer": "https://github.com/lemonade-backend",
            "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
        }
        if parameters:
            event["job"]["facets"]["parameters"] = parameters

        try:
            with httpx.Client(timeout=5.0) as client:
                client.post(
                    f"{self.settings.marquez_url}/api/v1/lineage",
                    json=event,
                )
        except Exception:
            pass
        return run_id


lineage_emitter = LineageEmitter()
