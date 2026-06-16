from __future__ import annotations

import json
import time
from typing import Any

from redis import Redis
from rq import Queue

from lemonade_backend.config import get_settings


def get_redis() -> Redis:
    return Redis.from_url(get_settings().redis_url)


def get_queue() -> Queue:
    return Queue("lemonade-jobs", connection=get_redis())


def enqueue_operation_job(
    job_id: str,
    tenant_id: str,
    user_id: str,
    operation_name: str,
    operation_id: int | None,
    parameters: dict[str, Any],
    input_ref: str | None,
    privacy_context: dict[str, Any] | None = None,
    pipeline_metadata: dict[str, Any] | None = None,
) -> str:
    queue = get_queue()
    queue.enqueue(
        "lemonade_backend.jobs.worker.run_spark_job",
        job_id=job_id,
        tenant_id=tenant_id,
        user_id=user_id,
        operation_name=operation_name,
        operation_id=operation_id,
        parameters=parameters,
        input_ref=input_ref,
        privacy_context=privacy_context or {},
        pipeline_metadata=pipeline_metadata or {},
        job_timeout="24h",
    )
    return job_id


def enqueue_pipeline_job(
    job_id: str,
    tenant_id: str,
    user_id: str,
    steps: list[dict[str, Any]],
    privacy_context: dict[str, Any] | None = None,
) -> str:
    queue = get_queue()
    queue.enqueue(
        "lemonade_backend.jobs.worker.run_pipeline_job",
        job_id=job_id,
        tenant_id=tenant_id,
        user_id=user_id,
        steps=steps,
        privacy_context=privacy_context or {},
        job_timeout="48h",
    )
    return job_id
