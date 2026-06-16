from __future__ import annotations

import asyncio
import json
import time
from typing import Any

from sqlalchemy import select

from lemonade_backend.audit.logger import AuditEvent, audit_logger
from lemonade_backend.audit.model_card import generate_model_card
from lemonade_backend.catalog.registry import get_registry, register_all_executors
from lemonade_backend.db.models import JobRecord, JobStatus
from lemonade_backend.db.session import get_session_factory
from lemonade_backend.executors.base import ExecutionContext
from lemonade_backend.pipeline.runner import PipelineRunner
from lemonade_backend.privacy.lineage import lineage_emitter
from lemonade_backend.spark.delta_io import read_delta
from lemonade_backend.spark.session import get_spark_session


async def _update_job(job_id: str, **kwargs) -> None:
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(select(JobRecord).where(JobRecord.id == job_id))
        job = result.scalar_one()
        for key, value in kwargs.items():
            setattr(job, key, value)
        await session.commit()


def run_spark_job(
    job_id: str,
    tenant_id: str,
    user_id: str,
    operation_name: str,
    operation_id: int | None,
    parameters: dict[str, Any],
    input_ref: str | None,
    privacy_context: dict[str, Any] | None = None,
    pipeline_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    register_all_executors()
    registry = get_registry()
    executor_cls = registry.get(operation_name)
    if not executor_cls:
        raise ValueError(f"Unknown operation: {operation_name}")

    start = time.time()
    asyncio.run(_update_job(job_id, status=JobStatus.RUNNING.value))

    spark = get_spark_session(app_name=f"lemonade-{job_id}")
    input_df = None
    if input_ref:
        input_df = read_delta(spark, input_ref, tenant_id)

    ctx = ExecutionContext(
        tenant_id=tenant_id,
        user_id=user_id,
        job_id=job_id,
        spark=spark,
        input_ref=input_ref,
        metadata=pipeline_metadata or {},
    )

    result = executor_cls().execute(ctx, parameters, input_df)
    duration = time.time() - start
    spark_app_id = spark.sparkContext.applicationId if spark.sparkContext else None

    lineage_ref = lineage_emitter.emit_job_event(
        job_id=job_id,
        tenant_id=tenant_id,
        operation_name=operation_name,
        input_ref=input_ref,
        output_ref=result.output_ref,
        parameters=parameters,
    )

    model_card = None
    if result.model_path:
        model_card = generate_model_card(
            job_id=job_id,
            tenant_id=tenant_id,
            operation_name=operation_name,
            parameters=parameters,
            metrics=result.metrics,
            dataset_ref=input_ref,
            lineage_ref=lineage_ref,
        )

    asyncio.run(
        _update_job(
            job_id,
            status=JobStatus.SUCCEEDED.value,
            output_ref=result.output_ref,
            spark_app_id=spark_app_id,
            row_count=result.row_count,
            duration_seconds=duration,
        )
    )

    asyncio.run(_log_completion(job_id, tenant_id, user_id, operation_name, operation_id, result))

    return {
        "job_id": job_id,
        "output_ref": result.output_ref,
        "model_path": result.model_path,
        "metrics": result.metrics,
        "model_card": model_card.to_dict() if model_card else None,
    }


def run_pipeline_job(
    job_id: str,
    tenant_id: str,
    user_id: str,
    steps: list[dict[str, Any]],
    privacy_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    register_all_executors()
    start = time.time()
    asyncio.run(_update_job(job_id, status=JobStatus.RUNNING.value))

    spark = get_spark_session(app_name=f"lemonade-pipeline-{job_id}")
    runner = PipelineRunner(tenant_id=tenant_id, user_id=user_id, job_id=job_id, spark=spark)
    result = runner.run(steps)
    duration = time.time() - start

    asyncio.run(
        _update_job(
            job_id,
            status=JobStatus.SUCCEEDED.value,
            output_ref=result.output_ref,
            row_count=result.row_count,
            duration_seconds=duration,
        )
    )
    return {"job_id": job_id, "output_ref": result.output_ref, "steps": len(steps)}


async def _log_completion(job_id, tenant_id, user_id, operation_name, operation_id, result) -> None:
    factory = get_session_factory()
    async with factory() as session:
        await audit_logger.log(
            session,
            AuditEvent(
                tenant_id=tenant_id,
                user_id=user_id,
                event_type="job_completed",
                operation_name=operation_name,
                operation_id=operation_id,
                job_id=job_id,
                output_ref=result.output_ref,
            ),
        )
