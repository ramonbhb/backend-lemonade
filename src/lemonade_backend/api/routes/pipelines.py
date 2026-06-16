from __future__ import annotations

import json
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from lemonade_backend.api.routes.operations import PrivacyContext, RunOperationResponse
from lemonade_backend.audit.logger import AuditEvent, audit_logger
from lemonade_backend.auth.dependencies import AuthContext, get_auth_context
from lemonade_backend.catalog.registry import register_all_executors
from lemonade_backend.db.models import JobRecord, JobStatus
from lemonade_backend.db.session import get_db
from lemonade_backend.jobs.queue import enqueue_pipeline_job
from lemonade_backend.privacy.policy import PrivacyPolicyEngine

router = APIRouter(prefix="/v1/pipelines", tags=["pipelines"])
policy_engine = PrivacyPolicyEngine()


class PipelineStep(BaseModel):
    operation: str
    parameters: dict[str, Any] = Field(default_factory=dict)
    input_ref: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class RunPipelineRequest(BaseModel):
    tenant_id: str | None = None
    steps: list[PipelineStep]
    privacy_context: PrivacyContext = Field(default_factory=PrivacyContext)


@router.post("/run", response_model=RunOperationResponse)
async def run_pipeline(
    body: RunPipelineRequest,
    auth: AuthContext = Depends(get_auth_context),
    db: AsyncSession = Depends(get_db),
):
    if not body.steps:
        raise HTTPException(status_code=422, detail="Pipeline must have at least one step")

    register_all_executors()
    tenant_id = body.tenant_id or auth.tenant_id
    if tenant_id != auth.tenant_id and "admin" not in auth.roles:
        raise HTTPException(status_code=403, detail="Cross-tenant access denied")

    for step in body.steps:
        decision = policy_engine.evaluate(
            operation_name=step.operation,
            tenant_id=tenant_id,
            purpose=body.privacy_context.purpose,
            parameters=step.parameters,
            input_ref=step.input_ref,
            pii_policy=body.privacy_context.pii_policy,
        )
        if not decision.allowed:
            raise HTTPException(status_code=403, detail=decision.reason)

    job_id = str(uuid4())
    job = JobRecord(
        id=job_id,
        tenant_id=tenant_id,
        user_id=auth.user_id,
        operation_name="pipeline",
        status=JobStatus.QUEUED.value,
        parameters=json.dumps([s.model_dump() for s in body.steps]),
    )
    db.add(job)
    await db.commit()

    audit_ref = await audit_logger.log(
        db,
        AuditEvent(
            tenant_id=tenant_id,
            user_id=auth.user_id,
            event_type="pipeline_requested",
            operation_name="pipeline",
            job_id=job_id,
            parameters={"steps": [s.operation for s in body.steps]},
            policy_decision="allow",
        ),
    )
    job.audit_ref = audit_ref
    await db.commit()

    enqueue_pipeline_job(
        job_id=job_id,
        tenant_id=tenant_id,
        user_id=auth.user_id,
        steps=[s.model_dump() for s in body.steps],
        privacy_context=body.privacy_context.model_dump(),
    )

    return RunOperationResponse(job_id=job_id, audit_ref=audit_ref, status=JobStatus.QUEUED.value)
