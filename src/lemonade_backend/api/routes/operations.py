from __future__ import annotations

import json
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from lemonade_backend.audit.logger import AuditEvent, audit_logger
from lemonade_backend.auth.dependencies import AuthContext, get_auth_context
from lemonade_backend.catalog.loader import load_model_catalog
from lemonade_backend.catalog.schema_generator import build_all_param_models, operation_to_schema
from lemonade_backend.config import get_settings
from lemonade_backend.db.models import JobRecord, JobStatus
from lemonade_backend.db.session import get_db
from lemonade_backend.jobs.queue import enqueue_operation_job
from lemonade_backend.privacy.policy import PrivacyPolicyEngine

router = APIRouter(prefix="/v1/operations", tags=["operations"])
policy_engine = PrivacyPolicyEngine()


class PrivacyContext(BaseModel):
    purpose: str = "model_training"
    pii_policy: str = "strict"


class RunOperationRequest(BaseModel):
    tenant_id: str | None = None
    inputs: dict[str, str] = Field(default_factory=dict)
    parameters: dict[str, Any] = Field(default_factory=dict)
    privacy_context: PrivacyContext = Field(default_factory=PrivacyContext)


class RunOperationResponse(BaseModel):
    job_id: str
    audit_ref: str
    status: str


@router.get("")
async def list_operations():
    catalog = load_model_catalog(str(get_settings().model_yaml))
    return {
        "library": catalog.library,
        "operations": [operation_to_schema(op) for op in catalog.methods],
    }


@router.get("/{name}")
async def get_operation(name: str):
    catalog = load_model_catalog(str(get_settings().model_yaml))
    op = next((m for m in catalog.methods if m.name == name), None)
    if not op:
        raise HTTPException(status_code=404, detail=f"Operation '{name}' not found")
    return operation_to_schema(op)


@router.post("/{name}/run", response_model=RunOperationResponse)
async def run_operation(
    name: str,
    body: RunOperationRequest,
    auth: AuthContext = Depends(get_auth_context),
    db: AsyncSession = Depends(get_db),
):
    catalog = load_model_catalog(str(get_settings().model_yaml))
    op = next((m for m in catalog.methods if m.name == name), None)
    if not op:
        raise HTTPException(status_code=404, detail=f"Operation '{name}' not found")

    tenant_id = body.tenant_id or auth.tenant_id
    if tenant_id != auth.tenant_id and "admin" not in auth.roles:
        raise HTTPException(status_code=403, detail="Cross-tenant access denied")

    param_models = build_all_param_models(catalog)
    param_model = param_models[name]
    try:
        validated = param_model(**body.parameters)
        params = validated.model_dump()
    except Exception as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    input_ref = body.inputs.get("dataframe_ref")
    decision = policy_engine.evaluate(
        operation_name=name,
        tenant_id=tenant_id,
        purpose=body.privacy_context.purpose,
        parameters=params,
        input_ref=input_ref,
        pii_policy=body.privacy_context.pii_policy,
    )
    if not decision.allowed:
        raise HTTPException(status_code=403, detail=decision.reason)

    job_id = str(uuid4())
    job = JobRecord(
        id=job_id,
        tenant_id=tenant_id,
        user_id=auth.user_id,
        operation_name=name,
        operation_id=op.operation_id,
        status=JobStatus.QUEUED.value,
        input_ref=input_ref,
        parameters=json.dumps(params),
    )
    db.add(job)
    await db.commit()

    audit_ref = await audit_logger.log(
        db,
        AuditEvent(
            tenant_id=tenant_id,
            user_id=auth.user_id,
            event_type="operation_requested",
            operation_name=name,
            operation_id=op.operation_id,
            job_id=job_id,
            parameters=params,
            input_ref=input_ref,
            policy_decision="allow",
            policy_reason=decision.reason,
        ),
    )
    job.audit_ref = audit_ref
    await db.commit()

    enqueue_operation_job(
        job_id=job_id,
        tenant_id=tenant_id,
        user_id=auth.user_id,
        operation_name=name,
        operation_id=op.operation_id,
        parameters=params,
        input_ref=input_ref,
        privacy_context=body.privacy_context.model_dump(),
    )

    return RunOperationResponse(job_id=job_id, audit_ref=audit_ref, status=JobStatus.QUEUED.value)
