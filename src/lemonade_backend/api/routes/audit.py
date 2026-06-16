from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from lemonade_backend.auth.dependencies import AuthContext, get_auth_context
from lemonade_backend.db.models import AuditEventRecord
from lemonade_backend.db.session import get_db

router = APIRouter(prefix="/v1/audit", tags=["audit"])


class AuditEventResponse(BaseModel):
    id: str
    tenant_id: str
    user_id: str
    event_type: str
    operation_name: str | None
    operation_id: int | None
    job_id: str | None
    policy_decision: str
    policy_reason: str | None
    input_ref: str | None
    output_ref: str | None
    created_at: datetime


@router.get("/events", response_model=list[AuditEventResponse])
async def list_audit_events(
    auth: AuthContext = Depends(get_auth_context),
    db: AsyncSession = Depends(get_db),
    job_id: str | None = Query(None),
    limit: int = Query(50, le=200),
):
    query = select(AuditEventRecord).where(AuditEventRecord.tenant_id == auth.tenant_id)
    if job_id:
        query = query.where(AuditEventRecord.job_id == job_id)
    query = query.order_by(AuditEventRecord.created_at.desc()).limit(limit)
    result = await db.execute(query)
    events = result.scalars().all()
    return [
        AuditEventResponse(
            id=e.id,
            tenant_id=e.tenant_id,
            user_id=e.user_id,
            event_type=e.event_type,
            operation_name=e.operation_name,
            operation_id=e.operation_id,
            job_id=e.job_id,
            policy_decision=e.policy_decision,
            policy_reason=e.policy_reason,
            input_ref=e.input_ref,
            output_ref=e.output_ref,
            created_at=e.created_at,
        )
        for e in events
    ]


@router.get("/events/{event_id}", response_model=AuditEventResponse)
async def get_audit_event(
    event_id: str,
    auth: AuthContext = Depends(get_auth_context),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(AuditEventRecord).where(
            AuditEventRecord.id == event_id,
            AuditEventRecord.tenant_id == auth.tenant_id,
        )
    )
    event = result.scalar_one_or_none()
    if not event:
        raise HTTPException(status_code=404, detail="Audit event not found")
    return AuditEventResponse(
        id=event.id,
        tenant_id=event.tenant_id,
        user_id=event.user_id,
        event_type=event.event_type,
        operation_name=event.operation_name,
        operation_id=event.operation_id,
        job_id=event.job_id,
        policy_decision=event.policy_decision,
        policy_reason=event.policy_reason,
        input_ref=event.input_ref,
        output_ref=event.output_ref,
        created_at=event.created_at,
    )
