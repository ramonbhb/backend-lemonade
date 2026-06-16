from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from lemonade_backend.auth.dependencies import AuthContext, get_auth_context
from lemonade_backend.db.models import JobRecord
from lemonade_backend.db.session import get_db

router = APIRouter(prefix="/v1/jobs", tags=["jobs"])


class JobResponse(BaseModel):
    id: str
    tenant_id: str
    user_id: str
    operation_name: str
    operation_id: int | None
    status: str
    input_ref: str | None
    output_ref: str | None
    error_message: str | None
    spark_app_id: str | None
    row_count: int | None
    duration_seconds: float | None
    audit_ref: str | None


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(
    job_id: str,
    auth: AuthContext = Depends(get_auth_context),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(JobRecord).where(JobRecord.id == job_id))
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.tenant_id != auth.tenant_id and "admin" not in auth.roles:
        raise HTTPException(status_code=403, detail="Access denied")
    return JobResponse(
        id=job.id,
        tenant_id=job.tenant_id,
        user_id=job.user_id,
        operation_name=job.operation_name,
        operation_id=job.operation_id,
        status=job.status,
        input_ref=job.input_ref,
        output_ref=job.output_ref,
        error_message=job.error_message,
        spark_app_id=job.spark_app_id,
        row_count=job.row_count,
        duration_seconds=job.duration_seconds,
        audit_ref=job.audit_ref,
    )
