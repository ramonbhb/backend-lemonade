from __future__ import annotations

import json

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from lemonade_backend.auth.dependencies import AuthContext, get_auth_context
from lemonade_backend.db.models import DatasetRecord
from lemonade_backend.db.session import get_db
from lemonade_backend.spark.delta_io import enforce_tenant_access, resolve_data_source_path

router = APIRouter(prefix="/v1/datasets", tags=["datasets"])


class DatasetCreate(BaseModel):
    id: int
    name: str
    delta_path: str | None = None
    schema: dict | None = None
    sensitive_columns: list[str] = Field(default_factory=list)


class DatasetResponse(BaseModel):
    id: int
    tenant_id: str
    name: str
    delta_path: str
    schema: dict | None = None
    sensitive_columns: list[str] = Field(default_factory=list)


def _to_response(record: DatasetRecord) -> DatasetResponse:
    return DatasetResponse(
        id=record.id,
        tenant_id=record.tenant_id,
        name=record.name,
        delta_path=record.delta_path,
        schema=json.loads(record.schema_json) if record.schema_json else None,
        sensitive_columns=json.loads(record.sensitive_columns) if record.sensitive_columns else [],
    )


@router.get("", response_model=list[DatasetResponse])
async def list_datasets(
    auth: AuthContext = Depends(get_auth_context),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(DatasetRecord).where(DatasetRecord.tenant_id == auth.tenant_id)
    )
    return [_to_response(r) for r in result.scalars().all()]


@router.post("", response_model=DatasetResponse, status_code=201)
async def register_dataset(
    body: DatasetCreate,
    auth: AuthContext = Depends(get_auth_context),
    db: AsyncSession = Depends(get_db),
):
    delta_path = body.delta_path or resolve_data_source_path(auth.tenant_id, body.id)
    try:
        enforce_tenant_access(auth.tenant_id, delta_path)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    existing = await db.get(DatasetRecord, body.id)
    if existing and existing.tenant_id != auth.tenant_id:
        raise HTTPException(status_code=403, detail="Dataset ID belongs to another tenant")

    record = existing or DatasetRecord(id=body.id, tenant_id=auth.tenant_id, name=body.name)
    record.name = body.name
    record.delta_path = delta_path
    record.schema_json = json.dumps(body.schema) if body.schema else None
    record.sensitive_columns = json.dumps(body.sensitive_columns) if body.sensitive_columns else None
    db.add(record)
    await db.commit()
    await db.refresh(record)
    return _to_response(record)


@router.get("/{dataset_id}", response_model=DatasetResponse)
async def get_dataset(
    dataset_id: int,
    auth: AuthContext = Depends(get_auth_context),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(DatasetRecord).where(
            DatasetRecord.id == dataset_id,
            DatasetRecord.tenant_id == auth.tenant_id,
        )
    )
    record = result.scalar_one_or_none()
    if not record:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return _to_response(record)
