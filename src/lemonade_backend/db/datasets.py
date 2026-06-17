from __future__ import annotations

import asyncio

from sqlalchemy import select

from lemonade_backend.db.models import DatasetRecord
from lemonade_backend.db.session import get_session_factory
from lemonade_backend.spark.delta_io import resolve_data_source_path


async def get_dataset_delta_path(tenant_id: str, data_source_id: int) -> str | None:
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(
            select(DatasetRecord).where(
                DatasetRecord.id == data_source_id,
                DatasetRecord.tenant_id == tenant_id,
            )
        )
        record = result.scalar_one_or_none()
        return record.delta_path if record else None


def resolve_read_data_path(tenant_id: str, data_source_id: int) -> str:
    """Resolve Delta path from catalog, falling back to MinIO path convention."""
    try:
        delta_path = asyncio.run(get_dataset_delta_path(tenant_id, data_source_id))
    except Exception:
        delta_path = None
    return resolve_data_source_path(tenant_id, data_source_id, delta_path)
