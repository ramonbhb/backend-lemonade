from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from lemonade_backend.db.models import AuditEventRecord


@dataclass
class AuditEvent:
    tenant_id: str
    user_id: str
    event_type: str
    operation_name: str | None = None
    operation_id: int | None = None
    job_id: str | None = None
    parameters: dict[str, Any] | None = None
    input_ref: str | None = None
    output_ref: str | None = None
    policy_decision: str = "allow"
    policy_reason: str | None = None
    spark_app_id: str | None = None
    metadata: dict[str, Any] | None = None


SENSITIVE_KEYS = {"password", "token", "secret", "api_key", "cpf", "ssn"}


def sanitize_params(params: dict[str, Any] | None) -> dict[str, Any]:
    if not params:
        return {}
    sanitized = {}
    for key, value in params.items():
        if any(s in key.lower() for s in SENSITIVE_KEYS):
            sanitized[key] = "***REDACTED***"
        else:
            sanitized[key] = value
    return sanitized


class AuditLogger:
    async def log(self, session: AsyncSession, event: AuditEvent) -> str:
        event_id = str(uuid4())
        record = AuditEventRecord(
            id=event_id,
            tenant_id=event.tenant_id,
            user_id=event.user_id,
            event_type=event.event_type,
            operation_name=event.operation_name,
            operation_id=event.operation_id,
            job_id=event.job_id,
            parameters_sanitized=json.dumps(sanitize_params(event.parameters)),
            input_ref=event.input_ref,
            output_ref=event.output_ref,
            policy_decision=event.policy_decision,
            policy_reason=event.policy_reason,
            spark_app_id=event.spark_app_id,
            metadata_json=json.dumps(event.metadata or {}),
        )
        session.add(record)
        await session.commit()
        return event_id


audit_logger = AuditLogger()
