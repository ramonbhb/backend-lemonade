from __future__ import annotations

import re

from lemonade_backend.config import get_settings


class TenantPathError(Exception):
    pass


_TENANT_PREFIX = re.compile(r"^s3a?://[^/]+/([^/]+)/")


def tenant_base_path(tenant_id: str) -> str:
    settings = get_settings()
    return f"s3a://{settings.minio_bucket}/{tenant_id}"


def pipeline_step_path(tenant_id: str, job_id: str, step: int) -> str:
    return f"{tenant_base_path(tenant_id)}/pipelines/{job_id}/step-{step}"


def model_artifact_path(tenant_id: str, job_id: str) -> str:
    return f"{tenant_base_path(tenant_id)}/models/{job_id}"


def audit_artifact_path(tenant_id: str, event_id: str) -> str:
    settings = get_settings()
    return f"s3a://{settings.audit_bucket}/{tenant_id}/{event_id}.json"


def extract_tenant_from_uri(uri: str) -> str | None:
    if not uri:
        return None
    match = _TENANT_PREFIX.match(uri)
    return match.group(1) if match else None


def enforce_tenant_access(tenant_id: str, uri: str) -> None:
    if not uri:
        return
    uri_tenant = extract_tenant_from_uri(uri)
    if uri_tenant and uri_tenant != tenant_id:
        raise TenantPathError(f"Cross-tenant access denied: {uri}")


def resolve_data_source_path(
    tenant_id: str,
    data_source_id: int,
    delta_path: str | None = None,
) -> str:
    if delta_path:
        enforce_tenant_access(tenant_id, delta_path)
        return delta_path
    return f"{tenant_base_path(tenant_id)}/datasets/{data_source_id}"


def write_delta(df, path: str, tenant_id: str, mode: str = "overwrite") -> str:
    enforce_tenant_access(tenant_id, path)
    df.write.format("delta").mode(mode).save(path)
    return path


def read_delta(spark, path: str, tenant_id: str, columns: list[str] | None = None):
    enforce_tenant_access(tenant_id, path)
    reader = spark.read.format("delta")
    df = reader.load(path)
    if columns:
        df = df.select(*columns)
    return df
