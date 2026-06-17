from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from lemonade_backend.spark.delta_io import (
    TenantPathError,
    enforce_tenant_access,
    extract_tenant_from_uri,
)

PII_PATTERNS = {
    "cpf": re.compile(r"\b\d{3}\.?\d{3}\.?\d{3}-?\d{2}\b"),
    "email": re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"),
    "phone": re.compile(r"\b\+?\d{10,14}\b"),
}

SENSITIVE_COLUMN_NAMES = {
    "cpf",
    "ssn",
    "email",
    "phone",
    "telefone",
    "nome_completo",
    "full_name",
    "address",
    "endereco",
    "date_of_birth",
    "data_nascimento",
}

OPERATION_PURPOSES: dict[str, set[str]] = {
    "read_data": {"data_access", "model_training", "analysis"},
    "sample": {"model_training", "analysis"},
    "split": {"model_training"},
    "features": {"model_training", "analysis"},
    "evaluator": {"model_evaluation", "analysis"},
    "default": {"model_training", "model_evaluation", "analysis", "data_access"},
}


@dataclass
class PolicyDecision:
    allowed: bool
    reason: str = ""
    redaction_rules: dict[str, Any] | None = None


class PrivacyPolicyEngine:
    def evaluate(
        self,
        operation_name: str,
        tenant_id: str,
        purpose: str,
        parameters: dict[str, Any],
        input_ref: str | None,
        schema_columns: list[str] | None = None,
        pii_policy: str = "strict",
    ) -> PolicyDecision:
        allowed_purposes = OPERATION_PURPOSES.get(
            operation_name, OPERATION_PURPOSES["default"]
        )
        if purpose not in allowed_purposes:
            return PolicyDecision(
                allowed=False,
                reason=f"Purpose '{purpose}' not allowed for operation '{operation_name}'",
            )

        if input_ref:
            try:
                enforce_tenant_access(tenant_id, input_ref)
            except TenantPathError as exc:
                return PolicyDecision(allowed=False, reason=str(exc))
            uri_tenant = extract_tenant_from_uri(input_ref)
            if uri_tenant is None:
                return PolicyDecision(
                    allowed=False,
                    reason="Input path must use s3a:// MinIO URI with tenant prefix",
                )

        if schema_columns and pii_policy == "strict":
            referenced = self._extract_column_refs(parameters)
            sensitive = set(schema_columns) & SENSITIVE_COLUMN_NAMES
            blocked = referenced & sensitive
            if blocked:
                return PolicyDecision(
                    allowed=False,
                    reason=f"PII columns blocked under strict policy: {sorted(blocked)}",
                )

        return PolicyDecision(allowed=True, reason="Policy check passed")

    def _extract_column_refs(self, parameters: dict[str, Any]) -> set[str]:
        refs: set[str] = set()
        for key, value in parameters.items():
            if key in ("features", "weight_col", "weight_attr", "weight_col"):
                if isinstance(value, str):
                    refs.update(c.strip() for c in value.split(",") if c.strip())
                elif isinstance(value, list):
                    refs.update(str(v) for v in value)
        return refs

    def scan_pii_in_sample(self, sample_values: list[str]) -> list[str]:
        findings: list[str] = []
        for value in sample_values:
            for name, pattern in PII_PATTERNS.items():
                if pattern.search(str(value)):
                    findings.append(name)
        return list(set(findings))
