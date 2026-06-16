from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from uuid import uuid4


@dataclass
class ModelCard:
    job_id: str
    tenant_id: str
    operation_name: str
    hyperparameters: dict[str, Any]
    metrics: dict[str, Any]
    dataset_ref: str | None
    lineage_ref: str | None
    fairness_summary: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "tenant_id": self.tenant_id,
            "operation_name": self.operation_name,
            "hyperparameters": self.hyperparameters,
            "metrics": self.metrics,
            "dataset_ref": self.dataset_ref,
            "lineage_ref": self.lineage_ref,
            "fairness_summary": self.fairness_summary,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


def generate_model_card(
    job_id: str,
    tenant_id: str,
    operation_name: str,
    parameters: dict[str, Any],
    metrics: dict[str, Any],
    dataset_ref: str | None,
    lineage_ref: str | None = None,
    fairness_summary: dict[str, Any] | None = None,
) -> ModelCard:
    return ModelCard(
        job_id=job_id,
        tenant_id=tenant_id,
        operation_name=operation_name,
        hyperparameters=parameters,
        metrics=metrics,
        dataset_ref=dataset_ref,
        lineage_ref=lineage_ref or str(uuid4()),
        fairness_summary=fairness_summary,
    )
