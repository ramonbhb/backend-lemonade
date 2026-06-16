from __future__ import annotations

from typing import Any

from lemonade_backend.catalog.registry import get_registry
from lemonade_backend.executors.base import ExecutionContext, ExecutionResult
from lemonade_backend.spark.delta_io import pipeline_step_path, read_delta


class PipelineRunner:
    def __init__(self, tenant_id: str, user_id: str, job_id: str, spark) -> None:
        self.tenant_id = tenant_id
        self.user_id = user_id
        self.job_id = job_id
        self.spark = spark
        self.registry = get_registry()

    def run(self, steps: list[dict[str, Any]]) -> ExecutionResult:
        input_df = None
        input_ref = None
        last_result: ExecutionResult | None = None

        for idx, step in enumerate(steps):
            operation_name = step["operation"]
            parameters = step.get("parameters", {})
            executor_cls = self.registry.get(operation_name)
            if not executor_cls:
                raise ValueError(f"Unknown pipeline operation: {operation_name}")

            if input_ref is None and step.get("input_ref"):
                input_ref = step["input_ref"]
                input_df = read_delta(self.spark, input_ref, self.tenant_id)
            elif last_result and last_result.output_ref:
                input_ref = last_result.output_ref
                input_df = read_delta(self.spark, input_ref, self.tenant_id)

            ctx = ExecutionContext(
                tenant_id=self.tenant_id,
                user_id=self.user_id,
                job_id=self.job_id,
                spark=self.spark,
                input_ref=input_ref,
                output_base_path=pipeline_step_path(self.tenant_id, self.job_id, idx),
                metadata=step.get("metadata", {}),
            )
            last_result = executor_cls().execute(ctx, parameters, input_df)

        if last_result is None:
            raise ValueError("Pipeline has no steps")
        return last_result
