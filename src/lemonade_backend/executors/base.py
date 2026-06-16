from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import DataFrame, SparkSession


@dataclass
class ExecutionContext:
    tenant_id: str
    user_id: str
    job_id: str
    spark: SparkSession
    input_ref: str | None = None
    output_base_path: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ExecutionResult:
    output_ref: str
    row_count: int | None = None
    model_path: str | None = None
    metrics: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


class BaseSparkExecutor(ABC):
    operation_name: str = ""

    @abstractmethod
    def execute(
        self,
        ctx: ExecutionContext,
        params: dict[str, Any],
        input_df: DataFrame | None,
    ) -> ExecutionResult:
        pass

    def apply_optional(self, builder: Any, param_map: dict[str, tuple[str, Any]]) -> Any:
        for spark_param, (key, transform) in param_map.items():
            if key in params and params[key] is not None:
                value = params[key] if transform is None else transform(params[key])
                builder = builder.set(spark_param, value)
        return builder


def set_if_present(builder: Any, params: dict[str, Any], key: str, spark_key: str) -> Any:
    if params.get(key) is not None:
        return builder.set(spark_key, params[key])
    return builder
