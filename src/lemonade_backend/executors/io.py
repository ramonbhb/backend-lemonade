from __future__ import annotations

import json
from typing import Any

from pyspark.ml.feature import PCA, VectorAssembler
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from lemonade_backend.executors.base import BaseSparkExecutor, ExecutionContext, ExecutionResult
from lemonade_backend.spark.delta_io import (
    pipeline_step_path,
    read_delta,
    resolve_data_source_path,
    write_delta,
)


class ReadDataExecutor(BaseSparkExecutor):
    operation_name = "read_data"

    def execute(
        self,
        ctx: ExecutionContext,
        params: dict[str, Any],
        input_df: DataFrame | None,
    ) -> ExecutionResult:
        path = resolve_data_source_path(ctx.tenant_id, params["data_source"])
        columns = ctx.metadata.get("columns")
        df = read_delta(ctx.spark, path, ctx.tenant_id, columns)
        out = pipeline_step_path(ctx.tenant_id, ctx.job_id, 0)
        write_delta(df, out, ctx.tenant_id)
        return ExecutionResult(output_ref=out, row_count=df.count())


class SampleExecutor(BaseSparkExecutor):
    operation_name = "sample"

    def execute(
        self,
        ctx: ExecutionContext,
        params: dict[str, Any],
        input_df: DataFrame | None,
    ) -> ExecutionResult:
        if input_df is None:
            raise ValueError("sample requires input dataframe")
        sample_type = params["type"]
        seed = params.get("seed")
        if sample_type == "percent":
            fraction = params["fraction"]
            df = input_df.sample(withReplacement=False, fraction=fraction, seed=seed)
        elif sample_type == "value":
            df = input_df.orderBy(col("_rand").alias("_r") if "_rand" in input_df.columns else col("*"))
            if seed is not None:
                df = input_df.sample(False, 1.0, seed).limit(params["value"])
            else:
                df = input_df.limit(params["value"])
        elif sample_type == "head":
            df = input_df.limit(params["value"])
        else:
            raise ValueError(f"Unknown sample type: {sample_type}")

        out = ctx.output_base_path or pipeline_step_path(ctx.tenant_id, ctx.job_id, 1)
        write_delta(df, out, ctx.tenant_id)
        return ExecutionResult(output_ref=out, row_count=df.count())


class SplitExecutor(BaseSparkExecutor):
    operation_name = "split"

    def execute(
        self,
        ctx: ExecutionContext,
        params: dict[str, Any],
        input_df: DataFrame | None,
    ) -> ExecutionResult:
        if input_df is None:
            raise ValueError("split requires input dataframe")
        ratio = params["ratio"]
        seed = params.get("seed", 42)
        train_df, test_df = input_df.randomSplit([ratio, 1.0 - ratio], seed=seed)
        base = ctx.output_base_path or pipeline_step_path(ctx.tenant_id, ctx.job_id, 1)
        train_path = f"{base}/train"
        test_path = f"{base}/test"
        write_delta(train_df, train_path, ctx.tenant_id)
        write_delta(test_df, test_path, ctx.tenant_id)
        return ExecutionResult(
            output_ref=train_path,
            metadata={"test_ref": test_path},
            row_count=train_df.count(),
        )


class FeaturesExecutor(BaseSparkExecutor):
    operation_name = "features"

    def execute(
        self,
        ctx: ExecutionContext,
        params: dict[str, Any],
        input_df: DataFrame | None,
    ) -> ExecutionResult:
        if input_df is None:
            raise ValueError("features requires input dataframe")
        feature_cols = [c.strip() for c in params["features"].split(",") if c.strip()]
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        df = assembler.transform(input_df)
        out = ctx.output_base_path or pipeline_step_path(ctx.tenant_id, ctx.job_id, 1)
        write_delta(df, out, ctx.tenant_id)
        return ExecutionResult(
            output_ref=out,
            row_count=df.count(),
            metadata={"feature_columns": feature_cols},
        )


class FeaturesReductionExecutor(BaseSparkExecutor):
    operation_name = "features_reduction"

    def execute(
        self,
        ctx: ExecutionContext,
        params: dict[str, Any],
        input_df: DataFrame | None,
    ) -> ExecutionResult:
        if input_df is None:
            raise ValueError("features_reduction requires input dataframe")
        k = params["k"]
        pca = PCA(k=k, inputCol="features", outputCol="pca_features")
        model = pca.fit(input_df)
        df = model.transform(input_df)
        out = ctx.output_base_path or pipeline_step_path(ctx.tenant_id, ctx.job_id, 1)
        write_delta(df, out, ctx.tenant_id)
        return ExecutionResult(output_ref=out, row_count=df.count(), metadata={"k": k})


IO_EXECUTORS: dict[str, type[BaseSparkExecutor]] = {
    ReadDataExecutor.operation_name: ReadDataExecutor,
    SampleExecutor.operation_name: SampleExecutor,
    SplitExecutor.operation_name: SplitExecutor,
    FeaturesExecutor.operation_name: FeaturesExecutor,
    FeaturesReductionExecutor.operation_name: FeaturesReductionExecutor,
}
