from __future__ import annotations

from typing import Any

from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    ClusteringEvaluator,
    MulticlassClassificationEvaluator,
    RegressionEvaluator,
)
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import DataFrame

from lemonade_backend.executors.base import BaseSparkExecutor, ExecutionContext, ExecutionResult
from lemonade_backend.spark.delta_io import pipeline_step_path, write_delta


class EvaluatorExecutor(BaseSparkExecutor):
    operation_name = "evaluator"

    def execute(self, ctx, params, input_df):
        if input_df is None:
            raise ValueError("evaluator requires input dataframe")
        task_type = params.get("task_type", 0)
        metrics: dict[str, float] = {}

        if task_type in (0, 1):
            metric_name = params.get("bin_metric", "areaUnderROC")
            evaluator = BinaryClassificationEvaluator(
                labelCol="label",
                metricName=metric_name,
            )
            metrics[metric_name] = evaluator.evaluate(input_df)
        elif task_type == 2:
            metric_name = params.get("multi_metric", "f1")
            evaluator = MulticlassClassificationEvaluator(
                labelCol="label",
                predictionCol="prediction",
                metricName=metric_name,
            )
            metrics[metric_name] = evaluator.evaluate(input_df)
        elif task_type == 3:
            metric_name = params.get("reg_metric", "rmse")
            evaluator = RegressionEvaluator(
                labelCol="label",
                predictionCol="prediction",
                metricName=metric_name,
            )
            metrics[metric_name] = evaluator.evaluate(input_df)
        else:
            metric_name = params.get("clust_metric", "silhouette")
            evaluator = ClusteringEvaluator(
                featuresCol="features",
                predictionCol="prediction",
                metricName=metric_name,
            )
            metrics[metric_name] = evaluator.evaluate(input_df)

        out = ctx.output_base_path or pipeline_step_path(ctx.tenant_id, ctx.job_id, 1)
        result_df = ctx.spark.createDataFrame(
            [(k, v) for k, v in metrics.items()],
            ["metric", "value"],
        )
        write_delta(result_df, out, ctx.tenant_id)
        return ExecutionResult(output_ref=out, metrics=metrics, row_count=len(metrics))


class GridExecutor(BaseSparkExecutor):
    operation_name = "grid"

    def execute(self, ctx, params, input_df):
        if input_df is None:
            raise ValueError("grid requires input dataframe and a pre-configured estimator in metadata")
        estimator = ctx.metadata.get("estimator")
        if estimator is None:
            raise ValueError("grid requires estimator in pipeline metadata")

        param_grid = ParamGridBuilder().build()
        evaluator = MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
        )
        cv = CrossValidator(
            estimator=estimator,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            numFolds=params.get("parallelism", 3),
            seed=params.get("seed"),
        )
        model = cv.fit(input_df)
        predictions = model.transform(input_df)
        out = ctx.output_base_path or pipeline_step_path(ctx.tenant_id, ctx.job_id, 1)
        write_delta(predictions, out, ctx.tenant_id)
        return ExecutionResult(
            output_ref=out,
            row_count=predictions.count(),
            metadata={"grid_strategy": params.get("strategy")},
        )


EVALUATION_EXECUTORS: dict[str, type[BaseSparkExecutor]] = {
    EvaluatorExecutor.operation_name: EvaluatorExecutor,
    GridExecutor.operation_name: GridExecutor,
}
