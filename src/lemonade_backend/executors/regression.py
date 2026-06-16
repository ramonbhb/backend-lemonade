from __future__ import annotations

from typing import Any

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import (
    AFTSurvivalRegression,
    DecisionTreeRegressor,
    GBTRegressor,
    GeneralizedLinearRegression,
    IsotonicRegression,
    LinearRegression,
    RandomForestRegressor,
)
from pyspark.sql import DataFrame

from lemonade_backend.executors.base import BaseSparkExecutor, ExecutionContext, ExecutionResult, set_if_present
from lemonade_backend.spark.delta_io import model_artifact_path, pipeline_step_path, write_delta


def _ensure_features(df: DataFrame, label_col: str = "label") -> DataFrame:
    if "features" not in df.columns:
        feature_cols = [c for c in df.columns if c != label_col]
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        df = assembler.transform(df)
    return df


def _train_regression(
    ctx: ExecutionContext,
    input_df: DataFrame,
    estimator,
    label_col: str = "label",
) -> ExecutionResult:
    df = _ensure_features(input_df, label_col)
    model = estimator.fit(df)
    predictions = model.transform(df)
    model_path = model_artifact_path(ctx.tenant_id, ctx.job_id)
    model.write().overwrite().save(model_path)
    out = ctx.output_base_path or pipeline_step_path(ctx.tenant_id, ctx.job_id, 1)
    write_delta(predictions, out, ctx.tenant_id)
    return ExecutionResult(
        output_ref=out,
        row_count=predictions.count(),
        model_path=model_path,
    )


class LinearRegressionExecutor(BaseSparkExecutor):
    operation_name = "linear_regression"

    def execute(self, ctx, params, input_df):
        if input_df is None:
            raise ValueError("linear_regression requires input dataframe")
        lr = LinearRegression(labelCol="label", featuresCol="features")
        for key, spark_key in [
            ("loss", "loss"),
            ("max_iter", "maxIter"),
            ("solver", "solver"),
            ("elastic_net", "elasticNetParam"),
            ("reg_param", "regParam"),
            ("epsilon", "epsilon"),
            ("tolerance", "tol"),
            ("standardization", "standardization"),
            ("fit_intercept", "fitIntercept"),
        ]:
            lr = set_if_present(lr, params, key, spark_key)
        return _train_regression(ctx, input_df, lr)


class GeneralizedLinearRegressorExecutor(BaseSparkExecutor):
    operation_name = "generalized_linear_regressor"

    def execute(self, ctx, params, input_df):
        if input_df is None:
            raise ValueError("generalized_linear_regressor requires input dataframe")
        family_link = params.get("family_link", "gaussian:identity")
        family, link = family_link.split(":", 1) if ":" in family_link else (family_link, "identity")
        glr = GeneralizedLinearRegression(
            labelCol="label",
            featuresCol="features",
            family=family,
            link=link,
        )
        glr = set_if_present(glr, params, "reg_param", "regParam")
        return _train_regression(ctx, input_df, glr)


class IsotonicRegressionExecutor(BaseSparkExecutor):
    operation_name = "isotonic_regression"

    def execute(self, ctx, params, input_df):
        if input_df is None:
            raise ValueError("isotonic_regression requires input dataframe")
        isotonic = params.get("isotonic", "True") == "True"
        iso = IsotonicRegression(featuresCol="features", labelCol="label", isotonic=isotonic)
        return _train_regression(ctx, input_df, iso)


class DecisionTreeRegressorExecutor(BaseSparkExecutor):
    operation_name = "decision_tree_regressor"

    def execute(self, ctx, params, input_df):
        if input_df is None:
            raise ValueError("decision_tree_regressor requires input dataframe")
        dt = DecisionTreeRegressor(labelCol="label", featuresCol="features")
        dt = set_if_present(dt, params, "max_depth", "maxDepth")
        dt = set_if_present(dt, params, "min_instance", "minInstancesPerNode")
        dt = set_if_present(dt, params, "min_info_gain", "minInfoGain")
        return _train_regression(ctx, input_df, dt)


class RandomForestRegressorExecutor(BaseSparkExecutor):
    operation_name = "random_forest_regressor"

    def execute(self, ctx, params, input_df):
        if input_df is None:
            raise ValueError("random_forest_regressor requires input dataframe")
        rf = RandomForestRegressor(labelCol="label", featuresCol="features")
        for key, spark_key in [
            ("max_depth", "maxDepth"),
            ("max_bins", "maxBins"),
            ("min_info_gain", "minInfoGain"),
            ("num_trees", "numTrees"),
            ("feature_subset_strategy", "featureSubsetStrategy"),
        ]:
            rf = set_if_present(rf, params, key, spark_key)
        return _train_regression(ctx, input_df, rf)


class GBTRegressorExecutor(BaseSparkExecutor):
    operation_name = "gbt_regressor"

    def execute(self, ctx, params, input_df):
        if input_df is None:
            raise ValueError("gbt_regressor requires input dataframe")
        gbt = GBTRegressor(labelCol="label", featuresCol="features")
        gbt = set_if_present(gbt, params, "max_depth", "maxDepth")
        gbt = set_if_present(gbt, params, "min_instance", "minInstancesPerNode")
        gbt = set_if_present(gbt, params, "min_info_gain", "minInfoGain")
        gbt = set_if_present(gbt, params, "seed", "seed")
        return _train_regression(ctx, input_df, gbt)


class FMRegressionExecutor(BaseSparkExecutor):
    operation_name = "fm_regression"

    def execute(self, ctx, params, input_df):
        if input_df is None:
            raise ValueError("fm_regression requires input dataframe")
        from pyspark.ml.regression import FMRegressor

        fm = FMRegressor(labelCol="label", featuresCol="features")
        for key, spark_key in [
            ("factor_size", "factorSize"),
            ("seed", "seed"),
            ("fit_linear", "fitLinear"),
            ("tolerance", "tol"),
            ("step_size", "stepSize"),
            ("reg_param", "regParam"),
            ("max_iter", "maxIter"),
            ("init_std", "initStd"),
            ("min_batch", "miniBatchFraction"),
        ]:
            fm = set_if_present(fm, params, key, spark_key)
        return _train_regression(ctx, input_df, fm)


REGRESSION_EXECUTORS: dict[str, type[BaseSparkExecutor]] = {
    LinearRegressionExecutor.operation_name: LinearRegressionExecutor,
    GeneralizedLinearRegressorExecutor.operation_name: GeneralizedLinearRegressorExecutor,
    IsotonicRegressionExecutor.operation_name: IsotonicRegressionExecutor,
    DecisionTreeRegressorExecutor.operation_name: DecisionTreeRegressorExecutor,
    RandomForestRegressorExecutor.operation_name: RandomForestRegressorExecutor,
    GBTRegressorExecutor.operation_name: GBTRegressorExecutor,
    FMRegressionExecutor.operation_name: FMRegressionExecutor,
}
