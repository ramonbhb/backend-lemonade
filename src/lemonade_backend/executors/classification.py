from __future__ import annotations

from typing import Any

from pyspark.ml.classification import (
    DecisionTreeClassifier,
    GBTClassifier,
    LogisticRegression,
    MultilayerPerceptronClassifier,
    NaiveBayes,
    RandomForestClassifier,
)
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import DataFrame

from lemonade_backend.executors.base import BaseSparkExecutor, ExecutionContext, ExecutionResult, set_if_present
from lemonade_backend.spark.delta_io import model_artifact_path, pipeline_step_path, write_delta


def _ensure_features(df: DataFrame, label_col: str = "label") -> DataFrame:
    if "features" not in df.columns:
        feature_cols = [c for c in df.columns if c != label_col]
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        df = assembler.transform(df)
    return df


def _train_and_predict(
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


class NaiveBayesExecutor(BaseSparkExecutor):
    operation_name = "naive_bayes"

    def execute(self, ctx, params, input_df):
        if input_df is None:
            raise ValueError("naive_bayes requires input dataframe")
        nb = NaiveBayes(labelCol="label", featuresCol="features")
        nb = set_if_present(nb, params, "model_type", "modelType")
        nb = set_if_present(nb, params, "smoothing", "smoothing")
        return _train_and_predict(ctx, input_df, nb)


class PerceptronExecutor(BaseSparkExecutor):
    operation_name = "perceptron"

    def execute(self, ctx, params, input_df):
        if input_df is None:
            raise ValueError("perceptron requires input dataframe")
        layers = [int(x) for x in params["layers"].split(",")]
        mlp = MultilayerPerceptronClassifier(
            labelCol="label",
            featuresCol="features",
            layers=layers,
        )
        mlp = set_if_present(mlp, params, "max_iter", "maxIter")
        mlp = set_if_present(mlp, params, "seed", "seed")
        mlp = set_if_present(mlp, params, "block_size", "blockSize")
        return _train_and_predict(ctx, input_df, mlp)


class DecisionTreeClassifierExecutor(BaseSparkExecutor):
    operation_name = "decision_tree_classifier"

    def execute(self, ctx, params, input_df):
        if input_df is None:
            raise ValueError("decision_tree_classifier requires input dataframe")
        dt = DecisionTreeClassifier(labelCol="label", featuresCol="features")
        for key, spark_key in [
            ("impurity", "impurity"),
            ("max_bins", "maxBins"),
            ("max_depth", "maxDepth"),
            ("min_info_gain", "minInfoGain"),
            ("min_instances_per_node", "minInstancesPerNode"),
            ("check_point_interval", "checkpointInterval"),
            ("cache_node_ids", "cacheNodeIds"),
        ]:
            dt = set_if_present(dt, params, key, spark_key)
        return _train_and_predict(ctx, input_df, dt)


class RandomForestClassifierExecutor(BaseSparkExecutor):
    operation_name = "random_forest_classifier"

    def execute(self, ctx, params, input_df):
        if input_df is None:
            raise ValueError("random_forest_classifier requires input dataframe")
        rf = RandomForestClassifier(labelCol="label", featuresCol="features")
        for key, spark_key in [
            ("impurity", "impurity"),
            ("max_bins", "maxBins"),
            ("max_depth", "maxDepth"),
            ("min_info_gain", "minInfoGain"),
            ("min_instances_per_node", "minInstancesPerNode"),
            ("num_trees", "numTrees"),
            ("seed", "seed"),
            ("subsampling_rate", "subsamplingRate"),
            ("feature_subset_strategy", "featureSubsetStrategy"),
            ("check_point_interval", "checkpointInterval"),
            ("cache_node_ids", "cacheNodeIds"),
        ]:
            rf = set_if_present(rf, params, key, spark_key)
        return _train_and_predict(ctx, input_df, rf)


class GBTClassifierExecutor(BaseSparkExecutor):
    operation_name = "gbt_classifier"

    def execute(self, ctx, params, input_df):
        if input_df is None:
            raise ValueError("gbt_classifier requires input dataframe")
        gbt = GBTClassifier(labelCol="label", featuresCol="features")
        for key, spark_key in [
            ("loss_type", "lossType"),
            ("max_bins", "maxBins"),
            ("max_depth", "maxDepth"),
            ("max_iter", "maxIter"),
            ("min_info_gain", "minInfoGain"),
            ("min_instances_per_node", "minInstancesPerNode"),
            ("step_size", "stepSize"),
            ("seed", "seed"),
            ("subsampling_rate", "subsamplingRate"),
            ("check_point_interval", "checkpointInterval"),
            ("cache_node_ids", "cacheNodeIds"),
        ]:
            gbt = set_if_present(gbt, params, key, spark_key)
        return _train_and_predict(ctx, input_df, gbt)


class LogisticRegressionExecutor(BaseSparkExecutor):
    operation_name = "logistic_regression"

    def execute(self, ctx, params, input_df):
        if input_df is None:
            raise ValueError("logistic_regression requires input dataframe")
        lr = LogisticRegression(labelCol="label", featuresCol="features")
        for key, spark_key in [
            ("family", "family"),
            ("max_iter", "maxIter"),
            ("tol", "tol"),
            ("reg_param", "regParam"),
            ("elastic_net_param", "elasticNetParam"),
            ("fit_intercept", "fitIntercept"),
            ("threshold", "threshold"),
            ("aggregation_depth", "aggregationDepth"),
        ]:
            lr = set_if_present(lr, params, key, spark_key)
        if params.get("weight_col"):
            lr = lr.setWeightCol(params["weight_col"][0])
        return _train_and_predict(ctx, input_df, lr)


class SVMExecutor(BaseSparkExecutor):
    operation_name = "svm"

    def execute(self, ctx, params, input_df):
        if input_df is None:
            raise ValueError("svm requires input dataframe")
        from pyspark.ml.classification import LinearSVC

        df = _ensure_features(input_df)
        if params.get("standardization"):
            scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
            scaler_model = scaler.fit(df)
            df = scaler_model.transform(df)
            features_col = "scaled_features"
        else:
            features_col = "features"
        svc = LinearSVC(labelCol="label", featuresCol=features_col)
        svc = set_if_present(svc, params, "max_iter", "maxIter")
        svc = set_if_present(svc, params, "tol", "tol")
        svc = set_if_present(svc, params, "threshold", "threshold")
        return _train_and_predict(ctx, df, svc)


class FMClassifierExecutor(BaseSparkExecutor):
    operation_name = "fm_classifier"

    def execute(self, ctx, params, input_df):
        if input_df is None:
            raise ValueError("fm_classifier requires input dataframe")
        from pyspark.ml.classification import FMClassifier

        fm = FMClassifier(labelCol="label", featuresCol="features")
        for key, spark_key in [
            ("factor_size", "factorSize"),
            ("seed", "seed"),
            ("fit_linear", "fitLinear"),
            ("tol", "tol"),
            ("tol", "tolerance"),
            ("step_size", "stepSize"),
            ("reg_param", "regParam"),
            ("max_iter", "maxIter"),
            ("init_std", "initStd"),
            ("min_batch", "miniBatchFraction"),
        ]:
            fm = set_if_present(fm, params, key, spark_key)
        return _train_and_predict(ctx, input_df, fm)


CLASSIFICATION_EXECUTORS: dict[str, type[BaseSparkExecutor]] = {
    NaiveBayesExecutor.operation_name: NaiveBayesExecutor,
    PerceptronExecutor.operation_name: PerceptronExecutor,
    DecisionTreeClassifierExecutor.operation_name: DecisionTreeClassifierExecutor,
    RandomForestClassifierExecutor.operation_name: RandomForestClassifierExecutor,
    GBTClassifierExecutor.operation_name: GBTClassifierExecutor,
    LogisticRegressionExecutor.operation_name: LogisticRegressionExecutor,
    SVMExecutor.operation_name: SVMExecutor,
    FMClassifierExecutor.operation_name: FMClassifierExecutor,
}
