from __future__ import annotations

from typing import Any

from pyspark.ml.clustering import BisectingKMeans, GaussianMixture, KMeans, LDA, PowerIterationClustering
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame

from lemonade_backend.executors.base import BaseSparkExecutor, ExecutionContext, ExecutionResult, set_if_present
from lemonade_backend.spark.delta_io import model_artifact_path, pipeline_step_path, write_delta


def _ensure_features(df: DataFrame) -> DataFrame:
    if "features" not in df.columns:
        feature_cols = [c for c in df.columns if c not in ("label", "id")]
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        df = assembler.transform(df)
    return df


def _train_clustering(ctx, input_df, estimator) -> ExecutionResult:
    df = _ensure_features(input_df)
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


class KMeansExecutor(BaseSparkExecutor):
    operation_name = "k_means"

    def execute(self, ctx, params, input_df):
        if input_df is None:
            raise ValueError("k_means requires input dataframe")
        cluster_type = params.get("type", "kmeans")
        if cluster_type == "bisecting":
            est = BisectingKMeans(featuresCol="features", k=params["number_of_clusters"])
            est = set_if_present(est, params, "max_iterations", "maxIter")
            est = set_if_present(est, params, "seed", "seed")
            est = set_if_present(est, params, "tolerance", "tol")
        else:
            est = KMeans(featuresCol="features", k=params["number_of_clusters"])
            est = set_if_present(est, params, "init_mode", "initMode")
            est = set_if_present(est, params, "max_iterations", "maxIter")
            est = set_if_present(est, params, "seed", "seed")
            est = set_if_present(est, params, "tolerance", "tol")
            if params.get("distance") == "cosine":
                est = est.setDistanceMeasure("cosine")
        return _train_clustering(ctx, input_df, est)


class GaussianMixExecutor(BaseSparkExecutor):
    operation_name = "gaussian_mix"

    def execute(self, ctx, params, input_df):
        if input_df is None:
            raise ValueError("gaussian_mix requires input dataframe")
        gm = GaussianMixture(
            featuresCol="features",
            k=params["number_of_clusters"],
            maxIter=params["max_iterations"],
        )
        gm = set_if_present(gm, params, "tolerance", "tol")
        return _train_clustering(ctx, input_df, gm)


class BKMClusteringExecutor(BaseSparkExecutor):
    operation_name = "bkm_clustering"

    def execute(self, ctx, params, input_df):
        if input_df is None:
            raise ValueError("bkm_clustering requires input dataframe")
        bkm = BisectingKMeans(featuresCol="features", k=params["number_of_clusters"])
        bkm = set_if_present(bkm, params, "max_iterations", "maxIter")
        bkm = set_if_present(bkm, params, "seed", "seed")
        bkm = set_if_present(bkm, params, "tolerance", "tol")
        bkm = set_if_present(bkm, params, "min_divisible_clusterSize", "minDivisibleClusterSize")
        if params.get("distance") == "cosine":
            bkm = bkm.setDistanceMeasure("cosine")
        return _train_clustering(ctx, input_df, bkm)


class LDAClusteringExecutor(BaseSparkExecutor):
    operation_name = "lda_clustering"

    def execute(self, ctx, params, input_df):
        if input_df is None:
            raise ValueError("lda_clustering requires input dataframe")
        lda = LDA(k=params["number_of_clusters"], maxIter=params.get("max_iterations", 20))
        for key, spark_key in [
            ("seed", "seed"),
            ("checkpoint_interval", "checkpointInterval"),
            ("optimizer", "optimizer"),
            ("learning_offset", "learningOffset"),
            ("learning_decay", "learningDecay"),
            ("optimize_doc_concentration", "optimizeConcentrations"),
            ("doc_concentration", "docConcentration"),
            ("topic_concentration", "topicConcentration"),
            ("keep_last_checkpoint", "keepLastCheckpoint"),
        ]:
            lda = set_if_present(lda, params, key, spark_key)
        if params.get("features"):
            lda = lda.setFeaturesCol(params["features"])
        return _train_clustering(ctx, input_df, lda)


class PICClusteringExecutor(BaseSparkExecutor):
    operation_name = "pic_clustering"

    def execute(self, ctx, params, input_df):
        if input_df is None:
            raise ValueError("pic_clustering requires input dataframe")
        pic = PowerIterationClustering(k=params["number_of_clusters"])
        pic = set_if_present(pic, params, "max_iterations", "maxIter")
        pic = set_if_present(pic, params, "init_mode", "initMode")
        return _train_clustering(ctx, input_df, pic)


CLUSTERING_EXECUTORS: dict[str, type[BaseSparkExecutor]] = {
    KMeansExecutor.operation_name: KMeansExecutor,
    GaussianMixExecutor.operation_name: GaussianMixExecutor,
    BKMClusteringExecutor.operation_name: BKMClusteringExecutor,
    LDAClusteringExecutor.operation_name: LDAClusteringExecutor,
    PICClusteringExecutor.operation_name: PICClusteringExecutor,
}
