import pytest

from lemonade_backend.catalog.loader import load_model_catalog
from lemonade_backend.catalog.schema_generator import build_all_param_models, operation_to_schema
from lemonade_backend.catalog.registry import register_all_executors, get_registry
from lemonade_backend.privacy.policy import PrivacyPolicyEngine


MODEL_YAML = "model.yaml"


def test_load_model_catalog():
    catalog = load_model_catalog(MODEL_YAML)
    assert catalog.library
    assert len(catalog.methods) >= 25
    names = {m.name for m in catalog.methods}
    assert "read_data" in names
    assert "random_forest_classifier" in names
    assert "evaluator" in names


def test_operation_ids_extracted():
    catalog = load_model_catalog(MODEL_YAML)
    rf = next(m for m in catalog.methods if m.name == "random_forest_classifier")
    assert rf.operation_id == 2361


def test_build_param_models():
    catalog = load_model_catalog(MODEL_YAML)
    models = build_all_param_models(catalog)
    assert "linear_regression" in models
    instance = models["read_data"](data_source=2100)
    assert instance.data_source == 2100


def test_operation_schema():
    catalog = load_model_catalog(MODEL_YAML)
    op = next(m for m in catalog.methods if m.name == "naive_bayes")
    schema = operation_to_schema(op)
    assert schema["name"] == "naive_bayes"
    assert "model_type" in schema["parameters"]["properties"]


def test_all_executors_registered():
    register_all_executors()
    registry = get_registry()
    catalog = load_model_catalog(MODEL_YAML)
    catalog_names = {m.name for m in catalog.methods}
    registered = set(registry.list_names())
    missing = catalog_names - registered
    assert not missing, f"Missing executors: {missing}"


def test_privacy_policy_blocks_cross_tenant():
    engine = PrivacyPolicyEngine()
    decision = engine.evaluate(
        operation_name="read_data",
        tenant_id="tenant-a",
        purpose="model_training",
        parameters={"data_source": 1},
        input_ref="s3://lake/tenant-b/datasets/1",
    )
    assert not decision.allowed


def test_privacy_policy_blocks_invalid_purpose():
    engine = PrivacyPolicyEngine()
    decision = engine.evaluate(
        operation_name="split",
        tenant_id="tenant-a",
        purpose="data_access",
        parameters={"strategy": 1, "seed": 1, "ratio": 0.8, "folds": 5},
        input_ref=None,
    )
    assert not decision.allowed


def test_pii_scan():
    engine = PrivacyPolicyEngine()
    findings = engine.scan_pii_in_sample(["user@example.com", "12345678901"])
    assert "email" in findings
