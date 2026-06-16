from lemonade_backend.audit.logger import sanitize_params
from lemonade_backend.audit.model_card import generate_model_card
from lemonade_backend.spark.delta_io import TenantPathError, enforce_tenant_access, tenant_base_path


def test_sanitize_params():
    result = sanitize_params({"cpf": "123", "max_depth": 10, "api_key": "secret"})
    assert result["cpf"] == "***REDACTED***"
    assert result["max_depth"] == 10


def test_tenant_base_path():
    path = tenant_base_path("tenant-a")
    assert "tenant-a" in path


def test_enforce_tenant_access_blocks_cross_tenant():
    try:
        enforce_tenant_access("tenant-a", "s3://lake/tenant-b/data")
        assert False, "Should have raised"
    except TenantPathError:
        pass


def test_model_card_generation():
    card = generate_model_card(
        job_id="job-1",
        tenant_id="tenant-a",
        operation_name="linear_regression",
        parameters={"max_iter": 100},
        metrics={"rmse": 0.5},
        dataset_ref="s3://lake/tenant-a/datasets/1",
    )
    data = card.to_dict()
    assert data["operation_name"] == "linear_regression"
    assert data["metrics"]["rmse"] == 0.5
