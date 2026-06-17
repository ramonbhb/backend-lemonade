# Lemonade Backend

Lemonade-compatible Data Science API built with **FastAPI** and **PySpark**. It exposes distributed ML and pipeline operations defined in [`model.yaml`](model.yaml), with multi-tenant isolation, audit logging, and privacy guardrails for on-prem data lakes.

Compatible with the [Lemonade Tahiti](https://dev.lemonade.org.br) operation catalog. The spec can be regenerated from upstream using [lemonade-llm](https://gitlab.com/waltersf/lemonade-llm).

## Features

- **27 ML/pipeline operations** — I/O, classification, regression, clustering, evaluation, and hyperparameter search
- **Operation catalog from `model.yaml`** — auto-generated OpenAPI schemas and Pydantic validation
- **Async job execution** — Redis + RQ workers running PySpark jobs
- **Delta Lake on MinIO** — on-prem object storage (not AWS S3); Spark uses `s3a://` URIs to reach MinIO
- **Responsible AI** — immutable audit trail, privacy policy engine, PII detection, OpenLineage → Marquez, model cards
- **Multi-tenancy** — tenant-scoped data paths, Keycloak OAuth2 (dev mode for local testing)
- **Production-ready manifests** — Docker Compose, Kubernetes, Helm, Spark Operator

## Architecture

```
Client → FastAPI → Policy Engine → Job Queue → Spark Worker → Delta Lake (MinIO)
              ↓                                        ↓
         Audit Store                              Marquez (lineage)
         (PostgreSQL)
```

## Prerequisites

- Python 3.11+
- Java 17 (required by PySpark)
- Docker & Docker Compose (for full stack)

## Quick start

### Local API only

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[spark,dev]"

cp .env.example .env
uvicorn lemonade_backend.main:app --reload
```

Open http://localhost:8000/docs for interactive API documentation.

In dev mode (`AUTH_DEV_MODE=true`), requests are authenticated as `dev-user` / `dev-tenant` with no token required.

### Full stack (Docker Compose)

Starts the API, worker, PostgreSQL, Redis, MinIO, and Marquez:

```bash
docker compose up -d
```

| Service   | URL                        |
|-----------|----------------------------|
| API       | http://localhost:8000      |
| MinIO     | http://localhost:9001      |
| Marquez   | http://localhost:5000      |

## API endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET`  | `/v1/datasets` | List registered datasets |
| `POST` | `/v1/datasets` | Register a dataset in the catalog |
| `GET`  | `/v1/datasets/{id}` | Get dataset metadata and MinIO path |
| `GET`  | `/health` | Health check |
| `GET`  | `/v1/operations` | List all operations from `model.yaml` |
| `GET`  | `/v1/operations/{name}` | Get operation schema |
| `POST` | `/v1/operations/{name}/run` | Enqueue a single operation |
| `POST` | `/v1/pipelines/run` | Run a chained pipeline |
| `GET`  | `/v1/jobs/{job_id}` | Job status and output refs |
| `GET`  | `/v1/audit/events` | Query audit events |

### Run an operation

```bash
# dataframe_ref uses s3a:// — Spark URI for MinIO, not AWS S3
curl -X POST http://localhost:8000/v1/operations/linear_regression/run \
  -H "Content-Type: application/json" \
  -d '{
    "inputs": {
      "dataframe_ref": "s3a://lake/dev-tenant/datasets/1"
    },
    "parameters": {
      "max_iter": 100,
      "reg_param": 0.01
    },
    "privacy_context": {
      "purpose": "model_training",
      "pii_policy": "strict"
    }
  }'
```

Response:

```json
{
  "job_id": "...",
  "audit_ref": "...",
  "status": "queued"
}
```

Poll job status with `GET /v1/jobs/{job_id}`.

### Run a pipeline

```bash
curl -X POST http://localhost:8000/v1/pipelines/run \
  -H "Content-Type: application/json" \
  -d '{
    "steps": [
      {
        "operation": "read_data",
        "parameters": { "data_source": 1 }
      },
      {
        "operation": "features",
        "parameters": { "features": "col1,col2,col3" }
      },
      {
        "operation": "random_forest_classifier",
        "parameters": { "impurity": "gini", "num_trees": 50 }
      }
    ],
    "privacy_context": { "purpose": "model_training" }
  }'
```

## Operations

Operations are defined in [`model.yaml`](model.yaml) and mapped to PySpark ML executors:

| Category | Operations |
|----------|------------|
| I/O & prep | `read_data`, `sample`, `split`, `features`, `features_reduction` |
| Classification | `naive_bayes`, `perceptron`, `decision_tree_classifier`, `random_forest_classifier`, `gbt_classifier`, `logistic_regression`, `svm`, `fm_classifier` |
| Regression | `linear_regression`, `generalized_linear_regressor`, `isotonic_regression`, `decision_tree_regressor`, `random_forest_regressor`, `gbt_regressor`, `fm_regression` |
| Clustering | `k_means`, `gaussian_mix`, `bkm_clustering`, `lda_clustering`, `pic_clustering` |
| Meta | `evaluator`, `grid` |

Regenerate `model.yaml` from the live Tahiti API:

```bash
./scripts/sync_model_yaml.sh <LEMONADE_API_TOKEN>
```

## Configuration

Copy [`.env.example`](.env.example) to `.env`. Key variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql+asyncpg://...` | PostgreSQL connection |
| `REDIS_URL` | `redis://localhost:6379/0` | Job queue |
| `MINIO_ENDPOINT` | `http://localhost:9000` | MinIO server URL (on-prem object storage) |
| `MINIO_ACCESS_KEY` | `minioadmin` | MinIO access key |
| `MINIO_SECRET_KEY` | `minioadmin` | MinIO secret key |
| `MINIO_BUCKET` | `lake` | MinIO bucket for the data lake |
| `SPARK_MODE` | `local` | `local` or `k8s` |
| `AUTH_DEV_MODE` | `true` | Bypass Keycloak for local dev |
| `KEYCLOAK_URL` | — | Required when `AUTH_DEV_MODE=false` |
| `MARQUEZ_URL` | `http://localhost:5000` | OpenLineage backend |

## Storage & data lake

This project uses **MinIO** as on-prem object storage — **not AWS S3**. MinIO runs locally via Docker Compose (`http://localhost:9000`, console at `:9001`) or on your own infrastructure in production.

Tables are stored as **Delta Lake** datasets inside MinIO. PySpark accesses MinIO through the Hadoop **S3A** filesystem, which is why paths look like `s3a://…`. That URI scheme is Spark’s way of addressing object storage; the actual backend is always your MinIO instance, configured via `MINIO_ENDPOINT` and related env vars.

```
MinIO bucket (lake)
└── {tenant_id}/
    ├── datasets/{data_source_id}/   ← Delta tables (read_data)
    ├── pipelines/{job_id}/step-{n}/   ← pipeline intermediates
    └── models/{job_id}/               ← trained models
```

Spark connects to MinIO in [`src/lemonade_backend/spark/session.py`](src/lemonade_backend/spark/session.py) by setting `spark.hadoop.fs.s3a.endpoint` to `MINIO_ENDPOINT`.

### Dataset catalog

The `read_data` operation maps an integer `data_source` ID to a Delta path by convention:

```
s3a://{MINIO_BUCKET}/{tenant_id}/datasets/{data_source_id}
```

For example, `data_source: 1` for tenant `dev-tenant` resolves to `s3a://lake/dev-tenant/datasets/1`.

Before calling `read_data`, load your Delta dataset into MinIO at the path above (via Spark, the MinIO console, or `mc` CLI). Optionally register it via `POST /v1/datasets` to store metadata (name, schema, sensitive columns) and override the default path.

Example — upload data to MinIO with the MinIO client:

```bash
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/lake/dev-tenant/datasets/1 --ignore-existing
# Write Delta data with PySpark or copy an existing Delta folder into that prefix
```


## Responsible AI

- **Audit** — every API call and job completion is logged with sanitized parameters, artifact refs, and policy decisions (`GET /v1/audit/events`)
- **Privacy** — purpose binding, cross-tenant access blocking, PII column detection under strict policy
- **Lineage** — OpenLineage events emitted to Marquez per pipeline step
- **Model cards** — auto-generated JSON on training jobs with hyperparameters, metrics, and lineage refs

## Testing

```bash
source .venv/bin/activate
pip install -e ".[spark,dev]"
pytest tests/ -v
```

## Production (Kubernetes)

Manifests are under [`k8s/`](k8s/):

- `api-deployment.yaml` — API service
- `spark-operator/` — SparkApplication template and tenant quotas
- `minio/tenant-bucket-policy.json` — per-tenant bucket isolation
- `keycloak/realm-config.yaml` — OAuth2 realm
- `helm/` — Helm chart
- `monitoring/prometheus.yaml` — scrape config

Deploy with Helm:

```bash
helm install lemonade-backend k8s/helm/ -f k8s/helm/values.yaml
```

Set `AUTH_DEV_MODE=false` and configure Keycloak for production auth.

## Project structure

```
BackendLemonade/
├── model.yaml                 # Operation catalog (source of truth)
├── src/lemonade_backend/
│   ├── api/routes/            # REST endpoints
│   ├── catalog/               # model.yaml loader and schema generation
│   ├── executors/             # PySpark operation implementations
│   ├── jobs/                  # RQ queue and worker
│   ├── pipeline/              # Chained pipeline runner
│   ├── audit/                 # Audit logging and model cards
│   ├── privacy/               # Policy engine, PII, lineage
│   ├── auth/                  # Keycloak integration
│   └── spark/                 # Spark session and Delta I/O
├── tests/
├── k8s/
├── scripts/
└── docker-compose.yml
```

## License

See repository license file (if applicable).
