from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://lemonade:lemonade@localhost:5432/lemonade"
    redis_url: str = "redis://localhost:6379/0"
    minio_endpoint: str = "http://localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "lake"
    spark_mode: str = "local"
    spark_master: str = "local[*]"
    auth_dev_mode: bool = True
    keycloak_url: str = ""
    keycloak_realm: str = "lemonade"
    keycloak_client_id: str = "lemonade-api"
    marquez_url: str = "http://localhost:5000"
    model_yaml_path: str = "model.yaml"
    audit_bucket: str = "audit"
    app_name: str = "lemonade-backend"

    @property
    def model_yaml(self) -> Path:
        return Path(self.model_yaml_path)


@lru_cache
def get_settings() -> Settings:
    return Settings()
