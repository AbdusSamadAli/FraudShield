from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", protected_namespaces=())

    kafka_bootstrap_servers: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"
    database_url: str = "postgresql://fraud@localhost:5432/fraudshield"
    redis_url: str = "redis://localhost:6379/0"
    payment_api_url: str = "http://localhost:8000"
    model_dir: str = "ml/artifacts"

    review_threshold: float = 0.5
    block_threshold: float = 0.8

    sim_tps: float = 20.0
    sim_users: int = 100000
    sim_fraud_rate: float = 0.02
    sim_seed: int = 7

    max_retries: int = 3
    retry_backoff_seconds: float = 1.0
    metrics_port: int = 9100
    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
