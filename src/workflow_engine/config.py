"""Typed application settings, read from the environment and an optional `.env` file."""

from __future__ import annotations

from functools import lru_cache
from typing import Annotated, Any

from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict
from sqlalchemy.engine import URL


class RateLimit(BaseModel):
    """Token-bucket parameters: refill `rate` tokens per second, hold at most `burst`."""

    rate: float = Field(gt=0)
    burst: int = Field(gt=0)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # --- General ---
    app_env: str = "development"
    log_level: str = "INFO"
    log_json: bool = True

    # --- API ---
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_keys: Annotated[list[SecretStr], NoDecode] = Field(default_factory=list)
    max_payload_bytes: int = Field(default=65536, gt=0)

    # --- PostgreSQL ---
    postgres_user: str = "workflow"
    postgres_password: SecretStr = SecretStr("")
    postgres_db: str = "workflow"
    postgres_host: str = "localhost"
    postgres_port: int = 5433
    # Optional full URL; when set it takes precedence over the POSTGRES_* parts.
    database_url: SecretStr | None = None
    db_pool_size: int = 10
    db_max_overflow: int = 5

    # --- Redis ---
    redis_url: SecretStr = SecretStr("redis://localhost:6379/0")
    redis_stream_prefix: str = "jobs"
    redis_consumer_group: str = "workers"
    stream_maxlen: int = Field(default=100_000, gt=0)

    # --- Worker ---
    worker_concurrency: int = Field(default=1, ge=1)
    worker_id: str | None = None
    worker_lease_seconds: float = Field(default=30, gt=0)
    worker_heartbeat_seconds: float = Field(default=10, gt=0)
    worker_shutdown_grace_seconds: float = Field(default=30, ge=0)
    worker_block_ms: int = Field(default=2000, gt=0)

    # --- Maintenance loops ---
    reaper_interval_seconds: float = Field(default=5, gt=0)
    sweeper_interval_seconds: float = Field(default=15, gt=0)
    scheduler_interval_seconds: float = Field(default=1, gt=0)
    # A queued job untouched for this long is re-XADDed; PEL entries idle this long are reclaimed.
    sweeper_stale_seconds: float = Field(default=30, gt=0)
    maintenance_batch_size: int = Field(default=500, ge=1)
    # How often a standby maintenance process retries the leader lock.
    maintenance_leader_retry_seconds: float = Field(default=5, gt=0)

    # --- Retries ---
    retry_base_delay_seconds: float = Field(default=1, ge=0)
    retry_max_delay_seconds: float = Field(default=300, ge=0)
    default_max_attempts: int = Field(default=5, ge=1)

    # --- Idempotency ---
    idempotency_key_ttl_hours: int = Field(default=24, gt=0)

    # --- Rate limiting ---
    rate_limits: dict[str, RateLimit] = Field(default_factory=dict)
    submit_rate_limit_per_second: float = Field(default=20, gt=0)
    submit_rate_limit_burst: int = Field(default=40, ge=1)

    # --- Embeddings ---
    embed_search_max_k: int = 50
    embed_model_name: str = "BAAI/bge-small-en-v1.5"
    embed_dim: int = 384
    embed_batch_size: int = 32
    fastembed_cache_dir: str = ".cache/fastembed"

    # --- Observability ---
    metrics_enabled: bool = True
    worker_metrics_port: int = 9100
    otel_enabled: bool = False
    otel_service_name: str = "workflow-engine"
    otel_exporter_otlp_endpoint: str = "http://localhost:4318"

    @field_validator("api_keys", mode="before")
    @classmethod
    def _split_api_keys(cls, value: Any) -> Any:
        if isinstance(value, str):
            return [part.strip() for part in value.split(",") if part.strip()]
        return value

    @field_validator("worker_id", mode="before")
    @classmethod
    def _empty_worker_id(cls, value: Any) -> Any:
        if isinstance(value, str) and not value.strip():
            return None
        return value

    @model_validator(mode="after")
    def _heartbeat_inside_lease(self) -> Settings:
        if self.worker_heartbeat_seconds >= self.worker_lease_seconds:
            raise ValueError("WORKER_HEARTBEAT_SECONDS must be smaller than WORKER_LEASE_SECONDS")
        return self

    @property
    def submit_rate_limit(self) -> RateLimit:
        return RateLimit(rate=self.submit_rate_limit_per_second, burst=self.submit_rate_limit_burst)

    @property
    def sqlalchemy_url(self) -> str:
        """Async SQLAlchemy URL (psycopg 3 driver)."""
        if self.database_url is not None and self.database_url.get_secret_value().strip():
            url = self.database_url.get_secret_value().strip()
            for prefix in ("postgresql+psycopg://", "postgresql://", "postgres://"):
                if url.startswith(prefix):
                    return "postgresql+psycopg://" + url[len(prefix) :]
            return url
        return URL.create(
            "postgresql+psycopg",
            username=self.postgres_user,
            password=self.postgres_password.get_secret_value() or None,
            host=self.postgres_host,
            port=self.postgres_port,
            database=self.postgres_db,
        ).render_as_string(hide_password=False)

    @property
    def api_key_values(self) -> list[str]:
        return [key.get_secret_value() for key in self.api_keys]


@lru_cache
def get_settings() -> Settings:
    return Settings()
