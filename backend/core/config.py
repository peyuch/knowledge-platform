"""Application settings loaded from environment /.env."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # PostgreSQL
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/knowledge_platform"
    database_url_sync: str = "postgresql+psycopg2://postgres:postgres@localhost:5432/knowledge_platform"

    # MinIO
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "knowledge-platform"
    minio_secure: bool = False

    # RabbitMQ / Celery
    celery_broker_url: str = "amqp://guest:guest@localhost:5672//"

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_chunks: str = "knowledge.ingestion.chunks"

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # MinerU
    mineru_api_url: str = "http://localhost:8001"

    # JWT
    jwt_secret_key: str = "change-me-in-production"
    jwt_algorithm: str = "HS256"
    jwt_expire_minutes: int = 1440

    # App
    app_env: str = "development"
    log_level: str = "INFO"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
