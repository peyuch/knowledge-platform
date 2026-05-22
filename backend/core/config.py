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

    # Elasticsearch
    es_url: str = "http://localhost:9200"

    # Milvus
    milvus_db_path: str = "data/milvus.db"

    # Index Consumer
    index_batch_size: int = 200
    index_batch_timeout: float = 3.0
    index_buffer_max_size: int = 2000
    index_kafka_group_id: str = "indexing-v1"
    index_metrics_port: int = 9090
    index_embed_batch_size: int = 64

    # LLM — read from environment: $env:DEEPSEEK_API_KEY / $env:DASHSCOPE_API_KEY
    deepseek_api_key: str = ""
    dashscope_api_key: str = ""

    # RAPTOR
    raptor_llm_api_url: str = "https://api.deepseek.com"
    raptor_llm_api_key: str = ""
    raptor_llm_model: str = "deepseek-chat"
    raptor_backup_llm_api_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1"
    raptor_backup_llm_api_key: str = ""
    raptor_backup_llm_model: str = "qwen-turbo"

    # Neo4j
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "password"

    # GraphRAG (also used by Corrective RAG)
    graphrag_llm_api_url: str = "https://api.deepseek.com"
    graphrag_llm_api_key: str = ""
    graphrag_llm_model: str = "deepseek-chat"
    graphrag_backup_llm_api_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1"
    graphrag_backup_llm_api_key: str = ""
    graphrag_backup_llm_model: str = "qwen-turbo"

    def model_post_init(self, __context):
        """Auto-fill LLM keys from top-level env vars if not explicitly set."""
        if not self.raptor_llm_api_key:
            self.raptor_llm_api_key = self.deepseek_api_key
        if not self.raptor_backup_llm_api_key:
            self.raptor_backup_llm_api_key = self.dashscope_api_key
        if not self.graphrag_llm_api_key:
            self.graphrag_llm_api_key = self.deepseek_api_key
        if not self.graphrag_backup_llm_api_key:
            self.graphrag_backup_llm_api_key = self.dashscope_api_key

    # Reranker
    reranker_model: str = "BAAI/bge-reranker-v2-m3"

    # App
    app_env: str = "development"
    log_level: str = "INFO"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
