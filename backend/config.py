"""Application configuration."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent


class Settings(BaseSettings):
    """Centralized runtime settings loaded from environment variables."""

    APP_ENV: str = "local"
    LOG_LEVEL: str = "INFO"

    OPENAI_API_KEY: str = ""
    DOUBAO_API_KEY: str = ""
    ARK_API_KEY: str = ""
    LLM_API_KEY: str = ""
    LLM_BASE_URL: str | None = "https://ark.cn-beijing.volces.com/api/v3"
    DATABASE_URL: str = ""
    LLM_MODEL: str = "doubao-lite-32k"
    LLM_TIMEOUT_SECONDS: float = 60.0
    LLM_MAX_RETRIES: int = 1

    EMBEDDING_API_KEY: str = ""
    EMBEDDING_BASE_URL: str | None = None
    EMBEDDING_MODEL: str = ""
    EMBEDDING_PATH: str = ""
    EMBEDDING_DIMENSION: int = 0

    MINIO_ENDPOINT: str = "localhost:9000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"
    MINIO_SECURE: bool = False
    MINIO_REGION: str | None = None
    MINIO_BRONZE_BUCKET: str = "openfinance-bronze"
    MINIO_WAREHOUSE_BUCKET: str = "openfinance-warehouse"

    ICEBERG_CATALOG_NAME: str = "openfinance"
    ICEBERG_CATALOG_URI: str = "postgresql+psycopg://openfinance:openfinance@localhost:5432/openfinance"
    ICEBERG_WAREHOUSE: str = "s3://openfinance-warehouse/"
    ICEBERG_NAMESPACE: str = "openfinance"
    ICEBERG_NEWS_TABLE: str = "news_articles"
    ICEBERG_SECURITY_MASTER_TABLE: str = "security_master"
    ICEBERG_TRADING_CALENDAR_TABLE: str = "trading_calendar"
    ICEBERG_DAILY_BARS_TABLE: str = "daily_bars"
    ICEBERG_ANNUAL_REPORT_TABLE: str = "annual_reports"
    ICEBERG_ANNUAL_REPORT_PAGE_TABLE: str = "annual_report_pages"
    ICEBERG_ANNUAL_REPORT_SECTION_TABLE: str = "annual_report_sections"
    ICEBERG_GOLD_METRICS_TABLE: str = "gold_security_daily_metrics"
    ICEBERG_S3_PATH_STYLE_ACCESS: bool = True

    FINANCE_DEFAULT_PROVIDER: str = "baostock"
    FINANCE_DEFAULT_ADJUSTMENT_TYPE: str = "qfq"
    FINANCE_DEFAULT_LOOKBACK_DAYS: int = 30
    BAOSTOCK_SOCKET_TIMEOUT_SECONDS: float = 15.0
    BAOSTOCK_SYMBOL_TIMEOUT_SECONDS: int = 45
    BAOSTOCK_MAX_RETRIES: int = 2
    BAOSTOCK_RETRY_BACKOFF_SECONDS: float = 2.0
    BAOSTOCK_DAILY_REQUEST_LIMIT: int = 50_000
    A_SHARE_GAP_LOOKBACK_DAYS: int = 60
    A_SHARE_REQUESTS_PER_SYMBOL: int = 2
    A_SHARE_MAX_SYMBOLS_PER_RUN: int = 10_000
    A_SHARE_INGEST_BATCH_SIZE: int = 20
    A_SHARE_DAILY_LOOKBACK_DAYS: int = 3
    A_SHARE_DAILY_AIRFLOW_SCHEDULE: str = "15 18 * * 1-5"
    A_SHARE_BACKFILL_START_DATE: str = "1990-01-01"
    A_SHARE_BATCH_SIZE: int = 100
    A_SHARE_MAX_ACTIVE_BATCHES: int = 4
    A_SHARE_UNIVERSE_LOOKBACK_DAYS: int = 30
    ICEBERG_WRITE_MAX_RETRIES: int = 10
    ICEBERG_WRITE_INITIAL_BACKOFF_SECONDS: float = 1.0
    ICEBERG_WRITE_MAX_BACKOFF_SECONDS: float = 30.0
    ICEBERG_WRITE_LOCK_TIMEOUT_SECONDS: float = 300.0

    NEWS_HTTP_TIMEOUT_SECONDS: float = 10.0
    # External API calls are quota-billed. Retries are opt-in and default to zero.
    NEWS_HTTP_MAX_RETRIES: int = 0
    NEWS_HTTP_RETRY_BACKOFF_SECONDS: float = 1.0
    CNINFO_USER_AGENT: str = "OpenFinance-Agent/0.1 (+https://www.cninfo.com.cn/)"
    CNINFO_REPORT_STOCKS_JSON: str = "[]"
    CNINFO_REPORT_YEARS_JSON: str = "[]"
    CNINFO_REPORT_AIRFLOW_SCHEDULE: str = "0 3 * * *"
    NEWS_DEFAULT_LIMIT: int = 50
    JUHE_NEWS_API_KEY: str = ""
    JUHE_NEWS_LIST_URL: str = "https://v.juhe.cn/toutiao/index"
    JUHE_NEWS_DETAIL_URL: str = "https://v.juhe.cn/toutiao/content"
    JUHE_NEWS_DEFAULT_PAGE: int = 1
    JUHE_NEWS_LIST_PAGE_SIZE: int = 20
    JUHE_NEWS_MAX_PAGES: int = 5
    JUHE_NEWS_DEFAULT_IS_FILTER: int = 1
    JUHE_NEWS_FETCH_DETAILS: bool = True
    JUHE_NEWS_POLL_INTERVAL_MINUTES: int = 30

    NEWS_EMPTY_RESULT_MESSAGE: str = "当前自有湖仓在指定时间范围内未检索到相关新闻。"
    NEWS_SYMBOL_ALIASES_JSON: str = Field(default="{}", alias="NEWS_SYMBOL_ALIASES")
    NEWS_INGEST_LIMIT: int = 20
    NEWS_INGEST_LOOKBACK_HOURS: int = 6
    NEWS_AIRFLOW_SCHEDULE: str = "*/30 * * * *"
    OPENFINANCE_INTERNAL_API_BASE_URL: str = "http://localhost:8000"
    OPENFINANCE_INTERNAL_API_TOKEN: str = "replace-with-a-shared-internal-token"

    RAG_VECTOR_TABLE: str = "news_chunks"
    RAG_ANNUAL_REPORT_VECTOR_TABLE: str = "annual_report_chunks"
    RAG_EMBEDDING_MODEL: str = "doubao-embedding-vision-251215"
    RAG_EMBEDDING_PATH: str = "/embeddings/multimodal"
    RAG_EMBEDDING_DIMENSION: int = 1536
    RAG_CHUNK_SIZE: int = 800
    RAG_CHUNK_OVERLAP: int = 120
    RAG_MIN_TEXT_LENGTH: int = 120
    RAG_DEFAULT_LOOKBACK_DAYS: int = 30
    RAG_TOP_K: int = 5
    RAG_CANDIDATE_LIMIT: int = 50
    RAG_RRF_K: int = 60
    RAG_MMR_LAMBDA: float = 0.7
    RAG_EVIDENCE_PER_ARTICLE: int = 2
    RAG_TIME_DECAY_HALF_LIFE_DAYS: float = 14.0
    RAG_SOURCE_WEIGHTS_JSON: str = "{}"

    OPENSEARCH_ENABLED: bool = False
    OPENSEARCH_URL: str = "http://localhost:9200"
    OPENSEARCH_INDEX: str = "openfinance_news_chunks"
    OPENSEARCH_USERNAME: str = ""
    OPENSEARCH_PASSWORD: str = ""
    OPENSEARCH_TIMEOUT_SECONDS: float = 10.0

    RAG_RERANK_ENABLED: bool = False
    RAG_RERANK_URL: str = ""
    RAG_RERANK_API_KEY: str = ""
    RAG_RERANK_MODEL: str = "rerank-v3.5"
    RAG_RERANK_TIMEOUT_SECONDS: float = 10.0
    RAG_RERANK_MIN_SCORE: float = 0.15

    INVESTMENT_MONITOR_AIRFLOW_SCHEDULE: str = "0 20 * * 1-5"
    INVESTMENT_MONITOR_MAX_SYMBOLS: int = 50
    INVESTMENT_MONITOR_CONCURRENCY: int = 4
    INVESTMENT_MONITOR_RECOMMENDATIONS_ENABLED: bool = False
    INVESTMENT_MONITOR_PRICE_MOVE_THRESHOLD: float = 0.03
    INVESTMENT_MONITOR_VOLUME_RATIO_THRESHOLD: float = 1.8

    AIRFLOW_EXECUTOR: str = "LocalExecutor"
    AIRFLOW_FERNET_KEY: str = "replace-with-a-generated-fernet-key"
    AIRFLOW_WEBSERVER_SECRET_KEY: str = "replace-with-a-secret-key"
    AIRFLOW_ADMIN_USERNAME: str = "admin"
    AIRFLOW_ADMIN_PASSWORD: str = "admin"
    AIRFLOW_ADMIN_FIRSTNAME: str = "OpenFinance"
    AIRFLOW_ADMIN_LASTNAME: str = "Admin"
    AIRFLOW_ADMIN_EMAIL: str = "admin@example.com"

    model_config = SettingsConfigDict(
        env_file=(PROJECT_ROOT / ".env", BASE_DIR / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def resolved_llm_api_key(self) -> str:
        """Return the first available API key for the configured LLM provider."""

        return self.LLM_API_KEY or self.DOUBAO_API_KEY or self.ARK_API_KEY or self.OPENAI_API_KEY

    @property
    def resolved_embedding_api_key(self) -> str:
        """Return the embedding API key with backward-compatible fallback."""

        return self.EMBEDDING_API_KEY or self.resolved_llm_api_key

    @property
    def resolved_embedding_base_url(self) -> str:
        """Return the embedding base URL with backward-compatible fallback."""

        return self.EMBEDDING_BASE_URL or self.LLM_BASE_URL or ""

    @property
    def resolved_embedding_model(self) -> str:
        return self.EMBEDDING_MODEL or self.RAG_EMBEDDING_MODEL

    @property
    def resolved_embedding_path(self) -> str:
        return self.EMBEDDING_PATH or self.RAG_EMBEDDING_PATH

    @property
    def resolved_embedding_dimension(self) -> int:
        return self.EMBEDDING_DIMENSION or self.RAG_EMBEDDING_DIMENSION

    @property
    def iceberg_news_table_identifier(self) -> str:
        """Return fully qualified Iceberg news table identifier."""

        return f"{self.ICEBERG_NAMESPACE}.{self.ICEBERG_NEWS_TABLE}"

    @property
    def rag_schema(self) -> str:
        """Return the PostgreSQL schema used by the vector index."""

        return self.ICEBERG_NAMESPACE

    @property
    def rag_table_identifier(self) -> str:
        """Return fully qualified pgvector table identifier."""

        return f"{self.rag_schema}.{self.RAG_VECTOR_TABLE}"

    @property
    def news_symbol_aliases(self) -> dict[str, list[str]]:
        """Return stock symbol aliases mapping."""

        try:
            raw_value: Any = json.loads(self.NEWS_SYMBOL_ALIASES_JSON or "{}")
        except json.JSONDecodeError as exc:  # pragma: no cover
            raise ValueError("NEWS_SYMBOL_ALIASES must be valid JSON") from exc

        parsed: dict[str, list[str]] = {}
        for symbol, aliases in dict(raw_value).items():
            parsed[str(symbol).upper()] = [str(alias).strip() for alias in aliases if str(alias).strip()]
        return parsed

    @property
    def cninfo_report_stocks(self) -> list[str]:
        """Return configured stock codes for annual-report ingestion."""

        try:
            value = json.loads(self.CNINFO_REPORT_STOCKS_JSON or "[]")
        except json.JSONDecodeError as exc:
            raise ValueError("CNINFO_REPORT_STOCKS_JSON must be valid JSON") from exc
        return [str(item).strip().upper() for item in value if str(item).strip()]

    @property
    def cninfo_report_years(self) -> list[int]:
        """Return configured report years for annual-report ingestion."""

        try:
            value = json.loads(self.CNINFO_REPORT_YEARS_JSON or "[]")
        except json.JSONDecodeError as exc:
            raise ValueError("CNINFO_REPORT_YEARS_JSON must be valid JSON") from exc
        return [int(item) for item in value]

    @property
    def rag_source_weights(self) -> dict[str, float]:
        """Return optional, auditable source weights (neutral by default)."""

        try:
            raw_value: Any = json.loads(self.RAG_SOURCE_WEIGHTS_JSON or "{}")
        except json.JSONDecodeError as exc:  # pragma: no cover
            raise ValueError("RAG_SOURCE_WEIGHTS_JSON must be valid JSON") from exc
        return {str(name).casefold(): float(weight) for name, weight in dict(raw_value).items()}

    @property
    def minio_http_scheme(self) -> str:
        """Return MinIO scheme for endpoint composition."""

        return "https" if self.MINIO_SECURE else "http"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached settings instance."""

    return Settings()


settings = get_settings()
