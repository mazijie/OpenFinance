"""Application configuration."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent


class Settings(BaseSettings):
    """Centralized runtime settings loaded from environment variables."""

    APP_ENV: str = "local"
    LOG_LEVEL: str = "INFO"

    DEEPSEEK_BASE_URL: str = "https://api.deepseek.com"
    DATABASE_URL: str = ""
    DEEPSEEK_STANDARD_MODEL: str = "deepseek-flash"
    DEEPSEEK_DEEP_MODEL: str = "deepseek-v4-pro"
    DEEPSEEK_ALLOWED_MODELS_JSON: str = '["deepseek-flash","deepseek-v4-pro"]'
    LLM_TIMEOUT_SECONDS: float = 60.0
    LLM_MAX_RETRIES: int = 1

    EMBEDDING_API_KEY: str = ""
    EMBEDDING_BASE_URL: str | None = "http://localhost:8001/v1"
    EMBEDDING_MODEL: str = "bge-m3"
    EMBEDDING_PATH: str = "/embeddings"
    EMBEDDING_DIMENSION: int = 1024
    EMBEDDING_BATCH_SIZE: int = 16
    EMBEDDING_TIMEOUT_SECONDS: float = 120.0

    MINIO_ENDPOINT: str = "localhost:9000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"
    MINIO_SECURE: bool = False
    MINIO_REGION: str | None = None
    MINIO_BRONZE_BUCKET: str = "openfinance-bronze"
    MINIO_DERIVED_BUCKET: str = "openfinance-derived"
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
    ICEBERG_ANNUAL_REPORT_PARSE_RUN_TABLE: str = "annual_report_parse_runs"
    ICEBERG_ANNUAL_REPORT_REGION_TABLE: str = "annual_report_regions"
    ICEBERG_ANNUAL_REPORT_CHUNK_TABLE: str = "annual_report_chunks"
    ICEBERG_GOLD_METRICS_TABLE: str = "gold_security_daily_metrics"
    ICEBERG_S3_PATH_STYLE_ACCESS: bool = True

    # Daily history is independent of BaoStock security metadata and calendars.
    FINANCE_DAILY_PROVIDER: Literal["tencent"] = "tencent"
    TENCENT_CONNECT_TIMEOUT_SECONDS: float = Field(default=5.0, gt=0)
    TENCENT_READ_TIMEOUT_SECONDS: float = Field(default=15.0, gt=0)
    TENCENT_MAX_RETRIES: int = Field(default=2, ge=0, le=2)
    TENCENT_RETRY_BACKOFF_SECONDS: float = Field(default=2.0, ge=0)
    # Zero means no configured quota, not an assertion about upstream limits.
    TENCENT_DAILY_REQUEST_LIMIT: int = Field(default=0, ge=0)
    FINANCE_DEFAULT_ADJUSTMENT_TYPE: Literal["none"] = "none"
    FINANCE_DEFAULT_LOOKBACK_DAYS: int = 30
    BAOSTOCK_SOCKET_TIMEOUT_SECONDS: float = 15.0
    BAOSTOCK_SYMBOL_TIMEOUT_SECONDS: int = 45
    BAOSTOCK_MAX_RETRIES: int = 2
    BAOSTOCK_RETRY_BACKOFF_SECONDS: float = 2.0
    BAOSTOCK_DAILY_REQUEST_LIMIT: int = 50_000
    A_SHARE_GAP_LOOKBACK_DAYS: int = 60
    A_SHARE_MAX_SYMBOLS_PER_RUN: int = 10_000
    A_SHARE_INGEST_BATCH_SIZE: int = 20
    A_SHARE_NORMALIZATION_BATCH_SIZE: int = 10
    FINANCE_JOB_STALE_SECONDS: int = 300
    FINANCE_NORMALIZATION_STALE_SECONDS: int = 600
    A_SHARE_DAILY_LOOKBACK_DAYS: int = 3
    A_SHARE_DAILY_AIRFLOW_SCHEDULE: str = "15 18 * * 1-5"
    A_SHARE_BACKFILL_START_DATE: str = "1990-01-01"
    A_SHARE_BATCH_SIZE: int = 100
    A_SHARE_MAX_ACTIVE_BATCHES: int = 4
    ICEBERG_WRITE_MAX_RETRIES: int = 10
    ICEBERG_WRITE_INITIAL_BACKOFF_SECONDS: float = 1.0
    ICEBERG_WRITE_MAX_BACKOFF_SECONDS: float = 30.0
    ICEBERG_WRITE_LOCK_TIMEOUT_SECONDS: float = 300.0

    NEWS_HTTP_TIMEOUT_SECONDS: float = 10.0
    # External request retries are opt-in and default to zero.
    NEWS_HTTP_MAX_RETRIES: int = 0
    NEWS_HTTP_RETRY_BACKOFF_SECONDS: float = 1.0
    CNINFO_USER_AGENT: str = "OpenFinance-Agent/0.1 (+https://www.cninfo.com.cn/)"
    CNINFO_REPORT_STOCKS_JSON: str = "[]"
    CNINFO_REPORT_YEARS_JSON: str = "[]"
    CNINFO_REPORT_AIRFLOW_SCHEDULE: str = "0 3 * * *"
    PDF_PARSER_URL: str = "http://pdf-parser:8090"
    PDF_PARSER_TOKEN: str = "replace-with-pdf-parser-token"
    PDF_PARSER_TIMEOUT_SECONDS: float = 14_400.0
    PDF_PARSER_VERSION: str = "annual-report-mineru-basic-4.0.12-v1"
    PDF_LAYOUT_ENGINE: str = "mineru"
    PDF_MINERU_TIER: str = "basic"
    PDF_RENDER_DPI: int = 200
    PDF_OCR_CONFIDENCE_THRESHOLD: float = 0.78
    RAY_ADDRESS: str = "auto"
    RAY_DASHBOARD_URL: str = "http://127.0.0.1:8265"
    RAY_RUNTIME_WORKING_DIR: str = ""
    # Storage settings used by Ray workers. These must be reachable from every
    # CPU/GPU node; they are intentionally separate from the Compose-local
    # DATABASE_URL/MINIO_ENDPOINT values.
    RAY_DATABASE_URL: str = ""
    RAY_MINIO_ENDPOINT: str = ""
    RAY_MINIO_ACCESS_KEY: str = ""
    RAY_MINIO_SECRET_KEY: str = ""
    RAY_MINIO_SECURE: bool | None = None
    RAY_MINIO_REGION: str | None = None
    RAY_ICEBERG_CATALOG_URI: str = ""
    RAY_ICEBERG_WAREHOUSE: str = ""
    # Production Ray workers should have their parsing dependencies installed.
    RAY_RUNTIME_PIP_PACKAGES_JSON: str = "[]"
    RAY_RUNTIME_PIP_INDEX_URL: str = ""
    RAY_RUNTIME_PIP_EXTRA_INDEX_URL: str = ""
    RAY_CPU_TASK_CPUS: int = 4
    RAY_GPU_TASK_CPUS: int = 4
    RAY_MAX_IN_FLIGHT_REPORTS: int = 24
    RAY_CPU_RESOURCE_LABEL: str = "annual_report_cpu"
    RAY_GPU_RESOURCE_LABEL: str = "annual_report_gpu"
    RAY_JOB_TIMEOUT_SECONDS: float = 14_400.0
    RAY_JOB_POLL_SECONDS: float = 5.0
    ANNUAL_REPORT_RECOVERY_SCHEDULE: str = "15 4 * * *"
    ANNUAL_REPORT_MAX_STAGE_ATTEMPTS: int = 5
    NEWS_DEFAULT_LIMIT: int = 50
    CAIXIN_NEWS_URL: str = "https://cxdata.caixin.com/api/dataplus/sjtPc/news"
    # Legacy collector settings retained for historical integrations.
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
    NEWS_INGEST_LOOKBACK_HOURS: int = 6
    NEWS_AIRFLOW_SCHEDULE: str = "*/30 * * * *"
    OPENFINANCE_INTERNAL_API_BASE_URL: str = "http://localhost:8000"
    OPENFINANCE_INTERNAL_API_TOKEN: str = "replace-with-a-shared-internal-token"

    RAG_VECTOR_TABLE: str = "news_chunks"
    RAG_ANNUAL_REPORT_VECTOR_TABLE: str = "annual_report_chunks"
    RAG_EMBEDDING_MODEL: str = "bge-m3"
    RAG_EMBEDDING_PATH: str = "/embeddings"
    RAG_EMBEDDING_DIMENSION: int = 1024
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
    OPENSEARCH_ANNUAL_REPORT_INDEX: str = "openfinance_annual_report_chunks"
    OPENSEARCH_USERNAME: str = ""
    OPENSEARCH_PASSWORD: str = ""
    OPENSEARCH_TIMEOUT_SECONDS: float = 10.0

    RAG_RERANK_ENABLED: bool = False
    RAG_RERANK_URL: str = ""
    RAG_RERANK_API_KEY: str = ""
    RAG_RERANK_MODEL: str = "rerank-v3.5"
    RAG_RERANK_TIMEOUT_SECONDS: float = 10.0
    RAG_RERANK_MIN_SCORE: float = 0.15


    HARNESS_V1_ENABLED: bool = True
    HARNESS_DYNAMIC_BUDGET_ENABLED: bool = True
    HARNESS_STANDARD_MAX_TOOL_CALLS: int = 6
    HARNESS_STANDARD_MAX_STEPS: int = 3
    HARNESS_STANDARD_TIMEOUT_SECONDS: int = 60
    HARNESS_DEEP_MAX_TOOL_CALLS: int = 12
    HARNESS_DEEP_MAX_STEPS: int = 5
    HARNESS_DEEP_TIMEOUT_SECONDS: int = 180
    HARNESS_BUDGET_SIMPLE_MAX_TOOL_CALLS: int = 3
    HARNESS_BUDGET_SIMPLE_MAX_STEPS: int = 2
    HARNESS_BUDGET_SIMPLE_TIMEOUT_SECONDS: int = 45
    HARNESS_BUDGET_MEDIUM_MAX_TOOL_CALLS: int = 8
    HARNESS_BUDGET_MEDIUM_MAX_STEPS: int = 4
    HARNESS_BUDGET_MEDIUM_TIMEOUT_SECONDS: int = 90
    HARNESS_BUDGET_COMPLEX_MAX_TOOL_CALLS: int = 16
    HARNESS_BUDGET_COMPLEX_MAX_STEPS: int = 6
    HARNESS_BUDGET_COMPLEX_TIMEOUT_SECONDS: int = 240
    HARNESS_BUDGET_HARD_MAX_TOOL_CALLS: int = 20
    HARNESS_BUDGET_HARD_MAX_STEPS: int = 8
    HARNESS_BUDGET_HARD_TIMEOUT_SECONDS: int = 360
    HARNESS_BUDGET_REVIEW_MIN_CONFIDENCE: float = 0.7
    HARNESS_BUDGET_MAX_NO_PROGRESS_ROUNDS: int = 2
    HARNESS_MAX_PARALLEL_TOOLS: int = 4
    HARNESS_CONTEXT_MAX_CHARS: int = 24_000
    HARNESS_CONTEXT_RECENT_MESSAGES: int = 12
    HARNESS_APPROVAL_TTL_SECONDS: int = 86_400
    HARNESS_CHECKPOINT_RETENTION_DAYS: int = 30
    AGENT_WEB_FETCH_TIMEOUT_SECONDS: float = 10.0
    AGENT_WEB_MAX_BYTES: int = 2_000_000
    AGENT_WEB_MAX_REDIRECTS: int = 3
    AGENT_WEB_CACHE_TTL_SECONDS: int = 3_600
    AGENT_WEB_SEARCH_ENABLED: bool = True
    AGENT_WEB_SEARCH_ENDPOINT: str = "https://www.cninfo.com.cn/new/fulltextSearch/full"
    AGENT_WEB_ANNOUNCEMENT_ENDPOINT: str = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
    AGENT_WEB_SEARCH_MAX_RESULTS: int = 8
    AGENT_WEB_SEARCH_FETCH_LIMIT: int = 3

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
    def resolved_embedding_api_key(self) -> str:
        """Return the independently configured embedding API key."""

        return self.EMBEDDING_API_KEY

    @property
    def resolved_embedding_base_url(self) -> str:
        """Return the independently configured embedding base URL."""

        return self.EMBEDDING_BASE_URL or ""

    @property
    def llm_allowed_models(self) -> tuple[str, ...]:
        """Return the model identifiers accepted from an API request."""

        try:
            models = json.loads(self.DEEPSEEK_ALLOWED_MODELS_JSON or "[]")
        except json.JSONDecodeError as exc:
            raise ValueError("DEEPSEEK_ALLOWED_MODELS_JSON must be valid JSON") from exc
        parsed = tuple(str(model).strip() for model in models if str(model).strip())
        if not parsed:
            raise ValueError("DEEPSEEK_ALLOWED_MODELS_JSON must contain at least one model")
        return parsed

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
