# OpenFinance

OpenFinance is an open-source research platform for China’s financial markets. It brings together market data, financial news, and listed-company reports in a lakehouse, then makes them available through a web research assistant and APIs for market data, news, reports, and agent workflows.

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

## What it does

- **Market data:** Ingests A-share daily bars, maintains security and trading-calendar data, and serves historical daily, weekly, and monthly market queries.
- **News and annual reports:** Ingests and normalizes news and company reports; parses annual-report PDFs into searchable, page-linked evidence.
- **Evidence-based research:** Provides a browser UI and LangGraph research agent with persistent sessions, streaming answers, approval and resume flows, and citations from market, news, and report sources. Hybrid retrieval uses PostgreSQL with pgvector and OpenSearch.
- **Data processing:** Stores raw data in MinIO Bronze, curated tables in Iceberg Silver, and calculated daily indicators in Gold. Airflow schedules ingestion and processing; Spark builds and maintains analytical tables.
- **Live quotes:** A Kafka and Flink pipeline collects market snapshots; its windows are sampled statistics, not tick-by-tick trades.

## Deploy with Docker Compose

### Requirements

- Docker Engine or Docker Desktop with the Docker Compose v2 plugin.
- Docker Compose and sufficient resources for local Ollama embeddings. The startup script selects NVIDIA GPU mode when both a GPU and Docker's NVIDIA runtime are available, otherwise CPU mode.
- For chat, a DeepSeek API key entered in the web UI’s model settings. The key is sent with requests and is not stored in `.env`.

### Quick start

```bash
git clone https://github.com/mazijie/OpenFinance.git
cd OpenFinance
cp .env.example .env
```

Review `.env` before deployment. Replace the example values for the internal API token, OpenSearch password, PDF parser token, and Airflow secrets and administrator password. PostgreSQL and MinIO also use development credentials defined in the Compose file; change them consistently in the services that connect to them before using the stack beyond local testing. Add provider credentials only for integrations you plan to use. Keep `.env` private and do not commit it.

Build and start the default single-host stack:

```bash
bash scripts/compose.sh up -d --build
bash scripts/compose.sh ps
```

The default stack includes the backend, PostgreSQL, MinIO, OpenSearch, local Ollama embeddings, workers, Spark, Airflow, and the real-time quote pipeline. The backend runs `alembic upgrade head` before starting the API. Ollama downloads the configured embedding model during initialization, so the first startup may take a while and use significant disk space.

Detailed pipeline behavior, configuration, and operational limits are documented in the [finance pipeline guide](docs/finance-pipeline-guide.md), [Spark guide](spark/README.md), and [real-time quotes guide](docs/realtime-quotes.md).

### Service URLs

| Service | URL |
| --- | --- |
| Research UI | <http://localhost:8000/ui> |
| API and OpenAPI docs | <http://localhost:8000/docs> |
| Airflow | <http://localhost:8080> |
| Spark Master UI | <http://localhost:8081> |
| Spark Worker UI | <http://localhost:8082> |
| Spark History Server | <http://localhost:18080> |
| MinIO Console | <http://localhost:9001> |
| Flink UI | <http://localhost:8083> |

The Compose files are intended as a local, single-host reference deployment. They do not provide high availability or production security defaults. Replace sample credentials, restrict network access to development services, and arrange backups for persistent data before using the stack with important datasets. Configure a reachable Ray cluster and its storage endpoints for annual-report parsing at scale; see `.env.example` and the pipeline guide.

## Development and contributions

Issues and pull requests are welcome. For larger changes, open an [issue](https://github.com/mazijie/OpenFinance/issues) first. When submitting a [pull request](https://github.com/mazijie/OpenFinance/pulls), describe the user-visible change and include focused tests or documentation updates where appropriate. Install the Python dependencies from `requirements.txt`, then run relevant tests with `pytest -q`.

This project is licensed under the [Apache License 2.0](LICENSE). It provides financial data and research tools and does not constitute investment advice.

### 后端模块边界

- `/api/chat`、`/api/chat/stream` 和 `/api/agent` 均使用 Harness。旧的 `HARNESS_V1_ENABLED` 开关和旧 Agent 已移除；曾设置为 `false` 的部署升级后也会使用 Harness，请从环境配置中删除该项。
- `backend/lakehouse` 负责湖仓表和读写；`backend/job_store` 负责采集任务、租约、配额和采集运行状态。`backend/repositories` 面向业务查询。
- `backend/providers` 负责上游接口访问与响应解析；`backend/ingestion` 负责采集编排、去重和入库。包含采集编排的新闻采集器仍放在 `ingestion`。
- `backend/rag/common`、`news` 和 `annual_reports` 分别存放公共、新闻和年报检索能力。
- 新闻采集、清洗和索引入口分别为 `python -m backend.ingestion.jobs.ingest_news`、`python -m backend.pipelines.news_bronze_to_silver` 和 `python -m backend.pipelines.news_rag_index`。
- 年报通过 Ray 执行解析；`backend/pdf_parser` 包含 Ray 使用的解析服务类，`pdf-parser/requirements.txt` 保存解析节点依赖。

升级前应完成旧接口调用迁移：年报解析改用 `/internal/jobs/annual-reports-ray` 系列接口，新闻任务改用上述 `backend` 模块；旧新闻 `limit` 参数已移除。停止或完成使用固定预算的 Agent 任务后再升级，所有新任务使用动态预算。实时窗口迁移的盘点和重算步骤见 `docs/realtime-quotes.md`。
