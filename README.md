# OpenFinance-Agent

OpenFinance-Agent 是面向中国金融市场的数据处理与智能研究平台，覆盖金融数据采集、湖仓处理、作业调度、可观测性和基于证据的问答。

## 数据架构

金融数据源 → Python Worker → MinIO Bronze → Silver（Iceberg）→ Spark Gold 指标 → 研究 API 与 Agent

- Airflow 负责采集与处理调度，PostgreSQL 保存任务状态及 Iceberg Catalog 元数据。
- Spark 负责 Gold 指标计算与 Silver 重建，History Server 保存并展示作业事件。
- 年报解析保留原文与可回溯的结构化结果；新闻和年报使用 PostgreSQL pgvector、OpenSearch 与 RAG 检索。
- LangGraph Harness 提供持久化会话、工具调用、审批和可追溯引用。

## 运行与作业观测

参见 [金融数据处理与运行指南](docs/finance-pipeline-guide.md)、[Spark 作业与配置](spark/README.md) 和 [性能基准说明](spark/benchmark/README.md)。
Spark Master、Worker 与 History Server 可通过 Docker Compose 启动。History Server 默认在
http://localhost:18080，事件日志保存在 .cache/spark/events。

监测入口和定时调度默认关闭，既有 API 与数据仍保留；历史功能见
[投研监测](docs/investment-monitor.md)。Gold 更新调度独立运行。

## 后续方向

实时行情流处理学习链路参见 [行情流处理指南](docs/realtime-quotes.md)。

- 改进 Bronze 到 Silver 的历史回填、质量校验和故障恢复。
- 复用 Harness 提供基于作业状态、性能指标和表元数据的只读诊断。
- 扩展任务事件流、流处理与表格式对比能力。

## 统一研究 Harness


主页面“行情、智能问答”是统一研究入口：

```text
持久化线程
→ LangGraph Agent 循环
→ research_search 统一证据
→ 新闻/年报/行情/白名单网页
→ 审批中断与恢复
→ SSE 流式回答和引用
```

只读工具并行执行，写操作通过 LangGraph interrupt 暂停并等待确认。

聊天面板支持多个持久化会话标签，可新建、切换、双击重命名和归档。模型规划、工具检索、
生成回答和总耗时以运行记录的形式插入消息流；对话消息和运行事件均保存在 PostgreSQL，
刷新页面或重新打开会话后可以恢复。

交互研究默认启用动态预算：规则优先的任务分析器选择 `simple`、`medium` 或 `complex`
初始档位，模糊问题使用 `deepseek-flash` 结构化补判；每轮证据收集后由固定 flash 复核证据
充分性与冲突，确定性控制器负责扩容、终止和不可突破的硬上限。可通过
`HARNESS_DYNAMIC_BUDGET_ENABLED=false` 回退到原有固定预算。`mode=standard|deep` 继续
保留以兼容现有调用方，但启用动态预算后两者使用相同硬上限。

### DeepSeek 对话配置

聊天模型使用 DeepSeek 的 OpenAI 兼容接口。API Key 不进入 `.env`、数据库或会话消息：

- Web 端在“智能问答”的模型设置中填写 Key，可临时选择 `deepseek-flash` 或 `deepseek-v4-pro`；
  Key 仅保存在当前标签页的 `sessionStorage`。
- 后端和脚本调用使用 `X-LLM-API-Key` 请求头；模型可用 `X-LLM-Model` 指定。
- 缺少 Key 时接口返回 401；不支持的模型返回 422。
- 审批恢复请求也必须再次携带 Key，因为服务端不会持久化用户凭据。

```bash
curl -X POST http://localhost:8000/api/chat \
  -H "Content-Type: application/json" \
  -H "X-LLM-API-Key: $DEEPSEEK_API_KEY" \
  -H "X-LLM-Model: deepseek-flash" \
  -d '{"question":"查询贵州茅台最近一个月行情"}'
```

embedding 使用独立的本地 OpenAI 兼容服务。Compose 默认启动 Ollama 容器并拉取
`bge-m3`（1024 维），后端通过 `http://embedding:11434/v1/embeddings` 调用：

```bash
docker compose up -d embedding embedding-init
```

本地 embedding 不要求 API Key。若切换 embedding 模型并改变向量维度，空向量表会自动重建；
已有数据时必须显式清空并重新索引，系统会拒绝混用不同维度的向量。reranker 暂未启用。

主要 API：

```text
GET/POST       /api/agent/threads
GET/PATCH/DELETE /api/agent/threads/{thread_id}
GET            /api/agent/threads/{thread_id}/messages
POST           /api/agent/threads/{thread_id}/turns
POST           /api/agent/threads/{thread_id}/approvals/{approval_id}
POST           /api/agent/threads/{thread_id}/turns/{turn_id}/cancel
GET            /api/agent/runs/{turn_id}
GET/POST/PATCH/DELETE /api/agent/web-domains
```

`/api/chat` 保留为非流式兼容入口。启用统一入口使用 `HARNESS_V1_ENABLED=true`。
历史监测报告继续保留在对话中。

### 外部网页边界

- 交互研究在内部行情、新闻和年报均为空时，会自动检索 CNInfo 公开公告；白名单页面抓取
  PDF/HTML 正文，无法抓取时保留带原始链接的公告标题作为降级证据
- 定时监测和回测仍只使用内部湖仓，不访问网页。
- 只允许 HTTPS 与白名单域名。
- 预置 CNInfo、上交所、深交所、北交所、证监会、央行、统计局和国务院域名。
- 抓取会校验 DNS 结果，拒绝私网、回环和链路本地地址，并重新校验重定向。
- 网页遵守 robots.txt，限制响应大小和超时，正文快照持久化后用于引用。
- 定时监测和回测禁止使用网页；只有交互研究可以联网。

LangGraph 检查点、会话和业务消息使用独立表，完整历史保留，模型上下文使用滚动摘要。
归档线程的检查点由每日 `openfinance_harness_maintenance_pipeline` 按
`HARNESS_CHECKPOINT_RETENTION_DAYS` 清理；也可手动运行：

```bash
python -m backend.maintenance.prune_harness_checkpoints --days 30
```

## 服务入口

- Web 界面：`http://localhost:8000/ui`
- Backend API：`http://localhost:8000`
- Airflow：`http://localhost:8080`
- Spark Master：`http://localhost:8081`
- MinIO Console：`http://localhost:9001`

## 说明

本项目提供金融数据与研究工具，不构成投资建议。
