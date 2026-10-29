# OpenFinance-Agent

面向中国金融市场的开源研究助手，提供从数据入湖、指标计算到自然语言问答的完整能力。

## 主要功能

### 金融数据入湖

- 定时采集 A 股行情、财经新闻和上市公司年报
- 支持 MinIO Bronze、Iceberg Silver 和 Gold 分层存储
- 自动发现缺失证券和交易日，并按每日 API 配额渐进补数
- 写入具备幂等性，任务重试不会产生重复业务数据

### 市场数据分析

- 查询单只股票的历史 K 线、成交量和成交额
- 提供收益率、移动平均、波动率和成交量均值等日频指标
- 支持股票列表、行情摘要和 Point-in-time 历史查询
- 为全市场排名、因子筛选和事件研究预留 Gold 数据层

### 新闻与年报检索

- 采集并标准化财经新闻
- 支持 PostgreSQL pgvector 与 OpenSearch 混合检索
- 支持年报 PDF 下载、正文提取、章节识别和页码引用
- 年报 PDF 默认通过 Ray 和 MinerU 按页面/区域选择原生文本、表格或 OCR/布局解析；完整 IR 保存在 MinIO Derived，Silver 只保留检索和回溯字段
- 回答中返回新闻、报告页码等可追溯证据

### 金融研究 Agent

- 基于 LangGraph 编排行情、新闻和年报工具
- 自动识别股票代码和问题类型
- 支持行情分析、事件解释和年报问答
- 数据不可用时明确返回限制，不绕过内部湖仓访问外部 API
- 提供自建 Agent Harness：多线程会话、模型工具循环、预算、审批、取消和 SSE 流式事件
- 新闻 RAG、年报 RAG、行情与白名单网页统一为一套证据和引用协议
- 支持 A 股监护清单和收盘后 5/20 交易日方向与条件建议
- 行情、新闻和年报按 `as_of` 时点检索，避免回测使用未来信息
- 数据降级时不生成建议，模型输出经过 Schema、引用和风险提示校验

### 调度与可靠性

- 使用 Airflow 调度采集、标准化、索引和补数任务
- 使用 PostgreSQL 持久化后台任务与每日 API 配额
- 支持动态批处理、幂等重试和失败任务恢复
- 提供 Spark、Iceberg 和 MinIO 数据处理链路
- 使用 PostgreSQL 持久化监护清单、监测运行、报告和执行轨迹
- 提供 `openfinance_gold_metrics_pipeline` 与 `openfinance_investment_monitor_pipeline`

年报 Silver 回放使用 `openfinance_annual_report_bronze_replay` DAG。它只扫描已有 Bronze
manifest，复用已有 Derived IR，然后写入当前 Silver 并重建 Gold，不会调用 CNInfo 或重复下载 PDF。
例如只回放贵州茅台 2024 年报：

```bash
docker compose --profile airflow up -d airflow-init airflow-scheduler airflow-webserver
docker exec openfinance-airflow-scheduler airflow dags trigger \
  openfinance_annual_report_bronze_replay \
  --conf '{"years":[2024],"stocks":["600519.SH"]}'
```

把 `stocks` 从配置中去掉即可回放该年份所有完整 Bronze manifest。首次回放没有 Derived IR
的报告会走 GPU PDF 解析，任务可以在 Airflow 中按报告重试和观察状态。

故障恢复、对账、`needs_review`、generation 切换和 Docker GPU 排查记录见
[年报 Pipeline 故障与恢复记录](docs/annual-report-pipeline-recovery.md)。

默认解析配置为 MinerU 4.0.12 的 `basic` 模式，依赖需安装在执行解析的 Ray worker 上。
在本地 `.env` 中设置 `RAY_DASHBOARD_URL`，指向 backend 容器可访问的远程 Ray Dashboard 地址，
例如 `http://<ray-server-host>:8265`。Ray worker 所需的存储地址也需要按 `.env.example` 配置为远程节点可访问的地址。
更换模式或模型时应同时更新 `PDF_PARSER_VERSION`，避免复用不兼容的解析结果。
可选的 `legacy-parser` Compose 服务仍使用其独立的 Paddle 环境，不参与默认 Ray 链路。

## 投研监测

运行链路：

```text
监护清单
→ Gold 指标 + 新闻 RAG + 年报 RAG
→ 确定性事件检测
→ 有界 LangGraph 研判
→ 站内监测收件箱
```

初始化监测表：

```bash
alembic upgrade head
```

监测 API：

```text
GET/POST       /api/watchlist
PATCH/DELETE   /api/watchlist/{symbol}
POST           /api/monitor/runs
GET            /api/monitor/runs/{run_id}
GET            /api/monitor/reports
GET            /api/monitor/reports/{report_id}
```

工作日调度：

- 19:30 Asia/Shanghai 增量刷新最近 45 天 Gold 指标。
- 20:00 Asia/Shanghai 运行监护清单并生成收盘后报告。
- Airflow 先检查本地交易日历，休市日跳过。
- Gold 任务从 scheduler 以 Spark client 模式提交，镜像内置 Iceberg 与 PostgreSQL JAR。
- `monitor-worker` 从 PostgreSQL 队列消费监测任务，多 worker 使用 `SKIP LOCKED` 竞争。

构建并启动完整环境：

```bash
docker compose --profile airflow --profile spark up -d --build
```

### Shadow mode

默认 `INVESTMENT_MONITOR_RECOMMENDATIONS_ENABLED=false`。Agent 仍将建议写入数据库供
评估，但公开 API 和网页仅显示事件、证据与数据限制。至少积累 20 个交易日结果后运行：

```bash
python -m backend.monitoring.evaluation --since 2026-01-01
```

启用门槛为：5 日方向命中率至少 52%，20 日至少 55%，20 日扣除 20bp 往返成本后平均
净收益为正，且相对 20 日动量基线至少提升 5 个百分点。通过后再将配置改为 `true`。

交易建议只包含方向、触发与失效条件，不包含目标价、仓位或订单，也不会连接券商。
数据未就绪时系统输出降级报告，不回退到外部行情或新闻接口。

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
监测任务从对话提交时会追加到当前线程；Airflow 和仪表盘提交会创建按交易日归档的系统线程，
监测完成后报告卡自动写入该线程。

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

本项目仅用于金融数据研究与技术学习，不构成投资建议。
