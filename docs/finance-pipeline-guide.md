# 金融数据处理与运行指南

面向中国金融市场的开源研究助手，提供从数据入湖、指标计算到自然语言问答的完整能力。

## 升级说明

升级到移除投资监测的版本前，停止旧版 `monitor-worker`、监测 DAG 和正在运行的 Agent 任务，并备份需要保留的数据。后端启动时自动执行的数据库迁移会永久删除监测、自选清单数据以及所有现有研究会话和对应 checkpoint。迁移不会清理 Gold 指标、行情、新闻或年报数据。更新实时采集器和 Gold 调度后，它们通过 `/internal/jobs/trading-day` 查询交易日。

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

- 采集财新数据通最新财经新闻摘要，保留原始响应、发布时间和原文链接；历史聚合数据仍可读取
- 默认每 30 分钟采集上游返回的全部有效新摘要。上游只提供最新列表，停机或更新过快可能漏采，无法保证历史回填；时间窗口参数仅记录在 Bronze 中，不能限制上游结果。采集接口不提供数量限制参数
- 检查 `caixin_collect_completed` 日志中的获取、无效、重复和入库候选数量，结合 Silver 和索引任务状态排查；上游返回满页时需关注漏采风险。部署环境应先核实接口访问和内容存储、索引及展示权限
- 若 Bronze 已写入但 Silver 任务失败，采集去重会识别已有 Bronze 新闻，不会自动重新提交该对象；修复故障后，通过 `/internal/jobs/news-bronze-to-silver` 指定失败的 `object_paths` 重放，再运行 `/internal/jobs/news-rag-index`。也可用该接口重放历史聚合数据 Bronze 对象
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
- 行情、新闻和年报按 `as_of` 时点检索，避免回测使用未来信息
- 数据降级时不生成建议，模型输出经过 Schema、引用和风险提示校验

### 调度与可靠性

- 使用 Airflow 调度采集、标准化、索引和补数任务
- 使用 PostgreSQL 持久化后台任务与每日 API 配额
- 支持动态批处理、幂等重试和失败任务恢复
- 提供 Spark、Iceberg 和 MinIO 数据处理链路

历史回填遇到部分股票采集失败时，采集任务会在日志中立即列出失败代码，保留成功批次的 Bronze 数据，并按股票单独重试。重试轮数由 `A_SHARE_FAILED_SYMBOL_RETRIES` 控制，默认 2；重试得到的数据与原始批次一起标准化。重试后仍失败的股票会在质量检查中列出，此时不会记录全市场覆盖，但已写入的数据保留。旧运行的结果快照不会随代码更新自动改变，恢复旧运行前需核对失败股票和数据源错误，不要直接把质量检查标记为成功。

A 股股票名单、证券信息和日线使用腾讯接口，日线固定为不复权（`none`）；交易日历暂时使用 BaoStock。回填以腾讯返回的股票名单为范围，质量检查保留成功结果并列出重试后失败或无数据的股票。任务会重新获取股票名单；历史退市证券如未出现在腾讯返回的名单中，需要另行补充。

旧版前复权数据不会因修改代码自动删除或变成不复权数据。切换线上运行前，必须先停止旧采集和归一化任务、备份并核对 Bronze、Silver 和 Gold 中的旧行情以及任务状态，再清理指定范围并重新回填；应保留新闻、年报和交易日历。不要直接将旧前复权数据改标签或重放旧 Bronze。

年报 Silver 回放使用 `openfinance_annual_report_bronze_replay` DAG。它只扫描已有 Bronze
manifest，复用已有 Derived IR，然后写入当前 Silver 并重建 Gold，不会调用 CNInfo 或重复下载 PDF。
例如只回放贵州茅台 2024 年报：

```bash
docker compose up -d airflow-init airflow-scheduler airflow-webserver
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
年报解析通过 Ray 执行。
