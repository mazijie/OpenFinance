# 金融数据处理与运行指南

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
- 历史监测能力和数据继续保留，默认关闭定时运行
- 行情、新闻和年报按 `as_of` 时点检索，避免回测使用未来信息
- 数据降级时不生成建议，模型输出经过 Schema、引用和风险提示校验

### 调度与可靠性

- 使用 Airflow 调度采集、标准化、索引和补数任务
- 使用 PostgreSQL 持久化后台任务与每日 API 配额
- 支持动态批处理、幂等重试和失败任务恢复
- 提供 Spark、Iceberg 和 MinIO 数据处理链路
- 使用 PostgreSQL 持久化监护清单、监测运行、报告和执行轨迹
- 提供 Gold 指标调度；历史监测调度默认关闭

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
