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
- 回答中返回新闻、报告页码等可追溯证据

### 金融研究 Agent

- 基于 LangGraph 编排行情、新闻和年报工具
- 自动识别股票代码和问题类型
- 支持行情分析、事件解释和年报问答
- 数据不可用时明确返回限制，不绕过内部湖仓访问外部 API

### 调度与可靠性

- 使用 Airflow 调度采集、标准化、索引和补数任务
- 使用 PostgreSQL 持久化后台任务与每日 API 配额
- 支持动态批处理、幂等重试和失败任务恢复
- 提供 Spark、Iceberg 和 MinIO 数据处理链路

## 服务入口

- Web 界面：`http://localhost:8000/ui`
- Backend API：`http://localhost:8000`
- Airflow：`http://localhost:8080`
- Spark Master：`http://localhost:8081`
- MinIO Console：`http://localhost:9001`

## 说明

本项目仅用于金融数据研究与技术学习，不构成投资建议。
