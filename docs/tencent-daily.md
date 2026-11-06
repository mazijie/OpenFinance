# 腾讯日线采集与时延验证

日线来源由 `FINANCE_DAILY_PROVIDER` 独立选择，支持 `tencent` 和 `baostock`。腾讯使用 `https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get`，证券目录、主数据和交易日历继续由 BaoStock 提供。腾讯请求不会获取 BaoStock 会话锁，也不会在失败后自动改用 BaoStock。

日线默认使用腾讯；本机 `.env`、新服务器的 `.env.example` 和 `docker-compose.server.yml` 均选择腾讯。Airflow 新建的回填和日常任务会固定该来源。已入队且显式指定来源的任务仍使用其已保存的来源。本次只切换配置，没有启动、重启或触发业务采集任务。

## 数据口径

- 支持 `none`、`qfq`、`hfq`。正式采集每次最多请求 640 条，以 730 个自然日分段，再过滤日期、排序去重；适配器不依赖上游严格执行起始日期。
- 开高低收由上游复权结果直接给出，前复权可以是负数，所有数值必须有限且 OHLC 一致。
- 成交量统一为股：科创板 `sh688`、科创板 CDR `sh689` 原始单位为股，其余支持的股票按手乘 100。成交额由万元转换为元；换手率沿用现有 BaoStock 百分数口径，`1.25` 表示 `1.25%`。
- 独立复权因子、历史 ST 和停牌标识未由该接口可靠提供，保存为 `null`，不根据当前证券名称推断历史 ST，也不把没有返回的日期伪造成停牌日线。新建 Arrow/Iceberg 表允许这些字段为空，Gold 和公开行情响应同样允许为空。Spark 仅排除已明确标记停牌的行，保留原有指标预热窗口。
- Bronze 混合载荷里的证券主数据保留真实 `baostock` 来源；日线对象、缺口键及补数完成记录使用实际日线来源。公开行情接口地址不变，内部提交接口支持可选的 `daily_provider_name`，入队时固定选择，幂等键和任务租约机制保留。

## 请求控制

默认连接超时 5 秒，读取超时 15 秒。临时网络错误、429、5xx 最多重试两次，默认等待 2、4 秒；永久 HTTP 错误和结构异常不重试。腾讯日线不使用 BaoStock 的整股超时或第二层重试，以免把三次 HTTP 尝试放大成九次。每次请求记录网络时间、含解析总时间、重试数和错误。

`TENCENT_DAILY_REQUEST_LIMIT=0` 表示没有配置本地每日配额，不代表腾讯没有上游限制。设置正数后，每次实际 HTTP 尝试（包括重试）均预留腾讯配额；BaoStock 元数据仍独立使用原预算。缺口规划按实际日期段估算请求数，不使用固定每股请求数配置。

## 只测试少量数据

在项目根目录使用项目 Python 环境运行：

```powershell
python -m backend.ingestion.jobs.benchmark_tencent_daily
```

固定测试贵州茅台、平安银行、中芯国际、宁德时代、中国平安、招商银行、艾融软件和万达轴承，每只 5 根前复权日线。串行和 8 并发各预热一轮、正式测试三轮，共 64 次逻辑查询。自动日期探测另计一次；也可用 `--end-date YYYY-MM-DD` 指定已收盘日期，避免日期探测。

脚本只输出文件，不使用采集队列、数据库、MinIO 或行情表。默认目录为 `.cache/tencent-daily/<时间>/`，包含：

- `raw_responses.json`：各次 HTTP 原始响应、重试、规范化记录和时延。
- `summary.json`：统计结果、每轮耗时、吞吐量和首次成功率。
- `report.md`：简短对比报告。预热不计入统计，任何样本失败则进程以非零状态退出。

可用 `--output <新目录>` 指定保存位置；脚本拒绝覆盖已存在的结果目录。本次结果见 [2026-11-05 实测报告](tencent-daily-latency-20261105.md)。原始数据保存在本机 `.cache/tencent-daily/20261105-latency/`。

## 新服务器部署

部署时使用服务器自己的 `.env`、空的金融数据表和独立仓库，不复制本机旧日线。服务器模板默认腾讯日线、8 个采集 Worker，每 Worker 串行，Airflow 最多同时运行 8 批；不存在批次内线程池放大并发。目录和日历仍为 BaoStock。

以下命令仅供后续服务器部署使用，本次没有执行：

```powershell
docker compose -f docker-compose.yml -f docker-compose.server.yml up -d
```

通过 `A_SHARE_INGEST_WORKERS` 和 `A_SHARE_MAX_ACTIVE_BATCHES` 调整 Worker 和批次数。HTTP 超时、重试及可选配额使用 `.env.example` 中的 `TENCENT_*` 配置。新建表包含可空字段；本次没有对已有表执行迁移，也没有实现旧数据拼接或迁移标记。服务器的全市场采集和历史补数需要后续单独执行。

## 验证

提供 Tencent 模拟 HTTP 测试、采集与规范化测试、缺口规划和队列来源测试，以及 `tests/spark/check_tencent_daily.py` 的独立 Spark 校验。测试覆盖复权格式、单位、分段、负前复权价、空响应、重试、并发、不经过 BaoStock 会话锁、缺失字段和 Gold 预热。

时延结果只代表这组请求在本机当前网络的表现，不能证明长期稳定性或不受限并发。
