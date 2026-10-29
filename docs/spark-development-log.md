# Spark 研发问题记录

本文记录 OpenFinance-Agent 接入 Spark 学习链路时遇到的工程问题。
每次新增记录应包含日期、现象、根因、处理方式和验证结果。

## 2026-10-25：Docker Hub 拉取 Spark 镜像超时

**现象**

`docker compose` 从 Docker Hub 拉取 `apache/spark` 时出现 TLS handshake
timeout。

**处理**

- 默认基础镜像切换到国内镜像 `docker.1ms.run/apache/spark:4.1.2-python3`。
- 使用 `SPARK_BASE_IMAGE` 作为可覆盖入口。
- 增加 `spark/Dockerfile`，通过阿里云 Maven 源安装 Iceberg 和 JDBC 驱动。
- 对下载的 JAR 执行 SHA-1 校验。

**验证**

Spark Master/Worker 可以从国内镜像构建并启动。

## 2026-10-25：Spark 独立于项目 Compose project

**现象**

首次实验使用 `spark/docker-compose.yml`，Spark 属于独立的
`openfinance-spark-learning` project，无法通过项目默认网络访问 MinIO 和
PostgreSQL。

**处理**

- 将 `spark-master` 和 `spark-worker` 合并到根目录 `docker-compose.yml`。
- 使用 `profiles: ["spark"]` 隔离，普通启动命令不会拉起 Spark。
- Spark UI 使用宿主端口 `8081`，避免与 Airflow 的 `8080` 冲突。
- Worker UI 使用宿主端口 `8082`。

**验证**

两个服务显示在 `openfinance-agent` project 中，并可通过 `minio:9000`、
`postgres:5432` 访问项目组件。

## 2026-10-25：应用完成后 Executor 状态为 KILLED

**现象**

Spark Worker UI 中 Executor 最终状态为 `KILLED`。

**根因**

作业完成时调用了 `SparkContext.stop()`，Driver 正常注销应用，Master
要求 Worker 释放 Executor。日志中的 `exitStatus 143` 表示进程收到
`SIGTERM`，属于正常资源回收。

**判断标准**

- 应用状态为 `FINISHED`、没有失败 Stage 时，Executor 最终 `KILLED` 正常。
- 应用仍在运行时 Executor 变成 `KILLED`、出现 `Lost executor` 或
  `exitStatus 137` 才需要排查。

## 2026-10-25：Spark 看不到现有 Iceberg 命名空间

**现象**

Spark SQL 能连接 PostgreSQL，但 `SHOW TABLES IN <catalog>.openfinance`
返回 `NoSuchNamespaceException`；PyIceberg 却能列出表。

**根因**

Iceberg JDBC Catalog 将 Spark 的 Catalog 名称写入
`iceberg_tables.catalog_name`。现有表记录使用 `openfinance`，而 Spark
Catalog 最初命名为 `lakehouse`，因此查询到了另一组元数据命名空间。

**处理**

将 Spark Catalog 名称改为 `openfinance`，访问路径变为：

```text
openfinance.openfinance.daily_bars
```

**验证**

`SHOW TABLES IN openfinance.openfinance` 能列出 `daily_bars`、
`security_master`、`trading_calendar` 和 `news_articles`。

## 2026-10-25：并行 Spark SQL 争用 Derby Metastore

**现象**

同时启动两个 `spark-sql` 进程时出现：

```text
Another instance of Derby may have already booted the database
.../metastore_db
```

**根因**

两个 Driver 都把项目共享目录作为工作目录，并尝试打开同一个本地 Derby
Hive Metastore。

**处理**

- 项目元数据已经由 PostgreSQL Iceberg JDBC Catalog 管理。
- Spark 配置 `spark.sql.catalogImplementation=in-memory`。
- Spark 临时 Warehouse 改为 `file:/tmp/spark-warehouse`。

**验证**

Spark SQL 不再在项目目录创建或争用 `metastore_db`。

## 2026-10-25：真实 Iceberg 全量统计耗时较高

**现象**

首次对 `daily_bars` 执行全量 Count 和 distinct 统计耗时约 106 秒。

**观察**

- 表包含 454,993 行、50 个有日线数据的证券。
- 日期范围为 1991-01-29 至 2026-09-03。
- 表按交易日、交易所和 provider 分区，MinIO 上存在较多小文件。
- 首次查询还包含 Spark 冷启动、Iceberg 元数据和 S3 连接开销。

**后续处理**

- 学习作业优先指定日期范围，验证 Iceberg 分区裁剪。
- 使用 `EXPLAIN` 和 Spark UI 区分扫描、Shuffle 和写入开销。
- 后续单独评估 Iceberg 小文件合并和分区策略。

## 2026-10-25：真实 Silver 行情窗口作业验证

**目标**

绕开合成样例，直接读取项目已有 Iceberg Silver 行情，并使用 Spark
计算日频指标。

**输入**

```text
openfinance.openfinance.daily_bars
```

过滤窗口为 `2025-01-01` 至当前最新交易日。

**结果**

- 输入和输出均为 24,330 行。
- 覆盖 50 个具有日线数据的证券，日期范围为 2025-01-02 至
  2026-09-03。
- 23,294 行具有完整的 20 日收益结果。
- 缺少 20 日指标的行主要位于每只证券的窗口起点，也包含部分历史数据
  缺口。
- 学习结果写入 `.cache/spark/real_daily_metrics`，未修改 Iceberg Silver。

**工程结论**

- Spark 可以通过 PostgreSQL Iceberg JDBC Catalog 读取 MinIO 中的 Silver。
- Spark 读取时使用 `provider + adjustment_type + security_id` 作为窗口
  分区键。
- 真实数据覆盖的证券数少于 `security_master` 中的记录数，后续需要单独
  建立行情覆盖率检查。
- 当前作业仍为批量 Parquet 输出，尚未接入 Airflow 或写入 Gold Iceberg。

## 2026-10-25：Silver 行情存在重复业务键

**现象**

首次构建 Gold 时，质量门禁发现 4,030 个重复业务键：

```text
provider + adjustment_type + security_id + trading_date
```

**影响**

直接对重复行执行 `lag`、移动平均和滚动波动率会重复计算同一交易日，
导致指标错误。此前读取合成数据时没有触发该问题。

**处理**

- 不改写 Silver，不擅自清理生产数据。
- Spark 在计算窗口前按业务键去重。
- 排序优先级为 `available_at`、`ingested_at`、`source_version`、
  `payload_hash`，保留最新可见版本。
- Gold 写入前继续执行重复键门禁。

**后续**

- 单独排查 Silver 重复数据的产生路径和写入幂等性。
- 评估是否增加 Iceberg 主键约束、MERGE 写入或定期质量报告。

## 2026-10-25：Gold 特征可见窗口少算一行

**现象**

初版 Gold 使用最近 20 行计算 `feature_available_at`，但 `return_20d`
来自 `lag(close, 20)`，20 个日收益率计算年化波动率也需要 21 个收盘价。

**影响**

如果第 21 个历史价格比前 20 行更晚可见，特征可见时间会被低估，可能
造成 Point-in-time 研究中的未来数据泄漏。

**处理**

`feature_available_at` 改为当前行加前 20 行，共 21 行。

**验证**

使用修正后的逻辑重建 Gold 表，并再次由 Spark 和 PyIceberg 分别验证。

修正后的 Gold 表结果：

```text
table:      openfinance.openfinance.gold_security_daily_metrics
rows:       20,300
securities: 50
dates:      2025-01-02 ~ 2026-09-03
complete:   20,133
partial:    167
snapshot:   8498662309727853200
partition:  month(trading_date)
```

Spark 与 PyIceberg 读取到的行数和快照一致，Spark 写出的表可以被项目现有
PyIceberg 运行时直接读取。重复业务键为 0，非法 OHLC 为 0，未修改 Silver。

## 2026-10-25：数据分层盘点发现小文件和链路缺口

**盘点结果**

- Bronze：35 个 finance JSON，约 4.94 GB；4 个 news JSON；
  尚无 annual-report 对象。
- Silver：`daily_bars` 454,993 行，但有 47,006 个数据文件；
  `trading_calendar` 169,557 行，对应 169,557 个数据文件。
- Gold：`gold_security_daily_metrics` 20,300 行，21 个数据文件。
- `news_chunks` 为 0，年报 Silver 三张表为空，在线 RAG 尚未形成数据。
- `a_share_universe_coverage` 为 0，尽管已有 28 个行情任务和 6 个规范化
  任务成功完成。
- Silver 和 Gold 的最新行情日期均为 2026-09-03，落后当前日期 52 天。

**影响**

- 大量小文件显著增加 Iceberg 元数据、S3 请求和 Spark 扫描开销。
- 全量行情统计实测约需 100 秒，不能代表 45 万行的真实计算成本。
- 覆盖率水位缺失意味着无法可靠判断某日全市场 universe 是否完整。
- RAG 和年报链路目前没有可查询数据。

**后续**

- 对 `daily_bars` 和 `trading_calendar` 执行 Iceberg
  `rewrite_data_files`，评估重分区或合并小文件。
- 排查每次采集/规范化写入都生成新文件的问题。
- 修复 coverage 写入与验收逻辑。
- Gold 每日增量上线前，补齐当前缺失日的行情。

## 2026-10-25：小文件根因归属判断

**结论**

当前小文件数量主要是系统入湖和表设计问题，不是数据 API 返回格式直接
造成的。数据 API 可能贡献重复或错误记录，但 API 不会直接决定 Iceberg
数据文件数量和分区文件布局。

**证据**

- Bronze 只有 35 个 finance JSON，但包含约 4.94 GB 数据，说明 API 响应
  已经被组织成较大的批次对象。
- Silver `daily_bars` 将同样的数据展开后写成 47,006 个文件，平均每个
  文件约 9.7 行。
- `trading_calendar` 和 `security_master` 基本一行一个文件，说明反复
  append 和按日期/交易所分区是主要放大因素。
- Spark 写出的 Gold 有 20,300 行、21 个文件，说明当前 Spark 写入不是
  Silver 大量小文件的来源。
- 同一业务键存在 4,030 个重复记录。重叠回看窗口、任务重试和 append
  写入共同造成逻辑重复，之后又被物理写入放大。

**需要分别处理**

1. 数据 API：校验响应是否重复、缺数、陈旧或字段错误。
2. 入湖逻辑：使用业务键 MERGE/幂等覆盖，避免同一批数据重复 append。
3. 分区设计：重新评估 Silver 按天 + 交易所 + provider 的粒度。
4. 物理布局：使用 Iceberg `rewrite_data_files` 合并小文件。

## 2026-10-25：Silver 去重与文件布局修复完成

**代码修复**

- `FinanceWriter` 由 append 改为业务键 upsert。
- 同一 Bronze 批次内先按业务键保留最新可见版本。
- 写入重试继续使用项目级 PostgreSQL advisory lock。
- upsert 返回实际插入和更新数量，不再依赖写入前全表扫描。
- 新表按月份、交易所和 provider 分区。

**历史迁移结果**

| 表 | 迁移前行数 | 迁移后行数 | 迁移前文件 | 迁移后文件 |
|---|---:|---:|---:|---:|
| `daily_bars` | 454,993 | 386,377 | 47,006 | 429 |
| `security_master` | 130 | 50 | 126 | 4 |
| `trading_calendar` | 169,557 | 13,044 | 169,557 | 4 |

完整行情统计查询从约 100.7 秒下降到约 9.6 秒。

**验证**

- 新表业务键无重复。
- Spark 与 PyIceberg 均可读取新表。
- PyIceberg 对三张新表执行相同行 upsert，返回
  `rows_inserted=0, rows_updated=0`，行数保持不变。
- 全量测试 `70 passed`。
- 原表保留为 `*_backup_20261025`，可回滚。

## 2026-10-25：失败重试的端到端幂等

**实现**

- Airflow 使用 `run_id + task_instance.map_index` 作为任务幂等键；同一次
  DAG 运行中的任务重试复用该键。
- PostgreSQL 任务表增加唯一 `idempotency_key`，重复提交返回同一个
  `job_id`；失败任务重新入队而不是新建任务。
- 后台 Job 的 `job_id` 用于生成稳定的 Bronze 对象路径：
  `finance/provider=<provider>/requests/<hash>.json`。任务跨小时重试也
  覆盖同一个原始对象，而不是生成新的 UUID 文件。
- Silver 写入按业务键 upsert，不依赖“先查询再写入”来保证唯一性。
- 行情统计中的 `written_daily_bars` 使用 upsert 返回的实际插入/更新数，
  相同数据重放时返回 0。

**验证**

- 相同幂等键连续提交两次，返回相同 `job_id`：
  `7e72d586-03ec-4bee-b127-11bfa60959f1`。
- 相同 Bronze 对象处理两次，第二次的证券、日历和行情写入数均为 0。
- 测试数据已按精确对象路径清理，并从备份恢复了一条被测试覆盖的合法
  行情记录；最终 `daily_bars` 行数恢复为 386,377。
- 单元测试 `72 passed`。

**注意**

幂等保证的是“重复执行不改变最终结果”，不是“任务绝不执行第二次”。
外部 API 调用仍可能重复发生，因此还需监控 provider 配额和任务执行次数。

## 2026-10-25：按湖仓缺口补数和 API 预算控制

**实现**

- A 股每日 DAG 不再固定请求最近 3 天，而是先通过后端读取
  `security_master`、`trading_calendar` 和 `daily_bars` 求缺失组合。
- 每只证券的多个缺失日期合并成一次日期区间，只发起一次历史行情请求和
  一次复权因子请求。
- 已有证券不再重复请求 `security_master`，本地交易日历新鲜时也不再请求
  日历接口。
- PostgreSQL `api_request_budget` 按中国日期原子扣减每日额度，默认上限
  50 次。
- 默认每只证券按 2 次请求估算，每次最多选择 24 只证券，为偶发日历请求
  保留余量。
- Airflow 调度时区显式改为 `Asia/Shanghai`。

**真实规划结果**

```text
缺失证券:       50
缺失证券/日期:  1,500
当前选中证券:   24
等待证券:       26
预计行情请求:   48
日历刷新请求:   1
```

随后只对 `000001.SZ` 做端到端抽样采集：

- 请求区间：2026-09-04 至 2026-10-23
- 获得并写入 30 条行情
- 请求消耗：2 次
- 当前剩余预算：46 次

**修复的额度浪费**

首次判断交易日历新鲜度时使用了“最近开市日”。日期为周六时，每次规划
都会误判日历过期并重复请求。现在改为使用日历最后一个记录日期判断覆盖
范围，重复规划不再消耗请求。

**验证**

- 重复规划返回 49 只缺失证券、1,470 个缺失组合，不再刷新日历。
- Gold 已重建为 20,330 行，最新日期为 2026-10-23。
- 全量测试 `75 passed`。

## 2026-10-25：本地密钥、运行环境与全市场补数

**安全**

- 新增 `.gitignore`，排除 `.env`、缓存、日志、临时文件和 Python 产物。
- `.env.example` 中疑似真实 Juhe Key 已替换为占位符。
- `.env` 当前未进入 Git 索引。
- 已暴露的 API Key 仍需在供应商控制台轮换，这一步需要账户权限。

**运行环境**

- Backend、Backend Worker、Airflow Scheduler、Airflow Webserver 和
  OpenSearch 已启动。
- 每日缺口 DAG 已解除暂停。
- Backend 根路径与 Airflow `/health` 均返回 HTTP 200。

**全市场补数**

- 每日全市场清单缓存到 PostgreSQL
  `openfinance.a_share_universe_cache`，每个交易日最多刷新一次。
- 当前市场清单包含 5,506 只普通 A 股。
- 缺口规划发现 5,505 只证券、208,798 个证券-日期组合。
- 预计完整补数需要 16,466 次请求。
- 每日 API 硬上限已更正为 50,000。
- 为降低单批失败影响，每批改为 20 只证券。
- 使用 8 个 Backend Worker 和最多 8 个并发 Airflow 映射任务。
- 单只证券增加 45 秒硬超时；失败证券不会终止整批，留待下一轮缺口
  检测继续处理。

当前全市场运行：

```text
dag_id: openfinance_a_share_daily_pipeline
run_id: manual__full_universe_v4_20261025
```

运行结束前，下游 `bronze_to_silver` 和 `quality_check` 会保持等待状态。

## 2026-10-25：单股行情 API 查询延迟

**实测**

`GET /api/market?symbol=000001.SZ&start_date=2026-09-01&end_date=2026-10-23`
当前耗时约 14.2 秒。

**根因**

`FinanceRepository._resolve_visible_bars` 每次请求都会调用
`scan_daily_bars()` 将整个 Silver 表读入 Python，再在内存中筛选单只
证券和日期范围。当前表有 386,407 行、430 个数据文件，单股请求只需要
其中 33 行。

**定向扫描实验**

在同一张表上增加以下 Iceberg 谓词，并只投影行情列：

- `security_id = 000001.SZ`
- `adjustment_type = qfq`
- `trading_date` 范围

定向扫描返回 33 行，耗时约 0.1 秒。

**建议**

1. 将证券、日期、复权类型和 `as_of` 条件下推到 Iceberg Scan。
2. 查询只投影 API 真正需要的列。
3. 复用 Catalog / Table 句柄，避免每次请求重新加载三张表。
4. 证券目录使用缓存，不再扫描整张日线表。
5. 计算指标优先读取 Gold，历史明细按需走 Silver。

## 2026-10-25：单股行情查询优化完成

**代码修复**

- `FinanceWriter.scan_daily_bars_for_query` 将证券、日期、复权类型、
  `available_at` 和 `ingested_at` 条件下推到 Iceberg Scan。
- 查询只投影行情记录需要的列。
- `FinanceRepository` 不再扫描整张 `daily_bars` 后在 Python 中筛选。
- 证券目录只扫描 `security_master` 的 `symbol/name/exchange` 三列，
  并由进程级 Repository 缓存 5 分钟。
- FastAPI 原本已通过 `lru_cache` 复用 FinanceService、Catalog 和 Table
  句柄。

**在线实测**

单股行情接口：

```text
优化前: 14.2 秒
首次:   1.04 秒
后续:   0.18 秒 / 0.08 秒
中位数: 0.184 秒
```

证券目录接口：

```text
首次:  0.307 秒
缓存:  0.008 秒
```

全量测试 `76 passed`。Backend 重启期间，全市场补数任务保持可重试状态，
当前 DAG 继续运行。

## 2026-10-25：全市场任务出现 up_for_retry

**定位**

`manual__full_universe_v4_20261025` 的 `map_index=9` 在
2026-10-24T20:46:27 进入 `up_for_retry`。

**直接原因**

Airflow 正在轮询后台 Job 状态时，Backend 因部署行情查询优化而重启，
状态接口返回 `Connection refused`。失败发生在任务结束前，并非 BaoStock
行情接口或数据写入失败。

对应后台 Job `d5a863fe-8f43-4e2c-ab7b-45d3cae21dab` 实际已于
20:46:35 成功完成，数据库中只有一条该 Job 记录。

**为何没有立即重试**

全市场运行期间 DAG 被意外暂停，`up_for_retry` 因暂停无法重新调度。
解除暂停后，第二次尝试使用相同幂等键，于 20:53:39 开始并在 0.5 秒内
成功完成。

**修复**

- Airflow 状态轮询将连接拒绝、超时、408、429 和 5xx 视为瞬态错误。
- 在 `A_SHARE_JOB_TIMEOUT_SECONDS` 超时前持续重试，不再立即把整个
  Airflow 任务标记失败。
- 404、明确失败的 Job 和最终超时仍会正确报错。

后续部署行情查询优化时，Airflow Scheduler 也被重启，导致
`map_index=43/45/47/48/49/50` 的本地任务子进程收到 `SIGTERM`。这说明
仅修复 Backend 轮询不够：LocalExecutor 的子任务仍依附于 Scheduler。

已在不重启服务的情况下清除当前 run 的 `failed` 和 `upstream_failed`
状态。失败映射任务以 try 3 重新调度，`map_index=9` 已成功，当前全市场
run 继续运行。对应后台采集 Job 是幂等的，没有重复业务数据。

## 2026-10-25：长时 running 任务与孤儿 Job

**现象**

多个 Airflow mapped task 运行超过 10 分钟，对应后台 Job 已运行约 2 小时，
但心跳停在 Worker 重启之前。

**根因**

- Worker 重启后，旧进程中的 8 个 Job 仍保持 `running`。
- A 股 Job 的孤儿回收阈值原为 24 小时，导致正常 Worker 长时间无法重新
  领取这些任务。
- Airflow 持续轮询原 Job，因此 mapped task 一直显示 running。

**处理**

- A 股 Job 心跳失效阈值调整为 5 分钟。
- Bronze → Silver 规范化 Job 心跳失效阈值调整为 10 分钟。
- 立即将心跳超过 5 分钟的 8 个孤儿 Job 重置为 queued。
- 8 个 Worker 已重新领取任务，心跳恢复正常，没有再次出现超过阈值的
  running Job。

**验证**

- 长时任务的映射实例已从等待状态进入实际运行。
- 全量测试 `94 passed`。

## 2026-10-25：BaoStock 黑名单导致末批行情失败

**现象**

全市场补数后段出现多个批次失败，Airflow 报错：

```text
A-share ingestion job <job_id> failed:
No A-share symbols completed successfully
```

其中 `fbdc1f26-9b73-4fff-bff7-a5a79069a6d9` 包含 20 只
`688xxx.SH` 科创板证券，Job 尝试 3 次后失败。

**根因**

后台日志中所有证券都在登录阶段失败：

```text
BaoStock login failed: 10001011 黑名单用户，请与管理员联系
```

部分请求还出现：

```text
BaoStock login failed: 10002007 网络接收错误
```

“No A-share symbols completed successfully” 是系统在整批证券全部失败后
生成的汇总错误，不是股票代码不存在或股票数据格式错误。

统计到 7 个失败 Job 使用同一汇总错误，另有 291 个 Job 成功。当前每日
请求使用量为 17,671 / 50,000，未超过配置额度。

**初步判断**

- 失败发生在 BaoStock 登录阶段，与 688 开头证券的个股数据无关。
- 当前每只证券会重新登录两次（证券主数据、日线），8 个 Worker 并发
  时会放大登录次数。
- 这更可能是账号/IP 被供应商限流或风控，而不是每日请求额度耗尽。

**后续处理**

- 等待供应商解除黑名单，必要时联系 BaoStock 管理员。
- 解除前暂停失败批次重试，避免继续放大登录。
- 将一次批次内的证券主数据和日线请求改为共用登录会话。
- 降低并发 Worker 数量并增加请求间隔。

**恢复**

BaoStock 解除黑名单后，已将全局并发固定为 1：

```text
Airflow max_active_tis_per_dag = 1
Backend Worker 实例数 = 1
```

保留成功批次，仅清除当前 run 的 failed 和 upstream_failed 任务，未创建
新的 DAG run。原失败批次 `fbdc1f26-9b73-4fff-bff7-a5a79069a6d9` 使用
相同幂等键重新执行，日志显示已连续成功处理证券且不再出现黑名单错误。

**额度配置更正**

实际免费额度是每日 50,000 次，而不是 50 次。默认配置已调整为：

```text
BAOSTOCK_DAILY_REQUEST_LIMIT=50000
A_SHARE_MAX_SYMBOLS_PER_RUN=10000
A_SHARE_INGEST_BATCH_SIZE=100
```

预算仍是硬上限，不会主动用完。相同缺口日期区间的证券会每 100 只合并为
一个 Airflow 任务，避免全市场出现数千个任务。

## 2026-10-25：Bronze 到 Silver 耗时随队列推进递增

**现象**

全市场补数过程中，`bronze_to_silver` 映射任务的 Airflow 耗时从约 51 秒
增长到约 922 秒，但后台规范化 Job 的实际执行时间通常只有 43～93 秒。

**根因**

- 同时存在 16 个 `running` 和 176 个 `scheduled` 映射任务。
- Airflow 只有一个 Worker，后启动的任务把大量排队时间也计入展示耗时。
- 每个 Bronze 对象原先单独提交 Job，分别加载 Iceberg Catalog、扫描业务键
  并提交快照。
- 即使一个批次全部是新业务键，原实现仍执行完整 `upsert` 合并。

因此“耗时递增”不是单批数据计算复杂度随时间增长，而是固定初始化成本、
写入合并成本和队列等待叠加后的结果。

**日常增量处理**

- Airflow 先等待所有采集映射任务结束，再按每个 Job 最多 10 个 Bronze
  对象分组。
- 规范化 Worker 在一个 Job 内读取并校验整批对象，跨对象按业务键去重，
  每张 Silver 表只提交一次写入。
- 每日行情写入前用受限谓词读取已有业务键及其版本字段。
- 纯新增业务键走 Iceberg `append`。
- 已有且版本未变化的数据直接跳过，保证重放幂等。
- 已有且内容或可见版本变化的数据才走 `upsert`。
- 采集 Worker 与规范化 Worker 分开运行；外部 API 采集保持单并发，规范化
  服务保持单实例，避免两个写者同时执行“先判断、后写入”。
- Airflow 不再为每个 Bronze 对象生成可见映射任务，减少了调度队列噪声。

**Spark 适用边界**

| 场景 | 数据规模/特征 | 当前选择 | 原因 |
|---|---|---|---|
| 每日缺口补数 | 几百到几万行 | PyIceberg 合并批次 | 数据小，避免 Spark 冷启动 |
| 多年历史回填 | 百万到千万级行 | Spark 批量规范化 | 可并行读取、Shuffle 去重和批量写 |
| Silver 文件整理 | 大量小文件 | Spark rewrite/重建 | 分区级并行，适合物理布局治理 |
| Gold 指标重算 | 全市场窗口计算 | Spark | Window、Shuffle 和 Iceberg 写入成熟 |
| 外部行情采集 | 受 API 限流 | 单并发 Python Worker | Spark 不能降低供应商请求限制 |

**Spark 历史回填方案**

1. 用显式 Schema 批量读取 Bronze JSON，保留 `raw_object_path`。
2. `explode` 行情数组，规范化证券、日期、复权类型和可见时间字段。
3. 按业务键排序，保留 `available_at`、`ingested_at`、`source_version`
   和 `payload_hash` 最新的记录。
4. 先写带日期后缀的 Silver staging 表并运行质量门禁。
5. 一次切换 staging 和 backup 表，失败时保留原表用于回滚。
6. 同一张 Iceberg 表同一时间只允许一个写入方；历史 Spark 作业与日常
   增量 DAG 通过 Airflow `max_active_runs=1` 或分布式锁互斥。

现有 `spark/jobs/rebuild_silver_finance.py` 已覆盖 Silver 去重、暂存表
构建、质量校验和安全切换；下一步只需把其输入来源从现有 Silver 扩展到
Bronze 多文件批处理。

**验证**

- 多对象批次测试确认：两个 Bronze 对象只在每张 Silver 表上调用一次写入。
- 纯新增、未变化重放、历史修正三种写入路径均有单元测试。
- 对 10 个已处理 Bronze 对象做真实幂等重放，共读取 6,162 条原始记录，
  耗时 39.737 秒，证券主数据、交易日历和行情写入均为 0。
- 作为对照，旧实现单个对象的后台 Job 实际执行通常需要 43～93 秒；
  新实现把 10 个对象的 Catalog 初始化、对象读取、去重和提交成本合并。
- 全量测试 `102 passed`。
- Docker Compose 配置校验通过。

## 2026-10-25：DAG 拓扑热切换导致质量检查 upstream_failed

**现象**

全市场 DagRun 的 `quality_check` 显示 `upstream_failed`。其直接原因是原先
排队的 171 个 `bronze_to_silver` 映射任务同时失败，日志为：

```text
RuntimeError: map_index passed to non-mapped task
```

**根因**

运行中的旧 DAG 使用 `bronze_to_silver.expand(...)`，Scheduler 随后解析到
新 DAG 的普通任务定义 `bronze_to_silver(ingest_results)`。LocalExecutor
仍按旧映射任务传入 `--map-index`，新任务不允许该参数，因此在任务函数
执行前就失败。采集 Bronze 数据没有丢失，`quality_check` 也没有真正执行。

**恢复**

- 从本次 DagRun 的 XCom 提取全部 276 个 Bronze 对象路径。
- 使用新批量规范化管道做一次幂等补处理。
- 共读取 172,443 条原始记录，失败 0，为 Silver 新增 103,859 条行情和
  2,699 条证券主数据。
- Silver 当前共 554,461 行、4,439 个有行情的证券，最新交易日为
  2026-10-23，业务键重复为 0。
- 本次计划覆盖 5,505 只证券；4,438 只成功返回数据，1,067 只由数据源
  返回空结果或失败，仍留待后续缺口扫描继续补。
- 核对结果后，将 171 个因拓扑不兼容而未执行的映射任务和
  `quality_check` 修复为成功，并写入包含 `repair_note` 的质量摘要 XCom。
- 将原 DagRun 修复为成功，随后部署独立的单写者
  `normalization-worker`。

**部署规则**

- mapped task 与普通 task 之间转换、任务 ID 变化、依赖图变化属于 DAG
  拓扑变更，只能在当前 DAG 没有 `running`、`queued` 或 `scheduled`
  任务时部署。
- 拓扑变更必须与正在执行的 DagRun 分离；需要热升级时应创建版本化 DAG
  ID，不能直接复用仍在运行的 DAG 定义。
- 本次恢复耗时 49.741 秒，而旧实现处理同样 276 个对象需要大量单对象
  upsert 和排队，进一步验证批量写入方案。
