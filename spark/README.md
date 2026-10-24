# Spark 学习实验室

这条学习链路与现有后端查询链路隔离。目标是使用项目真实的数据形态练习
Spark DataFrame、Spark SQL、Iceberg 和 Airflow，而不是只做教程示例。

## 当前实验：Bronze 行情 JSON → Parquet

输入是带嵌套结构的 Bronze JSON：

```text
raw_response.daily_bars
```

Spark 作业依次完成：

1. 使用显式 Schema 读取多行 JSON。
2. `explode` 嵌套行情数组。
3. 规范证券代码、交易所、日期和复权类型。
4. 过滤空值、负数和非法 OHLC。
5. 按 `provider + adjustment_type + security_id + trading_date` 去重。
6. 按 `trading_date + exchange` 分区写入 Parquet。
7. 输出原始行数、过滤/重复行数和写入行数。

样例包含 5 行数据，其中 1 行重复、1 行 OHLC 非法，预期最终写入 3 行。

## 启动 Spark

先启动 Docker Desktop，然后在项目根目录执行：

```powershell
docker compose --profile spark up -d spark-master spark-worker
```

默认使用国内镜像源：

```text
docker.1ms.run/apache/spark:4.1.2-python3
```

当前固定版本是 Spark 4.1.2 / Python 3.10，镜像不存在时才会拉取。
Spark 学习镜像会在此基础上加入 Iceberg、AWS S3FileIO 和 PostgreSQL JDBC：

```powershell
docker compose --profile spark build spark-master spark-worker
```

如需切换基础镜像站，可以设置 `SPARK_BASE_IMAGE`：

```powershell
$env:SPARK_BASE_IMAGE = "hub.rat.dev/apache/spark:4.1.2-python3"
docker compose --profile spark build spark-master spark-worker
```

如果本机已有同一镜像的本地别名，可以避免重新下载：

```powershell
docker tag spark:latest docker.1ms.run/apache/spark:4.1.2-python3
```

Spark Master UI：

```text
http://localhost:8081
```

Worker UI：

```text
http://localhost:8082
```

Spark 使用项目根目录的 Compose project 和默认网络，因此后续作业可以通过
`minio:9000`、`postgres:5432` 等 Compose 服务名访问项目已有组件。通过
`spark` profile 隔离，普通的 `docker compose up -d` 不会启动 Spark。

## 运行第一课

```powershell
docker compose exec spark-master `
  /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/openfinance/spark/jobs/bronze_finance_to_parquet.py `
  --input /opt/openfinance/spark/samples/bronze/finance `
  --output /opt/openfinance/.cache/spark/bronze_finance_sample
```

只停止 Spark 学习服务：

```powershell
docker compose --profile spark stop spark-master spark-worker
```

预期结果：

```json
{
  "raw_daily_bar_rows": 5,
  "filtered_or_duplicate_rows": 2,
  "normalized_daily_bar_rows": 3,
  "written_daily_bar_rows": 3
}
```

## 第一课要观察什么

- `read_bronze` 只构造执行计划，`raw_count` 的聚合才触发第一个 Job。
- `explode` 会把一行 Bronze 对象展开成多行行情。
- 按业务键去重会产生 Shuffle。
- `repartition` 同样产生 Shuffle，应观察它与输出分区数量是否匹配。
- 写 Parquet 是 Action；不要把所有 Action 都当成普通函数调用。

建议在 Spark UI 的 Jobs、Stages 和 SQL 页面各停留一次，确认最耗时的 Stage。

## 第二课：窗口指标

生成 25 个交易日的确定性样例：

```powershell
python spark/samples/generate_finance_sample.py `
  --output .cache/spark/generated_bronze `
  --days 25
```

将生成数据清洗为 Parquet：

```powershell
docker compose exec spark-master `
  /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/openfinance/spark/jobs/bronze_finance_to_parquet.py `
  --input /opt/openfinance/.cache/spark/generated_bronze `
  --output /opt/openfinance/.cache/spark/generated_daily_bars
```

计算窗口指标：

```powershell
docker compose exec spark-master `
  /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/openfinance/spark/jobs/daily_metrics.py `
  --input /opt/openfinance/.cache/spark/generated_daily_bars `
  --output /opt/openfinance/.cache/spark/daily_metrics
```

`daily_metrics.py` 当前计算：

- 1 日收益率
- 5 日和 20 日收益率
- 5 日和 20 日移动平均
- 20 日波动率
- 5 日成交量均值

所有 Window 都按 `provider + adjustment_type + security_id` 分组，并按
`trading_date` 排序。计算只使用当前行和之前的历史行，避免未来数据泄漏。

## 下一步

观察 `daily_metrics.py` 输出，并打开 Spark UI 的 SQL 页面，理解以下问题：

- 每个证券开头几行的 5 日和 20 日指标为什么为空？
- `PartitionBy` 和 Window 的 `OrderBy` 分别解决什么问题？
- 如果输入没有按证券聚集，`lag` 计算可能遇到什么风险？
- 为什么 `volatility_20d_annualized` 使用样本标准差而不是总体标准差？
- 停牌日为什么没有直接参与当前版本的均值和收益率计算？

## 读取项目真实 Iceberg 行情

先启动项目基础设施：

```powershell
docker compose up -d postgres postgres-bootstrap minio minio-init
```

使用只读查询确认 Catalog：

```powershell
docker compose exec spark-master `
  /opt/spark/bin/spark-sql `
  --master spark://spark-master:7077 `
  --properties-file /opt/openfinance/spark/conf/iceberg.properties `
  -e "SHOW TABLES IN openfinance.openfinance"
```

直接对真实 Silver 行情计算指标并写出学习结果：

```powershell
docker compose exec spark-master `
  /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --properties-file /opt/openfinance/spark/conf/iceberg.properties `
  /opt/openfinance/spark/jobs/daily_metrics.py `
  --table openfinance.openfinance.daily_bars `
  --start-date 2025-01-01 `
  --output /opt/openfinance/.cache/spark/real_daily_metrics
```

`start-date` 和 `end-date` 会下推到 Iceberg 分区扫描，避免为了练习指标
读取全部历史。生产学习作业应先复权口径、证券范围和日期窗口做过滤。

## 构建 Gold 指标表

将真实 Silver 行情构建为：

```text
openfinance.openfinance.gold_security_daily_metrics
```

```powershell
docker compose exec spark-master `
  /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --properties-file /opt/openfinance/spark/conf/iceberg.properties `
  /opt/openfinance/spark/jobs/build_gold_security_daily_metrics.py `
  --start-date 2025-01-01
```

作业会额外读取 45 个自然日作为指标预热，但 Gold 只保留目标开始日期
之后的数据。写入前检查空结果、业务键重复和非法 OHLC；写后核对行数。

Gold 表包含：

- 每行一个 `provider + adjustment_type + security_id + trading_date`
- OHLC、成交量和换手率
- 1/5/20 日收益、5/20 日均线
- 5 日成交量均值和 20 日年化波动率
- `metric_status` 数据完整性状态
- `feature_available_at` 特征可见时间
- `gold_built_at` 构建时间和 `schema_version`

表按月分区。当前作业采用完整重建方式，每次写入是一个新的 Iceberg
快照；验证通过后才适合增加 Airflow 调度。

## 修复 Silver 小文件和重复写入

Silver 行情写入已从 append 改为按业务键 upsert：

```text
daily_bars:
  provider + adjustment_type + security_id + trading_date

security_master:
  provider + security_id

trading_calendar:
  exchange + trading_date
```

失败重试或同一 Bronze 对象重复处理时，相同业务键只更新已有行，不新增
重复行。Silver 表的目标布局同时调整为：

- `daily_bars`：按月份、交易所和 provider 分区
- `security_master`：不分区
- `trading_calendar`：不分区

Airflow 重试使用稳定的 `run_id + map_index` 幂等键。PostgreSQL Job 表
通过唯一索引复用同一个 `job_id`，后台任务又用该 `job_id` 生成稳定的
Bronze 对象路径 `requests/<hash>.json`，因此同一次逻辑任务跨小时重试
也不会反复产生新的原始文件。

历史重建作业：

```powershell
# 1. 创建并校验暂存表，不切换
docker compose exec spark-master `
  /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --properties-file /opt/openfinance/spark/conf/iceberg.properties `
  /opt/openfinance/spark/jobs/rebuild_silver_finance.py `
  --mode build --suffix 20261025

# 2. 保留原表为 backup，再切换暂存表
docker compose exec spark-master `
  /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --properties-file /opt/openfinance/spark/conf/iceberg.properties `
  /opt/openfinance/spark/jobs/rebuild_silver_finance.py `
  --mode activate --suffix 20261025
```

旧表保留为：

```text
daily_bars_backup_20261025
security_master_backup_20261025
trading_calendar_backup_20261025
```

## 按缺口补行情与 API 预算

每日 DAG 会先执行：

```text
security_master
+ trading_calendar
- daily_bars 已有证券/日期
= 缺失的证券/日期组合
```

同一证券的多个缺失日期会合并成一个请求区间。预计每只证券消耗：

```text
1 次历史行情请求
+ 1 次复权因子请求
= 2 次请求
```

每日预算与默认选择上限：

```env
BAOSTOCK_DAILY_REQUEST_LIMIT=50000
A_SHARE_GAP_LOOKBACK_DAYS=60
A_SHARE_REQUESTS_PER_SYMBOL=2
A_SHARE_MAX_SYMBOLS_PER_RUN=10000
A_SHARE_INGEST_BATCH_SIZE=20
```

预算按 `Asia/Shanghai` 自然日记录在 PostgreSQL
`openfinance.api_request_budget`。50,000 次额度足以直接覆盖完整 A 股
市场的日常缺口；预算是硬上限而不是目标消耗量。相同日期区间按每批 20
只证券合并为 Airflow 任务。预算不足时只处理部分证券，其余缺口留到下
一个预算周期，不会继续超额请求。
