# 投研监测（历史功能）

定时调度默认关闭，现有 DAG 已暂停。需要手动触发时，先在 Airflow 中恢复该 DAG、触发运行；运行结束后可再次暂停。监测 API 与历史数据继续保留。若需恢复定时调度，设置 INVESTMENT_MONITOR_SCHEDULE_ENABLED=true 并重启 Airflow 服务，再恢复 DAG。


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
docker compose up -d --build
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
