# 历史日线回填：交易日历写入优化

## 变更与观察

每个 Bronze 批次可能包含同一份交易日历。此前每次都将全部日历分成最多 256 条的批次执行 upsert；本次在 Iceberg 写锁内读取并比较现有日历，仅对新增或变化的记录执行分批 upsert。比较覆盖 `exchange`、`trading_date`、`is_open`、`session_open_at` 和 `session_close_at`，并且与写入共享同一把锁。日志 `trading_calendar_write_completed` 提供输入、新增、变化、未变化、写入数量及耗时；`finance_bronze_to_silver_completed` 增加三个表的阶段耗时。

线上观察：用户反馈优化前约需 **15 分钟**，优化后约需 **1～2 分钟**；按这一观察值计算，耗时下降约 87%～93%。这是用户报告的近似值，具体统计范围、样本数量、数据规模及测量窗口尚未确认，不能作为严格的基准测试结论。

已有日志显示：2026-11-05 17:39 CST，一批包含 13,105 条日历和 55,108 条日线，日线成功写入；这是单批次运行记录，与上述耗时对比并非同一份基准数据。日志时间为 UTC。

本地 `lakehouse` 环境通过 finance writer 与 Bronze-to-Silver 针对性测试（15 项），包括真实本地 Iceberg 表的新增、重放、更新，以及并发和部分提交重试；该结果不能替代服务器线上测量。

## 线上测量与部署

开始前记录当前时间、容器重启次数和最近 20 个首次领取成功任务的执行时间。先确认 worker 实际运行的代码版本：目录为绑定挂载，若此前自动重启已经加载了新代码，本次查询不能作为优化前基线，应结合日志 `trading_calendar_write_completed` 及时间判断，无法确认时标记为不可比。

```bash
date -Is
sudo docker inspect openfinance-normalization-worker-1 --format 'RestartCount={{.RestartCount}} StartedAt={{.State.StartedAt}}'
sudo docker exec openfinance-postgres psql -U openfinance -d openfinance -c "SELECT count(*) AS n, round(avg(extract(epoch FROM finished_at-started_at))::numeric,2) AS avg_seconds, min(finished_at) AS first_finish, max(finished_at) AS last_finish FROM (SELECT started_at,finished_at FROM openfinance.finance_normalization_jobs WHERE status='success' AND attempts=1 AND started_at IS NOT NULL ORDER BY finished_at DESC LIMIT 20) s;"
```

重启 worker 使修改生效，记录重启时刻（不重启其他服务）：

```bash
date -Is
sudo NORMALIZATION_PYTHONFAULTHANDLER=1 bash scripts/compose.sh up -d --no-deps --force-recreate normalization-worker
```

重启会中断当前批次；旧任务心跳超时后才能被重新领取，默认阈值为 600 秒。避免清除或重复提交 Airflow 任务。Airflow 的等待超时包含排队时间，默认 6 小时。

观察最近日志和队列，确认任务完成、未变化日历被跳过，并留意新增异常：

```bash
sudo docker logs --since 10m openfinance-normalization-worker-1 2>&1 | tail -100
sudo docker exec openfinance-postgres psql -U openfinance -d openfinance -c "SELECT status, count(*), max(finished_at) AS latest_finished FROM openfinance.finance_normalization_jobs GROUP BY status ORDER BY status;"
sudo docker inspect openfinance-normalization-worker-1 --format 'RestartCount={{.RestartCount}} StartedAt={{.State.StartedAt}}'
```

在至少 20 个优化后首次领取并成功的任务完成后，重用上述 20 任务查询。比较前后任务执行时间、完成时间窗口及 worker 重启次数，并结合日志核对 `input_count`、`unchanged_count` 和 `written_count`。优先比较相近股票数、日期范围和数据规模；`started_at` 保留首次领取时间，所以重试任务不能纳入耗时比较。结果按实际测量补充下表：

| 指标 | 优化前 | 优化后 |
| --- | --- | --- |
| 样本数、完成时间范围 | 待采集 | 待采集 |
| 平均任务处理秒数 | 待采集 | 待采集 |
| 日历阶段秒数、跳过行数 | 待采集 | 待采集 |
| worker 重启次数 | 待采集 | 待采集 |

计数只是整个后端队列的统计，不一定与单个 Airflow DAG 一一对应。已失败的任务不会自动恢复，需确认其 Airflow 状态和重复提交情况后再制定恢复步骤。
