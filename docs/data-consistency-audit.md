# 数据一致性审计

本报告基于代码静态检查，不代表已复现线上故障。Kafka 可用于事件传递和重放，但不能自动使 PostgreSQL、MinIO、Iceberg 和 OpenSearch 的写入成为同一事务。

| 路径 | 证据和风险 | 建议 |
| --- | --- | --- |
| A 股采集任务与归一化任务 | `backend/job_store/a_share_ingestion_jobs.py` 和 `finance_normalization_jobs.py` 使用 `FOR UPDATE SKIP LOCKED` 领取、心跳过期后重领，但 heartbeat 和 complete/fail 仅按 job_id 更新。旧 worker 在长暂停后恢复，可能覆盖新 worker 状态。 | 采用每次领取独立的租约令牌或 attempt fencing；心跳、完成和失败更新均校验令牌。Kafka 本身不能防止旧 worker 写状态。 |
| Iceberg 多表写入与任务状态 | `backend/pipelines/annual_reports_ir_to_silver.py` 依次写 pages、regions、sections、chunks、report，再验证并写 parse-run 标记，随后更新任务状态；跨表和 PostgreSQL 不构成原子事务。 | 保留 parse-run 提交标记、投影校验和重放，补充异常中断与对账测试；消费者只读取已提交版本。`backend/lakehouse/write_coordination.py` 的 advisory lock 可协调合作的写入者，但不是跨存储事务。 |
| 新闻向量与全文索引 | `backend/rag/news/indexer.py` 先写 pgvector 再写 OpenSearch；后者失败时两侧可能暂时不一致。代码按 content hash 回填缺失全文索引。 | 验证重跑和周期对账；如果需要更低延迟或多消费者，可考虑 PostgreSQL 事务性 outbox + Kafka，消费者仍须幂等写入与对账。 |
| 年报双索引 | `backend/rag/annual_reports/annual_report_indexer.py` 分别写 pgvector 与 OpenSearch，校验计数和抽样命中后激活 generation；`annual_report_pipeline_state.py` 使用令牌完成阶段。 | 保留版本切换和重试，补充激活/清理中断测试；Kafka 并非替代 generation 校验。 |
| 实时行情 | Kafka 到 PostgreSQL 的提交可能在数据库提交后、offset 提交前中断，造成重放；采集源在 Kafka 写入前故障仍可能丢快照。 | 保留按证券与轮次幂等更新、checkpoint 和覆盖率标记；验证数据库提交后崩溃及 Flink 恢复。不要声称全链路 exactly-once。 |
| 全新库迁移 | 在隔离 PostgreSQL 中预建 `openfinance` schema 后，从空库执行 `alembic upgrade head`：0001 尚在迁移事务中，0002 的 `HarnessStore.ensure_schema()` 使用独立连接时不能看到 `monitor_runs`，报 `UndefinedTable`。 | 让迁移使用同一连接/事务或显式分阶段提交，并增加全新数据库迁移测试；本次没有修改既有迁移。对已在 0004 的库，单独验证 0005 升级成功。 |

这次仅出具审计意见，不修改上述非实时业务。
