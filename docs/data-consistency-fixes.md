# 数据一致性：问题、修复与待完成项

原始基线见 `docs/data-consistency-audit.md`。此记录区分已实现、已验证和仍待完成的内容；跨系统写入均不构成一个原子事务。

## 采集及归一化任务

- **问题与触发条件**：任务心跳过期并被重新领取后，旧执行者仍可按任务 ID 覆盖新执行者的状态。
- **修复**：领取次数 `attempts` 作为执行令牌；心跳、完成、失败均校验 ID、运行状态和令牌。失效返回 `JobLeaseLost`。worker 在证券、输入对象和 Silver 表写入阶段之间检查并刷新租约。
- **验证**：`tests/lakehouse/test_job_lease_integration.py` 在隔离 PostgreSQL 里重领两个任务类型，验证旧令牌三种更新均被拒绝、新令牌可以完成。
- **兼容与限制**：提交和查询 API 不变；内部状态更新必须提供 attempt。上线先停止旧 worker、执行迁移，再启动新版。检查与外部写入之间仍有竞争窗口，已经发出的请求或已提交的外部写入不能撤销，仍依靠幂等和对账；阻塞中的单次请求也不能被令牌立即中断。

## 年报 Silver 多表投影

- **问题与触发条件**：跨表写入中断或强制重建时，未提交数据可能替换旧投影并被页面或索引读取。
- **修复**：报表与子表按解析版本保留投影；页面、区域、目录及索引均选择带 parse-run 提交标记的版本。强制重建使用由源解析版本与 IR 内容确定的投影 ID；写完所有表并回读校验后才提交标记。
- **验证**：`tests/pipelines/test_annual_reports_ir_to_silver.py` 对每个投影表的写后故障注入，检查旧提交标记保留且重试可完成；`tests/lakehouse/test_annual_report_visibility.py` 检查未提交版本不会取代已提交版本。服务和索引相关回归测试通过。
- **兼容与限制**：历史已提交版本保持可读；无提交标记的旧数据需要按原流程重放。旧版本数据不会自动删除；上述故障注入采用模拟写入器，尚未覆盖真实 Iceberg 多表中断和每次提交前故障。

## 新闻双索引

- **问题与触发条件**：pgvector 写入成功、OpenSearch 批量写入部分失败时，按文章哈希判断完整会漏掉缺失分块；单侧不可用会导致整个检索失败。
- **修复**：从现有 Silver 新闻确定预期分块 ID，分别补齐向量和全文索引并回读确认。普通索引与对账共用 PostgreSQL advisory lock。独立 Airflow DAG `openfinance_news_index_reconciliation` 默认每 30 分钟调用原有索引入口，不采集新闻源。记录保存在 `openfinance.news_index_reconciliation`，包含状态、最近成功时间、缺失和修复数量及错误。检索支持单侧降级，状态提示经工具和 evidence pack 传递。
- **验证**：`tests/rag/test_indexer.py` 模拟 OpenSearch 部分写入后失败，验证重试补齐及重复执行；`tests/rag/test_news_degraded_search.py` 覆盖单侧失败、双侧失败和证据提示。`tests/rag/test_news_reconciliation_integration.py` 在隔离 PostgreSQL 验证互斥、成功状态记录，以及真实 pgvector 写入后模拟另一侧失败并恢复；首次建向量表时先创建扩展再注册类型，读回向量时转为普通列表。
- **兼容与限制**：结果新增可选 `warnings` 字段。对账无需重新调用新闻供应商，但缺失向量可能调用配置的 embedding 服务。状态记录上次运行情况，不是实时一致性证明；运行期间、失败后或未能读取状态时会提示可能不同步。索引之前的构造阶段、数据库本身不可用或进程突然终止时，状态可能无法更新，需结合调度日志判断。对账补齐缺失块，不清理历史多余块，也未做真实 OpenSearch 故障注入。

## 年报双索引 generation

- **问题与触发条件**：构建、激活后清理中断可能留下未完成清理；并发构建与清理可能干扰版本。
- **修复**：保留构建、计数与抽样校验、事务激活；构建和清理共用 advisory lock。即使目标版本已激活，重复执行也会重试清理；清理故障不将激活版本改成失败。修复过期版本查询的字典行读取。
- **验证**：现有 `tests/rag/test_annual_report_indexer.py` 覆盖构建中 embedding 失败后重试；`tests/lakehouse/test_annual_report_generation_integration.py` 在隔离 PostgreSQL 验证激活失败回滚、旧版本保留和到期版本读取。
- **兼容与限制**：激活版本仍由状态表选择；仅清理到期 retained 版本。真实 PostgreSQL/OpenSearch 跨系统清理中断、并发读取实验**尚未完成**，不能视为已充分验收。

## 实时行情

- **问题与触发条件**：发送 Kafka 失败或批次缺失可能只留日志；数据库提交后消费位置提交前中断会发生重放。
- **修复**：采集器启用 Kafka 生产者幂等和 `acks=all`；开始采集、发布、失败和不完整轮次写入 `openfinance.realtime_collection_rounds`，只读行情 API 新增 `collection`。数据库结果保持幂等写入，offset 在数据库事务后提交。现有 Flink SQL 配置 checkpoint。
- **验证**：以本项目 Kafka Python 客户端版本 2.2.15 实例化并检查幂等、确认与重试配置；`tests/realtime/persistence_integration.py` 在隔离 PostgreSQL 重复提交同一消息并记录模拟发送失败。另在隔离 Kafka、Flink、PostgreSQL 使用 `tests/realtime/kafka_fixture.py` 发送重复模拟快照，确认市场统计去重并写入数据库；停止并重启 TaskManager 后，Flink 报告从 checkpoint 1 恢复、作业重新运行；停止结果消费者期间发送另一轮，再重启消费者，数据库收到新轮次。该实验没有在数据库提交与 offset 提交之间强制杀进程，也没有验证 JobManager 容器重启后的作业恢复。
- **兼容与限制**：上线前需要 0007 迁移。采集进程或数据库在持久化状态前同时不可用时缺口仍可能没有记录；中断时状态可能停留在 `collecting` 或 `publishing`。确认数是已逐个确认的下界，发送异常后其他请求结果可能未知。Kafka 前丢失及上游未提供的数据不能补回；单 broker 没有副本高可用。
- **事件时间修复**：一分钟窗口改按 UTC `captured_at` 归窗，水位线迟到余量可配置（默认 120 秒），采集器向每个 Kafka 分区发送空闲心跳。超出水位线的有效样本进入 `quotes.late` 并持久化为待处理记录；页面和 API 提示。`realtime.recompute` 对 Kafka 保留范围内的指定整分钟区间重放，事务性写入采样统计与重算记录；重算结果避免被延迟到达的在线输出覆盖，冲突时标记待复核。迁移 0008 将旧处理时间结果标为历史口径，避免混用。
- **事件时间验证**：隔离 Kafka、Flink 1.20、PostgreSQL 的 `tests/realtime/event_time_integration.py` 验证两分钟水位线下乱序、重复、心跳关闭最终窗口、超期标记、按范围重算和重算后旧结果保护。另验证了真实 Kafka 截断后重算拒绝未证实覆盖的区间；在新作业的 checkpoint 完成后重启 TaskManager，Flink 报告从 checkpoint 恢复。`tests/realtime/quote_service_integration.py` 验证旧口径不展示及超期提示；隔离数据库从空库升级和含旧窗口的 0007 库升级至 0008 均通过，旧窗口保留为历史口径。
- **事件时间限制**：水位线依赖心跳和系统时钟；采集器或 Kafka 停止时不能保证终窗及时关闭。保留检查依赖 Kafka 记录时间与实际发送时间一致，只能检查 Kafka 覆盖，不能证明源头完整。重算只替换有有效样本的窗口、不删除其他窗口；跨分区重复轮次拒绝重算。Kafka 前的缺失无法补回，单 broker 不提供高可用。具体操作见 `docs/realtime-quotes.md`。

## 全新数据库迁移

- **问题与触发条件**：空库迁移时后续迁移使用独立连接，看不到前一个迁移事务中的对象；schema 也可能不存在。
- **修复**：0001 先创建 schema；0002、0003 建表使用 Alembic 管理的底层连接和事务。新增 0006、0007 保存对账和采集状态，没有手动修改版本号。
- **验证**：隔离 `pgvector/pgvector:pg16` 全新数据库从空库执行 `alembic upgrade head` 至 0007；另一个隔离库先升级至 0004，再升级到 0007，均成功。
- **兼容与限制**：已部署库按正常版本顺序升级，先停旧 worker；未检查所有历史版本组合，也没有对迁移中途故障执行回滚注入。无需重建或清空现有业务数据。

## 运维检查

启用调度后可查看 Airflow 中的 `openfinance_news_index_reconciliation`。也可经已有受保护的 `/internal/jobs/news-rag-index` 手动触发 `{ "rebuild_all": false }`。查看 `openfinance.news_index_reconciliation` 的状态、`details`、`error` 和 `last_success_at`；查看 `openfinance.realtime_collection_rounds` 排查采集失败或不完整轮次。长时间停留在运行中状态需要检查进程和调度日志。

## 2026-11-02 现有数据库迁移记录

按用户授权，现有 `openfinance` 数据库由 `0003_annual_report_state` 顺序迁移至 `0008_quote_event_time`。迁移前使用 `pg_dump` 创建本地备份 `.cache/db-backups/openfinance-pre-0008-20261102.dump`，副本与容器内原文件 SHA-256 一致，并通过 `pg_restore --list` 检查；未执行完整恢复演练。迁移后确认版本、新表及主要窗口字段，数据库连接和现有后端健康检查正常，已有业务表可读取。未重启数据库或后端，未启动 Kafka、Flink 或实时采集器，因此此记录不表示实时服务已经上线。

数据库仍提示创建时与当前系统的排序规则版本不同；本次未修改排序规则、重建索引或判断该提示的具体影响。
