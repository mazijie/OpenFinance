# Spark 性能基准

运行环境：Docker Compose 的 Spark、PostgreSQL 和 MinIO，以及本机 Python 3.10+ 和 Git。
结果、日志和报告保存在 .cache/benchmarks；输入和输出 Iceberg 表位于独立的 bench_ 数据库。
所有 ID 以字母开头，只包含 ASCII 字母、数字和下划线；suite ID 最多 10 个字符。
每个数据集和 suite 使用新的 ID。

在项目根目录执行：

    python spark/benchmark/benchmark.py prepare --dataset real01 --kind real
    python spark/benchmark/benchmark.py prepare --dataset smallsample --kind small
    python spark/benchmark/benchmark.py prepare --dataset mediumsample --kind medium
    python spark/benchmark/benchmark.py run --dataset real01 --suite realrun01
    python spark/benchmark/benchmark.py run --dataset mediumsample --suite mediumrun01
    python spark/benchmark/benchmark.py report --suite realrun01

小规模验证可加 --baseline-only --repeats 1。默认每个配置预热一次、计量三次，
Gold 和 Silver 顺序运行；完整矩阵运行时间较长。每个结果目录有 result.json、
submit.log、History Server 返回的 history.json（不可用时在结果中说明）。summary.csv
和 report.md 汇总计量结果。History 数据含作业和 Stage 详情；事件日志保存在
.cache/spark/events，History Server 仅在本机 18080 端口提供页面。

真实输入记录每张表的快照 ID 并复制该快照，避免后续正式表变化影响结果；三个
快照分别读取，并非跨表原子快照。合成数据生成固定数据与一条版本修正，使用日历日。
新 suite 始终写独立表；Silver 仅构建暂存表，不切换正式表。缓存状态未控制，
耗时包含校验及写入；结果不能作为冷缓存测试。各运行使用首个配置的预热结果作为
参考表，按业务键逐行比较；浮点值使用绝对和相对误差各 1e-9，Gold 构建时间不参与比较。
逐行比较在作业处理耗时之后执行，提交耗时包含比较所需时间。

清理时先核对所选 bench_ 命名空间及对应本地产物，再由操作人员显式删除；
不要清理仍需复现的输入快照和事件日志。不要删除正式 openfinance 命名空间。
配置 spark.sql.shuffle.partitions 默认值由 spark/conf/iceberg.properties 和
spark/conf/spark-defaults.conf 提供；自定义属性文件需自行设置默认值。

History Server 的 Stage 和 Shuffle 等总量覆盖整个 Spark 应用，包括写入后的逐行比较；
处理耗时不包括逐行比较，提交耗时包括比较。比较性能时请结合这些不同统计范围。
