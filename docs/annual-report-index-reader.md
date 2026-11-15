# 年报索引读取器试验

年报索引仍构建完整 generation，默认 `ANNUAL_REPORT_INDEX_READER=starrocks`。StarRocks 从 Iceberg 外表读取，不保存 PDF 或表副本；PyIceberg 仍用于获取各表快照元数据、写入和提交校验。读取失败时任务失败，不自动回退或切换数据来源。

先对相同快照执行只读比较，确认 StarRocks 应用用户具备四张年报表的 SELECT 权限：

```bash
docker compose --profile starrocks-init run --rm --no-deps starrocks-catalog-init python -m backend.maintenance.setup_starrocks_catalog --grant-only
docker compose exec backend python -m backend.maintenance.compare_annual_report_index_readers
```

输出每张表的 UUID、快照 ID、两种读取方式的耗时、行数及不同的行数；差异会返回非零退出码。比较覆盖索引所需的全部字段及重复行，日志不输出年报正文。快照保留期内才能读取指定版本；不要在比较期间清理所用快照。每张表的快照分别捕获，不构成跨表原子快照；建议在年报投影写入停止时执行比较。比较只衡量单次读取，缓存、数据规模和并发会影响耗时，需多次运行并监测 StarRocks 和 backend 资源，不能据此直接判断生产性能。

应确认 `FOR VERSION AS OF`、时间戳、删除文件和索引源字段在实际 StarRocks 环境下读取正确。索引读取日志 `annual_report_index_snapshots` 给出各表快照和 UUID，`annual_report_index_source_read` 给出读取方式、行数和耗时。提交标记与页面状态仍由索引器检查；单表快照固定不提供跨表事务保证。需要排查时可显式设置 `ANNUAL_REPORT_INDEX_READER=pyiceberg` 并重新创建 backend。
