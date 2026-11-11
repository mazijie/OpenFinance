# StarRocks 日线查询试验

StarRocks 是 Iceberg 日线表的只读查询层，不保存日线副本。日线、周线和月线历史页面均通过该读取器获取日线，周线和月线仍由前端聚合；采集写入继续使用原有路径。首次部署前必须验证 JDBC Catalog、字段类型、删除文件和快照语义；单节点配置仅供试验。

## 启动和接入

在项目根目录激活 `conda activate lakehouse` 并安装 `requirements.txt` 中的 PyMySQL。StarRocks 随主 Compose 启动，也可单独运行 `bash scripts/compose.sh up -d --no-deps starrocks`。默认本机端口 19030，容器内地址 `starrocks:9030`。固定镜像 3.5.21，默认上限 8 GiB、4 CPU，可通过 `STARROCKS_MEMORY_LIMIT` 和 `STARROCKS_CPUS` 修改。

初始化服务建立 PostgreSQL 只读账号和仅可读取 warehouse 对象的 MinIO 账号。PostgreSQL JDBC 驱动需在 StarRocks 镜像中可用。外部 Catalog 名称必须与现有 Iceberg SQL Catalog 名称一致，默认都是 `openfinance`。

在 `.env` 中设置三个独立的随机强密码：`STARROCKS_PASSWORD`、`STARROCKS_ICEBERG_JDBC_PASSWORD`、`STARROCKS_S3_SECRET_KEY`，并限制文件权限。配置 PostgreSQL 和 MinIO 管理员凭据，使其与主 Compose 中的服务一致。确认 PostgreSQL、MinIO、StarRocks 均已启动，后端镜像已构建，再依次运行：

```bash
bash scripts/compose.sh --profile starrocks-init run --rm --no-deps starrocks-postgres-init
bash scripts/compose.sh --profile starrocks-init run --rm --no-deps starrocks-minio-init
bash scripts/compose.sh --profile starrocks-init run --rm --no-deps starrocks-catalog-init
```

初始化服务使用容器内 JDBC 和 S3 地址，Catalog 元数据 TTL 设为 0，使每次查询重新获取快照；仍需实测新提交和历史快照可见性。不要将凭据写入仓库或终端日志。应用用户仅被授予 Catalog USAGE 和日线表 SELECT。Catalog 的 JDBC 或 S3 凭据变更时，需要按 StarRocks 文档更新 Catalog 属性；重跑账号初始化本身不会替换已有 Catalog 的密钥。

## 验证和切换

先在 `lakehouse` 环境中以应用用户设置连接参数，运行以下只读对比；它固定同一个 Iceberg snapshot 并比较全部字段和重复行。对多只证券、不同日期、空值和更新后的数据重复执行；还需在隔离表验证删除文件、UTC 边界、旧快照和失效快照。

```bash
bash scripts/compose.sh --profile starrocks-init run --rm --no-deps starrocks-catalog-init python -m backend.maintenance.compare_starrocks_daily --symbol 600519.SH --start-date 2025-01-01 --end-date 2026-01-01
```

对比命令的单次耗时不代表基准结果。后续可用 `scripts/benchmark_market_history.py` 检查单请求和并发负载下的延迟及资源占用；本次接入不以耗时作为切换条件。结果不一致或快照、权限检查失败时不要启用。

通过验收后，在后端 `.env` 配置 `MARKET_DAILY_READER=starrocks`、`STARROCKS_USER`、`STARROCKS_PASSWORD`、`STARROCKS_CATALOG`，重建后端镜像并重新创建后端容器。Compose 内自动使用 `starrocks:9030`；主机运行工具使用 `127.0.0.1:19030`。失败时历史接口返回 503，不静默切换存储。恢复原路径将 `MARKET_DAILY_READER` 设为 `iceberg` 并重新创建后端容器。

公开的历史存在性接口增加可选 `period` 参数，默认 `daily`；现有页面会发送当前周期。StarRocks 连接和查询超时分别由 `STARROCKS_CONNECT_TIMEOUT_SECONDS` 和 `STARROCKS_QUERY_TIMEOUT_SECONDS` 控制。
