# StarRocks 在线查询

StarRocks 是 Iceberg 日线表的只读查询层，不保存日线副本。日线、周线和月线历史页面均通过该读取器获取日线，周线和月线仍由前端聚合；采集写入继续使用原有路径。首次部署前必须验证 JDBC Catalog、字段类型、删除文件和快照语义。

## 启动和接入

在项目根目录激活 `conda activate lakehouse` 并安装 `requirements.txt` 中的 PyMySQL。StarRocks 随主 Compose 启动，也可单独运行 `bash scripts/compose.sh up -d --no-deps starrocks`。默认本机端口 19030，容器内地址 `starrocks:9030`。固定镜像 3.5.21，默认上限 8 GiB、4 CPU，可通过 `STARROCKS_MEMORY_LIMIT` 和 `STARROCKS_CPUS` 修改。

初始化服务建立 PostgreSQL 只读账号和仅可读取 warehouse 对象的 MinIO 账号。PostgreSQL JDBC 驱动需在 StarRocks 镜像中可用。外部 Catalog 名称必须与现有 Iceberg SQL Catalog 名称一致，默认都是 `openfinance`。

在 `.env` 中设置三个独立的随机强密码：`STARROCKS_PASSWORD`、`STARROCKS_ICEBERG_JDBC_PASSWORD`、`STARROCKS_S3_SECRET_KEY`，并限制文件权限。配置 PostgreSQL 和 MinIO 管理员凭据，使其与主 Compose 中的服务一致。确认 PostgreSQL、MinIO、StarRocks 均已启动，后端镜像已构建，再依次运行：

```bash
bash scripts/compose.sh --profile starrocks-init run --rm --no-deps starrocks-postgres-init
bash scripts/compose.sh --profile starrocks-init run --rm --no-deps starrocks-minio-init
bash scripts/compose.sh --profile starrocks-init run --rm --no-deps starrocks-catalog-init
```

初始化服务使用容器内 JDBC 和 S3 地址，Catalog 元数据 TTL 设为 0，使每次查询重新获取快照；仍需实测新提交和历史快照可见性。不要将凭据写入仓库或终端日志。应用用户只被授予 Catalog USAGE 和所需在线读取表的 SELECT 权限。Catalog 的 JDBC 或 S3 凭据变更时，需要按 StarRocks 文档更新 Catalog 属性；重跑账号初始化本身不会替换已有 Catalog 的密钥。

## 验证与运行

配置 `STARROCKS_USER`、`STARROCKS_PASSWORD` 和 `STARROCKS_CATALOG`，重建并重新创建 backend。Compose 内使用 `starrocks:9030`；主机运行工具使用 `127.0.0.1:19030`。在线行情、证券、交易日历、新闻、年报 Silver 和 Gold 指标统一通过 StarRocks 查询；服务不可用时返回 503。

升级代码前，运行 `bash scripts/compose.sh --profile starrocks-init run --rm --no-deps starrocks-catalog-init python -m backend.maintenance.setup_starrocks_catalog --grant-only`，为现有应用用户授予已有表的只读权限。脚本会报告缺失的表，表创建后需要再次运行以补充授权；未创建的表对应查询不可用。

验证日期、空值、新闻数组、年报数据和历史快照。可用 `scripts/benchmark_market_history.py` 检查单请求及并发负载下的延迟和资源占用。连接和查询超时分别由 `STARROCKS_CONNECT_TIMEOUT_SECONDS` 和 `STARROCKS_QUERY_TIMEOUT_SECONDS` 控制。

后台写入和采集校验使用 Iceberg，行情历史的快照元数据仍由 PyIceberg 读取。StarRocks 外部表读取 MinIO 中的 Iceberg 文件，需同时监控 backend 与 StarRocks 的打开文件数、数据库连接数、查询延迟和错误率。backend 在每个进程复用一个 Iceberg SQL catalog，进程关闭时释放连接池；FD 上限调整不能代替泄漏排查。
