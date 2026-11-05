# 单服务器 CPU 部署

在项目根目录运行。保留现有 `.env`，不要用示例文件覆盖。

要求 Docker Compose v2.24.0 或更新版本；CPU 覆盖文件通过 `!reset` 移除 Ollama 的 GPU 请求，模型仍为 bge-m3，向量维度仍为 1024。

```bash
docker compose -f docker-compose.yml -f docker-compose.cpu.yml config --quiet
docker compose -f docker-compose.yml -f docker-compose.cpu.yml up -d --build
docker compose -f docker-compose.yml -f docker-compose.cpu.yml ps -a
```

网页位于 `http://服务器IP:8000/ui`，API 文档位于 `/docs`。首次启动需要下载镜像和 embedding 模型；backend 会等待模型下载及数据库初始化完成。

启用数据调度前，在 `.env` 设置有效的 `AIRFLOW__CORE__FERNET_KEY`、`AIRFLOW__WEBSERVER__SECRET_KEY` 和管理员密码，并检查数据源凭据：

```bash
docker compose -f docker-compose.yml -f docker-compose.cpu.yml --profile airflow up -d --build airflow-init airflow-webserver airflow-scheduler
```

Airflow 位于 `http://服务器IP:8080`，查看 DAG 状态并按需启用采集任务。大规模历史回填应单独安排。

所有后续 Compose 操作均应带上两个 `-f` 参数，避免重新引入默认的 GPU 请求。例如：

```bash
docker compose -f docker-compose.yml -f docker-compose.cpu.yml logs --tail=100 backend embedding-init opensearch
```

## 数据和功能范围

- PostgreSQL、MinIO、OpenSearch 和 Ollama 模型使用 Docker named volumes 持久化。不要用 `down -v` 删除这些数据卷。
- `.env` 中的 `MINIO_DATA_PATH` 当前不被 Compose 引用，实际数据位置由 Docker volume 决定。
- CPU 模式只调整默认 Ollama 服务。不要启用仍请求 GPU 的 `legacy-parser` profile。
- 年报解析默认依赖外部 Ray 与 MinerU，本配置不包含 Ray 集群，不能仅凭启动成功判断年报解析已可用。
- 当前默认配置公开了多项存储和管理端口，且 OpenSearch 关闭认证。应通过服务器防火墙限制访问，并按 README 同步替换默认凭据。

## Docker 权限

如果出现 Docker socket permission denied，需由服务器管理员授权当前用户访问 Docker，或由有权限的账号执行上述命令。将用户加入 docker 组等同授予主机管理权限，应由管理员决定。若使用 sudo，请将上述命令改为 `sudo docker ...`，在自己的终端输入密码。
