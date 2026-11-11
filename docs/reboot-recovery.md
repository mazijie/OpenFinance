# 服务器重启后的恢复

## 当前故障

`docker ps` 默认只显示运行中或正在重启的容器。此前 PostgreSQL、MinIO、Kafka、OpenSearch、backend 和 Airflow 等没有配置重启策略，机器重启后不会自动启动；worker 的重启策略不能代替其依赖的服务启动。现已为常驻服务增加 `unless-stopped`，初始化容器仍保持一次性任务。

`libnvidia-ml.so.1` 错误来自 NVIDIA 容器运行时加载宿主机驱动库，不能仅凭这条错误确定是驱动未安装、内核升级后驱动未加载，还是容器运行时配置问题。先在宿主机检查 `nvidia-smi`、`lsmod | grep nvidia`、`journalctl -k -b | grep -i nvidia`；若宿主机 GPU 正常，再检查 NVIDIA Container Toolkit 和 Docker runtime 配置。不要删除数据库或数据卷。

先恢复基础服务并检查数据，避免 GPU 错误阻断恢复：

```bash
cd /home/mzj/OpenFinance
sudo docker compose up -d postgres minio opensearch kafka
sudo docker compose ps -a
sudo docker compose logs --tail=100 postgres minio opensearch kafka
```

CPU 临时方案（需要 Compose v2.24+，不要启用仍需 GPU 的 legacy-parser profile）：

```bash
sudo bash scripts/compose.sh config --quiet
sudo bash scripts/compose.sh up -d --no-build
sudo bash scripts/compose.sh ps -a
sudo bash scripts/compose.sh logs --tail=100 backend embedding-init flink-submit
```

后续操作使用 `scripts/compose.sh` 保持模式一致。CPU 模式可能较慢，首次模型下载依赖网络；实际恢复后请确认业务、Airflow DAG 和 Flink 作业状态。

## 开机重新执行 Compose

Docker 重启策略不执行 Compose 的依赖排序，也不保证一次性初始化任务重新执行。仓库提供 `deploy/systemd/openfinance-compose.service`，开机后用已构建镜像执行 Compose，失败时每 30 秒重试，并由脚本检测 CPU/GPU 模式。当前文件路径适用于 `/home/mzj/OpenFinance` 和本机 Snap Docker；其他安装方式需核对并修改 Docker 路径、systemd 单元名及下面的 enable 命令。仓库中的服务文件修改后，还须更新系统中已安装的副本。

在宿主机执行：

```bash
sudo systemctl enable snap.docker.dockerd.service
sudo install -m 644 deploy/systemd/openfinance-compose.service /etc/systemd/system/openfinance-compose.service
sudo systemctl daemon-reload
sudo systemctl enable --now openfinance-compose.service
sudo systemctl status openfinance-compose.service
sudo journalctl -u openfinance-compose.service -b --no-pager
```

切换 GPU 模式时，以所需模式重新创建 embedding 容器。配置改动只有重新创建容器后才会生效；已有 `unless-stopped` 容器被手动停止时不会由 Docker 自动重启，而开机的 systemd Compose 服务会尝试启动项目服务。systemd 服务成功退出后不会持续监控容器健康状态，Docker 也不会因为 healthcheck unhealthy 自动重启容器。Flink JobManager 重启后的作业和 checkpoint 恢复尚未在本项目验证；单机硬件损坏、数据损坏和 Kafka 保留期外的数据丢失也不由上述方案解决。需要备份和实际重启演练后才能验证恢复效果。
