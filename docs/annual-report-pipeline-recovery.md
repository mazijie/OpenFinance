# 年报 Pipeline 故障与恢复记录

本文记录年报 PDF 从 Bronze 到检索索引的恢复设计、实施过程中发现的问题和验证结果。Silver/Gold 是可重建投影；Bronze PDF 和 Derived Document IR 是优先复用的持久化产物。

## 在 Airflow 手动采集年报

在 Airflow 中打开 DAG，选择 **Trigger DAG with config**，填写表单后触发：

- `openfinance_annual_report_pipeline`：采集指定证券。在“年报年份”填写 JSON 数组，例如 `[2022, 2023, 2024]`；在“证券代码”每行填写一个代码，例如 `600519.SH`。
- `openfinance_annual_report_full_backfill`：采集当前全市场 A 股。在“年报年份”填写 JSON 数组，并可调整每批证券数量。每批采集完成后，解析阶段逐份提交 Ray，且同一时间只处理一份报告。全市场任务会下载和处理大量报告，耗时可能较长。
- `openfinance_annual_report_bronze_replay`：只重放已有 Bronze 报告，不负责从巨潮发现新报告。按下面的 JSON 示例传入年份和证券。

手动参数仅影响本次运行。定时任务使用环境变量 `CNINFO_REPORT_YEARS_JSON` 和 `CNINFO_REPORT_STOCKS_JSON`；全市场手动任务在未指定年份时使用环境变量，未配置时默认上一自然年。手动采集时所选年份应为已经公布的报告年度，能否找到报告取决于巨潮公告。

## 2026-11-10：重复表格片段导致 Silver 写入失败

2024 年的 300815、300851、300855 年报均已完成解析，但 Silver 写入报 `Duplicate rows found in source dataset based on the key columns`。实际 Document IR 中有重复的表格行组；切块重复表头后，产生了内容、位置和区域均相同的片段。原 chunk ID 只根据内容和位置生成，导致一次 upsert 中出现重复主键。尚未对照原始 PDF 判断重复行来自原表还是表格识别。

切块现在按相同内容的出现次数生成稳定的不同 ID，保留全部片段及其行。首次出现的 ID 保持原算法。Silver 阶段向 Ray 日志传递原始异常，便于定位后续问题。

全市场回填逐份处理报告：只有 Ray 作业明确返回 `failed` 或 `stopped` 时才重新提交，默认每份最多 3 次，初始等待 30 秒并指数退避，可分别通过 `ANNUAL_REPORT_REPLAY_MAX_ATTEMPTS`、`ANNUAL_REPORT_REPLAY_RETRY_DELAY_SECONDS` 设置。仍失败的报告不会使同批成功结果失败，路径和每次失败的 job ID 保存在该 `ray_replay` 映射任务的 XCom 返回值 `failures` 中，也会写入日志；所有报告均未成功时最终任务仍会失败。提交、通信超时等状态不明的作业不会自动重提，必须先确认原作业状态，避免重复执行。

兜底可通过 `openfinance_annual_report_recovery` 对账，基于 Bronze、Derived 与 Silver 产物识别缺失阶段并恢复；按 `report_ids` 可限制范围。阶段达到 `ANNUAL_REPORT_MAX_STAGE_ATTEMPTS` 后不再自动入队，需要先检查错误和状态。已经运行中的 Airflow 任务不会因源码更新而自动改变其已加载的重试行为；不要与进行中的回填并发重跑同一报告。

## 2026-10-28：重建 Silver 后前端年报列表为空

**现象**

删除或重建 Silver 表后，前端无法显示先前入湖的年报，容易被误判为原 PDF 已丢失。

**根因**

前端年报目录和检索都读取 Silver/Gold 投影。重建 Silver 会移除这些查询投影，但不应删除 MinIO Bronze 中的 PDF 和 manifest，也不应删除 Derived 中已经完成的解析结果。

**处理**

- 增加 `openfinance_annual_report_bronze_replay` DAG，从现有 Bronze manifest 重建 Silver 和 Gold。
- 正常回放优先复用已验证的 Derived IR；只有 IR 缺失、引用对象不完整或 parser version 改变时才继续解析。
- 增加 `openfinance_annual_report_recovery` DAG，根据实际产物和阶段状态从首个失效阶段恢复。

**运行方式**

手动触发 Bronze replay DAG，可用如下配置只回放 2024 年贵州茅台年报：

```json
{"years": [2024], "stocks": ["600519.SH"]}
```

对账恢复 DAG 可传 `report_ids` 限定报告；空列表表示扫描所有报告。常规新数据仍由 `openfinance_annual_report_pipeline` 负责发现、下载和处理。

**验证**

测试覆盖解析结果和 Silver 投影的复用、缺失 Derived 页面对象和 Silver chunk 的发现。没有在本次验证中启动完整 MinIO/ Iceberg/Airflow 栈对真实年报做端到端回放。

## 2026-10-28：中间阶段失败会重复下载、OCR 或向量化

**现象**

如果一个长年报在 OCR、Silver 写入或 embedding 中途失败，整份报告重跑会重复消耗已完成阶段的工作；如果两种检索索引写入进度不一致，也无法判断从哪里继续。

**处理**

- PostgreSQL 增加 `annual_report_processing_state`，按 `report_id + pdf_sha256 + stage + stage_version` 记录阶段状态、尝试次数、错误摘要、产物位置和租约。
- Bronze 复用前检查 manifest、PDF 对象存在性和 SHA-256；常规 Bronze replay 不下载 PDF。损坏对象只定向修复对应报告。
- Parser 将原始页 checkpoint 写入 Derived。重试校验 checkpoint 后只解析缺页；全部页面和区域产物通过检查后再写 `document.json` 完成标记。
- Silver 采用确定性主键和 upsert。所有投影回读验证通过后才写 parse-run 完成标记。
- Embedding 每批写入 pgvector staging generation；重试通过稳定 chunk ID 复用已写向量。
- pgvector 和 OpenSearch 在完成数量、chunk ID 与抽样检索校验后才激活 generation。构建失败时查询仍筛选旧 active generation。
- 阶段重试设有限次数并使用租约；恢复任务按报告隔离，不要求同批报告全部成功。

**验证**

第 2 页 OCR 故障测试确认重试只处理第 2 页；embedding 中间 batch 失败测试确认已写入 chunk 被复用。索引 generation 的测试覆盖 active generation 查询和失败后不切换的路径。

## 2026-10-28：`needs_review` 容易被误解为待重试状态

**现象**

扫描页 OCR 置信度低或页面解析失败时，若把 `needs_review` 当作通用任务失败，恢复 DAG 会不断重复 OCR；若整份报告直接丢弃，又会损失可用页面。

**处理**

- `needs_review` 表示解析质量状态，不代表任务仍在运行，也不自动触发无限 OCR 重试。
- 详细诊断保留在 Derived IR 的 `quality.json` 和页面诊断中；Silver 只保存页面状态与检索必需字段。
- 触及 `needs_review` 页的 chunk 不写入索引；通过质量检查的其他页面仍可检索。
- 如需在修正解析配置后重新处理，应执行受控的强制解析和 Silver/索引重建，而不是依赖普通失败重试。

**验证**

测试确认 `needs_review` 页不进入索引，正常页仍会被索引；空文本页不能标为 `ok`。

## 2026-10-28：恢复对账入队 SQL 返回列不匹配

**现象**

对账发现缺失或失效产物时，`enqueue_stage` 会在写入 PostgreSQL 后解包返回结果时报错，恢复计划无法正常生成。

**根因**

SQL 原来只 `RETURNING status`，调用端却按 `status, attempts` 两列读取。

**处理**

将 SQL 改为 `RETURNING status, attempts`，并增加状态入队回归测试。

**验证**

状态入队测试通过；缺失 Derived/Silver 产物的对账测试通过。

## 2026-10-28：Silver 校验异常留下未过期租约

**现象**

Silver 输入校验失败时，状态记录可能仍显示 `running`，后续任务需要等待租约超时才可恢复。

**根因**

阶段租约在输入验证和确定性 chunk 构造之前领取，早期异常没有对应的失败提交。

**处理**

先完成输入验证和投影构造，再领取租约。领取之后的验证、写入和回读都进入成功/失败状态提交路径。

**验证**

新增测试确认无效 IR 会在领取租约前失败，不留下活动租约。

## 2026-10-28：Bronze 修复可能把新版本 PDF 写到旧报告身份

**现象**

Bronze PDF 丢失或损坏后，如果 CNInfo 对同一公告返回内容不同的 PDF，修复操作可能覆盖确定性对象路径，并把旧的 `report_id/pdf_sha256` 阶段误标成功。

**处理**

下载后、写 Bronze 前校验候选 PDF 的 SHA-256 必须等于原 manifest 预期值，并校验报告身份。哈希变化时拒绝覆盖；新文件应由正常发现和入湖流程作为新版本处理。

**验证**

新增测试确认哈希不同的候选文件不会调用 Bronze 写入。

## 2026-10-28：OpenSearch 旧 mapping 在线追加字段

**现象**

年报 lexical index 已存在但缺少当前 generation mapping 时，旧实现尝试在线追加字段，无法证明旧 mapping 与当前 schema/analyzer 一致。

**处理**

年报 lexical index 是可重建 Gold 投影，mapping schema version 不匹配时删除并按当前 mapping 重建，不维护旧 schema 兼容逻辑。embedding model/chunker version 变化仍通过新的 generation 构建和校验后切换。

**影响**

mapping schema 变更会清空 OpenSearch 年报 lexical index，需通过年报索引任务重建；Silver、Bronze 和 Derived 不受影响。索引名称由 `OPENSEARCH_ANNUAL_REPORT_INDEX` 配置。

**验证**

单元测试覆盖旧 mapping 删除重建和当前 mapping 复用；索引 generation 测试覆盖 chunk ID、数量与抽样检索检查。

## 2026-10-28：Docker Hub 拉取 CUDA 基础镜像 TLS 超时

**现象**

Docker Engine 可正常使用，但解析服务构建在 Docker Hub `nvidia/cuda:12.9.1-cudnn-runtime-ubuntu22.04` 元数据请求处连续 TLS handshake timeout。

**处理**

- CUDA 基础镜像默认改为 `docker.1ms.run/nvidia/cuda:12.9.1-cudnn-runtime-ubuntu22.04`，与项目已有的 Ollama 镜像源配置一致。
- 保留 `PDF_PARSER_BASE_IMAGE` 覆盖入口，可按部署环境切回官方仓库或其他可信镜像源。
- 首次 GPU 镜像体积较大，Paddle GPU wheel 约 1.9 GB；本次完整解析镜像约 8.5 GB。

**验证**

`docker compose build pdf-parser` 成功。容器内 `paddle.is_compiled_with_cuda()` 为真，CUDA 设备数为 1；`/health` 返回 GPU 名称 `NVIDIA GeForce RTX 5060 Laptop GPU`。Smoke 容器已停止并清理。

## 2026-10-28：716 份 Bronze 回放的运行问题

**现象**

- Bronze 中发现 716 份 2024 年完整年报 manifest；回放只读取已有 PDF，没有重新下载。
- PP-StructureV3 首次加载和复杂页 OCR 会长时间占用 GPU；文本页和 IR 写入阶段则主要占用 parser CPU，GPU 可能短时为 0%。
- parser/backend 重启期间，少量 Airflow mapped task 收到 connection refused 或 remote disconnect，进入有限重试。
- 一份报告在图片裁剪阶段触发 PyMuPDF `Invalid bandwriter header dimensions/setup`。

**处理**

- parser 使用 MinIO 中已有模型缓存，并设置 `DISABLE_MODEL_SOURCE_CHECK=True`，避免启动时依赖外部模型站点。
- 重启服务后，Airflow 重试通过确定性的 `report_id + pdf_sha256 + parser_version` 复用已完成的 `raw_pages/page=NNNN.json`；444 页报告已验证可以从已写入的页继续。
- 将 parser HTTP 和 parse task 的默认等待上限提高到 4 小时，适配超长年报。
- 将图片/公式裁剪坐标限制在页面范围内；裁剪失败只把该页标为 `needs_review`，保留区域 JSON 和其余页面，不再让整份报告失败。
- replay parse 保持单 GPU 串行，避免 PP-StructureV3 模型在 8GB 显存上发生并发争用。

**运行观测**

- 同一份 323 页报告约 92 秒完成，约 3.5 页/秒；普通报告通常约 40–120 秒。
- OCR 高峰采样到 GPU 100%、显存约 7.8/8.15 GiB；文本/整理阶段采样到 GPU 0%、parser CPU 约 90–98%。显存占用不能单独代表 GPU 是否成为瓶颈。
- OCR 密集样本（366 页，其中首批检查点 77 页为 `complex`）采样到 GPU 持续 100%、显存约 7.8/8.15 GiB；3 分钟内 checkpoint 从 101 页增至 114 页。不能用普通报告耗时外推此类报告，也不能仅根据显存余量判断 GPU 是否限速。
- 硬件判断：当前 8GB GPU 在 OCR 密集页上已经接近显存上限；A100 适合长期高吞吐、并发 parser 或与 bge-m3 同机运行。当前 parser 有单进程锁，若保持单 worker，A100 不会按理论规格线性提速；采购前应以代表性年报测页/秒、GPU 利用率、CPU/MinIO 写入耗时。
- 本次回放的最终 Airflow、Silver、Gold 数量在任务结束后补录；中途失败只进入重试或 `needs_review`，不会回退到 Bronze 下载。

## 2026-10-28：PDF 展示与内容提取解耦

**现象**

回放期间已有报告的 PDF 和 Derived IR 已经存在，但 Silver 的 parse-run 提交尚未完成；原目录接口只读取 committed Silver，因此前端暂时只显示少量报告。

**处理**

- 年报目录改为扫描完整 Bronze manifest，并校验对应 PDF 对象存在。
- Silver 只负责合并解析状态、页数和文本可用性；没有 Silver 的报告显示为 `pending_parse`。
- PDF 下载直接读取 Bronze，不依赖解析、分块或向量索引。
- 文本页面、区域定位和 Agent 检索继续要求相应的 Silver/Gold 产物。

**验证**

当前 `/api/reports?page=1&page_size=100` 返回 Bronze 年报总数 716；待解析报告可显示并成功返回 `application/pdf`，不会因为 Silver 尚未提交而从目录消失。

## 最终验证记录

- `pytest -q`：`165 passed, 1 skipped`。
- `python -m compileall -q backend airflow/dags tests`：通过。
- `docker compose config --quiet`：通过。
- `git diff --check`：通过；Git 输出的 LF/CRLF 提示是工作区换行符提示，不是空白错误。
- 本次已启动真实 Airflow 回放并执行 MinIO Derived 写入；pgvector/OpenSearch generation 的最终联合验收待回放完成后补录。
## 2026-10-29：年报解析接入共享 Ray 集群

年报回放不再让 Airflow 的 LocalExecutor 直接占用本机 GPU parser。Airflow 通过内部 Ray Job 接口提交一个报告批次，Ray driver 按报告预检页面类型：纯文本报告使用 CPU 节点，需要 OCR 的报告申请唯一 GPU actor。每个报告仍写入原有 Derived 页面 checkpoint、Document IR 和 Silver 幂等投影。

当前资源约束为：两台 CPU 节点各向 Ray 暴露 72 个逻辑 CPU；GPU 节点只暴露一张 A100，并注册 `annual_report_gpu=1`。CPU 任务申请 `annual_report_cpu` 标签，OCR actor 同时申请 `num_gpus=1` 和 `annual_report_gpu=1`，避免任务误跑到 GPU 节点或并发占用同一张卡。Ray 的资源声明是调度上限，操作系统级硬隔离仍需 cgroup、Slurm 或 Kubernetes。

本机 bge-m3 embedding 保持独立运行。Ray 任务完成 Silver 后，Gold indexer 继续通过本机 embedding 服务构建 staging generation；因此 OCR 和向量化可以并行，但两者不共享同一张 GPU。

Ray 节点必须安装与 Head 相同版本的 Ray 和项目代码，GPU 节点还需预装 Paddle GPU、PP-StructureV3 模型和 PDF 解析依赖。当前 Docker Compose 中的 `pdf-parser` 已归入 `legacy-parser` profile，默认回放不会启动它，避免与远程 GPU actor 重复占卡。重启 Ray worker 不会删除 Bronze、Derived 或 Silver，重新提交会按确定性页面 checkpoint 继续。

### 部署后的远程依赖与存储检查

Ray Head 和三个节点的调度资源已验证可见：总 CPU 156（两台 CPU 节点各 72，GPU 节点另暴露 12 个 CPU）、GPU 1、`annual_report_cpu=2`、`annual_report_gpu=1`。CPU 节点和 GPU 节点均能访问本机通过 VPN 暴露的 PostgreSQL `10.132.93.141:5432` 和 MinIO `10.132.93.141:9000`。

首次在 worker 上执行依赖探测时发现，节点的 Python 环境只安装了 Ray，没有 `pydantic-settings`、`minio`、`PyMuPDF`、`pdfplumber`；GPU 节点还没有 Paddle/PaddleOCR。因此 Ray Head 正常不代表解析 Job 已具备运行条件。正式回放前必须选择一种方式：

1. 在每个 CPU 节点安装 `requirements.txt`；在 GPU 节点另外安装 `pdf-parser/requirements.txt` 和 `paddlepaddle-gpu==3.2.2`（CUDA 12.9 索引）。这是长期运行和共享集群的推荐方式。
2. 临时使用 `RAY_RUNTIME_PIP_PACKAGES_JSON` 让 Ray Job 创建 runtime env。该方式已经用 CPU 依赖做过验证，首次 setup 约 30 秒；GPU Paddle wheel 很大，每次新 runtime env 可能下载和占用大量磁盘，不建议作为长期默认。

backend 不再把 Compose 内的 `localhost`、`postgres` 或 `minio` 传给 worker。必须在 `.env` 配置 `RAY_DATABASE_URL`、`RAY_MINIO_ENDPOINT`、`RAY_ICEBERG_CATALOG_URI` 和 `RAY_ICEBERG_WAREHOUSE`，其中地址必须从所有 Ray 节点可达，例如本次探测使用的 `10.132.93.141`。缺少这些配置时，Ray Job 会在提交前拒绝，避免整个批次在远端 worker 上因错误地址失败。`RAY_RUNTIME_PIP_EXTRA_INDEX_URL` 可配置 Paddle CUDA 包索引。

配置依赖或 worker 镜像前不要启动 700 多份年报回放。依赖就绪后仍可直接重提 Bronze replay 或 recovery DAG；已有 Bronze PDF、Derived 页面 checkpoint 和成功的 Silver 投影会按确定性键复用，不会重新下载或重复已完成页面。
