# OpenBKN 数据备份恢复与 Kubernetes 配置留档

默认备份五个数据组件及 Kubernetes 配置留档：MariaDB 业务库、OpenSearch 索引、Redis 全部 DB、MinIO 业务 bucket 的当前对象和 Kafka 保留的已提交消息，以及 K8s 配置参考文件。可用 `--components` 选择一个或多个组件，详见下文。目标是在另一台已经部署好相同版本 OpenBKN 的环境中恢复数据。Hydra PostgreSQL、Kubernetes/Helm 配置、用户权限和应用密钥不在包内；该包不是完整环境的灾备副本。

## 按组件检查、备份和恢复

```bash
# 默认全部五个数据组件及 K8s 配置留档，生成一个备份包
bash deploy.sh backup run --output-dir /var/backups/openbkn

# 指定一个或多个；只检查、连接所选组件，生成一个备份包
bash deploy.sh backup check --components mariadb opensearch
bash deploy.sh backup run --components mariadb opensearch redis --output-dir /var/backups/openbkn

# 从包中恢复多个组件；不指定 --components 则恢复包内全部组件
bash deploy.sh restore check <备份包> --components mariadb opensearch
bash deploy.sh restore run <备份包> --components mariadb opensearch --writers-stopped --overwrite
```

需要停业务取得跨组件一致的备份时，可执行：

```bash
bash deploy.sh openbkn stop
bash deploy.sh backup run --output-dir /var/backups/openbkn
bash deploy.sh openbkn start
```

`openbkn stop` 将 OpenBKN 命名空间中的业务 Deployment 副本数设为 0，并等待这些 Deployment 的 Pod 全部删除后才返回；超时会报出尚未退出的 Pod。`openbkn start` 将副本数设为 1；MinIO、PostgreSQL 等数据服务保持运行。命令依赖当前环境业务服务固定为 1 副本的约定，不保存原副本数。独立 Job、CronJob 和外部写入方不属于 Deployment 缩容范围，仍需另行停写。

组件名称空格分隔，不能重复；不支持的名称、与单组件入口混用、与 `--skip-redis/--skip-minio` 或 `--include-kafka` 混用均报错。恢复选择的组件必须存在于包内，否则在连接集群前拒绝。执行顺序由工具固定，不受参数排列顺序影响；Kafka 消费组、空分区及账本确认等保护保持生效。

支持 `mariadb`、`opensearch`、`redis`、`minio`、`kafka` 五个数据组件，以及仅支持备份的 `k8s`。单组件命令只发现、连接和检查选中的存储；不要求其他存储存在或正常运行。命令格式：

```bash
bash deploy.sh mariadb backup check
bash deploy.sh mariadb backup run --output-dir /var/backups/openbkn
bash deploy.sh opensearch backup check
bash deploy.sh redis backup check
bash deploy.sh minio backup check
bash deploy.sh minio backup run --output-dir /var/backups/openbkn
```

恢复既可输入单组件备份包，也可从完整包中只恢复该组件。例如完整恢复在 MinIO 处失败，保留原包，仅重试 MinIO：

```bash
bash deploy.sh minio restore check /var/backups/openbkn/openbkn-<时间>-<后缀>.tar.gz --overwrite
bash deploy.sh minio restore run /var/backups/openbkn/openbkn-<时间>-<后缀>.tar.gz \
  --writers-stopped --overwrite
```

此时不检查 MariaDB/OpenSearch/Redis，也不需要注销或清空 OpenSearch 快照仓库。`restore check` 不写入业务数据，`restore run` 仍需 `--writers-stopped` 确认相关业务写入已停止；可用 `openbkn stop/start` 管理业务 Deployment。MariaDB 覆盖包内业务库，OpenSearch 替换同名索引，Redis 替换全部 DB，MinIO 覆盖同名对象并保留目标多出的对象。单独恢复 OpenSearch 默认仍要求空的、未注册的快照目录；明确指定 `--replace-repository --overwrite` 可自动替换目标原仓库。

单组件恢复先校验包的整体文件列表及路径安全，再只解包、校验选中组件的文件、清单和版本；不检查其他组件的 payload 校验和或内容。gzip 文件必须能正常读取；若压缩文件本身损坏，即使只恢复某组件也可能无法解包。包中不存在指定组件时，在连接 Kubernetes 前报错。

**默认范围变更：**`bash deploy.sh backup check|run` 现在包含五个数据组件（含 Kafka）及 K8s 配置留档，任何选中组件检查失败都会中止，不会静默跳过。原 `--include-kafka` 保留为兼容参数，已无需指定；`bash deploy.sh restore check|run <包>` 恢复该包声明的数据组件，自动跳过 K8s 配置留档；仅含 K8s 的包会提示没有可自动恢复的数据。`--skip-redis`、`--skip-minio` 仅供完整备份排除组件；恢复或显式选择组件时不接受这些开关。Python 入口也支持 `python3 scripts/backup.py restore run <包> --component minio ...`。

包含 MariaDB 的新备份写入 `format: 5`，每库独立 SQL 装入 `mariadb.tar`；不含 MariaDB 的包保持 `format: 4`。使用 `components` 明确声明内容；v1/v2/v3/v4 包仍支持恢复。旧脚本不能恢复 v5 包，目标机需要同步新版脚本。单组件文件名为 `openbkn-<组件>-<时间>-<后缀>.tar.gz`，包内只有清单和相应数据文件：

| 组件 | 数据文件 | 清单元数据 |
| --- | --- | --- |
| MariaDB | `mariadb.tar`（内含 `<数据库名>.sql`；旧包为 `mariadb.sql`） | `databases`、`mariadb_version` |
| OpenSearch | `repository.tar` | `snapshot`、`indices`、`opensearch_version` |
| Redis | `redis.rdb` | `redis` |
| MinIO | `minio.tar` | `minio` |
| Kafka | `kafka.tar` | `kafka`（Topic、分区边界、消息校验和、消费位置） |
| K8s（仅留档） | `k8s.tar` | `k8s`（命名空间、Secret 显式清单、不支持自动恢复） |

例如只备份 MinIO 的清单核心字段：

```json
{
  "format": 4,
  "components": ["minio"],
  "sha256": {"minio.tar": "<SHA-256>"},
  "minio": {"mode": "current-objects", "buckets": ["bkn-oss", "sandbox-workspace"]}
}
```

完整备份顺序为 MariaDB、OpenSearch、Redis、MinIO、Kafka、K8s 配置留档；完整恢复顺序为 OpenSearch、MariaDB、Redis、MinIO、Kafka，均只执行包内选中的组件。某一步失败时不会回滚其他已恢复组件。保持相关业务停写，修正问题后，用同一完整包执行失败组件的恢复命令；Kafka 已部分导入时需先按其失败处理说明准备空 Topic。不同时间或不同环境的单组件包可能存在业务关联不一致，例如数据库文件记录与 MinIO 对象不匹配；单组件重试建议使用原来那一次完整备份包。

## Kubernetes 配置留档（只备份，不自动恢复）

```bash
bash deploy.sh k8s backup check
bash deploy.sh k8s backup run --output-dir /var/backups/openbkn

# 与数据合并为一个压缩包
bash deploy.sh backup run --components mariadb opensearch k8s

# 自定义命名空间，按需显式导出应用 Secret
bash deploy.sh k8s backup run --k8s-namespaces openbkn resource \
  --k8s-secrets openbkn/app-key openbkn/app-tls

# 人工查看（文件可能很大，且可能含密码；仅在受控环境查看）
tar -xzf <备份包> -C <已创建的私有目录> k8s.tar
tar -xf <私有目录>/k8s.tar -C <已创建的恢复准备目录>
```

默认范围为 `openbkn` 和 `resource`；包含 Deployment、StatefulSet、DaemonSet、Service、Ingress、ConfigMap、CronJob、Job、HPA、NetworkPolicy、PDB、PVC、ServiceAccount、Role、RoleBinding，及所选命名空间关联的 PV 和集群 StorageClass。保存各命名空间可见 Helm release 的名称、Chart/应用版本、修订号及用户 values。它不是集群全部资源导出，不包含 CRD/自定义资源、集群级 RBAC、etcd、节点证书、kubeconfig 或 PVC 实际数据；不导出 Helm 内部 Secret 或 ServiceAccount token。缺少命名空间、权限不足或导出失败时中止，不发布成功包。

Secret 默认不导出，`--k8s-secrets namespace/name` 只允许选择 `--k8s-namespaces` 范围内的应用 Secret。**即使没有选择 Secret，Helm values、ConfigMap 和工作负载环境变量仍可能包含密码。** 备份包权限为 0600，不依赖 age、不自动加密，需限制访问和传输范围。

`k8s.tar` 按可供人工恢复的格式打包，而非只有一个汇总文件：

```text
manifest.json                              # 文件列表及 SHA-256
RESTORE.zh.md                              # 人工恢复顺序、命令和边界
inventory.json                             # 原配置参考及版本信息
manifests/cluster/namespace/<名称>.json
manifests/cluster/storageclass/<名称>.json
manifests/namespaced/<命名空间>/<资源类型>/<名称>.json
helm/<命名空间>/<release>/release.json      # Chart/应用版本、修订号
helm/<命名空间>/<release>/values.json       # 可传给 Helm -f
reference/persistentvolumes/<名称>.json     # 原磁盘路径、CSI 句柄，仅供存储重建参考
reference/jobs/<命名空间>/<名称>.json        # 已执行 Job，不自动重跑
```

资源文件是标准 Kubernetes JSON，能作为 `kubectl apply -f <文件>` 的输入；JSON 也可作为 Helm values。导出清单移除旧 UID、status、ownerReferences、finalizers、普通 Service ClusterIP/NodePort、PVC volumeName 等旧集群分配字段，保留 Headless Service 的 `clusterIP: None`。存储类、节点选择器、宿主机路径、域名及外部地址仍需按新环境审核修改。PVC 清单仅能重建存储申请，不含实际数据；业务数据必须通过各组件数据包恢复。

人工恢复时先创建命名空间和基础依赖，再按需恢复 Secret/ConfigMap/RBAC 等资源、安装业务，恢复数据后才启动业务及定时任务。Helm 管理的资源优先按包中的 Chart 版本和 `values.json` 重装；**本包不包含 Chart 文件和镜像，需从部署仓库或制品库准备对应版本**。不要对全部清单一次递归 apply，也不要对同一资源同时用 Helm 和清单重复恢复。未显式备份的 Secret 必须在恢复时另行准备；这不是完整集群离线灾备副本。

目前只实现备份，不执行自动恢复。`restore_supported: false` 表示当前工具不提供自动恢复，并非清单不能用于人工恢复。默认恢复混合包跳过 K8s；显式 `k8s restore` 或 `--components k8s` 在访问集群前拒绝，并提示查看包内 `RESTORE.zh.md`。默认恢复五个数据组件的原有备份包仍可正常执行，不要求存在 K8s 配置。

K8s 备份仅对 Kubernetes 和 Helm 执行只读命令，不创建工具 Pod，不暂停业务、不重启服务。执行机需安装 Helm，账号需读取上述资源、相关 PV/StorageClass，以及 Helm release 元数据；显式导出应用 Secret 时还需对应 Secret 的读取权限。

## 环境要求

适用环境：本仓库部署的单 Pod MariaDB（业务表必须使用 InnoDB）、单节点 HTTP 无认证 OpenSearch。MariaDB 主版本必须一致，OpenSearch 主次版本必须一致；建议源端与目标端使用完全相同版本。Redis 仅支持 Redis 7、单副本 StatefulSet、无 Redis Cluster、无已连接副本、无 Module，并要求数据目录直接挂载可写 PVC；源端与目标端 Redis 版本必须完全一致。多节点、HTTPS/认证 OpenSearch、外部数据库暂不支持，不能为使用此工具关闭已有集群认证。

执行机需要 Python 3.9+、kubectl 与可访问目标集群的 kubeconfig。MariaDB Pod 内需要 mariadb、mariadb-dump 及 MARIADB_ROOT_PASSWORD（或 MYSQL_ROOT_PASSWORD）环境变量；OpenSearch Pod 内需要 curl、tar、sh、mkdir、rmdir。Redis 容器需要 redis-cli、redis-server、redis-check-rdb、sha256sum 和 REDIS_PASSWORD 环境变量（默认 ACL 用户 root）。Redis CONFIG 默认重命名为 CONFIG_NEW，工具通过该命令读取持久化配置。数据组件认证密码直接从 Pod 环境使用，不主动写进数据 payload；K8s 配置留档可能包含部署密码，见上文。MinIO 容器需要 mc、sh、mkdir、rm、cat，以及 MINIO_ROOT_USER/MINIO_ROOT_PASSWORD（也支持 MINIO_ACCESS_KEY/MINIO_SECRET_KEY 或 root 凭证的 *_FILE 环境变量）。执行机不依赖 age、mc、jq、yq 或数据库客户端。

## 一次性配置 OpenSearch 快照目录

新版 `bash deploy.sh opensearch install` 在新安装和检查已有环境时默认补齐快照目录和 `path.repo`；完整部署调用同一安装流程。init 容器复用部署的 BusyBox 镜像，通过 `DirectoryOrCreate` 在 Pod 所在节点创建目录，并按 OpenSearch Helm values 中的 UID/GID 设置权限（默认 1000:1000、目录权限 700）。无需在执行部署命令的机器上手动建目录。旧环境首次补齐配置可能重启 OpenSearch，应安排维护窗口；配置已完整时不会为快照再次升级或重启。

```bash
# 新安装或为旧环境补齐配置，默认宿主机目录 /mnt/openbkn-snapshots
bash deploy.sh opensearch install

# 新安装时指定专用目录
OPENSEARCH_SNAPSHOT_HOST_PATH=/backup/opensearch bash deploy.sh opensearch install

# 不自动补齐快照配置（已有快照配置会保留）
OPENSEARCH_SNAPSHOT_ENABLED=false bash deploy.sh opensearch install

# 已有环境显式启用；会触发滚动更新，应安排维护窗口
bash deploy.sh opensearch enable-snapshots
```

已有环境运行 `install` 时读取现有 Helm values，保留安全设置、自定义配置及其他挂载。已挂载到快照路径的可写持久卷会沿用，包括自定义宿主机路径、PVC 或 NFS；只有缺少存储挂载时才按新安装方式补齐。不会自动迁移快照目录，也不清理快照文件；挂载冲突、只读或非持久卷、无法安全合并的自定义 `path.repo` 会报错，需人工处理。资源限制清理与快照补齐会合并为一次 Helm 更新。

`enable-snapshots` 仍可用于显式合并快照配置；已有 `snapshot-repo` 指向其他存储时拒绝切换。已有环境补齐配置要求匹配的本地 Chart。

宿主机目录仍是节点本地存储，不等于异地备份。独立存储须预先挂载到该节点的目录并确认 `findmnt -T <目录>`；多节点环境须使用共享仓库。init 容器需要以 root 设置该目录权限，禁止 root init 容器的环境需按下面步骤手动准备目录和挂载。

### 手动配置（兼容已有环境）

你前面确定的目录关系为：宿主机 `/mnt/openbkn-snapshots` → 容器 `/usr/share/opensearch/snapshots`。`path.repo` 必须允许这个容器路径。

先查看部署、所在节点和容器用户：

```bash
kubectl -n resource get pods -l app.kubernetes.io/instance=opensearch -o wide
kubectl -n resource exec opensearch-cluster-master-0 -c opensearch -- id
helm get values opensearch -n resource -a -o yaml > /tmp/opensearch-values-before-snapshot.yaml
```

在 **OpenSearch Pod 所在的节点** 创建宿主机目录，并赋予上面查到的 UID/GID 写权限。例如用户是 1000:1000：

```bash
install -d -m 0750 -o 1000 -g 1000 /mnt/openbkn-snapshots
```

参考 [conf/opensearch-snapshot-values.yaml](conf/opensearch-snapshot-values.yaml) 配置。应用前检查现有 `config.opensearch.yml`、`extraVolumes`、`extraVolumeMounts`，把原有自定义配置一起合并到该文件，避免 Helm 的数组替换导致已有挂载丢失。示例中的集群名和安全设置适用于本仓库默认 HTTP 部署。

下面的命令会更新 Helm 配置并重启 OpenSearch，应在安排好的维护窗口执行：

```bash
helm upgrade opensearch ./charts/opensearch-2.36.0.tgz \
  -n resource --reuse-values \
  -f ./conf/opensearch-snapshot-values.yaml \
  --wait --timeout 900s
kubectl -n resource rollout status statefulset/opensearch-cluster-master --timeout=900s
```

图表版本和 release 名需与实际部署一致。备份命令不会自动做 Helm 升级或重启。若回滚此次配置，可使用 `helm history opensearch -n resource` 找到上一版本，再执行 `helm rollback opensearch <上一版本号> -n resource --wait`；回滚不会清理宿主机快照文件。

hostPath 仅适合这里的单节点方案，Pod 迁往其他节点不会携带宿主机目录，多节点集群需要真正共享的快照存储。不要把快照目录与 OpenSearch 数据目录混用，也不要在备份期间手动增删仓库中的文件。

## 手动备份

从部署仓库执行：

```bash
bash ./deploy.sh backup check
bash ./deploy.sh backup run --output-dir /var/backups/openbkn
```

默认只备份五个业务库：`bkn_audit`、`bkn_trace`、`deploy`、`openbkn`、`safe`，不会自动加入其他非系统数据库。默认库缺失时检查报错，不会静默跳过。可通过 `--databases` 显式调整范围；库名支持字母、数字、下划线及横线，不允许指定系统库或重复库名：

```bash
bash ./deploy.sh backup run --output-dir /var/backups/openbkn \
  --databases openbkn safe bkn_trace
```

固定输出目录中，每次成功生成一个时间命名的 `openbkn-<时间>-<随机后缀>.tar.gz`，可以直接复制到另一台机器。命令输出 `result: success` 才表示完成；`check` 的 `ready` 仅表示前置检查通过。每个步骤会输出进度，长命令每 20 秒输出等待状态。

备份按顺序执行：

1. 对每个选中库分别执行 `mariadb-dump --single-transaction --quick --routines --events --triggers --hex-blob --add-drop-database --databases <库名>`，生成 `<库名>.sql` 并装入 `mariadb.tar`。包含结构、数据、触发器、过程和事件。系统库、数据库账号及授权不导出。每个库显示进度和耗时，失败后不发布备份包。
2. 注册专用 `offsite-backup` 文件系统仓库，并执行 `_verify`。
3. 使用 Snapshot API 创建快照，`indices: "*"`、`include_global_state: false`、`ignore_unavailable: false`、`partial: false`，轮询直到 `SUCCESS`。
4. 通过 Snapshot DELETE API 删除这个专用仓库内的所有旧快照。新快照失败时不会删除旧快照；不能与其他用途共用此仓库。
5. 从 Pod 挂载路径导出**整个快照仓库**。
6. 对 Redis 执行 `BGSAVE`，确认本次保存成功且进程未改变，校验并导出完整 RDB。RDB 包含所有 DB，源端即使启用 AOF 也只导出 RDB，不在线复制 AOF 文件组。
7. 使用 MinIO 容器内的 `mc ls/cat` 在线列出并读取对象，Python 在执行机生成 `minio.tar`；成功或失败均尝试清理容器临时客户端配置。
8. 与 SQL、清单、SHA-256 校验值一起压缩。校验通过后把 `.partial` 原子改名为 `.tar.gz`。

```text
openbkn-20261019010000-xxxxxxxx.tar.gz
├── manifest.json       # 版本、业务库、索引、快照名、文件校验和
├── mariadb.tar         # bkn_audit.sql、bkn_trace.sql、deploy.sql、openbkn.sql、safe.sql
├── repository.tar      # 完整 OpenSearch 快照仓库
├── redis.rdb           # Redis 全部 DB 的 RDB 快照（可选）
└── minio.tar           # 各 bucket 的当前对象，附对象映射清单，保留空 bucket
```

备份包校验并生成后，工具通过 Snapshot API 删除本次快照，释放快照占用；仓库可能留下少量元数据。删除失败时仍报告备份包成功，并在结果中返回 `cleanup_warning`，应根据提示处理本机残留快照。固定输出目录的历史 `.tar.gz` 包会保留，需自行管理保留期限。不要手动删除快照仓库内的单个文件。快照删除后，压缩包仍可独立恢复。备份过程的空间峰值不会因事后清理而降低。

MariaDB 在线导出不需要停数据库，但每个库使用独立事务快照，在线备份不保证跨库一致性。需要一致性时，先执行 `bash deploy.sh openbkn stop` 并另行停止外部写入、定时任务，再备份，完成后手动启动业务。备份时避免 DDL；需要 MariaDB、OpenSearch 和 Redis 之间业务一致时，先停止业务写入并等待异步任务完成，再备份。三套存储依次备份，不是跨系统的同一时间点事务。

输出盘需容纳 SQL、未压缩仓库、压缩包和校验解包的临时副本，建议预留未压缩备份体积（含 Redis RDB）约 3 倍再加压缩包的空间。压缩包包含业务数据，按敏感文件保管；默认文件权限 0600，不加密。SHA-256 用于检查损坏，不用于验证来源，恢复时只能使用可信备份。

### MariaDB 按库恢复和重试

新包恢复时按清单顺序逐库导入并检查可访问性。失败即停止，日志显示失败库（可能已部分写入）、已完成和未执行的库，不自动回滚。可以从同一包选择库恢复或重试：

```bash
bash deploy.sh mariadb restore check <备份包> --databases safe --overwrite
bash deploy.sh mariadb restore run <备份包> \
  --databases safe --writers-stopped --overwrite
```

不传 `--databases` 时恢复包内全部库，不根据目标已有库推断范围。包内没有的库、系统库或重复库名会被拒绝；未选择的库不覆盖。分库串行导入不保证速度提升，主要用于定位失败和单库重试。

旧版 v1～v4 的 `mariadb.sql` 继续整体导入，不能从合并 SQL 安全选择子集；需要按库选择时请重新生成 v5 备份。恢复前同步新版脚本。校验包时仍需临时空间容纳 `mariadb.tar` 及解出的 SQL，建议使用空间充足的 `TMPDIR`。

## 在新环境恢复

终端中可以直接运行 `bash deploy.sh restore run <备份包>` 进入交互恢复。缺少确认参数时，依次询问 Kafka 账本是否匹配、是否允许覆盖目标数据，以及是否允许留存并替换 OpenSearch 原快照仓库。未提供 `--writers-stopped` 时，会询问是否停止业务 Deployment；明确输入 `y` 后才执行停止，并等待业务 Pod 全部删除，再进入恢复。MinIO、PostgreSQL 等数据服务继续运行。缺省答案为否；拒绝必要确认或停止失败会终止恢复。

交互停业务默认使用 `openbkn` 命名空间，可通过 `--business-namespace <命名空间>` 指定。该操作不处理独立 Job/CronJob 和外部写入方，确认前需另行停写。恢复完成或失败后都不会自动启动业务，核验成功后执行 `openbkn start`。

`restore check` 始终只检查，不交互停业务。脚本或无终端执行时不提示，仍要求通过参数提供全部必要确认；可用 `--non-interactive` 显式禁用终端交互。已有带齐参数的命令继续适用。

先搭好 OpenBKN，并保持 MariaDB/OpenSearch/Redis/MinIO 运行，以便工具完成检查；恢复过程中会自动停止和重启 Redis。源端与目标端的应用版本、租户标识及业务加密密钥等配置应匹配；这些配置不会自动随数据恢复。

停止所有会读写待恢复存储的业务服务及定时任务（含会写库的初始化/迁移任务），确认不会被控制器自动拉起。可先执行 `bash deploy.sh openbkn stop` 缩容业务 Deployment；此命令不会停数据服务，也不会处理独立 Job/CronJob 或外部写入方。MariaDB、OpenSearch 与 MinIO 本身保持运行。保留新环境的数据副本后再考虑覆盖恢复。

目标端也要完成上面的快照路径配置。默认恢复要求目标快照目录**为空且尚未被任何快照仓库注册引用**，防止混合不同仓库。明确要替换时，在 `restore check/run` 中同时传入 `--overwrite --replace-repository`：check 只验证，run 先把目标原仓库打包到 `--output-dir`（默认 `/var/backups/openbkn`），再注销同名仓库、清空挂载目录内容并导入备份包。其他仓库若指向同一目录仍会拒绝。原仓库留存包为 `openbkn-repository-before-restore-*.tar`，需自行管理；确保输出盘有足够空间。不要删除或移走宿主机的挂载目录本身：正在运行的 Pod 会继续引用旧目录，导致解包出现 `Cannot open/mkdir: No such file or directory`。

如果宿主机快照目录已被删除或移走，重新建同名目录不会修复现有 Pod 的挂载。应在 Pod 所在节点重建原目录并按进程 UID/GID 设置权限，然后重启 OpenSearch 工作负载以重新挂载（单节点期间服务会中断），等待就绪后再执行 `restore check`。工具会识别 Linux 挂载信息中的 `//deleted` 并提前报错；不会自动创建宿主机目录或重启服务。

复制单个备份包到目标机器：

```bash
scp /var/backups/openbkn/openbkn-<时间>-<后缀>.tar.gz root@目标机器:/var/backups/openbkn/
```

校验及恢复（替换下面实际包名）：

```bash
bash ./deploy.sh restore check /var/backups/openbkn/openbkn-<时间>-<后缀>.tar.gz
bash ./deploy.sh restore run /var/backups/openbkn/openbkn-<时间>-<后缀>.tar.gz \
  --writers-stopped
```

新部署通常已经初始化了表或索引，此时默认拒绝恢复。要替换这些数据，明确传入 `--overwrite`：

```bash
bash ./deploy.sh restore check /var/backups/openbkn/openbkn-<时间>-<后缀>.tar.gz \
  --overwrite
bash ./deploy.sh restore run /var/backups/openbkn/openbkn-<时间>-<后缀>.tar.gz \
  --writers-stopped --overwrite
```

目标快照目录已被本工具仓库注册或不为空，且确定要覆盖时：

```bash
bash ./deploy.sh openbkn stop
bash ./deploy.sh restore check <备份包> --overwrite --replace-repository
bash ./deploy.sh restore run <备份包> --writers-stopped --overwrite --replace-repository
# 恢复并核验成功后
bash ./deploy.sh openbkn start
```

`--overwrite` 对 v2 包还会替换 Redis **全部 DB**，不是按 key 合并；会删除快照内同名的目标索引，并由 SQL 中的 DROP/CREATE DATABASE 替换清单内的业务库。其他数据库和不重名索引不删除。快照别名与目标已有索引/别名也可能冲突，需在新环境预先检查和处理。务必在正确 kubeconfig/context 下执行。

恢复过程先完整校验本地包的格式、文件路径、校验和、版本和目标冲突，再将完整仓库复制到空的挂载目录，注册只读仓库并核对快照。随后通过 Restore API 恢复索引，导入 SQL，检查各库可访问及 OpenSearch 至少达到 yellow。含 Redis 的包随后恢复 Redis，流程见下文；含 MinIO 的包最后恢复对象。单节点因副本未分配出现 yellow 是允许的。校验不等同于业务验收，工具不会自动启动业务服务。

恢复检查和执行都会在执行机的临时目录解包，需要足够的未压缩备份空间。默认使用系统临时目录；空间不足时，可先创建大容量目录，再在命令前加 `TMPDIR=/大容量临时目录`。

恢复是多步写入，没有跨 MariaDB/OpenSearch/Redis 的自动回滚。失败或中断时应保持业务停止，查看错误；SQL 导入中断可能已部分生效。需要重试时先保存现场，将目标快照仓库注销并整理为空目录，再使用同一可信备份包重新执行覆盖恢复。也可在重新部署的空环境中恢复。只有包内所有存储均恢复成功且业务核验通过后才恢复服务。

恢复后的快照仓库保持只读，数据文件保留。下一次正常备份会把同一专用仓库注册为可写，并在新快照成功后删除旧快照；备份包落盘后也会删除本次快照。

## 参数与故障排查

```bash
bash ./deploy.sh backup run --help
bash ./deploy.sh restore run --help
```

默认 namespace 为 `resource`。Redis 默认 selector 为 `app=redis-redis`、容器 `redis`、ACL 用户 `root`。可通过 `--redis-namespace`、`--redis-selector`、`--redis-pod`、`--redis-container`、`--redis-user` 指定；原生 CONFIG 环境传 `--redis-config-command CONFIG`。默认 MariaDB selector 为 `app=mariadb`，OpenSearch selector 为 `app.kubernetes.io/instance=opensearch`；必须分别匹配唯一就绪 Pod，可按实际环境指定：

```bash
bash ./deploy.sh backup check \
  --mariadb-namespace resource --mariadb-pod mariadb-0 --mariadb-container mariadb \
  --opensearch-namespace resource --opensearch-pod opensearch-cluster-master-0 \
  --opensearch-container opensearch
```

`--repository` 可修改专用仓库名，`--repository-path` 可修改容器内仓库路径（需要与 path.repo 和持久卷挂载完全一致）。`--timeout 3600` 控制每个长命令及快照等待的超时秒数，`--poll-interval 10` 控制快照轮询间隔。备份与恢复必须独占仓库，禁止同时用手动命令或其他定时任务修改它。

工具通过选中 MariaDB/OpenSearch/MinIO Pod 内各自的 `/tmp/openbkn-data-backup.lock` 互斥，Redis 使用 PVC 内的锁。完整命令与单组件命令共用组件锁；单组件操作不依赖其他 Pod 的锁。异常断连或强制终止可能遗留锁目录，先确认没有正在执行的备份/恢复进程及服务端任务，再手动移除，不能在任务仍运行时删除。

若命令超时或 SSH 断开，服务端 Snapshot/Restore 请求可能仍在执行；先查看快照及恢复状态，再决定重试。需要离开 SSH 时，可使用现有终端会话管理工具持续执行并保存日志。

执行离线验证：

```bash
make test
make lint
```

正式使用前，应在可丢弃的相同版本环境完成一次真实备份、跨环境恢复及业务核对。离线测试不验证真实数据库权限、磁盘空间、Pod 镜像工具是否齐全、应用密钥匹配或完整业务一致性。

## Redis 恢复与失败处理

含 Redis 的新 v4 包在 `components` 中声明 `redis`，清单包含 `redis.version`、`redis.snapshot_time`、参考 keyspace 及 `redis.rdb` 的 SHA-256。参考 keyspace 是在线读取值，不能当作快照精确清单。v1/v2/v3 旧包仍可恢复；v1 包不含 Redis。完整备份同时传 `--skip-redis --skip-minio` 会生成仅含 MariaDB/OpenSearch 的 v4 包。恢复范围由包及组件选择共同决定，恢复时不接受 `--skip-redis`。

`restore check` 检查 Redis 版本、已有数据、StatefulSet、PVC，以及临时 Pod 创建/删除、exec、StatefulSet 缩容权限；不停止 Redis。`restore run` 要求 `--writers-stopped`，已有任何 DB 数据时还需 `--overwrite`。工具不会停止业务服务或处理 GitOps/其他控制器的并发修改；恢复期间必须保证它们不会重新拉起业务、修改副本数或部署配置。

Redis 恢复顺序：

1. 独占 Redis PVC 锁，记录当前部署，StatefulSet 缩至 0，等待原 Pod 完全退出（同 Pod 的 Sentinel/exporter 一并停止）。
2. 创建临时恢复 Pod，使用目标 Redis 镜像、实际 UID/GID、原节点和同一 PVC。不通过 ConfigMap 传递 RDB，因此不受其大小限制。
3. 上传并校验 RDB。将原 RDB、AOF 目录及兼容旧 AOF 文件移动到 PVC 的 `.openbkn-redis-rollback-<随机后缀>`，保留目标环境 ACL、证书和部署配置。
4. 在临时 Pod 内启动关闭 AOF 的 Redis 加载 RDB，仅通过 Unix socket 访问、不开放 TCP。若目标原来启用 AOF，运行时开启 AOF，等待重写完成、状态成功并生成 manifest；随后关闭临时 Redis 并释放 PVC。
5. 正式 StatefulSet 恢复到 1 副本，按原配置启动；检查持久化和各 DB key 数量，再停启一次检查持久化确实生效。允许 TTL 到期导致 key 减少；含字段过期的 DB 使用宽松数量检查。该检查不代替业务数据验收。

源端和目标端允许使用不同 Redis 密码，恢复数据不会替换目标凭证。恢复时保留绝对过期时间，过期 key 不会因恢复延长寿命。执行 BGSAVE 可能增加内存和磁盘压力；目标 PVC 需同时容纳原文件、上传 RDB 和新生成的 AOF，执行机也需预留解包空间。工具不会自动删除回退文件，成功并完成业务验收后再由管理员清理。

Redis 恢复异常时，工具尝试停止正式 Redis、关闭临时进程、把失败文件移入回退目录的 `failed/`，恢复原文件并重新启动原 Redis。此回退只覆盖 Redis，不撤销此前 MariaDB/OpenSearch 的恢复。业务必须继续保持停止，排查后再决定处理方式。

若自动回退失败或执行机被强制终止，日志会提供 StatefulSet、PVC、临时 Pod 和回退目录。保持业务停止及 Redis StatefulSet 为 0，确认没有遗留 Redis 进程读写 PVC 后，利用临时 Pod 恢复原文件，再启动正式 Redis。PVC 下 `.openbkn-redis-restore.lock` 可能遗留，确认没有备份/恢复任务后才能清理；不要盲目重新缩容至 1 或删除回退目录。

离线测试覆盖格式兼容、AOF 转换顺序、重启校验及失败回退，但不验证真实 Kubernetes 调度、Redis 权限、AOF 生成与镜像可用性。上线前必须在独立测试 PVC 上完成真实备份、覆盖恢复和失败回退演练。仓库提供显式调用的集成测试（会创建、写入及删除独立 StatefulSet/PVC，需要有对应集群操作授权）：

```bash
python3 scripts/redis_backup_integration.py --execute \
  --namespace resource --storage-class local-path \
  --image <目标Redis镜像>
```

测试验证多个 DB、Hash/List、TTL、恢复后的再次重启，以及注入 AOF 转换失败后的自动回退；仅针对随机命名的独立资源。

参考：[Redis 持久化](https://redis.io/docs/latest/management/persistence/)、[OpenSearch Snapshot/Restore](https://docs.opensearch.org/latest/tuning-your-cluster/availability-and-recovery/snapshots/snapshot-restore/)、[MariaDB mariadb-dump](https://mariadb.com/docs/server/clients-and-utilities/backup-restore-and-import-clients/mariadb-dump)。


## MinIO 在线备份与恢复

仅含 MinIO 的新备份使用 `format: 4`，含 MariaDB 时使用 `format: 5`，在 `components` 中声明 `minio`，包含 `minio.tar`、该文件的 SHA-256 和 `minio.mode/buckets` 清单。v1/v2/v3 旧包仍可恢复；v1/v2 不含 MinIO。完整备份可用 `--skip-minio` 排除 MinIO；单组件备份使用 `minio backup run`。恢复完整包时默认处理包内全部组件，也可用 `minio restore run <完整包>` 仅恢复 MinIO。

默认 namespace 为 `openbkn`，selector 为 `component=minio`，容器优先选择 `minio`，否则选择唯一容器。必须匹配唯一就绪 Pod；标签不同可直接指定 Pod，例如：

```bash
bash ./deploy.sh backup check --minio-namespace openbkn --minio-pod minio-69c578c57b-ljdzm
bash ./deploy.sh backup run --output-dir /var/backups/openbkn \
  --minio-namespace openbkn --minio-pod minio-69c578c57b-ljdzm
```

MinIO 备份不暂停业务、不停止 MinIO、不创建临时 Pod/PVC。工具通过 `kubectl exec` 使用现有 Pod 的 mc；认证信息在容器内读取，临时 mc 配置位于独立目录，不修改容器已有 alias，也不会打包凭证。默认 S3 API 为容器内 `http://127.0.0.1:9000`，可用 `--minio-endpoint` 修改（HTTPS 需容器已信任证书）。可用 `--minio-container` 指定容器；用 `--minio-buckets bucket-a bucket-b` 仅备份指定 bucket。

备份逐个 bucket 列出当前对象、检查对象名，然后通过 `kubectl exec` 执行容器内 `mc cat`，流式传到执行机，由 Python 写入 `minio.tar`。恢复反向使用 `mc pipe` 上传，默认分片为 8 MiB、并发数为 1，以限制内存占用；可用 `--minio-part-size` 调整分片。MinIO 容器无需 tar、无需暂存完整对象副本，只需要少量空间保存临时客户端配置；对象中转文件在执行机上逐个创建和清理，内存不随对象大小增长。执行机输出盘除完整对象包外还需容纳最大单个对象的临时副本，以及压缩包和校验解包副本。进程被强制终止可能遗留容器 `/tmp/openbkn-minio-*` 客户端配置目录，确认没有相关任务后再手动清理。

这是持续读写下的当前对象导出，不保证整个 bucket 同一时间点。列出对象后新增的对象可能不被收录；已列出的对象被删除或读取大小变化会导致备份失败，此时不会发布成功包，可重试。同名同大小内容变化无法靠大小校验识别。并发写入繁忙时建议选择低峰期，仍不要求停业务。对象版本历史、删除标记、对象元数据、用户/策略、bucket 权限、生命周期与通知等配置不随对象包恢复；如业务依赖这些配置或元数据，需要另行扩展。新环境使用重新部署的凭证、权限与配置。

内层 `minio.tar` 使用 `objects.json` 保存 bucket、原始对象名、大小及编号文件映射，对象内容保存为 `objects/000000000000` 等编号路径，因此支持 `a` 与 `a/b` 同时存在；S3 的零字节目录标记会跳过，因为它不承载文件数据且不是 `mc pipe` 可安全上传的普通对象。由于 mc 可能规范化路径，包含 `..`、重复斜线、反斜线或控制字符的对象名仍会拒绝，避免默默修改对象名。空 bucket 和零字节普通对象支持恢复；S3FS 空目录标记不恢复。

恢复使用相同的 `restore check/run` 命令，可传目标环境的 `--minio-namespace/--minio-pod/--minio-container/--minio-endpoint`。先校验压缩包、内层 tar 路径与文件类型，再创建缺少的 bucket、使用 `mc pipe` 上传对象，并验证备份对象的名称和大小。恢复仍需现有的 `--writers-stopped` 确认；不停止 MinIO 本身。

目标 bucket 已有对象时默认拒绝，显式 `--overwrite` 后覆盖同名对象，**保留目标多出的对象和未列入备份的 bucket**。需要完全回到备份内容时，应使用预先准备好的空 bucket；工具不会自动清空 bucket，也不使用 `mc mirror --remove`。恢复失败可能已写入部分对象，没有自动回滚；保留目标原数据副本后再执行覆盖恢复。完成后应抽查应用文件下载及关键对象内容，数量/大小检查不代替完整业务验证。

本实现的离线测试覆盖真实 tar 导出/解包、空 bucket、零字节对象、S3FS 目录标记跳过、文件/前缀共存、覆盖保护、失败清理、无容器 tar 依赖、并发大小变化、安全路径和 v1/v2/v3 包兼容。实际 MinIO 版本、凭证权限、容器工具、磁盘空间和跨环境读写仍需在测试环境演练。

MinIO 客户端参考：[mc cat](https://docs.min.io/aistor/reference/cli/mc-cat/)、[mc pipe](https://docs.min.io/aistor/reference/cli/mc-pipe/)。

## Kafka 在线备份及向已部署环境恢复

执行机只需 Python 3.9+、kubectl 和本仓库完整文件。工具自动复用现有 Broker 的镜像和节点，临时 Pod 使用 `imagePullPolicy: Never`，无需构建、导入或拉取备份镜像。约 20 KB 的 Java 小工具已随 `tools/kafka-backup/kafka-backup.jar` 交付，运行时不需要 Docker、JDK、pip、联网或手工编译。

当前支持 Kafka 3.9.x 镜像已有的 Java 17+、Kafka 客户端和 Jackson 库，默认库目录 `/opt/bitnami/kafka/libs`。临时 Pod 使用独立 CPU/内存限制，只运行客户端，不启动 Broker、不挂载 Broker 数据卷。

### 执行命令

```bash
bash deploy.sh kafka backup check
bash deploy.sh kafka backup run --output-dir /var/backups/openbkn

# 按需选择 Topic；默认枚举所有非内部 Topic。
bash deploy.sh kafka backup run --output-dir /var/backups/openbkn \
  --kafka-topics tenant_a.dip.model_manager.quota_data

# 默认备份五个数据组件及 K8s 配置留档。
bash deploy.sh backup run --output-dir /var/backups/openbkn

# 向已经部署好的 Kafka 恢复；也可从完整包只恢复 Kafka。
bash deploy.sh kafka restore check /var/backups/openbkn/<备份包>.tar.gz
bash deploy.sh kafka restore run /var/backups/openbkn/<备份包>.tar.gz --writers-stopped
```

包内包含非空审计/Evidence 分区时，恢复运行还需确认停写后同批备份的原 MariaDB 账本已保留或恢复，且沿用原消费组位置：

```bash
bash deploy.sh kafka restore run /var/backups/openbkn/<备份包>.tar.gz \
  --writers-stopped --kafka-ledger-restored
```

生成 `openbkn-kafka-<时间>-<后缀>.tar.gz`（或包含 Kafka 的完整包），可复制到其他机器。包内 `kafka.tar` 保存 Topic 关键配置、分区范围、消息及消费位置，外层清单保存 SHA-256；新旧 Kafka 包沿用相同逻辑格式。

Kafka 在备份和恢复时都保持运行。备份不要求停业务、不修改源消费组。恢复前停止涉及这些 Topic 的目标业务生产者和消费者；`--writers-stopped` 是人工确认，工具不自动停止业务。

### 自动发现与可选参数

默认 namespace 为 `resource`，Broker selector 为 `app.kubernetes.io/name=kafka,app.kubernetes.io/component=broker`。多个就绪 Broker 时按名称选择一个作为镜像和节点来源；通过节点亲和性调度到同一节点，使用实际镜像摘要（可获取时）和缓存镜像。节点不能调度时会显示原因。

按 Helm release 推导 Service `<release>.<namespace>.svc.cluster.local:9092` 和 Secret `<release>-sasl`。认证默认 SASL_PLAINTEXT / PLAIN，用户 `kafkauser`，密码读取 Secret 的 `client-passwords`。Secret 只挂载到临时 Pod，不读到执行机、不写入备份包；这是本仓库默认部署约定，不自动猜测任意外部 Kafka 的认证方式。

| 参数 | 用途 |
| --- | --- |
| `--kafka-namespace` | Kafka 所在 namespace |
| `--kafka-pod` / `--kafka-selector` | 指定用于镜像及节点发现的现有 Broker |
| `--kafka-container` | 容器名，默认 `kafka` |
| `--kafka-bootstrap` | 从临时 Pod 访问的 Kafka 地址 |
| `--kafka-libs` | 镜像内 Kafka/Jackson 库目录，例如 Apache 镜像的 `/opt/kafka/libs` |
| `--kafka-protocol` / `--kafka-mechanism` | PLAINTEXT、SSL、SASL_PLAINTEXT、SASL_SSL；PLAIN 或 SCRAM |
| `--kafka-user` / `--kafka-secret` / `--kafka-password-key` | 已有账号及 Secret 引用 |
| `--kafka-ca-secret` | TLS CA Secret，需包含 `ca.crt` |
| `--kafka-replicas` | 新建目标 Topic 副本数，默认 1 |
| `--kafka-ledger-restored` | 确认原审计/Evidence MariaDB 账本已保留或同批恢复，恢复后沿用原消费组 |
| `--kafka-work-size` | 临时磁盘上限，默认 10Gi，至少容纳导出/导入数据 |

多用户逗号分隔密码需要另提供只含单个账号密码的 Secret；暂不支持双向证书或 OAuth。工具 Pod 需要访问 Kafka `advertised.listeners` 的所有 Broker；有 NetworkPolicy 时需允许流量。执行账号需要 get/list/create/exec/delete Pods、读取 Events、create/delete 锁 ConfigMap 的权限。Kafka 账号需要读取 Topic、配置和消费位置；恢复额外需要建 Topic、修改 Topic 配置、写消息及修改消费位置。

镜像不可用、Secret/key 缺失、容器启动失败时尽早报错；调度等待每 10 秒输出状态，最长 180 秒。无需 `--kafka-helper-image` 等旧镜像参数。

### 数据范围与恢复保护

每个分区开始时固定 `[low, high)`，`high` 为 read_committed 下的稳定结束位置。仅保存仍保留的已提交消息；新增消息不扩大范围，已过期、未提交及已中止事务中的消息不导出。Base64 保留二进制 key/value、null/空值、tombstone，以及 Header 的名称、顺序和重复项；保留原分区、源 offset 和消息时间。

默认备份及恢复始终跳过 `connect-configs`、`connect-offsets`、`connect-status` 三个 Kafka Connect Topic，并输出跳过提示；即使显式传入这些名称，也不会备份它们。旧备份包含这些 Topic 时，仍校验包和文件完整性，但不恢复它们的消息或消费位置；`--overwrite` 也不会删除或重建目标同名 Topic。只按这三个名称精确排除，不跳过任意 `connect-*` 名称。Connect 的自定义 Topic 名称需另行确认。环境若只有被排除的 Topic，Kafka 部分生成空业务范围的清单，恢复时不写 Kafka 数据。

其他 Topic 当前仅支持 `cleanup.policy=delete`，compacted Topic 会拒绝。保留期推进导致数据缺失、Topic 删除重建、分区数或关键配置变化时备份失败，不发布成功包。不同分区、消费组及 MariaDB 不是全局原子快照；不恢复源事务 ID、事务边界或生产者 epoch。

恢复前校验包路径、文件、分区范围、数量、SHA-256 和消费位置。目标保留自身集群身份、凭证、内部 Topic 和数据卷。已有 Topic 默认必须是全新空分区，分区数匹配；关键配置以备份包为准，`restore check` 只报告差异，`restore run` 在写入消息前自动对齐并读回验证。空 Topic 只修改包内记录的配置项，其他配置不变；缺少的 Topic 按备份配置创建。显式指定 `--overwrite` 时，备份内非空、offset 不从 0 开始、分区数或副本数不足以匹配备份要求的目标 Topic 会被删除并按备份配置重建，原消息及未记录的 Topic 自定义配置会丢失。`restore check --overwrite` 只报告待替换清单，不删除 Topic。包外 Topic 和内部 Topic 不修改，不向原消息后追加。相关消费者必须停止，消费位置、消息范围和账本确认保护不会被 `--overwrite` 绕过。

按原分区顺序发送，逐条等待 Broker 确认并回读 key/value/headers 校验。消费位置按目标实际 offset 映射：源消息 `[100,101,105]` 导入为 `[0,1,2]`，源 committed offset `102` 映射到目标 `2`，全部已消费则映射到目标导入结束位置。所有分区通过后才修改消费组并回读确认；相关目标消费组必须停止，备份之外的消费位置会报错，无关 Topic 的位置不修改。

`CreateTime` 尽量保留原时间，`LogAppendTime` 由目标 Broker 重新赋时；目标时间及 retention 约束仍生效。恢复保留原消费进度，不从头重放已经消费的审计/Evidence 记录；积压记录在业务重新启动后按原提交位置继续消费。依赖原 Broker 时间的业务语义仍需在业务核验中确认。

对 `openbkn.audit.v1`、`openbkn.evidence.v1`，以下情况支持恢复：

- 全新空分区（`low=high=count=0`），不要求存在消费组或账本确认。
- 每个分区从 offset 0 完整保留、没有空洞（`low=0` 且消息数等于 `high`），非空分区有对应账本消费组的有效提交位置（`0 <= offset <= high`），允许存在积压。默认账本组分别为 `bkn-trace-audit-ledger-v1`、`bkn-trace-evidence-ledger-v1`；其他审计组名暂不自动推测。包中其他消费组也按各自记录的位置恢复。

第二种情况保持每条消息的原 offset，并把原消费组恢复到备份记录的位置。例如 117 条消息、offset 0～116、已提交位置 80，恢复后仍是原坐标，消费组从 80 继续处理剩余 37 条；已提交位置是 117 时则从末尾接续。新增消息从 offset 117 开始，避免复用原账本中的 Kafka 坐标。备份前应停止全部相关生产者和消费者并等待 Pod 删除，再备份 MariaDB 与 Kafka，确保两者取自稳定状态。原业务账本须已保留或同批恢复，运行时要求 `--kafka-ledger-restored`，检查时会报告 `required_run_options`；该开关只是确认前置条件，不会替你恢复数据库。Kafka 备份本身仍可在线执行，但跨组件在线备份不能保证账本与消费位置一致。

缺少对应消费位置、保留范围从非零开始，或由于事务/保留策略出现 offset 空洞时，仍拒绝审计/Evidence 恢复，不能通过确认账本或 `--overwrite` 绕过。不把缺少的消费位置猜测为 0，也不强行推进至末尾。普通 Topic 仍使用上面的 offset 映射规则。工具不提供从头重放历史消息来重建账本的功能。

备份检查按当前分区范围和消费进度提示限制，导出后再核对实际消息数量和连续性；受限的 Topic 在完成结果中列入 `restore_blocked_topics`。包含非空审计/Evidence 的包会列出 `kafka_ledger_restore_required`。受限包的恢复检查在连接 Kubernetes 前拒绝，不创建临时 Pod、不写目标数据。旧备份无需重做，使用新版脚本重新检查即可。Java 输出固定 UTF-8，并配置日志，避免中文问号及日志配置警告掩盖实际原因。

配额链路的自动提交 offset、内存聚合后定时落库，仍可能使消费位置和 MariaDB 不同步；消息导入和 offset 映射不能替代持久化后提交或幂等重放改造，不承诺用量数据零丢失或不重复。

### 失败处理与容量

临时 Pod 内存限制 512MiB、Java 堆上限 256MiB，顺序处理分区，单条 Base64 记录上限 24MiB。emptyDir 暂存导出文件，tar 流传回执行机；恢复反向上传并校验。执行机需为内层 tar、压缩包及校验副本保留空间。超时、空间不足或校验失败不发布成功包。

恢复失败可能已有部分消息或消费位置写入。保持目标业务停止，确认目标可替换后可使用同一可信备份包和 `--overwrite` 删除重建包内非空 Topic 后重试；也可人工准备全新空 Topic。覆盖删除和消息导入没有自动回滚，不能靠重试保留目标原消息。

正常结束自动清理临时 Pod。同 namespace、同 bootstrap 字符串使用原子 ConfigMap 锁，不能阻止其他工具或不同地址的并发操作。强制杀进程可能遗留 `openbkn-kafka-backup-*` Pod 或 `openbkn-kafka-lock-*` ConfigMap，确认没有任务后再清理。

### 维护者构建与验证

Java 源码和预编译 JAR 一起交付，`artifact.json` 保存校验和。修改源码后维护者需使用 Java 17+ JDK 和 Kafka 3.9 库重新构建；普通备份用户不用执行：

```bash
python3 tools/kafka-backup/build.py --classpath '/path/to/kafka/libs/*'
make ci
```

使用已有 Kafka 3.9 发行包和 Java 17+，启动两个隔离本地进程演练，不连接 Kubernetes 或业务 Kafka：

```bash
python3 scripts/kafka_backup_integration.py --kafka-home /path/to/kafka --java /path/to/java
```

加 `--sasl` 可验证默认部署使用的 SASL/PLAIN 及密码转义。该隔离测试的 SASL 模式要求 Linux（含 Ubuntu CI），使用 memfd 和 `/proc/self/fd` 向 Java 提供内存中的凭证文件，不把测试密码写入磁盘配置；Broker 通过显式配置的环境变量提供器读取 JAAS。演练同时使用 Zstd 压缩消息；工具将 Java 临时目录指向可写的 `/work`，使压缩库在只读容器文件系统中也能加载。

或复用原生 Kafka 镜像运行隔离 Docker Compose 测试，无需构建备份镜像：

```bash
make test-integration-kafka
docker compose -p openbkn-kafka-backup-test -f tools/kafka-backup/compose.test.yaml down -v
```

覆盖在线生产期间导出、已中止事务排除、跨集群恢复、二进制/null/Unicode/重复 Header、原时间、offset 空洞映射、回读及重复导入拒绝。正式使用前仍需核对实际 Kubernetes 认证、调度、容量和业务结果。API 参考：[Kafka Admin](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/admin/Admin.html)、[KafkaConsumer](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)。
