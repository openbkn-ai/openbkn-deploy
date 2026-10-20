# MariaDB + OpenSearch 备份恢复（一期）

此版本只包含 MariaDB 业务库与 OpenSearch 索引。目标是在另一台已经部署好相同版本 OpenBKN 的环境中恢复数据。MinIO、Kafka、Redis、Hydra PostgreSQL、Kubernetes/Helm 配置、用户权限和应用密钥不在包内，恢复后需单独检查这些依赖；该包不是完整环境的灾备副本。

适用环境：本仓库部署的单 Pod MariaDB（业务表必须使用 InnoDB）、单节点 HTTP 无认证 OpenSearch。MariaDB 主版本必须一致，OpenSearch 主次版本必须一致；建议源端与目标端使用完全相同版本。多节点、HTTPS/认证、外部数据库暂不支持，不能为使用此工具关闭已有集群认证。

执行机需要 Python 3.9+、kubectl 与可访问目标集群的 kubeconfig。MariaDB Pod 内需要 mariadb、mariadb-dump 及 MARIADB_ROOT_PASSWORD（或 MYSQL_ROOT_PASSWORD）环境变量；OpenSearch Pod 内需要 curl、tar、sh、find、mkdir、rmdir。密码直接从 Pod 环境使用，不写进备份包。执行机不依赖 age、mc、jq、yq 或数据库客户端。

## 一次性配置 OpenSearch 快照目录

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

默认发现所有非系统数据库，如 `openbkn`、`safe`、`bkn_trace`；可明确指定：

```bash
bash ./deploy.sh backup run --output-dir /var/backups/openbkn \
  --databases openbkn safe bkn_trace
```

固定输出目录中，每次成功生成一个时间命名的 `openbkn-<时间>-<随机后缀>.tar.gz`，可以直接复制到另一台机器。命令输出 `result: success` 才表示完成；`check` 的 `ready` 仅表示前置检查通过。每个步骤会输出进度，长命令每 20 秒输出等待状态。

备份按顺序执行：

1. 使用一次 `mariadb-dump --single-transaction --quick --routines --events --triggers --hex-blob --add-drop-database --databases ...` 导出选中业务库，包含结构、数据、触发器、过程和事件。系统库、数据库账号及授权不导出。
2. 注册专用 `offsite-backup` 文件系统仓库，并执行 `_verify`。
3. 使用 Snapshot API 创建快照，`indices: "*"`、`include_global_state: false`、`ignore_unavailable: false`、`partial: false`，轮询直到 `SUCCESS`。
4. 通过 Snapshot DELETE API 删除这个专用仓库内的所有旧快照。新快照失败时不会删除旧快照；不能与其他用途共用此仓库。
5. 从 Pod 挂载路径导出**整个快照仓库**，与 SQL、清单、SHA-256 校验值一起压缩。校验通过后把 `.partial` 原子改名为 `.tar.gz`。

```text
openbkn-20261019010000-xxxxxxxx.tar.gz
├── manifest.json       # 版本、业务库、索引、快照名、文件校验和
├── mariadb.sql         # 所有选中业务库的单次逻辑导出
└── repository.tar      # 完整 OpenSearch 快照仓库
```

快照仓库只保留本次新快照；固定输出目录的历史 `.tar.gz` 包会保留，方便拿走及处理失败时回退，需自行管理保留期限。不要手动删除快照仓库内的单个文件。快照删除后，已有导出的压缩包仍可独立恢复。

MariaDB 在线导出不需要停数据库。备份时避免 DDL；需要 MariaDB 和 OpenSearch 之间业务一致时，先停止业务写入并等待异步任务完成，再备份。两种在线备份不是跨系统的同一时间点事务。

输出盘需容纳 SQL、未压缩仓库、压缩包和校验解包的临时副本，建议预留未压缩备份体积约 3 倍再加压缩包的空间。压缩包包含业务数据，按敏感文件保管；默认文件权限 0600，不加密。SHA-256 用于检查损坏，不用于验证来源，恢复时只能使用可信备份。

## 在新环境恢复

先搭好 OpenBKN，并保持 MariaDB/OpenSearch 运行。源端与目标端的应用版本、租户标识及业务加密密钥等配置应匹配；这些配置不会自动随数据恢复。

停止所有会读写这两套存储的业务服务及定时任务（含会写库的初始化/迁移任务），确认不会被控制器自动拉起；工具不会自动缩容业务服务。MariaDB 与 OpenSearch 本身无需停止。保留新环境的数据副本后再考虑覆盖恢复。

目标端也要完成上面的快照路径配置。恢复要求目标快照目录**为空且尚未被任何快照仓库注册引用**，防止混合不同仓库。如果新环境已经有快照仓库，先保存整个仓库，再注销相关仓库（注销本身不删文件），由操作人员将原目录移到其他位置保存，再创建同权限的空目录。不要直接删除现有仓库文件。

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

`--overwrite` 会删除快照内同名的目标索引，并由 SQL 中的 DROP/CREATE DATABASE 替换清单内的业务库。其他数据库和不重名索引不删除。快照别名与目标已有索引/别名也可能冲突，需在新环境预先检查和处理。务必在正确 kubeconfig/context 下执行。

恢复过程先完整校验本地包的格式、文件路径、校验和、版本和目标冲突，再将完整仓库复制到空的挂载目录，注册只读仓库并核对快照。随后通过 Restore API 恢复索引，导入 SQL，检查各库可访问及 OpenSearch 至少达到 yellow。单节点因副本未分配出现 yellow 是允许的。校验不等同于业务验收，工具不会自动启动业务服务。

恢复检查和执行都会在执行机的临时目录解包，需要足够的未压缩备份空间。默认使用系统临时目录；空间不足时，可先创建大容量目录，再在命令前加 `TMPDIR=/大容量临时目录`。

恢复是多步写入，没有跨 MariaDB/OpenSearch 的自动回滚。失败或中断时应保持业务停止，查看错误；SQL 导入中断可能已部分生效。需要重试时先保存现场，将目标快照仓库注销并整理为空目录，再使用同一可信备份包重新执行覆盖恢复。也可在重新部署的空环境中恢复。只有两个存储均恢复成功且业务核验通过后才恢复服务。

恢复后的快照仓库保持只读，数据文件保留。下一次正常备份会把同一专用仓库注册为可写，并在新快照成功后删除旧快照。

## 参数与故障排查

```bash
bash ./deploy.sh backup run --help
bash ./deploy.sh restore run --help
```

默认 namespace 为 `resource`。默认 MariaDB selector 为 `app=mariadb`，OpenSearch selector 为 `app.kubernetes.io/instance=opensearch`；必须分别匹配唯一就绪 Pod，可按实际环境指定：

```bash
bash ./deploy.sh backup check \
  --mariadb-namespace resource --mariadb-pod mariadb-0 --mariadb-container mariadb \
  --opensearch-namespace resource --opensearch-pod opensearch-cluster-master-0 \
  --opensearch-container opensearch
```

`--repository` 可修改专用仓库名，`--repository-path` 可修改容器内仓库路径（需要与 path.repo 和持久卷挂载完全一致）。`--timeout 3600` 控制每个长命令及快照等待的超时秒数，`--poll-interval 10` 控制快照轮询间隔。备份与恢复必须独占仓库，禁止同时用手动命令或其他定时任务修改它。

工具通过目标 OpenSearch Pod 内 `/tmp/openbkn-data-backup.lock` 互斥；异常断连或强制终止可能遗留该目录。先确认没有正在执行的备份/恢复进程及服务端任务，再手动移除锁目录，不能在任务仍运行时删除。

若命令超时或 SSH 断开，服务端 Snapshot/Restore 请求可能仍在执行；先查看快照及恢复状态，再决定重试。需要离开 SSH 时，可使用现有终端会话管理工具持续执行并保存日志。

执行离线验证：

```bash
make test
make lint
```

正式使用前，应在可丢弃的相同版本环境完成一次真实备份、跨环境恢复及业务核对。离线测试不验证真实数据库权限、磁盘空间、Pod 镜像工具是否齐全、应用密钥匹配或完整业务一致性。

参考：[OpenSearch Snapshot/Restore](https://docs.opensearch.org/latest/tuning-your-cluster/availability-and-recovery/snapshots/snapshot-restore/)、[MariaDB mariadb-dump](https://mariadb.com/docs/server/clients-and-utilities/backup-restore-and-import-clients/mariadb-dump)。
