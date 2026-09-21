# OpenBKN 0.1.5 权限模型一次性迁移

中文 | [English](README.md)

本目录是 OpenBKN 0.1.5 安装后的权限模型一次性迁移入口。正常执行不要求填写 manifest、资源 ID、权限 ID、数据库参数或报告目录；迁移会识别已安装的目标 bkn-safe Chart 版本，创建运行报告，并以保守方式处理无法证明来源的历史授权。

全新安装由 bkn-safe seed 直接写入当前迁移标记；后续版本未改变授权存储合同时，不再执行本迁移。

## 固定步骤

统一入口按顺序执行并在首个失败处停止：

1. `bkn-data`：校验或迁移空分支、权威资源父级关系、托管代理账号、Resource/Tool Box/MCP/Skill 授权来源、物化代理策略、BKN 代理映射与同步版本。Skill 授权按尽力而为处理：授权人无权执行的已挂载 Skill 列入报告的 `skipped_skill_grants`，不会导致该网络迁移失败；Skill 挂载也不改变网络的模型版本。
2. `vega-data`：读取权威的 Catalog 与 Resource 元数据，对账全部 `resource → catalog` 父子关系，并为每个非内置 Catalog 登记规范的创建者业务权限包与授权权限。
3. `authorization`：调用本目录下的 `authz_migrate`，完成 Core 来源与稳定 grant 分类、Enterprise 规则对账，并在全部成功后写入带校验和的迁移标记。

BKN 步骤不再删除或重建 caller 权限，也不会写入 `task_manage`。历史 Core allow/deny 全部交给授权步骤分类和保留。

Vega 步骤只写 bkn-safe，Vega 数据库只读。若检测到逻辑备份客户端，前置 BKN 步骤也会备份 Safe。

## 环境要求

- Python 3.9+、PyMySQL 1.1.0；
- 可选的 `mariadb-dump` 或 `mysqldump`，以及足够保存 BKN、Safe 完整逻辑备份的空间；
- 目标集群的 `helm` 与 `kubectl` 权限，包括读取已安装 release values、bkn-safe ConfigMap/Secret、控制登记的 Deployment，以及对集群内数据库 Service 建立 port-forward；
- 在 Linux amd64/arm64 或 Apple 芯片 macOS 主机上执行命令；`migrate.py` 会自动选用仓库内置的对应平台程序 `authz_migrate/authz-migrate-<os>-<arch>`；
- 对外部 MariaDB/MySQL 地址的网络访问能力（若数据库部署在集群外）。

正常入口会从已安装集群自动发现 namespace、BKN/Vega RDS values，以及 bkn-safe 最终生效的数据库 ConfigMap/Secret；不要求用户提供数据库变量，也不会打印或持久化数据库密码。集群内 Service 地址通过临时 kubectl port-forward 访问，外部数据库地址直接连接。检测到 dump 工具时可通过 `OPENBKN_MIGRATION_BACKUP_DIR` 指定备份目录；未检测到时 BKN 报告会标明备份已跳过。运行报告默认保存到 `~/.openbkn-ai/migrations`，仅在该目录不适用时设置 `OPENBKN_MIGRATION_WORKDIR`。

## 执行流程

发行内容已携带授权迁移程序，执行迁移不需要 Go。仅在有意修改其 Go 源码时才重新构建；不带参数时构建全部内置平台，也可传入 `<os>/<arch>` 指定目标：

```bash
./authz_migrate/build.sh
```

执行一条命令：

```bash
./migrate.py upgrade
```

请先完成 OpenBKN 0.1.5 安装，再执行该命令。它自动发现 namespace 和数据库配置、验证 bkn-safe 已安装版本为 0.1.5、建立必要的数据库隧道并先完成 dry-run；随后停止登记的业务 Deployment、执行 apply，并在成功后自动按副本快照恢复工作负载。若检测到 `mariadb-dump` 或 `mysqldump`，BKN 步骤会在首次写入前创建并校验 BKN、Safe 完整逻辑备份；未检测到时会在报告中写入 `backup.status=skipped`，并在无逻辑备份的情况下继续迁移。任一步失败时，不会继续下一步；若服务已停止，则保持停止。

报告目录包含 `dry-run/01-bkn-data.json`、`dry-run/02-vega-data.json`、`dry-run/03-authorization.json`，以及对应的 apply 报告和副本快照。历史 Core 规则无法从权威生命周期数据证明时保留为 `legacy`；历史 Enterprise 规则会迁移为 inactive，绝不因迁移自动启用。后续如确需启用 EE 规则，应通过升级后的权限管理流程完成审计和授权。

`dry-run`、`stop`、`apply`、`start` 子命令仍保留，仅用于故障诊断和恢复；它们不是正常操作入口，可能需要显式提供环境配置。

## 失败恢复

任一步失败都不得启动业务服务。若 `01-bkn-data.json` 记录了已创建的备份，使用其中命令恢复；若记录为 `skipped`，使用环境已有的数据库快照/PITR，或进行人工修复。禁止手工伪造成功标记或在部分迁移的数据上继续执行。

## 聚焦测试

```bash
python3 -m unittest -v test_bkn_data.py vega/test_vega_data.py test_migrate.py
./test_service_control.sh
(cd authz_migrate && go test -p=1 ./...)
```
