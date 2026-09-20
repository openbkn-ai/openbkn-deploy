# OpenBKN 0.1.5 权限模型一次性迁移

中文 | [English](README.md)

本目录是 OpenBKN 0.1.4 → 0.1.5 权限模型首次切换的可选运维入口。正常执行不要求填写 manifest、资源 ID、权限 ID、数据库参数或报告目录；迁移会识别已安装的 bkn-safe Chart 版本，创建备份和运行报告，并以保守方式处理无法证明来源的历史授权。

全新安装由 bkn-safe seed 直接写入当前迁移标记；后续版本未改变授权存储合同时，不再执行本迁移。

## 固定步骤

统一入口按顺序执行并在首个失败处停止：

1. `bkn-data`：校验或迁移空分支、权威资源父级关系、托管代理账号、Resource/Tool Box/MCP/Skill 授权来源、物化代理策略、BKN 代理映射与同步版本。Skill 授权按尽力而为处理：授权人无权执行的已挂载 Skill 列入报告的 `skipped_skill_grants`，不会导致该网络迁移失败；Skill 挂载也不改变网络的模型版本。
2. `vega-data`：读取权威的 Catalog 与 Resource 元数据，对账全部 `resource → catalog` 父子关系，并为每个非内置 Catalog 登记规范的创建者业务权限包与授权权限。
3. `authorization`：调用本目录下的 `authz_migrate`，完成 Core 来源与稳定 grant 分类、Enterprise 规则对账，并在全部成功后写入带校验和的迁移标记。

BKN 步骤不再删除或重建 caller 权限，也不会写入 `task_manage`。历史 Core allow/deny 全部交给授权步骤分类和保留。

Vega 步骤只写 bkn-safe，其改动由前置 BKN 步骤生成的 Safe 备份覆盖；Vega 数据库只读。

## 环境要求

- Python 3.9+、PyMySQL 1.1.0；
- `mariadb-dump` 或 `mysqldump`，以及足够保存 BKN、Safe 完整逻辑备份的空间；
- 目标集群的 `kubectl` 权限；
- 仓库内置的、适用于 Linux 部署环境的 `authz_migrate/authz-migrate` 可执行程序；
- BKN、Vega 和 Safe 数据库访问权限。

迁移运行在部署环境中并复用现有服务的数据库配置。数据步骤读取 `BKN_DB_*`、`VEGA_DB_*`、`SAFE_DB_*`；密码可通过 `*_PASSWORD_FILE` 提供。Safe 的 Go 与 Python 步骤都支持 `SAFE_DB_PASSWORD_FILE`。如脚本旁目录不适合保存备份，设置 `OPENBKN_MIGRATION_BACKUP_DIR`。运行报告默认保存到 `/var/lib/openbkn/migrations`，仅在该目录不适用时设置 `OPENBKN_MIGRATION_WORKDIR`。

## 执行流程

发行内容已携带授权迁移程序，执行迁移不需要 Go。仅在有意修改其 Go 源码时才重新构建：

```bash
./authz_migrate/build.sh
```

执行一条命令：

```bash
./migrate.py upgrade
```

该命令自动验证 bkn-safe 已安装版本为 0.1.4，生成本次运行目录，依次执行 dry-run、停止登记的业务 Deployment、apply。BKN 步骤会在首次写入前创建并校验 BKN、Safe 完整逻辑备份。任一步失败时，不会继续下一步；无论成功或失败，业务服务都会保持停止。该独立脚本不部署目标版本，必须先完成外部 0.1.5 发布，再按命令输出使用同一份 state file 恢复副本。

报告目录包含 `dry-run/01-bkn-data.json`、`dry-run/02-vega-data.json`、`dry-run/03-authorization.json`，以及对应的 apply 报告和副本快照。历史 Core 规则无法从权威生命周期数据证明时保留为 `legacy`；历史 Enterprise 规则会迁移为 inactive，绝不因迁移自动启用。后续如确需启用 EE 规则，应通过升级后的权限管理流程完成审计和授权。

`dry-run`、`stop`、`apply`、`start` 子命令仍保留，仅用于故障诊断和恢复；它们不是正常操作入口。

## 失败恢复

任一步失败都不得启动业务服务。使用 `01-bkn-data.json` 中记录的命令同时恢复 BKN、Safe 备份，部署全部旧版本二进制，验证旧权限行为后才能开放流量。禁止手工伪造成功标记或在部分迁移的数据上继续执行。

## 聚焦测试

```bash
python3 -m unittest -v test_bkn_data.py vega/test_vega_data.py test_migrate.py
./test_service_control.sh
(cd authz_migrate && go test -p=1 ./...)
```
