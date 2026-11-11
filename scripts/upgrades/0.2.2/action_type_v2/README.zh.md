# OpenBKN 0.2.2 行动类定义一次性迁移

本目录是从旧行动类切换到 `object`、`relation`、`tool` 操作模型的一次性数据迁移入口。它属于 `openbkn-deploy` 升级工具，不随业务模块启动，也不会由 Helm 自动执行。先升级 `bkn-foundry` 的 `migrations/bkn-backend/mariadb/0.2.2/02-action-operation.sql` 结构，再迁移数据；所有服务应在数据迁移完成后统一切换到新行动模型。

## 预检与执行

运行环境需要 Python 3.9+、PyMySQL、目标数据库访问权限。默认读取 `openbkn` namespace 中 `bkn-backend` Helm release 的 `depServices.rds`。如果地址是 Kubernetes Service DNS，脚本会临时转发到本机 `127.0.0.1`，结束时关闭。也可以通过 `--namespace`、`--release`、`--kube-context` 选择部署，或用 `--host`、`--port`、`--user`、`--password`、`--database` / `BKN_DB_*` 覆盖连接参数；优先级为命令行、环境变量、Helm。密码建议使用 Helm 或环境变量，不放进命令行。

先停行动类定义写入并备份 `t_action_type`、`t_action_schedule`；在切换运行服务前停用旧行动执行入口。以下命令默认**只读数据库**，自动生成迁移计划和不含参数值的报告：

```bash
python3 migrate_action_definitions.py \
  --tool-schemas /secure/action-tool-schemas.json
```

不缺参数类型时，`--tool-schemas` 也可省略。脚本会打印文件的完整路径。默认目录是 `~/.openbkn-ai/migrations/0.2.2/action-type-v2/<目标部署指纹>/`：预检写 `dry-run-plan.json`、`dry-run-report.json`；执行写 `rollback.json`（分批执行时也会生成 `rollback-<摘要>.json`）、`apply-report.json`；回滚写 `rollback-report.json`。不同集群、数据库和 `--kn-id` 范围使用不同目录。可用 `OPENBKN_MIGRATION_WORKDIR` 改变根目录；仅特殊场景才需要 `--output`、`--report` 覆盖默认文件名。快照和报告文件权限为 `0600`，目录权限为 `0700`。

`--tool-schemas` 是可选的权威工具输入定义快照。旧参数缺类型或 MCP 没有显式参数时，必须提供对应快照，不会猜测类型。格式为 JSON 数组：

```json
[
  {
    "kn_id": "network-id",
    "branch": "main",
    "action_type_id": "action-id",
    "input_schema": {
      "type": "object",
      "properties": { "message": { "type": "string" } },
      "required": ["message"]
    }
  }
]
```

报告的 `errors` 和 `skipped_actions` 列出不能转换的行动名称、知识网络、分支、工具来源标识及失败原因，不包含参数值。旧的“新建对象”行动若含无效适用条件，脚本会清除数据库条件和原始 BKN 的条件章节；报告的 `removed_create_conditions` 列出非空条件的行动名称供评审。定时任务缺少明确执行对象、工具参数定义不完整或不一致时，预检拒绝该记录；默认**只要有错误，`--apply` 不会写入任何行动**。

评审预检报告、确认数据库备份及旧写入已停止后执行。脚本会自动将可回滚的原值写入同一目标目录中的 `rollback.json`：

```bash
python3 migrate_action_definitions.py \
  --tool-schemas /secure/action-tool-schemas.json \
  --apply
```

写入在一个事务中完成，并逐行核对原值和更新时间，避免覆盖并发修改。已迁移的记录重复运行不会再次转换。

若要先迁移可转换的行动，再逐项修复失败项，显式增加 `--allow-partial`：

```bash
python3 migrate_action_definitions.py \
  --tool-schemas /secure/action-tool-schemas.json \
  --apply --allow-partial
```

此时可转换行动在一个事务内写入，`apply-report.json` 的 `applied_count` 是本次写入数，`skipped_actions` 明确列出失败项。存在失败项时进程仍以退出码 `2` 结束；即使已写入部分数据，也不能把这次执行视为切换完成。修复工具定义后重复执行，脚本只处理尚未迁移的行动。**旧执行服务须保持停止，直到全库预检无错误、全部行动迁移完成，再统一切换新服务；不支持新旧行动模型并行运行。**

需要恢复时，先停新写入；默认读取同一目标目录下所有已生成的回滚快照，跳过未实际写入或已经恢复的记录：

```bash
python3 migrate_action_definitions.py \
  --rollback --apply
```

如需只使用指定快照，可运行 `--rollback /path/to/snapshot.json --apply`。预检计划会随重复预检更新；正式执行的回滚快照不会被后续预检覆盖。回滚快照含旧参数值，应按敏感备份保管。

`--kn-id` 可限制单个知识网络预检；正式切换前仍需对全库预检。`--input` 可离线转换 JSON 导出，`--mounts` 可离线规划历史能力挂载的候选行动；两者都不会写数据库。

## 测试

```bash
python3 -m unittest -v test_migrate_action_definitions.py
```

可选的 MariaDB 集成测试会创建并删除随机命名的测试数据库。仅在专用测试数据库具备建库权限时运行，设置 `BKN_MIGRATION_TEST_DB_*` 和 `BKN_FOUNDRY_ROOT`（指向包含 0.2.2 schema 的 bkn-foundry 检出）：

```bash
python3 -m unittest -v integration_action_definitions_mariadb.py
```
