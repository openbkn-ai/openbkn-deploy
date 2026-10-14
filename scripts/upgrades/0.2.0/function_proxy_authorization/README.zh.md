# OpenBKN 0.2.0 Function 代理授权修复

该迁移修复历史知识网络代理快照中把 Function 工具箱错误保存为
`tool_box/<box-id>:execute` 的数据。0.2.0 执行端按实际工具箱元数据使用
`function/<box-id>:execute`，旧快照会导致 Function Action 或逻辑属性执行返回 403。

脚本以平台为扫描范围、以单个知识网络为事务和恢复单元。它不会把所有
`tool_box` 盲目替换为 `function`，只转换 `t_toolbox.f_metadata_type=function`
的真实 Function 工具箱。

## 安全约束

- 默认只执行 `dry-run`；
- apply 要求业务 Deployment 已停止，并显式传入 `--confirm-services-stopped`；
- apply 前要求 `mariadb-dump` 或 `mysqldump` 可用；已有外部快照/PITR 时可显式使用
  `--allow-no-backup`；
- planned 与 published 快照、mapping version/generation、bkn-safe snapshot 必须一致；
- 存在未完成 outbox、活动发布锁或授权人权限不足时拒绝迁移；
- 每个知识网络独立提交，失败后可重复执行；已修复网络会成为 no-op；
- 保留 bkn-safe 为 `requires` 前置操作生成的 `requirement_derived` 来源，不会把它们
  当成 BKN 显式快照误删；
- Safe 侧先提交、BKN 侧提交失败时，下一次执行会识别该中间态并幂等完成 BKN 侧。

迁移会同步更新：

- `t_kn_proxy_planned_grant_source`；
- `t_kn_proxy_published_grant_source`；
- `t_kn_proxy_account` 的 model version 和 generation；
- bkn-safe 的 `proxy_grant_source`、`proxy_grant_policy`、`authorization_grant`、
  Casbin policy；
- bkn-safe managed proxy 的 snapshot version 和 generation。

## 数据库配置

沿用 0.1.5 迁移的环境变量：

- `BKN_DB_HOST/PORT/USER/PASSWORD[_FILE]/NAME`；
- `SAFE_DB_HOST/PORT/USER/PASSWORD[_FILE]/NAME`。

Execution Factory 默认复用 BKN 数据库连接；独立部署时使用：

- `OPERATOR_DB_HOST/PORT/USER/PASSWORD[_FILE]/NAME`。

脚本不会在报告中输出数据库密码。

## 执行

先对故障网络执行 dry-run：

```bash
python3 migrate.py --kn-id ecommerce_ops_bkn_zh_attrs_import_sdk \
  --mode dry-run --report /tmp/function-proxy-dry-run.json
```

停止业务服务并确认数据库备份后，修复单个网络：

```bash
python3 migrate.py --kn-id ecommerce_ops_bkn_zh_attrs_import_sdk \
  --mode apply --confirm-services-stopped \
  --report /tmp/function-proxy-apply.json
```

单网络验证通过后扫描或修复整个平台：

```bash
python3 migrate.py --all --mode dry-run
python3 migrate.py --all --mode apply --confirm-services-stopped --batch-size 20
```

脚本优先使用 mapping 的 `last_grantor_id` 并重新校验其完整权限。需要使用明确的
平台迁移授权人时，可以传入 `--grantor-id <account-id>`；该账号仍必须拥有所有待同步
资源的实际权限。

## 验证

迁移报告应没有 `failures`，受影响网络的 `retyped_sources` 大于 0。恢复服务后还应验证：

1. planned/published 中对应来源为 `function`；
2. 代理账号的 Function `execute` 检查为 allow；
3. 原 Action 执行成功；
4. 再次运行 dry-run 时 `affected_networks` 为空。

聚焦测试：

```bash
python3 -m unittest -v test_migrate.py
```
