# BKN Proxy Outbox 停机升级

中文 | [English](README.md)

本目录提供 OpenBKN 0.2.0 BKN Proxy Outbox 的部署侧检查和数据初始化工具。
Schema DDL 仍由 `bkn-foundry/migrations/bkn-backend/mariadb/0.2.0/` 维护，并由
现有 data migrator 执行；本工具不重复维护 DDL。

本次数据转换在标准部署完成数据库备份和 Schema migration 后执行，采用停机
方式，并要求先修复所有非 `ready` 的 proxy mapping。数据库
连接通过部署环境中的 `BKN_DB_HOST`、`BKN_DB_PORT`、`BKN_DB_USER`、
`BKN_DB_PASSWORD` 和 `BKN_DB_NAME` 提供；报告不会记录密码或完整授权 payload。

```bash
./migrate.py apply
```

`apply` 是正常操作的无参数统一入口。它会依次执行数据库预检查、记录并停止
`bkn-backend`、确认期望/实际/就绪副本数全部为 0、将
`published_generation` 初始化为 `sync_generation`、只为 `ready` 网络复制
published snapshot 到 planned 表、校验 Outbox 为空且两个快照一致、写入报告，
最后恢复升级前记录的副本数。执行是幂等的，并自动把初始化及校验结果写入用户状态目录下带 UTC 时间戳的
`~/.openbkn-ai/migrations/0.2.0/kn_proxy_outbox/proxy-outbox-apply-*.json`；
已有报告不会被覆盖。流程开始前会检查报告目录是否可写；BKN 停止后的任一步骤
失败都会保持停服，排查后重新运行 `apply` 会从已记录的停服状态继续。如果事务
提交后才发生极端的文件系统错误，命令会明确提示迁移已经完成，并仍然恢复 BKN。
部署后先完成一次知识网络变更冒烟验证，再恢复正常流量。

`dry-run`、`stop`、`verify-stopped`、`verify` 和 `start` 仅保留用于诊断与恢复。
数据库备份和 Schema 执行继续复用标准部署与 data migrator 流程，本数据转换
不会重复执行。

定向测试：

```bash
python3 -m unittest -v test_kn_proxy_outbox.py test_migrate.py
```
