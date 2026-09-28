# BKN Proxy Outbox 停机升级

中文 | [English](README.md)

本目录提供 OpenBKN 0.2.0 BKN Proxy Outbox 的部署侧检查和数据初始化工具。
Schema DDL 仍由 `bkn-foundry/migrations/bkn-backend/mariadb/0.2.0/` 维护，并由
现有 data migrator 执行；本工具不重复维护 DDL。

本次升级采用停机方式：先修复所有非 `ready` 的 proxy mapping，停止 BKN
工作负载并排空请求，完成常规数据库备份，再执行 Schema migration。数据库
连接通过部署环境中的 `BKN_DB_HOST`、`BKN_DB_PORT`、`BKN_DB_USER`、
`BKN_DB_PASSWORD` 和 `BKN_DB_NAME` 提供；报告不会记录密码或完整授权 payload。

```bash
./migrate.py dry-run --report ./reports/proxy-outbox-precheck.json
./migrate.py stop --namespace openbkn --expected-context YOUR_CONTEXT
# 执行常规数据库备份和 bkn-foundry 0.2.0 data migrator。
./migrate.py apply --confirm-bkn-stopped --report ./reports/proxy-outbox-apply.json
./migrate.py verify --report ./reports/proxy-outbox-verify.json
./migrate.py start --namespace openbkn --expected-context YOUR_CONTEXT
```

`apply` 将 `published_generation` 初始化为 `sync_generation`，只为 `ready`
网络复制 published snapshot 到 planned 表，并校验 Outbox 为空、两个快照一致。
执行是幂等的，但不会覆盖已有报告。任何校验失败时都不要启动新版 BKN。部署后
启用固定 Worker 池，并先完成一次知识网络变更冒烟验证，再恢复正常流量。

`stop` 会把 `bkn-backend` 当前副本数写入同时绑定 kubectl context 和 namespace
的状态文件。`start` 只在原副本数全部 ready 后删除状态文件。数据库备份和
Schema 执行继续复用部署与 data migrator 的标准流程。

定向测试：

```bash
python3 -m unittest -v test_kn_proxy_outbox.py test_migrate.py
```
