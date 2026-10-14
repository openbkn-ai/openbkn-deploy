# 015 → 020 历史日志、Trace 和业务溯源升级工具

完整的中文操作说明请阅读 [015-020-历史日志-Trace-业务溯源升级指南.md](015-020-历史日志-Trace-业务溯源升级指南.md)。

本目录是完整的升级工具包，`run_upgrade.py` 依赖同目录 Python 模块、`contracts/` 与 `span-codec/`；不得只复制入口脚本。

本入口脚本和同目录代码由 OpenBKN 升级包统一提供；不要在业务容器内另配运行环境或手工拼接历史数据。管理节点从 `openbkn-deploy` 仓库根目录运行时，入口为 `scripts/upgrades/0.2.0/historical_trace_data/run_upgrade.py`。

Audit 发布后最多统一回读 5 轮，共享 8 秒回读窗口，轮间退避为 1、2、4 秒（最后一次等待不超过剩余窗口）。每轮复用 `fetch_audits()`，只查询未确认项；数据库调用耗时也计入窗口，但不打断已在执行的查询。到期未确认项记为 `publication_requires_readback`，不会算作迁移成功。

`progress.json` 的 `audit_total`、`audit_already_verified`、`audit_published`、`audit_verified`、`audit_pending`、`audit_retained`、`audit_readback_round` 和 `audit_readback_elapsed_seconds` 表示当前批次。`audit_published` 统计 Kafka 已确认数量，`audit_verified` 包含发布前已确认项；`report.md` 汇总所有批次的 Audit 数量。
