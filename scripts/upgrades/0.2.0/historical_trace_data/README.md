# 015 → 020 历史日志、Trace 和业务溯源升级工具

完整的中文操作说明请阅读 [015-020-历史日志-Trace-业务溯源升级指南.md](015-020-历史日志-Trace-业务溯源升级指南.md)。

本目录是完整的升级工具包，`run_upgrade.py` 依赖同目录 Python 模块、`contracts/` 与 `span-codec/`；不得只复制入口脚本。

本入口脚本和同目录代码由 OpenBKN 升级包统一提供；不要在业务容器内另配运行环境或手工拼接历史数据。管理节点从 `openbkn-deploy` 仓库根目录运行时，入口为 `scripts/upgrades/0.2.0/historical_trace_data/run_upgrade.py`。
