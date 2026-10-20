# OpenBKN 0.2.1 对象指标 V1 历史数据迁移

本工具用于从 0.2.0 升级到 0.2.1。必须先完成 0.2.1 schema 迁移并确认 `openbkn.t_object_metric_definition` 已创建。运行环境需要安装 Python `pymysql` 包。

脚本默认只扫描并生成 JSON 报告，不写数据库。支持将旧版对象类范围的原子指标迁移为 Object Metric V1：

- 没有业务条件的指标生成一个原子指标；带常量 `condition` 的指标生成“基础原子指标 + 衍生指标”。旧指标 ID 保留给最终衍生指标，基础指标使用确定性生成的迁移 ID。条件会在聚合前应用，以保持过滤后再聚合的计算顺序。
- `having`、`group_by`、`order_by` 按查询/试算级条件处理，不写入指标定义；报告会逐项记录被忽略的能力。
- 对象类逻辑属性中的指标绑定关系不迁移；指标本身仍迁移为未绑定指标，报告会注明旧绑定位置。
- `last_24h` 默认时间范围会转换为衍生指标的 24 小时滚动窗口，并保留事件时间属性。其它未实现等价转换的时间策略会列为 `manual_review`。
- 小数结果使用 Studio 对象指标表单的默认值：precision=18、scale=2、rounding=half_up。报告会标明使用了默认精度，可能改变超过 2 位小数的结果。
- `count`、`count_distinct` 等计数结果若单位为空或旧单位为 `none`，默认补为 UCUM `1`、quantity kind `count`。已配置的 `piece`、`transaction` 等计数单位保留为注册单位；与聚合结果不兼容的单位列为 `manual_review`。

报告按每条旧指标提供迁移状态、生成的目标指标 ID/类型、丢失的能力、单位/精度默认处理及失败原因。带条件的旧指标会生成两条目标定义，因此“成功迁移的旧指标数”和“写入的目标定义数”不同。脚本不会重建旧逻辑属性绑定或授权关系。

默认情况下，脚本会从 `openbkn` namespace 的 `bkn-backend` Helm release 读取 `depServices.rds`，自动取得数据库 host、port、user、password 和 database。运行机器需要有目标集群的 Helm 访问权限；如果数据库 host 是 Kubernetes Service DNS，还需要 `kubectl` port-forward 权限。脚本会仅在 `127.0.0.1` 上建立临时端口转发，并在退出时关闭。

```bash
python3 migrate_object_metrics.py --report report.json
python3 migrate_object_metrics.py --report report.json --apply
```

连接参数支持逐项覆盖，优先级为：命令行参数 > `BKN_DB_*` 环境变量 > Helm 中的 `depServices.rds`。显式提供完整连接参数时无需访问 Helm，可用于外部数据库。参数不完整且 Helm 配置读取失败时，脚本会明确报错，不会回退连接本机 MySQL。非默认部署可通过 `--namespace`、`--release`、`--kube-context`（别名 `--context`）指定；namespace 和 context 也支持 `BKN_NAMESPACE`、`KUBECTL_CONTEXT` 环境变量。示例：

```bash
export BKN_DB_HOST=db.customer.example.com BKN_DB_PORT=3306 BKN_DB_USER=openbkn BKN_DB_NAME=openbkn
read -s BKN_DB_PASSWORD && export BKN_DB_PASSWORD
python3 migrate_object_metrics.py --report report.json
```

指定非默认部署的示例：`python3 migrate_object_metrics.py --namespace customer --release bkn-backend --context customer-cluster --report report.json`。

执行 `--apply` 前必须备份数据库并评审 dry-run 报告。目标指标链全部已存在时报告 `already_exists`；若仅部分目标定义存在，脚本会停止迁移该旧指标并要求人工检查，避免重复或断链。稳定 ID/code 冲突会列为 `manual_review`。写入使用单个数据库事务，失败会回滚本次批次；原旧指标表不会被删除或修改。

无需连接数据库即可运行单元测试：

```bash
python3 -m unittest -v test_migrate_object_metrics.py
```
