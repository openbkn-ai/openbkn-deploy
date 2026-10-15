# OpenBKN 0.2.1 对象指标 V1 历史数据迁移

本工具用于从 0.2.0 升级到 0.2.1。必须先完成 0.2.1 schema 迁移并确认 `openbkn.t_object_metric_definition` 已创建，再执行本脚本。运行环境需要安装 Python `pymysql` 包。

脚本默认只扫描并生成 JSON 报告，不写数据库。它只自动迁移能够无损映射的“对象类范围、无业务条件、无固定分组”的历史计数型原子指标，并确认归属对象类仍存在。历史衍生/复合指标、混入业务限定的原子指标、带有旧版默认时间范围策略的指标，以及 `sum/avg/min/max` 等需要明确精度与舍入方式的指标，均标记为 `manual_review`，避免用技术字段猜测业务语义。仍被对象类逻辑属性引用的旧指标也列为 `manual_review`，报告会注明绑定位置；自动迁移的 `object_set` 指标不能充当 `object_instance` 逻辑属性。计数指标仅自动接受无单位或 `countUnit` + `1`（UCUM）的单位组合。

```bash
python3 migrate_object_metrics.py --host 127.0.0.1 --database openbkn --report report.json
python3 migrate_object_metrics.py --host 127.0.0.1 --database openbkn --report report.json --apply
```

连接参数也可通过 `BKN_DB_HOST`、`BKN_DB_PORT`、`BKN_DB_USER`、`BKN_DB_PASSWORD`、`BKN_DB_NAME` 提供。执行 `--apply` 前必须备份数据库、评审 dry-run 报告，并确认旧指标授权父子关系仍在（新旧指标沿用相同资源类型和 ID）；脚本只迁移定义数据，不重建授权关系。脚本会在写入前显式检查目标版本和跨版本稳定编码：已存在的版本记为 `already_exists`，编码冲突记为 `manual_review`，不会静默忽略冲突；因此可重复运行，且不会删除或修改原表。`manual_review` 条目不可视为已经迁移。

无需连接数据库即可运行单元测试：

```bash
python3 -m unittest -v test_migrate_object_metrics.py
```
