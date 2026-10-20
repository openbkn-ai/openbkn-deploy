# OpenBKN 0.2.1 Object Metrics V1 historical-data migration

This tool migrates legacy object-type atomic metrics during an upgrade from 0.2.0 to 0.2.1. Apply the 0.2.1 schema migration first and verify that `openbkn.t_object_metric_definition` exists. Install Python `pymysql` in the execution environment.

The default mode only reads the database and writes a JSON report. It supports these mappings:

- A legacy metric without a business condition becomes one atomic metric. A metric with a constant `condition` becomes a base atomic metric plus a derived metric. The legacy ID remains on the final derived metric; the helper base ID is deterministic. The condition is applied to source rows before aggregation, preserving filter-then-aggregate order.
- Legacy `having`, `group_by`, and `order_by` are treated as query/trial-time settings and omitted from metric definitions. The report records each omitted capability.
- Metric bindings referenced from object-type logic properties are not migrated. The metric definition itself is still migrated as unbound, and the report lists the old binding locations.
- A legacy `last_24h` default is translated into a rolling 24-hour derived-metric window with its event-time property. Other time policies without an equivalent mapping require manual review.
- Decimal results use the Studio Object Metrics form defaults: precision 18, scale 2, rounding `half_up`. The report records this default; values with more than two fractional digits may be rounded.
- Count results with a missing unit or legacy `none` unit default to UCUM `1` with quantity kind `count`. Existing labels such as `piece` and `transaction` remain registered count units. Units incompatible with the aggregation result require manual review.

Each legacy metric gets a report entry with its migration status, target IDs/types, omitted capabilities, defaulted unit/precision, and failure reason. Metrics with a condition generate two target definitions, so the number of migrated legacy metrics differs from the number of inserted definitions. The script does not recreate logic-property bindings or authorization grants.

By default, the script reads `depServices.rds` from the installed `bkn-backend` Helm release in namespace `openbkn`. It then uses the release's host, port, user, password, and database. The execution host must have Helm access to the target cluster; if the DB host is a Kubernetes service DNS name, it also needs `kubectl` port-forward permission. The temporary port-forward listens only on `127.0.0.1` and is closed when the script exits.

```bash
python3 migrate_object_metrics.py --report report.json
python3 migrate_object_metrics.py --report report.json --apply
```

Connection settings can be overridden per field. Precedence is command-line option, then `BKN_DB_*` environment variable, then the corresponding `depServices.rds` value from Helm. A complete explicit connection skips Helm discovery, which is useful for an external database. If settings are incomplete and Helm discovery fails, the script exits with an error instead of falling back to a local MySQL instance. Use `--namespace`, `--release`, and `--kube-context` (or `--context`) for non-default installs; namespace and context also support `BKN_NAMESPACE` and `KUBECTL_CONTEXT`:

```bash
export BKN_DB_HOST=db.customer.example.com BKN_DB_PORT=3306 BKN_DB_USER=openbkn BKN_DB_NAME=openbkn
read -s BKN_DB_PASSWORD && export BKN_DB_PASSWORD
python3 migrate_object_metrics.py --report report.json
```

To target a non-default deployment, for example: `python3 migrate_object_metrics.py --namespace customer --release bkn-backend --context customer-cluster --report report.json`.

Back up the database and review the dry-run report before using `--apply`. If the entire target metric chain already exists, the report says `already_exists`. If only part of a chain exists, that legacy metric is held for manual review to avoid duplicates or dangling dependencies. Stable ID/code conflicts also require review. Inserts run in one database transaction and roll back on failure; legacy rows are never deleted or modified.

Run the isolated tests without a database:

```bash
python3 -m unittest -v test_migrate_object_metrics.py
```

See [README.zh.md](README.zh.md) for the Chinese instructions.
