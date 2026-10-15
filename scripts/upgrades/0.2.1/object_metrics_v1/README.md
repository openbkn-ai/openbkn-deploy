# OpenBKN 0.2.1 Object Metrics V1 data migration

This script migrates eligible legacy metric definitions during an upgrade from 0.2.0 to 0.2.1. Apply the 0.2.1 database schema migration first and verify that `openbkn.t_object_metric_definition` exists. Install the Python `pymysql` package in the execution environment.

The default mode only scans the database and writes a JSON report. `--apply` inserts eligible definitions; it does not delete or modify legacy rows. Only unqualified atomic count metrics owned by an existing object type are migrated automatically. Derived and composite metrics, business-qualified metrics, non-count aggregations requiring precision decisions, unsupported units, and legacy metrics still referenced by object-type logical properties are marked `manual_review`. The report includes the affected logical-property locations. An automatically migrated `object_set` metric cannot replace an `object_instance` logical-property binding.

```bash
python3 migrate_object_metrics.py --host 127.0.0.1 --database openbkn --report report.json
python3 migrate_object_metrics.py --host 127.0.0.1 --database openbkn --report report.json --apply
```

Connection settings may also be supplied through `BKN_DB_HOST`, `BKN_DB_PORT`, `BKN_DB_USER`, `BKN_DB_PASSWORD`, and `BKN_DB_NAME`. Before applying, back up the database, review the dry-run report, and verify that legacy metric authorization relationships are still available: this script migrates definitions only and does not recreate grants. Existing target versions are reported as `already_exists`; code collisions require manual review. Do not treat `manual_review` entries as migrated.

Run the isolated tests without a database:

```bash
python3 -m unittest -v test_migrate_object_metrics.py
```

See [README.zh.md](README.zh.md) for the Chinese instructions.
