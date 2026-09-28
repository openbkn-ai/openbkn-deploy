# BKN Proxy Outbox Stopped Upgrade

[中文](README.zh.md) | English

This directory contains the operator-side checks and data initialization for
the OpenBKN 0.2.0 BKN proxy outbox transition. Schema DDL remains owned by
`bkn-foundry/migrations/bkn-backend/mariadb/0.2.0/` and is executed by the
normal data migrator; this tool does not duplicate that DDL.

Use a stopped upgrade. Repair all non-ready proxy mappings first, stop BKN
workloads, drain requests, take the normal database backup, and run the Schema
migration. Database credentials must be supplied through the deployment
environment as `BKN_DB_HOST`, `BKN_DB_PORT`, `BKN_DB_USER`, `BKN_DB_PASSWORD`,
and `BKN_DB_NAME`; reports never contain the password or authorization payloads.

```bash
./migrate.py dry-run --report ./reports/proxy-outbox-precheck.json
./migrate.py stop --namespace openbkn --expected-context YOUR_CONTEXT
# Run the normal bkn-foundry 0.2.0 data migrator and database backup procedure.
./migrate.py apply --confirm-bkn-stopped --report ./reports/proxy-outbox-apply.json
./migrate.py verify --report ./reports/proxy-outbox-verify.json
./migrate.py start --namespace openbkn --expected-context YOUR_CONTEXT
```

`apply` initializes `published_generation` from `sync_generation`, copies only
ready networks' published snapshots into the planned table, and verifies that
the Outbox is empty and both snapshots are identical. It is idempotent, but a
report path is never overwritten. Do not start the new BKN version if any check
fails. After deployment, enable the fixed worker pool and perform one network
mutation smoke test before reopening normal traffic.

`stop` stores the existing `bkn-backend` replica count in a context- and
namespace-bound state file. `start` restores that exact count and removes the
state file only after all replicas are ready. Database backup and Schema
execution stay in the normal deployment/data-migrator workflow.

Focused tests:

```bash
python3 -m unittest -v test_kn_proxy_outbox.py test_migrate.py
```
