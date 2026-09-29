# BKN Proxy Outbox Stopped Upgrade

[中文](README.zh.md) | English

This directory contains the operator-side checks and data initialization for
the OpenBKN 0.2.0 BKN proxy outbox transition. Schema DDL remains owned by
`bkn-foundry/migrations/bkn-backend/mariadb/0.2.0/` and is executed by the
normal data migrator; this tool does not duplicate that DDL.

Use a stopped data transition after the normal deployment has taken the database
backup and run the Schema migration. Repair all non-ready proxy mappings first.
Database credentials must be supplied through the deployment
environment as `BKN_DB_HOST`, `BKN_DB_PORT`, `BKN_DB_USER`, `BKN_DB_PASSWORD`,
and `BKN_DB_NAME`; reports never contain the password or authorization payloads.

```bash
./migrate.py apply
```

`apply` is the normal zero-argument entry point. It runs the database preflight,
records and stops the `bkn-backend` workload, verifies that desired, observed,
and ready replicas are all zero, initializes `published_generation` from
`sync_generation`, copies only ready networks' published snapshots into the
planned table, verifies that the Outbox is empty and both snapshots are
identical, writes the report, and restores the recorded replica count. It is
idempotent and
automatically writes the initialization and verification result to a UTC-stamped
`~/.openbkn-ai/migrations/0.2.0/kn_proxy_outbox/proxy-outbox-apply-*.json`
file without overwriting an existing report.
The report destination is checked before the workflow starts. If any step fails
after BKN is stopped, BKN remains stopped for investigation; rerunning `apply`
resumes from that recorded stopped state. A stop failure prints the underlying
Kubernetes error, exits before database changes, and preserves the original
replica record so `apply` can retry the stop safely. If an exceptional
filesystem failure occurs only after the transaction commits, the command
explicitly reports that the migration completed and still restores BKN. After
deployment, perform one network mutation smoke test before reopening normal
traffic.

`dry-run`, `stop`, `verify-stopped`, `verify`, and `start` remain available only
for diagnosis and recovery. Database backup and Schema execution stay in the
normal deployment/data-migrator workflow and are not repeated by this data
transition.

Focused tests:

```bash
python3 -m unittest -v test_kn_proxy_outbox.py test_migrate.py
```
