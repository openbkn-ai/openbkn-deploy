# OpenBKN 0.1.5 Permission-Model Transition

[中文](README.zh.md) | English

This directory is the optional operator-facing entry for the one-time OpenBKN
0.1.4 to 0.1.5 permission-model transition. The normal workflow requires no
manifest, resource IDs, permission IDs, database parameters, or report paths:
it identifies the installed bkn-safe chart, creates backup and report evidence,
and conservatively handles historical authorization whose source is unproven.

Fresh installations receive the current authorization marker from bkn-safe
seed data. Later releases that keep the same authorization storage contract do
not run this workflow again.

## Registered steps

The entry runs a fixed, fail-fast sequence:

1. `bkn-data` validates or applies blank-branch normalization, authoritative
   BKN resource-parent rows, managed proxy accounts, Resource/Tool Box/MCP/Skill
   grant sources, materialized proxy policies, and BKN proxy mappings. Skill
   grants are best effort: a mounted Skill the grantor cannot execute is listed
   under `skipped_skill_grants` in the report instead of failing the network,
   and Skill mounts never change a network's model version.
2. `vega-data` reads authoritative Catalog and Resource metadata, reconciles
   every `resource -> catalog` parent row, and registers each non-built-in
   Catalog's canonical creator bundle and authorization grant.
3. `authorization` invokes this directory's `authz_migrate` executable to
   classify Core provenance and stable grants, reconcile Enterprise rules, and
   persist the checksummed marker.

The BKN step never deletes or rebuilds caller authorization policies. In
particular, it never writes `task_manage`. Historical Core allow/deny rows are
classified by the authorization step instead of being replaced.

The Vega step writes only bkn-safe. Its changes are covered by the Safe backup
created by the preceding BKN step; the Vega database is read-only.

## Requirements

- Python 3.9+ and PyMySQL 1.1.0;
- `mariadb-dump` or `mysqldump` and enough space for complete BKN and Safe
  logical backups;
- `kubectl` access to the target cluster;
- the checked-in `authz_migrate/authz-migrate` executable for the Linux
  deployment environment;
- MariaDB/MySQL access to the BKN, Vega, and Safe databases.

The migration runs in the deployment environment and reuses the services'
existing database configuration. Data steps use `BKN_DB_*`, `VEGA_DB_*`, and
`SAFE_DB_*`; both the Python and Go Safe steps support `SAFE_DB_PASSWORD_FILE`.
Set `OPENBKN_MIGRATION_BACKUP_DIR` when the directory beside this script is not
an appropriate backup volume. Reports default to `/var/lib/openbkn/migrations`;
set `OPENBKN_MIGRATION_WORKDIR` only when that location is unsuitable.

## Workflow

The release includes the authorization executable, so Go is not required to
run this migration. Rebuild it only when intentionally changing its Go source:

```bash
./authz_migrate/build.sh
```

Run the complete transition with one command:

```bash
./migrate.py upgrade
```

The command verifies that bkn-safe is installed at 0.1.4, allocates a run
directory, and executes dry-run, stop, and apply in that order. The BKN step
creates and verifies complete BKN and Safe logical backups before its first
write. A failure never proceeds to the next step; workloads remain stopped
after either success or failure. This standalone script does not deploy the
target release: deploy 0.1.5 externally, then use the printed state-file
command to restore replicas.

The run directory contains the dry-run and apply reports plus the replica
snapshot. Core rules without authoritative lifecycle proof remain `legacy`.
Historical Enterprise rules are migrated as inactive and are never activated
by the transition. Any later EE activation must use the post-upgrade
authorization workflow with its own audit trail.

The `dry-run`, `stop`, `apply`, and `start` subcommands remain only for
diagnosis and recovery; they are not the normal operator interface.

## Failure recovery

Do not start workloads after a failed step. Restore both logical backups named
in `01-bkn-data.json`, deploy all previous binaries, and verify the previous
authorization behavior before reopening traffic. Never create a success marker
manually or continue on a partially migrated database.

## Focused tests

```bash
python3 -m unittest -v test_bkn_data.py vega/test_vega_data.py test_migrate.py
./test_service_control.sh
(cd authz_migrate && go test -p=1 ./...)
```
