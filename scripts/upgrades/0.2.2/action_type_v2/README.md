# OpenBKN 0.2.2 one-time action definition migration

This upgrade tool lives in `openbkn-deploy`, outside runtime modules. Apply the `bkn-foundry` 0.2.2 `02-action-operation.sql` schema change first. Stop action writes and back up `t_action_type` and `t_action_schedule` before applying data changes. Switch all action services to the new model only after the full migration succeeds.

Only existing action definitions are migrated. Manually mounted Function, API, and MCP capabilities remain independent in the dynamic model; this tool does not convert them into action types or remove their bindings.

The default database mode reads `depServices.rds` from the installed `bkn-backend` Helm release in namespace `openbkn`. It temporarily port-forwards in-cluster Service DNS addresses to `127.0.0.1`. Select another deployment with `--namespace`, `--release`, and `--kube-context`; explicit connection arguments or `BKN_DB_*` take precedence over Helm. Use an environment variable or Helm for the password.

```bash
python3 migrate_action_definitions.py \
  --tool-schemas /secure/action-tool-schemas.json
```

The default run does not write the database. The script prints its artifact paths. The default directory is `~/.openbkn-ai/migrations/0.2.2/action-type-v2/<target fingerprint>/`: dry runs write `dry-run-plan.json` and `dry-run-report.json`; applies write `rollback.json` (or `rollback-<digest>.json` for later batches) and `apply-report.json`; rollbacks write `rollback-report.json`. Distinct deployment targets and `--kn-id` scopes use distinct directories. Override the base with `OPENBKN_MIGRATION_WORKDIR`; `--output` and `--report` remain optional advanced overrides. Snapshot/report files use mode `0600` and the target directory uses `0700`.

The dry-run report contains IDs, names of legacy create conditions that will be removed, and `skipped_actions` entries with action name, knowledge network, branch, tool source identifiers, and error reason, without parameter values. The optional `--tool-schemas` file is a JSON array of `{ "kn_id", "branch", "action_type_id", "input_schema" }` entries. The input schema must be an authoritative resolved JSON object schema. Missing types and ambiguous scheduled targets stop a full apply; types are never guessed.

After reviewing an error-free report, taking a database backup, and stopping old action writes, use `--apply`. The rollback snapshot is created automatically in the same target directory:

```bash
python3 migrate_action_definitions.py \
  --tool-schemas /secure/action-tool-schemas.json \
  --apply
```

For an explicit partial apply, add `--allow-partial --apply`. Convertible actions are written in one transaction; `apply-report.json` records `applied_count` and `skipped_actions`. The command still exits `2` while any action is skipped. Repair its tool definition and rerun to process the remaining actions. Keep old execution services stopped until an unscoped dry run has no errors and all actions are migrated; old and new action models do not run together. All updates use optimistic checks. To restore, stop new writes and run `python3 migrate_action_definitions.py --rollback --apply`; this reads all default batch snapshots and skips records never applied or already restored. A specific snapshot can be selected with `--rollback /path/to/snapshot.json --apply`. Dry-run plans may be refreshed without replacing immutable rollback snapshots. Keep them as sensitive backup data. `--kn-id` scopes a dry run, while `--input` converts an action export offline without database writes. Historical capability mounts are outside this migration. Do not use a scoped result as the full cutover gate.

Focused tests: `python3 -m unittest -v test_migrate_action_definitions.py`. Optional MariaDB integration tests require `BKN_MIGRATION_TEST_DB_*`, `BKN_FOUNDRY_ROOT`, and CREATE DATABASE permission on a dedicated test server.

See [README.zh.md](README.zh.md) for the Chinese instructions.
