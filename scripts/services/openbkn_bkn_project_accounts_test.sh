#!/usr/bin/env bash
# Copyright openbkn.ai
#
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.
#
# bkn-project gets its own service and migration accounts on the installer's
# MariaDB. Cluster and database are mocked; the SQL itself was checked against
# MariaDB 11.4 with the real bkn-project migration (see the PR).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/scripts/lib/common.sh"
source "${ROOT}/scripts/services/openbkn.sh"
SCRIPT_DIR="${ROOT}"
work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }
log_info() { echo "INFO: $*" >>"${work}/log"; }
log_error() { echo "ERROR: $*" >>"${work}/log"; }
log_warn() { echo "WARN: $*" >>"${work}/log"; }

CONFIG_YAML_PATH="${work}/config.yaml"
printf 'depServices:\n  rds:\n    source_type: internal\n    user: openbkn\n' >"${CONFIG_YAML_PATH}"
_OPENBKN_RUNTIME_DIR="${work}"
CORE_SET_VALUES=()
tables_exist=true
: >"${work}/sql"
kubectl() {
    case "$*" in
        (*"get pods -l app=mariadb"*) echo mariadb-0 ;;
        (*"exec -i mariadb-0"*)
            local sql
            sql="$(cat)"
            printf '%s\n' "${sql}" >>"${work}/sql"
            case "${sql}" in
                (*"table_name IN ("*) printf 't_agent\nt_agent_prompt\n' ;;
                (*"LIKE 't"*) [[ "${tables_exist}" != true ]] || printf 't_bkn_project_project\nt_bkn_project_kb_build_file\n' ;;
            esac ;;
        ("get secret "*) [[ -f "${work}/secret-$3" ]] && cat "${work}/secret-$3" ;;
        ("create -f -")
            python3 -c '
import base64, json, sys
s = json.load(sys.stdin)
open(sys.argv[1] + "/secret-" + s["metadata"]["name"], "w").write(base64.b64encode(s["stringData"]["password"].encode()).decode())
assert s["stringData"]["user"] in ("bkn_project", "bkn_project_migrate")
' "${work}" ;;
        (*) echo "unexpected kubectl $*" >&2; return 99 ;;
    esac
}

# Managed on the installer's MariaDB; the chart gets the two installer Secrets.
_openbkn_bkn_project_accounts_managed || fail "internal MariaDB must be managed"
_openbkn_release_extra_sets bkn-project openbkn
[[ " ${CORE_RELEASE_EXTRA_SET_STRINGS[*]} " == *" depServices.rds.existingSecret=bkn-project-db-service "* ]] || fail "service Secret not passed"
[[ " ${CORE_RELEASE_EXTRA_SET_STRINGS[*]} " == *" migration.existingSecret=bkn-project-db-migration "* ]] || fail "migration Secret not passed"

# Before Helm: accounts, migration grants, install-window grant.
_openbkn_prepare_bkn_project_accounts openbkn
grep -q "CREATE USER IF NOT EXISTS 'bkn_project_migrate'@'%'" "${work}/sql" || fail "migration account"
grep -q "CREATE USER IF NOT EXISTS 'bkn_project'@'%'" "${work}/sql" || fail "service account"
grep -q 'GRANT CREATE, ALTER, SELECT, INSERT ON `openbkn`.\* TO .bkn_project_migrate.' "${work}/sql" || fail "migration grants"
grep -q 'GRANT UPDATE ON `openbkn`.`t_agent_prompt` TO .bkn_project_migrate.' "${work}/sql" || fail "Agent update grant"
grep -q 'GRANT SELECT, INSERT, UPDATE, DELETE ON `openbkn`.\* TO .bkn_project.' "${work}/sql" || fail "install-window grant"
if grep -q 'ALL PRIVILEGES\|WITH GRANT OPTION\|\*\.\*' "${work}/sql"; then fail "accounts must not be root-like"; fi
service_pw="$(base64 -d <"${work}/secret-bkn-project-db-service" 2>/dev/null || base64 -D <"${work}/secret-bkn-project-db-service")"
[[ ${#service_pw} -ge 20 ]] || fail "service password must be generated"
if grep -qF "${service_pw}" "${work}/log"; then fail "passwords must not be logged"; fi

# A re-run keeps the stored passwords instead of generating new ones.
: >"${work}/sql"
_openbkn_prepare_bkn_project_accounts openbkn
grep -qF "IDENTIFIED BY '${service_pw}'" "${work}/sql" || fail "existing Secret password must be reused"

# After Helm: per-table grants, ledger read, then the database-wide grant goes.
: >"${work}/sql"
_openbkn_narrow_bkn_project_service_account
grep -q 'GRANT SELECT, INSERT, UPDATE, DELETE ON `openbkn`.`t_bkn_project_kb_build_file` TO .bkn_project.' "${work}/sql" || fail "kb_build_file grant"
grep -q 'GRANT SELECT ON `openbkn`.`ee_bkn_project_schema_migration`' "${work}/sql" || fail "ledger grant"
grep -q 'REVOKE SELECT, INSERT, UPDATE, DELETE ON `openbkn`.\* FROM .bkn_project.' "${work}/sql" || fail "install-window revoke"

# No tables (the migration never ran): keep access, report the failure.
tables_exist=false
: >"${work}/sql"
if _openbkn_narrow_bkn_project_service_account; then fail "narrowing without tables must fail"; fi
if grep -q REVOKE "${work}/sql"; then fail "must not revoke before per-table grants exist"; fi

# Operator choices and external databases are left alone.
printf '{"migration": {"existingSecret": "my-migration"}}' >"${work}/bkn-project.values.json"
if _openbkn_bkn_project_accounts_managed; then fail "an operator migration Secret must win"; fi
rm "${work}/bkn-project.values.json"
CORE_SET_VALUES=("depServices.rds.user=custom")
if _openbkn_bkn_project_accounts_managed; then fail "an operator database user must win"; fi
CORE_SET_VALUES=()
printf 'depServices:\n  rds:\n    source_type: external\n' >"${CONFIG_YAML_PATH}"
if _openbkn_bkn_project_accounts_managed; then fail "an external database must be left alone"; fi
_openbkn_release_extra_sets bkn-project openbkn
[[ ${#CORE_RELEASE_EXTRA_SET_STRINGS[@]} -eq 0 ]] || fail "no Secrets for an external database"

echo "openbkn_bkn_project_accounts_test: passed"
