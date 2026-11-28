#!/usr/bin/env bash
# Copyright openbkn.ai
#
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.
# The example configs' REPLACE_ME counts as unset only while no MariaDB data exists.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=../services/mariadb.sh
source "${SCRIPT_DIR}/scripts/lib/common.sh"
source "${SCRIPT_DIR}/scripts/services/mariadb.sh"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
CONFIG_YAML_PATH="${work}/config.yaml"
MARIADB_NAMESPACE=resource
log_warn() { echo "WARN: $*" >>"${work}/warnings"; }
data_exists=false
helm() { [[ "${data_exists}" == true ]]; }
kubectl() { [[ "${data_exists}" == true ]] && [[ "$*" != *"-o name"* ]]; }

write_config() {
    printf 'depServices:\n  rds:\n    password: %s\n    root_password: %s\n' "$1" "$2" >"${CONFIG_YAML_PATH}"
}
fail() { echo "FAIL: $*" >&2; exit 1; }

write_config "'REPLACE_ME'" "'REPLACE_ME'"
[[ -z "$(_mariadb_config_secret password)" ]] || fail "placeholder must read as unset on a fresh install"
[[ -z "$(_mariadb_config_secret root_password)" ]] || fail "placeholder root password must read as unset on a fresh install"
[[ ! -e "${work}/warnings" ]] || fail "fresh install must not warn"

data_exists=true
[[ "$(_mariadb_config_secret root_password)" == REPLACE_ME ]] || fail "existing data keeps the live placeholder password"
grep -q 'still the example placeholder' "${work}/warnings" || fail "keeping a placeholder must warn"

data_exists=false
write_config "'chosen-by-operator'" "''"
[[ "$(_mariadb_config_secret password)" == chosen-by-operator ]] || fail "a real configured password is kept"
[[ -z "$(_mariadb_config_secret root_password)" ]] || fail "an empty password stays unset"

echo "mariadb_password_test: passed"
