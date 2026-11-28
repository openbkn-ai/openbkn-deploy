#!/usr/bin/env bash
# Copyright openbkn.ai
#
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.
#
# onboard's test user: the SDK gets an initial password, the password is
# generated and stored instead of a fixed default, and neither the log nor the
# completion report carries it. openbkn, curl, helm and kubectl are mocked.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${SCRIPT_DIR}/scripts/lib/common.sh"
source "${SCRIPT_DIR}/scripts/lib/onboard_test_user.sh"
source "${SCRIPT_DIR}/scripts/lib/onboard_report.sh"
work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }
log_info() { echo "INFO: $*" >>"${work}/log"; }
log_warn() { echo "WARN: $*" >>"${work}/log"; }
onboard_log_warn() { log_warn "$@"; }
onboard_default_access_base_url() { echo http://platform.test; }
ONBOARD_TEST_USER_PASSWORD_FILE="${work}/home/test-user-password"
CONFIG_YAML_PATH="${work}/config.yaml"
printf 'bknSafe:\n  initialPassword: admin-initial-secret\n' >"${CONFIG_YAML_PATH}"
unset ONBOARD_TEST_USER_PASSWORD ONBOARD_DEFAULT_TEST_USER_PASSWORD
ONBOARD_ASSUME_YES=true
user_exists=false

openbkn() {
    echo "openbkn $*" >>"${work}/argv"
    case "$*" in
        ("admin --json user list --limit 1") echo '{"users":[]}' ;;
        ("admin --json user list --keyword test --limit 200")
            [[ "${user_exists}" == true ]] && echo '{"users":[{"account":"test","id":"u-1"}]}' || echo '{"users":[]}' ;;
        ("admin --json user create --login test --display-name test")
            # SDK 0.2.x: no initial password is an error (exit 2), not a default.
            [[ -n "${BKN_NEW_USER_PASSWORD:-}" ]] || return 2
            printf '%s' "${BKN_NEW_USER_PASSWORD}" >"${work}/initial"
            echo '{"id":"u-1"}' ;;
        ("admin user reset-password u-1 --password "*) ;;
        ("admin --json role list --limit 1000") echo '{"roles":[]}' ;;
        (*) echo "unexpected openbkn $*" >&2; return 99 ;;
    esac
}
curl() {
    echo "curl $*" >>"${work}/argv"
    cat >"${work}/change-password-body"
    printf 204
}
helm() { [[ "$*" == "list -A -q" ]] && printf 'mf-model-api\nbkn-safe\nbkn-studio\n'; }
kubectl() { return 1; }

onboard_bkn_safe_detected || fail "bkn-safe release must be detected"
set -o pipefail
onboard_bkn_safe_detected || fail "detection must survive pipefail"

onboard_provision_bkn_safe_test_user
[[ "${ONBOARD_REPORT_TEST_USER}" == created* ]] || fail "test user not created: ${ONBOARD_REPORT_TEST_USER}"
password="$(cat "${ONBOARD_TEST_USER_PASSWORD_FILE}")"
[[ ${#password} -ge 12 && "${password}" != 111111 ]] || fail "a random password must be generated"
[[ "$(python3 -c 'import os, stat, sys; print(oct(stat.S_IMODE(os.stat(sys.argv[1]).st_mode))[2:])' "${ONBOARD_TEST_USER_PASSWORD_FILE}")" == 600 ]] || fail "password file must be 0600"
[[ "$(cat "${work}/initial")" == "${password}.init" ]] || fail "user create must get the initial password"
python3 - "${work}/change-password-body" "${password}" <<'PY'
import json, sys
body = json.load(open(sys.argv[1]))
assert body == {"account": "test", "old_password": sys.argv[2] + ".init", "new_password": sys.argv[2]}, body
PY
if grep -F -- "${password}" "${work}/argv" | grep -q '^curl'; then fail "passwords must not be in curl argv"; fi
if grep -qF -- "${password}" "${work}/log"; then fail "the log must not carry the password"; fi
grep -qF "stored in ${ONBOARD_TEST_USER_PASSWORD_FILE}" "${work}/log" || fail "the log must say where the password is"

report="$(YELLOW='' GREEN='' NC='' onboard_print_completion_report 2>&1)"
if grep -qF -- "${password}" <<<"${report}" || grep -qF 'admin-initial-secret' <<<"${report}"; then
    fail "the report must not print passwords"
fi
grep -q 'Install type   bkn-safe$' <<<"${report}" || fail "report must show bkn-safe as installed"
grep -qF "bknSafe.initialPassword in ${CONFIG_YAML_PATH}" <<<"${report}" || fail "report must point at the admin password"

# A second run reuses the stored password for an existing user, generating none.
user_exists=true
: >"${work}/argv"
onboard_provision_bkn_safe_test_user
[[ "$(cat "${ONBOARD_TEST_USER_PASSWORD_FILE}")" == "${password}" ]] || fail "stored password must be kept"
[[ "${ONBOARD_REPORT_TEST_USER}" == ready* ]] || fail "existing user must be reported ready"

echo "onboard_test_user_test: passed"
