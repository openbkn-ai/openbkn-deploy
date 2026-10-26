#!/usr/bin/env bash
# Focused regression coverage; all cluster and network commands are mocked.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${SCRIPT_DIR}/scripts/lib/oss_storage.sh"
work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
log_info() { echo "INFO: $*"; }
log_warn() { echo "WARN: $*"; }
sleep() { command sleep 0.01; }
# The mock port-forward exits; the test still verifies cleanup of its files/traps.
kubectl() {
    echo "$*" >>"${work}/calls"
    case "$*" in
        (*'get svc'*)
            [[ "${scenario}" != services-fail ]] || return 1
            [[ "${scenario}" != absent ]] || return 0
            printf 'service/oss-gateway-backend\nservice/minio\n' ;;
        (*'rollout status'*) [[ "${scenario}" != not-ready ]] ;;
        (port-forward*)
            [[ "${scenario}" != forward-fail ]] || return 1
            echo 'Forwarding from 127.0.0.1:23456 -> 8080' ;;
        (*'get pod'*) echo minio-test ;;
        (*printenv*) echo fake-credential ;;
        (*'sh -c'*) [[ "${scenario}" != bucket-fail ]] ;;
        (*) return 99 ;;
    esac
}
curl() {
    echo "$*" >>"${work}/calls"
    case "$*" in
        (*'enabled=true'*)
            case "${scenario}" in
                (query-fail) return 22 ;;
                (invalid-json) echo 'not-json' ;;
                (existing) echo '{"data":[{"storage_id":"existing"}]}' ;;
                (*) echo '{"data":[]}' ;;
            esac ;;
        (*'/check'*)
            [[ "${scenario}" != check-fail ]] || return 22
            echo '{"data":{"connected":true}}' ;;
        (*)
            cat >"${work}/body"
            [[ "${scenario}" != post-fail ]] || return 22
            if [[ "${scenario}" == bad-response ]]; then
                echo '{"code":"failed","data":{"id":"misleading"}}'
            else
                echo '{"data":{"status":"ok","id":"new-storage"}}'
            fi ;;
    esac
}
for scenario in success existing absent services-fail not-ready forward-fail query-fail invalid-json bucket-fail post-fail bad-response check-fail; do
    : >"${work}/calls"
    rm -f "${work}/body"
    # Executed as a simple command in an errexit shell: any nonzero exit fails
    # this test, proving optional initialization never aborts its caller.
    oss_provision_default_storage custom >"${work}/output"
    if [[ "${scenario}" == success ]]; then
        rg -q 'Default OSS storage ready' "${work}/output"
        python3 - "${work}/body" <<'PY'
import json, sys
body = json.load(open(sys.argv[1]))
assert body['endpoint'] == 'http://minio.custom.svc.cluster.local:9000'
assert body['bucket_name'] == 'bkn-oss' and body['is_default'] is True
PY
        rg -q -- '--ignore-existing' "${work}/calls"
        rg -q -- '--address=127.0.0.1.*:8080' "${work}/calls"
    elif [[ "${scenario}" == existing || "${scenario}" == absent ]]; then
        ! rg -q 'initialization failed' "${work}/output"
        [[ ! -e "${work}/body" ]]
    else
        rg -q 'initialization failed; platform installation is not affected' "${work}/output"
        rg -q 'NAMESPACE=custom.*onboard.sh.*--skip-test-user' "${work}/output"
        if [[ "${scenario}" != post-fail && "${scenario}" != bad-response && "${scenario}" != check-fail ]]; then
            [[ ! -e "${work}/body" ]]
        fi
    fi
    ! rg -q 'fake-credential' "${work}/output"
    echo "PASS: ${scenario}"
done
: >"${work}/calls"
ONBOARD_SKIP_OSS_STORAGE=true oss_provision_default_storage custom
[[ ! -s "${work}/calls" ]]
# Both callers share the implementation, with the deployment call placed after
# install-status verification and outside the per-release version-skip loop.
python3 - "${SCRIPT_DIR}" <<'PY'
from pathlib import Path
import sys
root = Path(sys.argv[1])
s = (root/'scripts/services/openbkn.sh').read_text()
start = s.index('_install_openbkn_impl()')
assert s.index('oss_provision_default_storage "${namespace}"', start) > s.index('if ! gen_install_status_json', start)
assert 'oss_provision_default_storage "$@"' in (root/'scripts/lib/onboard_oss_storage.sh').read_text()
PY
# Execute the actual install function with dependencies mocked. Even with bucket
# failure under set -e, the final Studio summary must still be printed.
source "${SCRIPT_DIR}/scripts/services/openbkn.sh"
OFFLINE_MODE=true
CONFIG_YAML_PATH="${work}/config.yaml"
YELLOW='' NC=''
_openbkn_resolve_latest_manifest() { :; }
_openbkn_require_version_manifest() { :; }
_openbkn_apply_default_set_values() { :; }
ensure_platform_prerequisites() { :; }
reconcile_openbkn_firewall() { :; }
_openbkn_resolve_target_namespace() { echo custom; }
_openbkn_resolve_charts_dir() { echo "${work}"; }
_openbkn_release_exists() { return 1; }
init_openbkn_databases() { :; }
_openbkn_release_names() { printf 'oss-gateway-backend\nsandbox\n'; }
bkn_mapfile_compat() { release_names=(oss-gateway-backend sandbox); }
_openbkn_resolve_release_version() { echo 1.0.0; }
_install_openbkn_release_local() { echo "installed $1"; }
_openbkn_uninstall_retired_releases() { :; }
gen_install_status_json() { echo install-status-ready; }
_read_access_address_field() { :; }
config_yaml_top_field() { :; }
scenario=bucket-fail
_install_openbkn_impl >"${work}/install-output"
python3 - "${work}/install-output" <<'PYTEST'
from pathlib import Path
import sys
out = Path(sys.argv[1]).read_text()
assert out.index('installed sandbox') < out.index('install-status-ready') < out.index('initialization failed') < out.index('Open Studio to verify')
PYTEST
# The compatibility entry point must be equally non-fatal for onboard.
source "${SCRIPT_DIR}/scripts/lib/onboard_oss_storage.sh"
onboard_provision_oss_default_storage custom >"${work}/output"
rg -q 'initialization failed' "${work}/output"
echo 'OSS storage regression tests passed'
