#!/usr/bin/env bash
# Copyright openbkn.ai
#
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${SCRIPT_DIR}/scripts/lib/common.sh"
source "${KAFKA_SERVICE_FILE:-${SCRIPT_DIR}/scripts/services/kafka.sh}"

fail() { echo "FAIL: $*" >&2; exit 1; }
log_info() { :; }
log_warn() { :; }
log_error() { :; }
test_dir="$(mktemp -d)"
trap 'rm -rf "${test_dir}"' EXIT
capture="${test_dir}/helm.json"
upgrade_marker="${test_dir}/upgrade"

KAFKA_RELEASE_NAME=kafka
KAFKA_NAMESPACE=resource
KAFKA_HEAP_OPTS='-Xms256m -Xmx256m'
KAFKA_CHART_TGZ="${SCRIPT_DIR}/charts/kafka-32.4.3.tgz"
KAFKA_IMAGE='example.invalid/kafka:test'
KAFKA_AUTH_ENABLED=false
KAFKA_PERSISTENCE_ENABLED=false
AUTO_GENERATE_CONFIG=false
OFFLINE_MODE=true

is_helm_installed() { [[ "${installed}" == true ]]; }
bkn_helm_release_has_resource_limits() { return "${limits_status}"; }
config_yaml_dep_field() { printf '%s' test-password; }
bkn_require_helm_min_for_bitnami() { :; }
_kafka_resolve_image_defaults() { :; }
bkn_helm_uninstall_if_not_deployed() { :; }
bkn_helm_upgrade_without_resource_limits() {
    : >"${upgrade_marker}"
    shift 2
    helm "$@"
}
helm() {
    python3 -c 'import json,sys; json.dump(sys.argv[1:], sys.stdout)' "$@" >"${capture}"
}
kubectl() {
    if [[ "$*" == *'get statefulset kafka-controller -o json'* ]]; then
        printf '%s' "${state_json}"
        return "${api_status}"
    elif [[ "$*" == *' wait '* ]]; then
        return 0
    fi
    fail "unexpected kubectl call: $*"
}
state_for_heap() {
    state_json="$(python3 -c 'import json,sys
print(json.dumps({"spec":{"template":{"spec":{"containers":[{"name":"kafka","env":[{"name":"KAFKA_HEAP_OPTS","value":sys.argv[1]}]}]}}}}))' "$1")"
}
reset_case() {
    rm -f "${capture}" "${upgrade_marker}"
    installed=true
    limits_status=1
    api_status=0
    KAFKA_HEAP_OPTS='-Xms256m -Xmx256m'
    state_for_heap '-XX:InitialRAMPercentage=75 -XX:MaxRAMPercentage=75'
}
assert_heap() {
    python3 - "${capture}" "$1" "${KAFKA_HEAP_OPTS}" <<'PY'
import json, sys
with open(sys.argv[1]) as stream:
    args = json.load(stream)
controller = [arg.split("=", 1)[1] for arg in args if arg.startswith("controller.heapOpts=")]
assert len(controller) == 1 and json.loads(controller[0]) == sys.argv[2], args
assert args.count("broker.heapOpts=" + sys.argv[3]) == 1, args
PY
}

# The reported bug: a release without limits must still migrate its old heap.
reset_case
install_kafka
[[ -e "${upgrade_marker}" ]] || fail 'legacy heap did not trigger upgrade'
assert_heap "${KAFKA_HEAP_OPTS}"
state_for_heap "${KAFKA_HEAP_OPTS}"
rm -f "${capture}"
install_kafka
[[ ! -e "${capture}" ]] || fail 'second install restarted an already migrated controller'

reset_case
installed=false
KAFKA_HEAP_OPTS='-Xms512m -Xmx512m'
install_kafka
assert_heap "${KAFKA_HEAP_OPTS}"
[[ ! -e "${upgrade_marker}" ]] || fail 'fresh install used upgrade preservation'

reset_case
state_for_heap $' -XX:MaxRAMPercentage=75.0\t-XX:InitialRAMPercentage=75.00 '
install_kafka
assert_heap "${KAFKA_HEAP_OPTS}"

# Customized live values survive both skip and resource-limit cleanup paths.
for custom in '-Xms2g -Xmx2g' \
    '-XX:InitialRAMPercentage=40 -XX:MaxRAMPercentage=40 -XX:+AlwaysPreTouch' \
    '-XX:InitialRAMPercentage=75 -XX:MaxRAMPercentage=75 -XX:+AlwaysPreTouch' \
    '-Xms512m -Xmx512m -Xlog:gc,heap:file=/tmp/kafka-gc.log'; do
    reset_case
    state_for_heap "${custom}"
    install_kafka
    [[ ! -e "${capture}" ]] || fail 'customized heap should skip without limits'
    limits_status=0
    install_kafka
    assert_heap "${custom}"
done

reset_case
limits_status=0
install_kafka
assert_heap "${KAFKA_HEAP_OPTS}"

# Inspection failures must stop before any Helm mutation.
reset_case
api_status=1
if install_kafka; then fail 'API failure was ignored'; fi
[[ ! -e "${capture}" ]] || fail 'upgraded after API failure'

reset_case
limits_status=2
if install_kafka; then fail 'manifest inspection failure was ignored'; fi
[[ ! -e "${capture}" ]] || fail 'upgraded after manifest inspection failure'

for invalid in '' '{invalid' '{}' \
    '{"spec":{"template":{"spec":{"containers":[{"name":"kafka","env":[]}]}}}}' \
    '{"spec":{"template":{"spec":{"containers":[{"name":"kafka","env":[{"name":"KAFKA_HEAP_OPTS","valueFrom":{}}]}]}}}}' \
    '{"spec":{"template":{"spec":{"containers":[{"name":"kafka","env":[{"name":"KAFKA_HEAP_OPTS","value":"a"},{"name":"KAFKA_HEAP_OPTS","value":"b"}]}]}}}}'; do
    reset_case
    state_json="${invalid}"
    if install_kafka 2>/dev/null; then fail 'invalid live configuration was ignored'; fi
    [[ ! -e "${capture}" ]] || fail 'upgraded after invalid live configuration'
done

echo 'PASS: Kafka heap installation, legacy migration, customization and failure handling'
