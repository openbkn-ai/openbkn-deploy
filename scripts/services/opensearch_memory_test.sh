#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${SCRIPT_DIR}/scripts/lib/common.sh"
source "${SCRIPT_DIR}/scripts/services/opensearch.sh"

fail() { echo "FAIL: $*" >&2; exit 1; }
log_info() { :; }
log_warn() { :; }
log_error() { ERROR_LOG+="$*"$'\n'; }

OPENSEARCH_RELEASE_NAME=opensearch
OPENSEARCH_NAMESPACE=resource
OPENSEARCH_CLUSTER_NAME=opensearch-cluster
OPENSEARCH_NODE_GROUP=master
OPENSEARCH_CHART_TGZ="${SCRIPT_DIR}/charts/opensearch-2.36.0.tgz"
OPENSEARCH_CHART_VERSION=2.36.0
OPENSEARCH_HELM_ATOMIC=false
OFFLINE_MODE=false
HELM_REPO_OPENSEARCH=https://opensearch-project.github.io/helm-charts/
ERROR_LOG=""
NODE_MEMORY_OUTPUT=""
UPGRADE_CALL=""
ROLLOUT_CALL=""
RELEASE_INSTALLED=true

is_helm_installed() { [[ "${RELEASE_INSTALLED}" == true ]]; }
kubectl() {
    if [[ "$1" == get && "$2" == nodes ]]; then
        [[ "$*" == *'{"\n"}'* ]] || fail "node JSONPath must produce real newlines"
        printf '%s\n' "${NODE_MEMORY_OUTPUT}"
    elif [[ "$1" == rollout && "$2" == status ]]; then
        ROLLOUT_CALL="$*"
    fi
}
bkn_helm_upgrade_without_resource_limits() { UPGRADE_CALL="$*"; }

OPENSEARCH_MEMORY_REQUEST=""
OPENSEARCH_JAVA_OPTS=""
NODE_MEMORY_OUTPUT=$'false|9565284Ki\ntrue|4194304Ki'
_opensearch_apply_fresh_install_memory_defaults
[[ "${OPENSEARCH_MEMORY_REQUEST}" == 2Gi ]] || fail "9Gi schedulable node should select 2Gi"
[[ "${OPENSEARCH_JAVA_OPTS}" == '-Xms1g -Xmx1g -XX:MaxDirectMemorySize=256m' ]] || fail "JVM values should match 2Gi request"

OPENSEARCH_MEMORY_REQUEST=""
OPENSEARCH_JAVA_OPTS=""
NODE_MEMORY_OUTPUT='false|8388608Ki'
_opensearch_apply_fresh_install_memory_defaults
[[ "${OPENSEARCH_MEMORY_REQUEST}" == 512Mi ]] || fail "8Gi boundary should keep compatibility profile"

OPENSEARCH_MEMORY_REQUEST=""
OPENSEARCH_JAVA_OPTS=""
NODE_MEMORY_OUTPUT=$'false|33554432Ki\nfalse|16777216Ki'
_opensearch_apply_fresh_install_memory_defaults
[[ "${OPENSEARCH_MEMORY_REQUEST}" == 2Gi ]] || fail "smallest schedulable node should choose the profile"

OPENSEARCH_MEMORY_REQUEST=""
OPENSEARCH_JAVA_OPTS=""
NODE_MEMORY_OUTPUT=""
_opensearch_apply_fresh_install_memory_defaults
[[ "${OPENSEARCH_MEMORY_REQUEST}" == 512Mi ]] || fail "node query failure should use compatibility profile"

OPENSEARCH_MEMORY_REQUEST=4Gi
OPENSEARCH_JAVA_OPTS='-Xms2g -Xmx2g'
NODE_MEMORY_OUTPUT='false|8388608Ki'
_opensearch_apply_fresh_install_memory_defaults
[[ "${OPENSEARCH_MEMORY_REQUEST}" == 4Gi && "${OPENSEARCH_JAVA_OPTS}" == '-Xms2g -Xmx2g' ]] || fail "explicit overrides should be kept"

if set_opensearch_memory 3Gi; then fail "invalid size should fail"; fi
[[ "${ERROR_LOG}" == *'Supported values: 1Gi, 2Gi, 4Gi, 8Gi, 16Gi'* ]] || fail "invalid size should list accepted values"
[[ -z "${UPGRADE_CALL}" ]] || fail "invalid size should not upgrade"

set_opensearch_memory 2Gi
[[ "${UPGRADE_CALL}" == *'--set resources.requests.memory=4Gi'* ]] || fail "manual command should derive twice-heap request"
[[ "${UPGRADE_CALL}" == *'opensearchJavaOpts=-Xms2g -Xmx2g -XX:MaxDirectMemorySize=512m'* ]] || fail "manual command should use the requested heap"
[[ "${UPGRADE_CALL}" == *'--reset-values'* ]] || fail "manual command should not merge old limits back"
[[ "${ROLLOUT_CALL}" == *'rollout status statefulset opensearch-cluster-master'* ]] || fail "manual command should wait for rollout"

UPGRADE_CALL=""
ROLLOUT_CALL=""
bkn_helm_uninstall_if_not_deployed() { :; }
config_yaml_dep_field() { printf '%s' 'test-password'; }
_opensearch_resolve_image_defaults() {
    OPENSEARCH_IMAGE='example.invalid/opensearch:2.19.4'
    OPENSEARCH_INIT_IMAGE='example.invalid/busybox:1.36.1'
}
helm() {
    if [[ "$1" == get && "$2" == values ]]; then
        printf '%s\n' '{"opensearchJavaOpts":"-Xms1g -Xmx1g -XX:MaxDirectMemorySize=256m","resources":{"requests":{"memory":"2Gi"}}}'
    else
        fail "unexpected direct Helm call: $*"
    fi
}
OPENSEARCH_IMAGE='example.invalid/opensearch:2.19.4'
OPENSEARCH_INIT_IMAGE='example.invalid/busybox:1.36.1'
OPENSEARCH_PERSISTENCE_ENABLED=false
OPENSEARCH_PROTOCOL=http
OPENSEARCH_DISABLE_SECURITY=true
OPENSEARCH_SINGLE_NODE=true
OPENSEARCH_SYSCTL_INIT_ENABLED=false
OPENSEARCH_SYSCTL_VM_MAX_MAP_COUNT=262144
AUTO_GENERATE_CONFIG=false
OPENSEARCH_MEMORY_REQUEST=4Gi
OPENSEARCH_JAVA_OPTS='-Xms2g -Xmx2g -XX:MaxDirectMemorySize=512m'
bkn_helm_release_has_resource_limits() { return 0; }
install_opensearch
[[ "${UPGRADE_CALL}" == *'upgrade --install opensearch '* ]] || fail "existing release with limits should be reconciled"
[[ "${UPGRADE_CALL}" == *'opensearchJavaOpts=-Xms1g -Xmx1g -XX:MaxDirectMemorySize=256m'* ]] || fail "upgrade should keep the release's JVM settings"
[[ "${UPGRADE_CALL}" == *'resources.requests.memory=2Gi'* ]] || fail "upgrade should keep the release's request"
[[ "${UPGRADE_CALL}" == *'--wait --timeout=900s'* ]] || fail "limit reconciliation should wait for Helm readiness"

UPGRADE_CALL=""
bkn_helm_release_has_resource_limits() { return 1; }
install_opensearch
[[ -z "${UPGRADE_CALL}" ]] || fail "existing release without limits should not be upgraded"

# Exercise the real values-sanitizing helper used by set-memory. The Helm
# upgrade itself is intercepted, but its temporary values file is rendered
# with the vendored chart and parsed client-side, so this verifies the final
# StatefulSet contains no container resource limits.
source "${SCRIPT_DIR}/scripts/lib/common.sh"
SANITIZED_VALUES_RENDERED=false
TEST_VALUES='{"resources":{"requests":{"memory":"2Gi"},"limits":{"memory":"3Gi"}},"initResources":{"limits":{"memory":"1Gi"}},"sidecarResources":{"limits":{"memory":"1Gi"}},"custom":{"keep":true}}'
is_helm_installed() { return 0; }
helm() {
    if [[ "$1" == get && "$2" == values ]]; then
        printf '%s' "${TEST_VALUES}"
        return 0
    fi
    [[ "$1" == upgrade ]] || fail "unexpected Helm command: $*"

    local values_file=""
    local -a render_set_args=()
    while [[ "$#" -gt 0 ]]; do
        case "$1" in
            -f)
                values_file="$2"
                shift 2
                ;;
            --set|--set-string|--set-json|--set-file)
                render_set_args+=("$1" "$2")
                shift 2
                ;;
            *)
                shift
                ;;
        esac
    done
    [[ -n "${values_file}" && -f "${values_file}" ]] || fail "sanitized Helm values file is missing"
    python3 -c 'import json, sys; values = json.load(open(sys.argv[1])); assert "limits" not in values["resources"]; assert "limits" not in values["initResources"]; assert "limits" not in values["sidecarResources"]; assert values["custom"]["keep"] is True' "${values_file}" || fail "sanitized values must remove only resource limits"

    command helm template opensearch "${OPENSEARCH_CHART_TGZ}" -f "${values_file}" "${render_set_args[@]}" \
        | command kubectl create --dry-run=client --validate=false -f - -o json \
        | python3 "${HELM_JSON_HELPER}" has-resource-limits \
        | grep -qx false || fail "rendered OpenSearch containers must not have resource limits"
    SANITIZED_VALUES_RENDERED=true
}
set_opensearch_memory 2Gi
[[ "${SANITIZED_VALUES_RENDERED}" == true ]] || fail "manual memory update did not render sanitized values"

echo 'PASS: OpenSearch memory profiles and manual adjustment'
