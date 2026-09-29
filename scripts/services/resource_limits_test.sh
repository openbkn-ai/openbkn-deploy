#!/usr/bin/env bash
set -euo pipefail

# Verify the data-service resource-limit migration without changing a cluster.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

contains() {
    local file="$1"
    local expected="$2"
    grep -Fq -- "${expected}" "${file}" || fail "${file##*/} must contain: ${expected}"
}

not_contains() {
    local file="$1"
    local unexpected="$2"
    if grep -Fq -- "${unexpected}" "${file}"; then
        fail "${file##*/} must not contain: ${unexpected}"
    fi
}

mariadb="${SCRIPT_DIR}/mariadb.sh"
redis="${SCRIPT_DIR}/redis.sh"
kafka="${SCRIPT_DIR}/kafka.sh"
opensearch="${SCRIPT_DIR}/opensearch.sh"
common="${SCRIPT_DIR}/../lib/common.sh"

contains "${mariadb}" 'resources.limits=null'
contains "${redis}" 'resources.limits=null'
contains "${opensearch}" 'resources.limits=null'
contains "${kafka}" 'controller.resourcesPreset=none'
contains "${kafka}" 'broker.resourcesPreset=none'
contains "${kafka}" 'controller.resources.limits=null'
contains "${kafka}" 'broker.resources.limits=null'

# Existing releases are upgraded only after inspecting their persisted manifest.
contains "${common}" 'bkn_helm_release_has_resource_limits()'
for service in "${mariadb}" "${redis}" "${kafka}" "${opensearch}"; do
    contains "${service}" 'bkn_helm_release_has_resource_limits'
    contains "${service}" 'Skipping upgrade.'
done

# Inspect real YAML through kubectl's client-side dry run. A ConfigMap setting
# named `limits` and `limits: null` must not trigger an upgrade.
(
    source "${common}"
    helm() { printf '%s' "${TEST_MANIFEST}"; }
    TEST_MANIFEST=$'apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: limits-test\ndata:\n  config.yaml: |\n    limits:\n      retries: 3\n---\napiVersion: v1\nkind: Pod\nmetadata:\n  name: limits-test\nspec:\n  containers:\n    - name: app\n      image: busybox\n      resources:\n        requests:\n          memory: 128Mi\n        limits: null'
    if bkn_helm_release_has_resource_limits release namespace; then
        fail 'unrelated or null limits must not trigger an upgrade'
    fi
    TEST_MANIFEST=$'apiVersion: v1\nkind: Pod\nmetadata:\n  name: limits-test\nspec:\n  containers:\n    - name: app\n      image: busybox\n      resources:\n        limits:\n          memory: 512Mi'
    bkn_helm_release_has_resource_limits release namespace

    helm() { return 1; }
    if bkn_helm_release_has_resource_limits release namespace; then
        fail 'unreadable manifest must not trigger an upgrade'
    else
        [[ "$?" == 2 ]] || fail 'unreadable manifest must return an inspection error'
    fi
)

# The Helm values export must fail closed and preserve non-resource settings.
(
    source "${common}"
    log_error() { :; }
    TEST_VALUES=''
    TEST_GET_VALUES_STATUS=0
    TEST_UPGRADE_STATUS=0
    TEST_UPGRADE_CALLED=false
    TEST_HELM_VALUES_FILE=''
    helm() {
        if [[ "$1" == get && "$2" == values ]]; then
            [[ "${TEST_GET_VALUES_STATUS}" == 0 ]] || return "${TEST_GET_VALUES_STATUS}"
            printf '%s' "${TEST_VALUES}"
            return 0
        fi
        TEST_UPGRADE_CALLED=true
        while [[ $# -gt 0 ]]; do
            if [[ "$1" == -f ]]; then
                TEST_HELM_VALUES_FILE="$2"
                [[ -f "${TEST_HELM_VALUES_FILE}" ]] || fail 'sanitized values file is missing during upgrade'
                shift 2
            else
                shift
            fi
        done
        return "${TEST_UPGRADE_STATUS}"
    }

    TEST_GET_VALUES_STATUS=1
    if bkn_helm_upgrade_without_resource_limits release namespace upgrade release chart; then
        fail 'upgrade must stop when helm get values fails'
    fi
    [[ "${TEST_UPGRADE_CALLED}" == false ]] || fail 'upgrade ran after helm get values failed'

    TEST_GET_VALUES_STATUS=0
    TEST_VALUES='{invalid-json'
    if bkn_helm_upgrade_without_resource_limits release namespace upgrade release chart 2>/dev/null; then
        fail 'upgrade must stop when helm values are invalid JSON'
    fi
    [[ "${TEST_UPGRADE_CALLED}" == false ]] || fail 'upgrade ran after invalid JSON'

    TEST_VALUES=''
    if bkn_helm_upgrade_without_resource_limits release namespace upgrade release chart 2>/dev/null; then
        fail 'upgrade must stop when helm returns empty values'
    fi
    [[ "${TEST_UPGRADE_CALLED}" == false ]] || fail 'upgrade ran after empty values'

    TEST_VALUES='{"resources":{"requests":{"cpu":"100m"},"limits":{"cpu":"1"}},"app":{"limits":{"retries":3}}}'
    TEST_UPGRADE_STATUS=1
    if bkn_helm_upgrade_without_resource_limits release namespace upgrade release chart; then
        fail 'upgrade failure must be returned to the caller'
    fi
    [[ "${TEST_UPGRADE_CALLED}" == true ]] || fail 'valid values did not reach Helm upgrade'
    [[ ! -e "${TEST_HELM_VALUES_FILE}" ]] || fail 'sanitized values file remains after upgrade failure'

    TEST_UPGRADE_CALLED=false
    TEST_UPGRADE_STATUS=0
    helm() {
        if [[ "$1" == get && "$2" == values ]]; then
            printf '%s' "${TEST_VALUES}"
            return 0
        fi
        TEST_UPGRADE_CALLED=true
        while [[ $# -gt 0 ]]; do
            if [[ "$1" == -f ]]; then
                TEST_HELM_VALUES_FILE="$2"
                jq -e '(.resources | has("limits") | not) and .resources.requests.cpu == "100m" and .app.limits.retries == 3' "${TEST_HELM_VALUES_FILE}" >/dev/null || fail 'resource-only limit removal changed other values'
                shift 2
            else
                shift
            fi
        done
    }
    bkn_helm_upgrade_without_resource_limits release namespace upgrade release chart
    [[ "${TEST_UPGRADE_CALLED}" == true ]] || fail 'sanitized values did not reach Helm upgrade'
    [[ ! -e "${TEST_HELM_VALUES_FILE}" ]] || fail 'sanitized values file remains after successful upgrade'
)

not_contains "${common}" 'KAFKA_MEMORY_LIMIT='
not_contains "${common}" 'OPENSEARCH_MEMORY_LIMIT='
not_contains "${common}" 'REDIS_MEMORY_LIMIT='
not_contains "${common}" 'MARIADB_MEMORY_LIMIT='

echo "PASS: data-service Helm installs and upgrades clear resource limits"
