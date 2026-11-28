#!/usr/bin/env bash
# Copyright openbkn.ai
#
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/scripts/services/openbkn.sh"
work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
log_info() { :; }
log_error() { printf '%s\n' "$*" >&2; }
_openbkn_drop_literal_env_now_from_secret() { :; }
_openbkn_adopt_unowned_resources() { return 1; }
# No image probe, server-side dry run, or additional cluster permissions.
kubectl() { echo 'unexpected kubectl call' >&2; return 99; }
_openbkn_preflight_images() { echo 'obsolete image preflight called' >&2; return 99; }
helm() {
    printf '%s\n' "$@" >"${work}/helm-args"
    printf '%s\n' 'native Helm diagnostic: timed out waiting for readiness'
    return "${HELM_TEST_STATUS}"
}
HELM_TEST_STATUS=0
args=(upgrade --install trace chart --namespace isolated --wait --atomic --timeout=30s)
_openbkn_helm_upgrade_release trace isolated "${args[@]}" >"${work}/output"
printf '%s\n' "${args[@]}" >"${work}/expected"
cmp "${work}/expected" "${work}/helm-args"
HELM_TEST_STATUS=1
if _openbkn_helm_upgrade_release trace isolated "${args[@]}" >"${work}/output" 2>&1; then
    echo 'failed Helm upgrade was reported successful' >&2
    exit 1
fi
cmp "${work}/expected" "${work}/helm-args"
grep -q 'native Helm diagnostic: timed out waiting for readiness' "${work}/output"
echo 'PASS: Helm runs without image probes; arguments and native failure output retained'

# Repository installs must wait for actual workload readiness, like local charts.
_openbkn_resolve_chart_name() { printf '%s' "$1"; }
_openbkn_should_skip_upgrade() { return 1; }
build_chart_ref() { printf '%s/%s' "$1" "$2"; }
_openbkn_release_extra_sets() {
    CORE_RELEASE_EXTRA_SETS=()
    CORE_RELEASE_EXTRA_SET_STRINGS=()
}
helm() {
    if [[ "$1" == status ]]; then
        printf '%s' '{"info":{"status":"deployed"}}'
    else
        printf '%s\n' "$@" >"${work}/helm-args"
    fi
}
CONFIG_YAML_PATH="${work}/config.yaml"
CORE_SET_VALUES=()
CORE_HELM_TIMEOUT=37s
_install_openbkn_release_repo trace isolated repo 0.2.1
grep -qx -- '--wait' "${work}/helm-args"
grep -qx -- '--timeout=37s' "${work}/helm-args"
echo 'PASS: repository path waits for readiness with the configured timeout'

# A leftover commercial auth file must never modify a community install.
_OPENBKN_COMMERCIAL_DIR="${work}"
CORE_COMMERCIAL_MANIFEST=""
: >"${work}/registry-auth.json"
_install_openbkn_release_repo trace isolated repo 0.2.1
if grep -qx -- '--post-renderer' "${work}/helm-args"; then
    echo 'community install unexpectedly enabled commercial post-renderer' >&2
    exit 1
fi
echo 'PASS: community install ignores commercial authentication artifacts'

# The default small model onboard wrote into the live ConfigMap survives an
# installer re-run. It is the first --set: above every values file, including a
# commercial runtime values file still holding the previous release's model, and
# below the operator's own --set values that follow it.
SCRIPT_DIR="${ROOT}"
kubectl() {
    [[ "$*" == "get configmap bkn-backend-cm -n isolated -o json" ]] || { echo "unexpected kubectl $*" >&2; return 99; }
    python3 -c 'import json; print(json.dumps({"data": {"bkn-backend-config.yaml":
        "server:\n  defaultSmallModelEnabled: true\n  defaultSmallModelName: text-embedding-v4\n  httpPort: 13014\nlog:\n  logLevel: info\n"}}))'
}
helm() { printf '%s\n' "$@" >"${work}/helm-args"; }
printf 'config:\n  server:\n    defaultSmallModelName: model-a\n    defaultSmallModelEnabled: true\n' >"${work}/bkn-backend.values.json"
args=(upgrade --install bkn-backend chart --namespace isolated -f "${work}/config.yaml" -f "${work}/bkn-backend.values.json" --wait --set operator=value)
_openbkn_helm_upgrade_release bkn-backend isolated "${args[@]}" >"${work}/output"
printf '%s\n' upgrade --install bkn-backend chart \
    --set-string config.server.defaultSmallModelName=text-embedding-v4 --set config.server.defaultSmallModelEnabled=true \
    --namespace isolated -f "${work}/config.yaml" -f "${work}/bkn-backend.values.json" --wait --set operator=value >"${work}/expected"
cmp "${work}/expected" "${work}/helm-args"
if command -v helm >/dev/null 2>&1 && type -P helm >/dev/null; then
    # Real Helm precedence: the leading --set beats the stale runtime values file.
    mkdir -p "${work}/chart/templates"
    printf 'apiVersion: v2\nname: c\nversion: 0.1.0\n' >"${work}/chart/Chart.yaml"
    printf 'name: {{ .Values.config.server.defaultSmallModelName }}\n' >"${work}/chart/templates/cm.yaml"
    rendered="$(command helm template x "${work}/chart" $(sed -n '5,8p' "${work}/helm-args" | tr '\n' ' ') -f "${work}/bkn-backend.values.json")"
    [[ "${rendered}" == *"name: text-embedding-v4"* ]] || { echo "live model lost to the runtime values file: ${rendered}" >&2; exit 1; }
fi
# A Chart-rendered ConfigMap without an onboard choice adds nothing.
kubectl() { python3 -c 'import json; print(json.dumps({"data": {"bkn-backend-config.yaml": "server:\n  defaultSmallModelName: \"\"\n  defaultSmallModelEnabled: false\n"}}))'; }
_openbkn_helm_upgrade_release bkn-backend isolated "${args[@]}" >"${work}/output"
printf '%s\n' "${args[@]}" >"${work}/expected"
cmp "${work}/expected" "${work}/helm-args"
echo 'PASS: onboard default small model is carried into upgrades above values files'
