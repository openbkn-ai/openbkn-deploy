#!/usr/bin/env bash
# Cluster-free regressions for the internal Trace control installation contract.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${root}/scripts/services/openbkn.sh"
config_yaml_dep_field() { :; }
log_info() { :; }
log_warn() { :; }
should_skip_upgrade_same_chart_version() { return 0; }
helm() { printf '%s' "${INSTALLED_VALUES}"; }
CORE_SET_VALUES=()
INSTALLED_VALUES='{}'
for release in bkn-backend ontology-query agent-retrieval agent-operator-integration bkn-agent otelcol-contrib; do
    CORE_RELEASE_EXTRA_SETS=()
    CORE_RELEASE_EXTRA_SET_STRINGS=()
    _openbkn_release_extra_sets "${release}" openbkn
    strings="${CORE_RELEASE_EXTRA_SET_STRINGS[*]:-}"
    for endpoint in policy configuration endpoints:heartbeat operations; do
        [[ "${strings}" == *"http://agent-observability-internal:8081/api/agent-observability/v1/internal/trace-evidence/${endpoint}"* ]] || {
            echo "FAIL: ${release} missing internal ${endpoint}" >&2; exit 1;
        }
    done
    if [[ "${strings}" =~ clientID|clientSecret|tokenURL|currentKey|PublicKey|audience ]]; then
        echo "FAIL: ${release} requires retired Admission credentials" >&2; exit 1
    fi
    if _openbkn_should_skip_upgrade "${release}" openbkn "${release}" 0.1.5; then
        echo "FAIL: ${release} skipped an old disabled/missing control profile" >&2; exit 1
    fi
done
# Valid explicit Collector endpoint overrides must not cause repeated rollouts.
CORE_SET_VALUES=("traceAdmission.policyURL=http://custom:8081/policy")
INSTALLED_VALUES='{"traceAdmission":{"policyURL":"http://custom:8081/policy","configurationURL":"http://agent-observability-internal:8081/api/agent-observability/v1/internal/trace-evidence/configuration","heartbeatURL":"http://agent-observability-internal:8081/api/agent-observability/v1/internal/trace-evidence/endpoints:heartbeat","ackURLBase":"http://agent-observability-internal:8081/api/agent-observability/v1/internal/trace-evidence/operations"}}'
_openbkn_should_skip_upgrade otelcol-contrib openbkn otelcol-contrib 0.1.5 || {
    echo 'FAIL: matched explicit endpoint override caused a rollout' >&2; exit 1;
}
# Failed and malformed reads cannot be mistaken for proof to change a release.
helm() { return 1; }
_openbkn_should_skip_upgrade bkn-backend openbkn bkn-backend 0.1.5 || exit 1
helm() { printf '%s' '{not-json}'; }
_openbkn_should_skip_upgrade bkn-backend openbkn bkn-backend 0.1.5 || exit 1
echo 'openbkn_trace_control_test: PASS (six profiles, same-version reconciliation, explicit overrides, failed reads)'
