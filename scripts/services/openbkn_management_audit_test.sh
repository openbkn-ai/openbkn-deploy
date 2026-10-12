#!/usr/bin/env bash
# Management Audit profiles: real Chart rendering, no cluster access.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
foundry="${FOUNDRY_ROOT:-${root}/../bkn-foundry}"
source "${root}/scripts/lib/common.sh"
source "${root}/scripts/services/openbkn.sh"
case_dir="$(mktemp -d)"
trap 'rm -rf "${case_dir}"' EXIT
CONFIG_YAML_PATH="${case_dir}/config.yaml"
printf '{}\n' >"${CONFIG_YAML_PATH}"
CORE_NAMESPACE=audit-test
CORE_SET_VALUES=()
SUPPLIED='{}'
INSTALLED='{}'
SAME_VERSION=true
config_yaml_dep_field() { case "$2" in mqHost) printf broker ;; mqPort) printf 9092 ;; esac; }
log_info() { :; }
log_warn() { :; }
should_skip_upgrade_same_chart_version() { [[ "${SAME_VERSION}" == true ]]; }
_openbkn_trace_profile_sets() { CORE_RELEASE_EXTRA_SETS+=("evidencePublisher.enabled=true"); }
_openbkn_trace_control_profile_matches() { return 0; }
helm() {
    [[ "$1 $2" == 'get values' ]] || return 1
    [[ "$*" != *agent-observability* && "$*" != *bkn-safe* ]] || { printf '{}'; return; }
    if [[ "$*" == *--all* ]]; then printf '%s' "${INSTALLED}"; else printf '%s' "${SUPPLIED}"; fi
}
for release in bkn-backend vega-backend mf-model-manager; do
    case "${release}" in
        bkn-backend) chart="${foundry}/adp/bkn/bkn-backend/helm/bkn-backend"; prefix=BKN_AUDIT ;;
        vega-backend) chart="${foundry}/vega/vega-backend/helm/vega-backend"; prefix=VEGA_AUDIT ;;
        mf-model-manager) chart="${foundry}/infra/mf-model-manager/charts"; prefix=MODEL_MANAGER_AUDIT ;;
    esac
    SUPPLIED='{}'; INSTALLED='{"auditPublisher":{"enabled":false}}'; CORE_SET_VALUES=()
    if _openbkn_should_skip_upgrade "${release}" audit-test "${release}" 0.2.0; then
        echo "Missing Audit configuration must reconcile: ${release}" >&2; exit 1
    fi
    _openbkn_release_extra_sets "${release}" audit-test
    args=()
    for item in "${CORE_RELEASE_EXTRA_SETS[@]}"; do args+=(--set "${item}"); done
    command helm template "${release}" "${chart}" "${args[@]}" >"${case_dir}/manifest"
    python3 - "${case_dir}/manifest" "${prefix}" <<'PY'
import re,sys
manifest=open(sys.argv[1]).read(); prefix=sys.argv[2]
for name,value in [('KAFKA_ENABLED','true'),('ENVIRONMENT','development'),('KAFKA_BROKERS','broker:9092')]:
    assert re.search(r'name: '+prefix+'_'+name+r'\s+value: ["\']?'+re.escape(value),manifest),(prefix,name)
assert 'bkn-trace-evidence-kafka' in manifest
assert 'secretKeyRef:' in manifest
PY
    # Capture effective profile and ensure a second same-version run is a no-op.
    INSTALLED="$(python3 - "${CORE_RELEASE_EXTRA_SETS[@]}" <<'PY'
import json,sys
p={}
for item in sys.argv[1:]:
    key,value=item.split('=',1)
    if key.startswith('auditPublisher.'):
        value=value.replace('\\,',',')
        p[key.split('.',1)[1]]=value=='true' if value in ('true','false') else value
print(json.dumps({'auditPublisher':p}))
PY
)"
    SUPPLIED="${INSTALLED}"
    _openbkn_should_skip_upgrade "${release}" audit-test "${release}" 0.2.0
    # Explicit opt-out and per-release connection survive upgrades.
    SUPPLIED='{"auditPublisher":{"enabled":false,"environment":"production","brokers":"custom-a:9092,custom-b:9092","credentialsSecretName":"custom-secret","usernameSecretKey":"user","passwordSecretKey":"pass","securityProtocol":"SASL_PLAINTEXT","saslMechanism":"PLAIN"}}'
    INSTALLED="${SUPPLIED}"
    _openbkn_should_skip_upgrade "${release}" audit-test "${release}" 0.2.0
    _openbkn_release_extra_sets "${release}" audit-test
    [[ " ${CORE_RELEASE_EXTRA_SETS[*]} " == *'auditPublisher.enabled=false'* ]]
    [[ " ${CORE_RELEASE_EXTRA_SETS[*]} " == *'auditPublisher.credentialsSecretName=custom-secret'* ]]
    CORE_SET_VALUES=(auditPublisher.enabled=true auditPublisher.environment=test)
    if _openbkn_should_skip_upgrade "${release}" audit-test "${release}" 0.2.0; then exit 1; fi
    _openbkn_release_extra_sets "${release}" audit-test
    [[ " ${CORE_RELEASE_EXTRA_SETS[*]} " == *'auditPublisher.environment=test'* ]]
done
printf 'Management Audit install/reconcile/preservation and Chart rendering: PASS\n'
