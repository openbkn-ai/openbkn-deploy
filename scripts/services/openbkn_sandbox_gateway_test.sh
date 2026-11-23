#!/usr/bin/env bash
# Copyright openbkn.ai
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${SCRIPT_DIR}/scripts/services/openbkn.sh"
test_dir="$(mktemp -d)"
trap 'rm -rf "${test_dir}"' EXIT
log_info() { :; }
log_error() { :; }
_openbkn_drop_literal_env_now_from_secret() { :; }
_openbkn_adopt_unowned_resources() { return 1; }
installed=false
old_values='{}'
services='{"items":[{"metadata":{"name":"custom-gateway","namespace":"edge"},"spec":{"selector":{"app":"edge"},"ports":[{"name":"http","port":8080,"targetPort":"web","nodePort":32080}]}}]}'
nodes='{"items":[{"status":{"addresses":[{"type":"InternalIP","address":"192.0.2.10"},{"type":"Hostname","address":"node-a"}]}}]}'
pods='{"items":[]}'
kubectl() {
    printf '%s\n' "$*" >>"${test_dir}/discovery-calls"
    [[ "${services}" != unavailable ]] || return 1
    case "$1 $2" in
        'get nodes') [[ "${nodes}" != unavailable ]] || return 1; printf '%s' "${nodes}" ;;
        'get pods') [[ "${pods}" != unavailable ]] || return 1; printf '%s' "${pods}" ;;
        *) printf '%s' "${services}" ;;
    esac
}
helm() {
    case "$1" in
        list) if [[ "${installed}" == true ]]; then printf 'sandbox\n'; fi ;;
        get) [[ " $* " != *' --all '* ]] || return 1; printf '%s' "${old_values}" ;;
        install) command helm "$@" >"${test_dir}/render.json"; cat "${test_dir}/render.json" ;;
        upgrade) printf 'upgrade\n' >>"${test_dir}/upgrades" ;;
        *) return 1 ;;
    esac
}
foundry_source="${BKN_FOUNDRY_SOURCE:?Set BKN_FOUNDRY_SOURCE to the matching Foundry checkout}"
chart="${foundry_source}/infra/sandbox/deploy/helm/sandbox"
args=(upgrade --install sandbox "${chart}" --namespace openbkn)
_openbkn_helm_upgrade_release sandbox openbkn "${args[@]}"
python3 - "${test_dir}/render.json" <<'PY'
import json, sys
r = json.load(open(sys.argv[1]))
assert 'BKN_BASE_URL: "http://custom-gateway.edge.svc:8080"' in r['manifest']
p = r['config']['networkPolicy']['gateway']
assert p['namespace'] == 'edge' and p['podSelector'] == {'app': 'edge'} and p['port'] == 'web'
assert 'app.kubernetes.io/name: ingress-nginx' not in r['manifest']
assert 'BKN_SANDBOX_MCP_URL: "http://agent-retrieval:30780/' in r['manifest']
PY
# A hostNetwork controller answers on node addresses. kube-router never selects
# it by Pod labels, so the rendered rule must list nodes on the numeric port.
services='{"items":[{"kind":"Service","metadata":{"name":"ingress-nginx-controller","namespace":"ingress-nginx"},"spec":{"selector":{"app":"edge"},"ports":[{"name":"http","port":80,"targetPort":"http"}]}}]}'
pods='{"items":[{"kind":"Pod","metadata":{"namespace":"ingress-nginx","labels":{"app":"edge"}},"spec":{"hostNetwork":true,"containers":[{"ports":[{"name":"http","containerPort":80,"protocol":"TCP"}]}]},"status":{"phase":"Running"}}]}'
_openbkn_helm_upgrade_release sandbox openbkn "${args[@]}"
python3 - "${test_dir}/render.json" <<'PY'
import json, sys
r = json.load(open(sys.argv[1]))
p = r['config']['networkPolicy']['gateway']
assert p['port'] == 80 and p['ipBlocks'] == [{'cidr': '192.0.2.10/32'}], p
policy = r['manifest'].split('name: sandbox-executor-egress', 1)[1].split('\n---', 1)[0]
assert '- ipBlock:\n            cidr: 192.0.2.10/32\n      ports:\n        - protocol: TCP\n          port: 80\n' in policy, policy
PY
# Unreadable nodes stop only a hostNetwork controller; Pod networking needs none.
saved_nodes="${nodes}"
nodes=unavailable
if _openbkn_helm_upgrade_release sandbox openbkn "${args[@]}" 2>"${test_dir}/stderr"; then
    echo 'hostNetwork gateway without node addresses must fail before rollout' >&2
    exit 1
fi
# The fix it suggests must name both settings: an explicit origin skips discovery.
grep -q 'Cannot read cluster nodes.*controlPlane.env.BKN_BASE_URL.*networkPolicy.gateway' "${test_dir}/stderr"
assert_pod_network_rule() {
    python3 - "${test_dir}/render.json" <<'PY'
import json, sys
p = json.load(open(sys.argv[1]))['config']['networkPolicy']['gateway']
assert p['port'] == 'http' and p['ipBlocks'] == [], p
PY
}
pods="${pods//\"hostNetwork\":true/\"hostNetwork\":false}"
_openbkn_helm_upgrade_release sandbox openbkn "${args[@]}"
assert_pod_network_rule
# Unreadable Pods keep the previous Pod-network rule instead of stopping installation.
pods=unavailable
_openbkn_helm_upgrade_release sandbox openbkn "${args[@]}" 2>/dev/null
assert_pod_network_rule
nodes="${saved_nodes}"
pods='{"items":[]}'
# An explicit current or saved override needs no cluster-wide Service discovery.
rm "${test_dir}/discovery-calls"
services=unavailable
_openbkn_helm_upgrade_release sandbox openbkn "${args[@]}" --set controlPlane.env.BKN_BASE_URL=http://explicit:80
[[ ! -f "${test_dir}/discovery-calls" ]]
installed=true
old_values='{"controlPlane":{"env":{"BKN_BASE_URL":"http://saved:80"}}}'
_openbkn_helm_upgrade_release sandbox openbkn "${args[@]}"
python3 - "${test_dir}/render.json" <<'PY'
import json, sys
assert 'BKN_BASE_URL: "http://saved:80"' in json.load(open(sys.argv[1]))['manifest']
PY
_openbkn_helm_upgrade_release sandbox openbkn "${args[@]}" --set controlPlane.env.BKN_BASE_URL=http://current:80
python3 - "${test_dir}/render.json" <<'PY'
import json, sys
assert 'BKN_BASE_URL: "http://current:80"' in json.load(open(sys.argv[1]))['manifest']
PY
[[ ! -f "${test_dir}/discovery-calls" ]]
# A rendered system fallback is lower priority than the explicit origin.
_openbkn_helm_upgrade_release sandbox openbkn "${args[@]}" --set controlPlane.gateway.baseURL=http://discovered:80
python3 - "${test_dir}/render.json" <<'PY'
import json, sys
assert 'BKN_BASE_URL: "http://saved:80"' in json.load(open(sys.argv[1]))['manifest']
PY
# A current values file overrides saved explicit configuration as well.
printf '{"controlPlane":{"env":{"BKN_BASE_URL":"http://values-file:80"}}}' >"${test_dir}/explicit.json"
_openbkn_helm_upgrade_release sandbox openbkn "${args[@]}" -f "${test_dir}/explicit.json"
python3 - "${test_dir}/render.json" <<'PYTEST'
import json, sys
assert 'BKN_BASE_URL: "http://values-file:80"' in json.load(open(sys.argv[1]))['manifest']
PYTEST
[[ ! -f "${test_dir}/discovery-calls" ]]
# No gateway, multiple gateways and discovery failure must stop before upgrade;
# the default MCP URL must never silently become the REST base.
old_values='{}'
for services in unavailable '{"items":[]}' '{"items":[{"metadata":{"name":"a","namespace":"edge"},"spec":{"selector":{"app":"edge"},"ports":[{"name":"http","port":80}]}},{"metadata":{"name":"b","namespace":"edge"},"spec":{"selector":{"app":"edge"},"ports":[{"name":"http","port":80}]}}]}'; do
    if _openbkn_helm_upgrade_release sandbox openbkn "${args[@]}" 2>/dev/null; then
        echo 'missing or ambiguous gateway must fail before rollout' >&2
        exit 1
    fi
done
for bad_url in 'http://gateway/api/agent-retrieval/v1/mcp/' 'http://user:password@gateway' 'ftp://gateway' 'http://gateway:0'; do
    if _openbkn_helm_upgrade_release sandbox openbkn "${args[@]}" --set "controlPlane.env.BKN_BASE_URL=${bad_url}" 2>/dev/null; then
        echo 'invalid REST origin must fail before rollout' >&2
        exit 1
    fi
done
[[ "$(wc -l <"${test_dir}/upgrades")" -eq 9 ]]
# Both charts render separate REST and MCP rules; a REST gateway never changes
# the namespace selected for a cross-namespace MCP Service.
for chart_name in sandbox sandbox_standalone; do
    command helm template sandbox "${foundry_source}/infra/sandbox/deploy/helm/${chart_name}" \
        --set controlPlane.env.BKN_BASE_URL=http://gateway.edge.svc:80 \
        --set controlPlane.env.BKN_SANDBOX_MCP_URL=http://agent-retrieval.knowledge.svc:30780/api/agent-retrieval/v1/mcp/ \
        --set networkPolicy.bkn.enabled=true --set networkPolicy.gateway.enabled=true \
        --set networkPolicy.gateway.namespace=edge --set networkPolicy.gateway.podSelector.app=edge >"${test_dir}/chart.yaml"
    grep -q 'kubernetes.io/metadata.name: "knowledge"' "${test_dir}/chart.yaml"
    grep -q 'kubernetes.io/metadata.name: "edge"' "${test_dir}/chart.yaml"
    grep -q 'port: 30780' "${test_dir}/chart.yaml"
    grep -q 'port: http' "${test_dir}/chart.yaml"
    if command helm template sandbox "${foundry_source}/infra/sandbox/deploy/helm/${chart_name}" \
        --set networkPolicy.bkn.enabled=true \
        --set controlPlane.env.BKN_SANDBOX_MCP_URL=http://agent-retrieval:30779/api/agent-retrieval/v1/mcp/ >/dev/null 2>&1; then
        echo 'trusted Retrieval listener must remain blocked' >&2
        exit 1
    fi
done
echo 'Sandbox gateway discovery, override, upgrade and separate egress checks passed'
