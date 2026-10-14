#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
log_info() { :; }
log_warn() { :; }
get_access_address_base_url() { printf '%s' 'https://untrusted-public.example'; }
source "${SCRIPT_DIR}/scripts/services/openbkn.sh"
CORE_SET_VALUES=()
expected='http://ingress-nginx-controller.ingress-nginx.svc.cluster.local'
_openbkn_release_extra_sets bkn-sample-studio openbkn
[[ "${CORE_RELEASE_EXTRA_SET_STRINGS[*]}" == "baseURL=${expected}" ]]
should_skip_upgrade_same_chart_version() { return 0; }
HELM_EXIT=0
helm() { [[ "${HELM_EXIT}" == 0 ]] || return "${HELM_EXIT}"; printf '%s' "${HELM_VALUES}"; }
for old in 'https://untrusted-public.example' 'http://bkn-backend-svc.openbkn.svc.cluster.local:13014' ''; do
    HELM_VALUES="{\"baseURL\":\"${old}\"}"
    if _openbkn_should_skip_upgrade bkn-sample-studio openbkn bkn-sample-studio 0.2.0; then
        echo 'Incorrect gateway was skipped at the same Chart version' >&2; exit 1
    fi
done
HELM_VALUES="{\"baseURL\":\"${expected}\"}"
_openbkn_should_skip_upgrade bkn-sample-studio openbkn bkn-sample-studio 0.2.0
CORE_SET_VALUES=('baseURL=http://operator-custom.example')
_openbkn_release_extra_sets bkn-sample-studio openbkn
[[ "${CORE_RELEASE_EXTRA_SET_STRINGS[*]}" == 'baseURL=http://operator-custom.example' ]]
HELM_VALUES='{"baseURL":"http://operator-custom.example"}'
_openbkn_should_skip_upgrade bkn-sample-studio openbkn bkn-sample-studio 0.2.0
HELM_VALUES='{"baseURL":"http://ingress-nginx-controller.ingress-nginx.svc.cluster.local"}'
if _openbkn_should_skip_upgrade bkn-sample-studio openbkn bkn-sample-studio 0.2.0; then exit 1; fi
CORE_SET_VALUES=()
for invalid in 'not-json' '[]'; do
    HELM_VALUES="${invalid}"
    if _openbkn_should_skip_upgrade bkn-sample-studio openbkn bkn-sample-studio 0.2.0; then
        echo 'Unverifiable sample gateway was silently skipped' >&2; exit 1
    fi
done
HELM_EXIT=1
if _openbkn_should_skip_upgrade bkn-sample-studio openbkn bkn-sample-studio 0.2.0; then exit 1; fi
HELM_EXIT=0
printf '%s\n' 'PASS: sample internal gateway defaults, old-value reconciliation and explicit overrides'
