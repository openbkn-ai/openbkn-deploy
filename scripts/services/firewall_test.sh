#!/usr/bin/env bash
# Regression tests for firewalld discovery helpers; no firewalld or cluster required.
set -euo pipefail

FAILED=0
fail() { echo "FAIL: $*"; FAILED=1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${SCRIPT_DIR}/scripts/lib/common.sh"
source "${SCRIPT_DIR}/scripts/services/firewall.sh"

marker_file="$(mktemp)"
rm -f "${marker_file}"
OPENBKN_FIREWALL_MARKER_FILE="${marker_file}"
OPENBKN_FIREWALL_ENABLED=""
openbkn_firewall_enabled && fail "unset mode enabled before marker exists" || true
touch "${marker_file}"
openbkn_firewall_enabled || fail "persisted firewall mode was not inherited"
OPENBKN_FIREWALL_ENABLED="false"
openbkn_firewall_enabled && fail "explicit false did not override persisted mode" || true
OPENBKN_FIREWALL_ENABLED="forget"
_firewall_forget_management >/dev/null || fail "forget mode was not accepted"
[[ ! -e "${marker_file}" ]] || fail "forget mode did not remove persisted marker"
rm -f "${marker_file}"
OPENBKN_FIREWALL_ENABLED=""

_firewall_valid_ipv4_cidr "10.96.0.0/12" || fail "valid Service CIDR rejected"
_firewall_valid_ipv4_cidr "192.168.1.20/32" || fail "valid host CIDR rejected"
_firewall_valid_ipv4_cidr "10.0.0.0/33" && fail "invalid prefix accepted" || true
_firewall_valid_ipv4_cidr "300.0.0.0/16" && fail "invalid octet accepted" || true
_firewall_valid_ipv4_cidr "not-a-cidr" && fail "invalid CIDR accepted" || true
_firewall_valid_cidr "2001:db8::/64" || fail "valid IPv6 CIDR rejected"
[[ "$(_firewall_cidr_family "2001:db8::/64")" == "ipv6" ]] || fail "IPv6 family detection"

unset API_SERVER_ADVERTISE_ADDRESS
KUBECONFIG="$(mktemp)"
printf '%s\n' 'server: https://192.168.1.20:6443' > "${KUBECONFIG}"
[[ "$(_firewall_local_api_cidr)" == "192.168.1.20/32" ]] || fail "kubeconfig local API CIDR"
rm -f "${KUBECONFIG}"
unset KUBECONFIG

[[ "$(API_SERVER_ADVERTISE_ADDRESS=127.0.0.1 _firewall_local_api_cidr)" == "" ]] || fail "loopback should not need a firewalld API rule"
[[ "$(API_SERVER_ADVERTISE_ADDRESS=10.0.0.8 _firewall_local_api_cidr)" == "10.0.0.8/32" ]] || fail "advertise address local API CIDR"

KUBECONFIG="$(mktemp)"
printf '%s\n' 'server: https://[2001:db8::8]:6443' > "${KUBECONFIG}"
[[ "$(_firewall_local_api_cidr)" == "2001:db8::8/128" ]] || fail "IPv6 kubeconfig local API CIDR"
rm -f "${KUBECONFIG}"
unset KUBECONFIG

# The public firewall rules must use the installed controller's live ports,
# not the 80/443 defaults from common.sh.
kubectl() {
    case "$*" in
        *hostNetwork*) printf '%s' 'true' ;;
        *https*) printf '%s' '8443' ;;
        *http*) printf '%s' '8080' ;;
        *) return 1 ;;
    esac
}
live_ports="$(_firewall_live_ingress_ports)"
[[ "${live_ports}" == $'8080\n8443' ]] || fail "live ingress host ports: ${live_ports}"
unset -f kubectl

# Reconciliation replaces only ports previously recorded as OpenBKN-owned.
state_file="$(mktemp)"
printf '%s\n' 80 443 > "${state_file}"
OPENBKN_FIREWALL_STATE_FILE="${state_file}"
firewall_actions=""
_firewall_live_ingress_ports() { printf '%s\n' 8080 8443; }
_firewall_add_port() { firewall_actions+=" add:$2"; }
_firewall_remove_port() { firewall_actions+=" remove:$2"; }
_firewall_reconcile_ingress_ports || fail "successful ingress reconciliation failed"
[[ "${firewall_actions}" == *"remove:80"* && "${firewall_actions}" == *"remove:443"* ]] || fail "old owned ports were not removed"
[[ "${firewall_actions}" == *"add:8080"* && "${firewall_actions}" == *"add:8443"* ]] || fail "live ports were not added"
[[ "$(<"${state_file}")" == $'8080\n8443' ]] || fail "live port state was not persisted"

# A transient kubectl/query failure must preserve both rules and state.
firewall_actions=""
_firewall_live_ingress_ports() { return 1; }
_firewall_reconcile_ingress_ports && fail "query failure unexpectedly reconciled ports" || true
[[ -z "${firewall_actions}" ]] || fail "query failure changed firewall ports"
[[ "$(<"${state_file}")" == $'8080\n8443' ]] || fail "query failure changed persisted port state"
rm -f "${state_file}"

if [[ "${FAILED}" -ne 0 ]]; then
    exit 1
fi
echo "OK firewall_test.sh"
