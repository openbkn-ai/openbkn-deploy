#!/usr/bin/env bash
# Host firewall reconciliation for a single-node OpenBKN deployment.
#
# This file intentionally manages only rules owned by OpenBKN. It never
# changes the default zone/policy and never removes user-created rules.

# Empty means "inherit the persisted OpenBKN firewall mode". An explicitly
# supplied false disables management for this invocation.
OPENBKN_FIREWALL_ENABLED="${OPENBKN_FIREWALL_ENABLED:-}"
OPENBKN_FIREWALL_PUBLIC_ZONE="${OPENBKN_FIREWALL_PUBLIC_ZONE:-public}"
OPENBKN_FIREWALL_INTERNAL_ZONE="${OPENBKN_FIREWALL_INTERNAL_ZONE:-trusted}"
OPENBKN_FIREWALL_STATE_FILE="${OPENBKN_FIREWALL_STATE_FILE:-/var/lib/openbkn-deploy/firewall-ingress-ports}"
OPENBKN_FIREWALL_MARKER_FILE="${OPENBKN_FIREWALL_MARKER_FILE:-/etc/openbkn-deploy/firewall-enabled}"
# Comma-separated CIDRs allowed to access the Kubernetes API in addition to
# this node itself. Leave empty for node-local kubectl/helm only.
K8S_API_ALLOWED_CIDRS="${K8S_API_ALLOWED_CIDRS:-}"

openbkn_firewall_enabled() {
    if [[ "${OPENBKN_FIREWALL_ENABLED}" == "true" ]]; then
        return 0
    fi
    [[ -z "${OPENBKN_FIREWALL_ENABLED}" && -f "${OPENBKN_FIREWALL_MARKER_FILE}" ]]
}

_firewall_forget_management() {
    if [[ "${OPENBKN_FIREWALL_ENABLED}" != "forget" ]]; then
        return 1
    fi
    rm -f "${OPENBKN_FIREWALL_MARKER_FILE}"
    log_info "OpenBKN firewalld management state removed; existing firewalld service and rules were left unchanged."
}

_firewall_persist_enabled() {
    mkdir -p "$(dirname "${OPENBKN_FIREWALL_MARKER_FILE}")"
    printf '%s\n' "managed=true" > "${OPENBKN_FIREWALL_MARKER_FILE}"
}

_firewall_command_ready() {
    if ! command -v firewall-cmd >/dev/null 2>&1; then
        log_error "OPENBKN_FIREWALL_ENABLED=true requires firewalld (firewall-cmd was not found)."
        return 1
    fi
    if ! firewall-cmd --state >/dev/null 2>&1; then
        log_info "OPENBKN_FIREWALL_ENABLED=true: enabling firewalld."
        if ! systemctl enable --now firewalld >/dev/null 2>&1 || ! firewall-cmd --state >/dev/null 2>&1; then
            log_error "Failed to enable firewalld required by OPENBKN_FIREWALL_ENABLED=true."
            return 1
        fi
    fi
    _firewall_persist_enabled
}

_firewall_valid_ipv4_cidr() {
    local cidr="$1" ip prefix octet
    [[ "${cidr}" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}/[0-9]{1,2}$ ]] || return 1
    ip="${cidr%/*}"
    prefix="${cidr#*/}"
    ((10#${prefix} <= 32)) || return 1
    IFS='.' read -r -a _firewall_octets <<< "${ip}"
    for octet in "${_firewall_octets[@]}"; do
        ((10#${octet} <= 255)) || return 1
    done
}

_firewall_cidr_family() {
    local cidr="$1" prefix
    if _firewall_valid_ipv4_cidr "${cidr}"; then
        printf '%s' "ipv4"
        return 0
    fi
    if [[ "${cidr}" =~ ^[0-9A-Fa-f:]+/[0-9]{1,3}$ ]]; then
        prefix="${cidr#*/}"
        if ((10#${prefix} <= 128)); then
            printf '%s' "ipv6"
            return 0
        fi
    fi
    return 1
}

_firewall_valid_cidr() {
    _firewall_cidr_family "$1" >/dev/null
}

_firewall_add_source() {
    local zone="$1" cidr="$2"
    firewall-cmd --permanent --zone="${zone}" --query-source="${cidr}" >/dev/null 2>&1 \
        || firewall-cmd --permanent --zone="${zone}" --add-source="${cidr}" >/dev/null
}

_firewall_add_interface() {
    local zone="$1" interface="$2"
    firewall-cmd --permanent --zone="${zone}" --query-interface="${interface}" >/dev/null 2>&1 \
        || firewall-cmd --permanent --zone="${zone}" --add-interface="${interface}" >/dev/null
}

_firewall_add_port() {
    local zone="$1" port="$2"
    firewall-cmd --permanent --zone="${zone}" --query-port="${port}/tcp" >/dev/null 2>&1 \
        || firewall-cmd --permanent --zone="${zone}" --add-port="${port}/tcp" >/dev/null
}

_firewall_remove_port() {
    local zone="$1" port="$2"
    firewall-cmd --permanent --zone="${zone}" --query-port="${port}/tcp" >/dev/null 2>&1 \
        && firewall-cmd --permanent --zone="${zone}" --remove-port="${port}/tcp" >/dev/null || true
}

_firewall_reconcile_ingress_ports() {
    local port old_port tmp_file live_output
    local -a live_ports=() old_ports=()

    if ! live_output="$(_firewall_live_ingress_ports)"; then
        log_error "Unable to resolve live ingress ports; existing firewall port rules were preserved."
        return 1
    fi
    while IFS= read -r port; do
        [[ -z "${port}" ]] && continue
        ((port >= 1 && port <= 65535)) || { log_error "Invalid live ingress port: ${port}"; return 1; }
        [[ " ${live_ports[*]} " == *" ${port} "* ]] || live_ports+=("${port}")
    done <<< "${live_output}"
    if [[ ${#live_ports[@]} -eq 0 ]]; then
        log_error "No live ingress ports found; preserving existing firewall ports."
        return 1
    fi

    if [[ -r "${OPENBKN_FIREWALL_STATE_FILE}" ]]; then
        while IFS= read -r old_port; do
            [[ "${old_port}" =~ ^[0-9]+$ ]] && old_ports+=("${old_port}")
        done < "${OPENBKN_FIREWALL_STATE_FILE}"
    fi

    # Only ports recorded in our state file are removed; user-created firewall
    # rules are never touched.
    for old_port in "${old_ports[@]}"; do
        [[ " ${live_ports[*]} " == *" ${old_port} "* ]] || _firewall_remove_port "${OPENBKN_FIREWALL_PUBLIC_ZONE}" "${old_port}"
    done
    for port in "${live_ports[@]}"; do
        _firewall_add_port "${OPENBKN_FIREWALL_PUBLIC_ZONE}" "${port}"
    done

    mkdir -p "$(dirname "${OPENBKN_FIREWALL_STATE_FILE}")"
    tmp_file="${OPENBKN_FIREWALL_STATE_FILE}.tmp.$$"
    printf '%s\n' "${live_ports[@]}" > "${tmp_file}"
    mv -f "${tmp_file}" "${OPENBKN_FIREWALL_STATE_FILE}"
}

_firewall_add_api_rule() {
    local cidr="$1" family
    family="$(_firewall_cidr_family "${cidr}")" || return 1
    local rule="rule family=\"${family}\" source address=\"${cidr}\" port port=\"6443\" protocol=\"tcp\" accept"
    firewall-cmd --permanent --zone="${OPENBKN_FIREWALL_PUBLIC_ZONE}" --query-rich-rule="${rule}" >/dev/null 2>&1 \
        || firewall-cmd --permanent --zone="${OPENBKN_FIREWALL_PUBLIC_ZONE}" --add-rich-rule="${rule}" >/dev/null
}

_firewall_collect_pod_cidrs() {
    command -v kubectl >/dev/null 2>&1 || return 0
    kubectl get nodes -o jsonpath='{range .items[*]}{.spec.podCIDR}{"\n"}{end}' 2>/dev/null \
        | awk 'NF && !seen[$0]++ { print }'
}

_firewall_detect_service_cidrs() {
    local args cidr
    args="$(ps -eo args= 2>/dev/null || true)"
    while IFS= read -r cidr; do
        [[ -n "${cidr}" ]] && printf '%s\n' "${cidr}"
    done < <(printf '%s\n' "${args}" | sed -n 's/.*--service-cluster-ip-range=\([^[:space:]]*\).*/\1/p' | tr ',' '\n')
}

_firewall_collect_service_cidrs() {
    local found=false cidr
    while IFS= read -r cidr; do
        [[ -z "${cidr}" ]] && continue
        found=true
        printf '%s\n' "${cidr}"
    done < <(_firewall_detect_service_cidrs)

    if [[ "${found}" != "true" && -n "${SERVICE_CIDR:-}" ]]; then
        log_warn "Could not discover the live Service CIDR; using configured SERVICE_CIDR=${SERVICE_CIDR}."
        printf '%s\n' "${SERVICE_CIDR}"
    fi
}

_firewall_collect_cni_interfaces() {
    command -v ip >/dev/null 2>&1 || return 0
    ip -o link show 2>/dev/null | awk -F': ' '
        $2 ~ /^(cni0|flannel\.1|cilium_host|cilium_net|cilium_vxlan|weave|tunl0)$/ { print $2 }
    ' | awk '!seen[$0]++'
}

_firewall_local_api_cidr() {
    local address="${API_SERVER_ADVERTISE_ADDRESS:-}" server
    if [[ -z "${address}" && -n "${KUBECONFIG:-}" && -r "${KUBECONFIG}" ]]; then
        server="$(sed -n 's|^[[:space:]]*server:[[:space:]]*https://||p' "${KUBECONFIG}" | head -1)"
        if [[ "${server}" == \[* ]]; then
            address="${server#\[}"
            address="${address%%]*}"
        else
            address="${server%%[:/]*}"
        fi
    fi
    if [[ -z "${address}" ]]; then
        address="$(hostname -I 2>/dev/null | awk '{print $1}' || true)"
    fi
    [[ "${address}" == "127.0.0.1" || "${address}" == "::1" || "${address}" == "localhost" ]] && return 0
    if [[ "${address}" == *:* ]]; then
        [[ -n "${address}" ]] && printf '%s/128\n' "${address}"
    else
        [[ -n "${address}" ]] && printf '%s/32\n' "${address}"
    fi
}

_firewall_reconcile_api_access() {
    local cidr
    while IFS= read -r cidr; do
        _firewall_valid_cidr "${cidr}" || { log_error "Invalid Kubernetes API allowed CIDR: ${cidr}"; return 1; }
        _firewall_add_api_rule "${cidr}"
    done < <({ _firewall_local_api_cidr; printf '%s\n' "${K8S_API_ALLOWED_CIDRS}" | tr ',' '\n' | sed 's/[[:space:]]//g'; } | awk 'NF && !seen[$0]++')
}

_firewall_reconcile_internal() {
    local cidr interface
    while IFS= read -r cidr; do
        _firewall_valid_cidr "${cidr}" || { log_error "Invalid Pod CIDR: ${cidr}"; return 1; }
        _firewall_add_source "${OPENBKN_FIREWALL_INTERNAL_ZONE}" "${cidr}"
    done < <(_firewall_collect_pod_cidrs)

    while IFS= read -r cidr; do
        _firewall_valid_cidr "${cidr}" || { log_error "Invalid Service CIDR: ${cidr}"; return 1; }
        _firewall_add_source "${OPENBKN_FIREWALL_INTERNAL_ZONE}" "${cidr}"
    done < <(_firewall_collect_service_cidrs)

    while IFS= read -r interface; do
        [[ -n "${interface}" ]] && _firewall_add_interface "${OPENBKN_FIREWALL_INTERNAL_ZONE}" "${interface}"
    done < <(_firewall_collect_cni_interfaces)

    _firewall_reconcile_api_access
}

_firewall_live_ingress_ports() {
    local host_network http_port https_port
    command -v kubectl >/dev/null 2>&1 || return 0
    if ! host_network="$(kubectl -n ingress-nginx get deploy ingress-nginx-controller -o jsonpath='{.spec.template.spec.hostNetwork}' 2>/dev/null)"; then
        log_error "Failed to read the live ingress-nginx controller; preserving existing firewall ports."
        return 1
    fi
    if [[ "${host_network}" == "true" ]]; then
        http_port="$(kubectl -n ingress-nginx get deploy ingress-nginx-controller -o jsonpath='{.spec.template.spec.containers[0].ports[?(@.name=="http")].hostPort}' 2>/dev/null)" || return 1
        https_port="$(kubectl -n ingress-nginx get deploy ingress-nginx-controller -o jsonpath='{.spec.template.spec.containers[0].ports[?(@.name=="https")].hostPort}' 2>/dev/null)" || return 1
    else
        http_port="$(kubectl -n ingress-nginx get svc ingress-nginx-controller -o jsonpath='{.spec.ports[?(@.name=="http")].nodePort}' 2>/dev/null)" || return 1
        https_port="$(kubectl -n ingress-nginx get svc ingress-nginx-controller -o jsonpath='{.spec.ports[?(@.name=="https")].nodePort}' 2>/dev/null)" || return 1
    fi
    if [[ ! "${http_port}" =~ ^[0-9]+$ || ! "${https_port}" =~ ^[0-9]+$ ]]; then
        log_error "Live ingress-nginx ports are missing or invalid; preserving existing firewall ports."
        return 1
    fi
    printf '%s\n%s\n' "${http_port}" "${https_port}"
}

# Reconcile only OpenBKN-owned firewalld rules. Phase is `api` before cluster
# initialization, `internal` after CNI is ready, or `all` after ingress-nginx
# is installed.
reconcile_openbkn_firewall() {
    local phase="${1:-all}"
    if _firewall_forget_management; then
        return 0
    fi
    openbkn_firewall_enabled || return 0
    _firewall_command_ready || return 1

    if [[ "${phase}" == "api" ]]; then
        _firewall_reconcile_api_access || return 1
        firewall-cmd --reload >/dev/null
        log_info "OpenBKN firewalld rules reconciled (${phase})."
        return 0
    fi

    _firewall_reconcile_internal || return 1
    if [[ "${phase}" == "all" ]]; then
        _firewall_reconcile_ingress_ports || return 1
    fi
    firewall-cmd --reload >/dev/null
    log_info "OpenBKN firewalld rules reconciled (${phase})."
}
