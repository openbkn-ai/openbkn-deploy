#!/usr/bin/env bash
# Copyright openbkn.ai
#
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.

# Shared by deploy and the opt-in preflight fixes. Never initializes a cluster.
k8s_dependency_minor() {
    local version="${PREFLIGHT_K8S_APT_MINOR:-}" installed=""
    if [[ -z "${version}" ]] && command -v dpkg-query >/dev/null 2>&1; then
        installed="$(dpkg-query -W -f='${Version}' kubeadm 2>/dev/null || true)"
        [[ "${installed}" =~ ^([0-9]+\.[0-9]+)\. ]] && version="v${BASH_REMATCH[1]}"
    fi
    if [[ -z "${version}" ]] && command -v kubeadm >/dev/null 2>&1; then
        installed="$(kubeadm version -o short 2>/dev/null || true)"
        [[ "${installed}" =~ ^v([0-9]+\.[0-9]+)\. ]] && version="v${BASH_REMATCH[1]}"
    fi
    version="${version:-v1.28}"
    if [[ ! "${version}" =~ ^v[0-9]+\.[0-9]+$ ]]; then
        log_error "Invalid Kubernetes repository minor: ${version}"
        return 1
    fi
    printf '%s\n' "${version}"
}

k8s_dependency_download() {
    local url="$1" target="$2"
    log_info "Downloading ${url} (connect 10s, request 60s, at most 2 retries)"
    if ! curl --fail --show-error --location --connect-timeout 10 --max-time 60 \
        --retry 2 --retry-delay 1 --retry-max-time 180 --output "${target}" "${url}"; then
        log_error "Download failed: ${url}"
        return 1
    fi
    [[ -s "${target}" ]] || { log_error "Empty download: ${url}"; return 1; }
}

k8s_dependency_validate() {
    local bin output
    for bin in kubeadm kubelet kubectl; do
        command -v "${bin}" >/dev/null 2>&1 || return 1
        case "${bin}" in
            kubeadm) output="$(kubeadm version -o short 2>/dev/null)" || return 1 ;;
            kubelet) output="$(kubelet --version 2>/dev/null)" || return 1 ;;
            kubectl) output="$(kubectl version --client=true -o json 2>/dev/null)" || return 1 ;;
        esac
        [[ "${output}" =~ v[0-9]+\.[0-9]+\.[0-9]+ ]] || return 1
        if [[ -n "${1:-}" && "${BASH_REMATCH[0]}" != "${1}."* ]]; then
            return 1
        fi
    done
}

k8s_dependency_legacy_source() {
    grep -Eq '^[[:space:]]*deb(-src)?[[:space:]].*(packages\.cloud\.google\.com/apt|apt\.kubernetes\.io)' "$1"
}

# A failed download never touches existing configuration. Keep backups and roll
# back all edited source files and the keyring if replacement or refresh fails.
k8s_dependency_apt_repo() (
    local root="${K8S_SYSTEM_ROOT:-}" minor aptdir key source stage f i
    local committed=false
    local -a originals=() saved=()
    aptdir="${root}/etc/apt"
    key="${aptdir}/keyrings/kubernetes-apt-keyring.gpg"
    source="${aptdir}/sources.list.d/kubernetes.list"
    minor="$(k8s_dependency_minor)" || return 1
    stage="$(mktemp -d)" || return 1
    trap '
        if [[ "${committed}" != true ]]; then
            for ((i=${#originals[@]}-1; i>=0; i--)); do
                if [[ "${saved[i]}" == absent ]]; then
                    rm -f "${originals[i]}"
                else
                    cp -p "${saved[i]}" "${originals[i]}" || log_error "Restore failed: ${originals[i]}"
                fi
            done
        fi
        rm -rf "${stage}"
    ' EXIT
    k8s_dependency_download "https://pkgs.k8s.io/core:/stable:/${minor}/deb/Release.key" "${stage}/Release.key" || return 1
    gpg --batch --yes --dearmor --output "${stage}/keyring" "${stage}/Release.key" || return 1
    gpg --batch --show-keys "${stage}/keyring" >/dev/null 2>&1 || { log_error "Invalid Kubernetes signing key"; return 1; }
    mkdir -p "${aptdir}/keyrings" "${aptdir}/sources.list.d" || return 1
    for f in "${aptdir}/sources.list" "${aptdir}"/sources.list.d/*.list "${key}"; do
        [[ -f "${f}" ]] || continue
        if [[ "${f}" != "${source}" && "${f}" != "${key}" ]]; then
            k8s_dependency_legacy_source "${f}" || continue
        fi
        cp -p "${f}" "${stage}/original.${#originals[@]}" || return 1
        # Visible recovery copy, retained after a successful migration too.
        cp -p "${f}" "${f}.bak.$(date +%s).$$" || return 1
        saved+=("${stage}/original.${#originals[@]}")
        originals+=("${f}")
    done
    for f in "${source}" "${key}"; do
        [[ -f "${f}" ]] || { originals+=("${f}"); saved+=(absent); }
    done
    for f in "${originals[@]}"; do
        [[ "${f}" == "${key}" || ! -f "${f}" ]] && continue
        sed -E '/^[[:space:]]*deb(-src)?[[:space:]].*(packages\.cloud\.google\.com\/apt|apt\.kubernetes\.io)/s/^/# Disabled legacy Kubernetes source: /' "${f}" > "${stage}/source" || return 1
        cat "${stage}/source" > "${f}" || return 1
    done
    install -m 644 "${stage}/keyring" "${key}" || return 1
    printf 'deb [signed-by=%s] https://pkgs.k8s.io/core:/stable:/%s/deb/ /\n' "${key}" "${minor}" > "${source}" || return 1
    log_info "Refreshing Kubernetes apt repository (${minor})"
    k8s_dependency_apt_update || return 1
    committed=true
)

k8s_dependency_apt_update() {
    apt-get update -o Acquire::ForceIPv4=true -o Acquire::http::Timeout=30 \
        -o Acquire::https::Timeout=30 -o Acquire::Retries=2 \
        -o APT::Update::Error-Mode=any || { log_error "Kubernetes apt repository refresh failed"; return 1; }
}

k8s_dependency_repo() {
    local root="${K8S_SYSTEM_ROOT:-}" f legacy=false candidate="" pm
    if command -v apt-get >/dev/null 2>&1; then
        for f in "${root}/etc/apt/sources.list" "${root}"/etc/apt/sources.list.d/*.list; do
            [[ -f "${f}" ]] || continue
            k8s_dependency_legacy_source "${f}" && legacy=true
        done
        candidate="$(apt-cache policy kubeadm 2>/dev/null | awk '/Candidate:/{print $2; exit}')" || return 1
        if [[ "${legacy}" == false && -n "${candidate}" && "${candidate}" != '(none)' ]]; then
            return 0
        fi
        k8s_dependency_apt_repo || return 1
    else
        if command -v dnf >/dev/null 2>&1; then pm=dnf
        elif command -v yum >/dev/null 2>&1; then pm=yum
        else log_error "No supported Kubernetes package manager"; return 1
        fi
        if "${pm}" -q list installed kubeadm >/dev/null 2>&1 \
            || "${pm}" -q list available --disableexcludes=kubernetes kubeadm >/dev/null 2>&1; then
            return 0
        fi
        k8s_dependency_rpm_repo "${pm}" || return 1
    fi
}

# Keep RPM repository replacement recoverable just like the apt path.
k8s_dependency_rpm_repo() (
    local pm="$1" root="${K8S_SYSTEM_ROOT:-}" minor f stage base key
    local committed=false existed=false
    minor="$(k8s_dependency_minor)" || return 1
    base="${K8S_RPM_REPO_BASEURL:-https://pkgs.k8s.io/core:/stable:/${minor}/rpm/}"
    key="${K8S_RPM_REPO_GPGKEY:-${base%/}/repodata/repomd.xml.key}"
    f="${root}/etc/yum.repos.d/kubernetes.repo"
    stage="$(mktemp -d)" || return 1
    trap '
        if [[ "${committed}" != true ]]; then
            if [[ "${existed}" == true ]]; then
                cp -p "${stage}/original" "${f}" || log_error "Restore failed: ${f}"
            elif [[ -f "${stage}/new" ]]; then
                rm -f "${f}"
            fi
        fi
        rm -rf "${stage}"
    ' EXIT
    k8s_dependency_download "${key}" "${stage}/Release.key" || return 1
    gpg --batch --show-keys "${stage}/Release.key" >/dev/null 2>&1 || { log_error "Invalid Kubernetes RPM signing key"; return 1; }
    mkdir -p "$(dirname "${f}")" || return 1
    if [[ -f "${f}" ]]; then
        cp -p "${f}" "${stage}/original" || return 1
        existed=true
        cp -p "${f}" "${f}.bak.$(date +%s).$$" || return 1
    fi
    cat > "${stage}/new" <<EOF
[kubernetes]
name=Kubernetes
baseurl=${base}
enabled=1
gpgcheck=1
gpgkey=${key}
exclude=kubelet kubeadm kubectl cri-tools kubernetes-cni
timeout=30
retries=2
EOF
    [[ $? -eq 0 ]] || return 1
    install -m 644 "${stage}/new" "${f}" || return 1
    "${pm}" makecache --disablerepo='*' --enablerepo=kubernetes || { log_error "Kubernetes RPM repository refresh failed"; return 1; }
    committed=true
)

k8s_dependency_install_bins() {
    local pm minor
    if k8s_dependency_validate; then return 0; fi
    if [[ "${OFFLINE_MODE:-false}" == true ]]; then
        log_error "Offline mode requires usable kubeadm, kubelet and kubectl"
        return 1
    fi
    k8s_dependency_repo || return 1
    minor="$(k8s_dependency_minor)" || return 1
    if command -v apt-get >/dev/null 2>&1; then
        k8s_dependency_apt_update || return 1
        apt-get install -y -o Acquire::http::Timeout=30 -o Acquire::https::Timeout=30 -o Acquire::Retries=2 \
            "kubeadm=${minor#v}.*" "kubelet=${minor#v}.*" "kubectl=${minor#v}.*" || { log_error "Kubernetes package installation failed"; return 1; }
        apt-mark hold kubeadm kubelet kubectl || { log_error "Kubernetes version hold failed"; return 1; }
    else
        if command -v dnf >/dev/null 2>&1; then pm=dnf; else pm=yum; fi
        "${pm}" install -y --setopt=timeout=30 --setopt=retries=2 --disableexcludes=kubernetes \
            "kubeadm-${minor#v}.*" "kubelet-${minor#v}.*" "kubectl-${minor#v}.*" || { log_error "Kubernetes package installation failed"; return 1; }
        if ! "${pm}" versionlock --help >/dev/null 2>&1; then
            if [[ "${pm}" == dnf ]]; then
                "${pm}" install -y --setopt=timeout=30 --setopt=retries=2 python3-dnf-plugin-versionlock || return 1
            else
                "${pm}" install -y --setopt=timeout=30 --setopt=retries=2 yum-plugin-versionlock || return 1
            fi
        fi
        "${pm}" versionlock add kubeadm kubelet kubectl || { log_error "Kubernetes version lock failed"; return 1; }
    fi
    k8s_dependency_validate "${minor}" || { log_error "Installed Kubernetes components failed ${minor} version validation"; return 1; }
    systemctl daemon-reload || return 1
    systemctl enable kubelet || return 1
}

# kubernetes-cni (RPM/apt) or containernetworking/plugins tarball provides
# /opt/cni/bin/{loopback,bridge,...}. If only kubeadm/kubelet were pre-installed,
# this directory is empty and kubelet fails with: failed to find plugin "loopback".
_k8s_ensure_cni_bin_plugins() {
    local cni_dir="${K8S_SYSTEM_ROOT:-}/opt/cni/bin"
    if [[ -x "${cni_dir}/loopback" ]]; then
        return 0
    fi

    if [[ "${OFFLINE_MODE:-false}" == true ]]; then
        log_error "Offline mode requires pre-installed /opt/cni/bin/loopback"
        return 1
    fi

    log_warn "Missing /opt/cni/bin/loopback — installing CNI plugins for kubelet pod sandbox"
    detect_package_manager || return 1

    if [[ "${PKG_MANAGER}" == "dnf" ]] || [[ "${PKG_MANAGER}" == "yum" ]]; then
        log_info "Trying package kubernetes-cni..."
        if [[ "${PKG_MANAGER}" == "dnf" ]]; then
            dnf install -y --setopt=timeout=30 --setopt=retries=2 --disableexcludes=kubernetes kubernetes-cni 2>/dev/null \
                || dnf install -y --setopt=timeout=30 --setopt=retries=2 kubernetes-cni \
                || true
        else
            yum install -y --setopt=timeout=30 --setopt=retries=2 --disableexcludes=kubernetes kubernetes-cni 2>/dev/null \
                || yum install -y --setopt=timeout=30 --setopt=retries=2 kubernetes-cni \
                || true
        fi
    elif [[ "${PKG_MANAGER}" == "apt" ]]; then
        ${PKG_MANAGER_INSTALL} -o Acquire::http::Timeout=30 -o Acquire::https::Timeout=30 -o Acquire::Retries=2 kubernetes-cni 2>/dev/null || true
    fi

    if [[ -x "${cni_dir}/loopback" ]]; then
        log_info "CNI plugins available under /opt/cni/bin"
        return 0
    fi

    local v="${CNI_PLUGINS_VERSION:-v1.4.0}"
    local arch=""
    case "$(uname -m)" in
        x86_64|amd64) arch="amd64" ;;
        aarch64|arm64) arch="arm64" ;;
        *)
            log_error "No loopback CNI and unsupported arch for tarball fallback: $(uname -m)"
            return 1
            ;;
    esac

    local tgz="cni-plugins-linux-${arch}-${v}.tgz"
    local url="https://github.com/containernetworking/plugins/releases/download/${v}/${tgz}"
    log_info "Fetching ${tgz} into /opt/cni/bin..."
    local archive
    archive="$(mktemp)" || return 1
    if ! k8s_dependency_download "${url}" "${archive}"; then
        rm -f "${archive}"
        return 1
    fi
    mkdir -p "${cni_dir}" || { rm -f "${archive}"; return 1; }
    if tar -C "${cni_dir}" -xzf "${archive}"; then
        rm -f "${archive}"
        chmod a+x "${cni_dir}"/* 2>/dev/null || true
        if [[ -x "${cni_dir}/loopback" ]]; then
            log_info "Installed CNI plugins from containernetworking/plugins ${v}"
            return 0
        fi
    fi

    rm -f "${archive}"
    log_error "Still no /opt/cni/bin/loopback. Install kubernetes-cni (RPM/apt) or unpack CNI plugins there, then restart kubelet."
    return 1
}
