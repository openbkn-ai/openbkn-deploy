
_opensearch_resolve_image_defaults() {
    local image_registry
    image_registry="$(resolve_openbkn_image_registry "${OPENSEARCH_IMAGE_REGISTRY:-}")"
    OPENSEARCH_IMAGE_REGISTRY="${image_registry}"
    if [[ -z "${OPENSEARCH_IMAGE}" ]]; then
        OPENSEARCH_IMAGE="$(compose_image_ref "${image_registry}" "${OPENSEARCH_IMAGE_REPOSITORY:-opensearch}" "${OPENSEARCH_IMAGE_TAG}")"
    fi
    if [[ -z "${OPENSEARCH_INIT_IMAGE}" ]]; then
        OPENSEARCH_INIT_IMAGE="$(compose_image_ref "${image_registry}" "${OPENSEARCH_INIT_IMAGE_REPOSITORY:-busybox}" "${OPENSEARCH_INIT_IMAGE_TAG}")"
    fi
}

_opensearch_memory_profile_for_allocatable_mib() {
    local allocatable_mib="$1"
    [[ "${allocatable_mib}" =~ ^[0-9]+$ ]] || return 1

    if (( allocatable_mib <= 8192 )); then
        OPENSEARCH_MEMORY_REQUEST="512Mi"
        OPENSEARCH_JAVA_OPTS="-Xms512m -Xmx512m -XX:MaxDirectMemorySize=128m"
    elif (( allocatable_mib <= 16384 )); then
        OPENSEARCH_MEMORY_REQUEST="2Gi"
        OPENSEARCH_JAVA_OPTS="-Xms1g -Xmx1g -XX:MaxDirectMemorySize=256m"
    elif (( allocatable_mib <= 32768 )); then
        OPENSEARCH_MEMORY_REQUEST="4Gi"
        OPENSEARCH_JAVA_OPTS="-Xms2g -Xmx2g -XX:MaxDirectMemorySize=512m"
    else
        OPENSEARCH_MEMORY_REQUEST="8Gi"
        OPENSEARCH_JAVA_OPTS="-Xms4g -Xmx4g -XX:MaxDirectMemorySize=512m"
    fi
}

# On a fresh install, use the smallest schedulable node's allocatable memory.
# Explicit request/JVM overrides keep their previous independent semantics.
_opensearch_apply_fresh_install_memory_defaults() {
    if [[ -n "${OPENSEARCH_MEMORY_REQUEST}" || -n "${OPENSEARCH_JAVA_OPTS}" ]]; then
        : "${OPENSEARCH_MEMORY_REQUEST:=512Mi}"
        : "${OPENSEARCH_JAVA_OPTS:=-Xms512m -Xmx512m -XX:MaxDirectMemorySize=128m}"
        log_info "Using explicit OpenSearch memory settings: request=${OPENSEARCH_MEMORY_REQUEST}; JVM=${OPENSEARCH_JAVA_OPTS}."
        return 0
    fi

    local node_memory allocatable allocatable_mib min_allocatable_mib="" unschedulable quantity
    node_memory="$(kubectl get nodes -o jsonpath='{range .items[*]}{.spec.unschedulable}{"|"}{.status.allocatable.memory}{"\n"}{end}' 2>/dev/null || true)"
    while IFS='|' read -r unschedulable allocatable; do
        [[ -z "${allocatable}" || "${unschedulable}" == "true" ]] && continue
        case "${allocatable}" in
            *Ki) quantity="${allocatable%Ki}"; [[ "${quantity}" =~ ^[0-9]+$ ]] || continue; allocatable_mib=$(( quantity / 1024 )) ;;
            *Mi) quantity="${allocatable%Mi}"; [[ "${quantity}" =~ ^[0-9]+$ ]] || continue; allocatable_mib=${quantity} ;;
            *Gi) quantity="${allocatable%Gi}"; [[ "${quantity}" =~ ^[0-9]+$ ]] || continue; allocatable_mib=$(( quantity * 1024 )) ;;
            *) continue ;;
        esac
        if [[ -z "${min_allocatable_mib}" ]] || (( allocatable_mib < min_allocatable_mib )); then
            min_allocatable_mib="${allocatable_mib}"
        fi
    done <<< "${node_memory}"

    if [[ -z "${min_allocatable_mib}" ]]; then
        min_allocatable_mib=0
        log_warn "Could not determine schedulable node allocatable memory; using OpenSearch compatibility profile."
    fi
    _opensearch_memory_profile_for_allocatable_mib "${min_allocatable_mib}"
    log_info "OpenSearch memory profile: node allocatable=${min_allocatable_mib}Mi, request=${OPENSEARCH_MEMORY_REQUEST}, JVM=${OPENSEARCH_JAVA_OPTS}."
}

_opensearch_memory_settings_for_heap() {
    local heap_size="$1" heap request direct_memory
    case "${heap_size}" in
        1Gi)  heap="1g";  request="2Gi";  direct_memory="256m" ;;
        2Gi)  heap="2g";  request="4Gi";  direct_memory="512m" ;;
        4Gi)  heap="4g";  request="8Gi";  direct_memory="512m" ;;
        8Gi)  heap="8g";  request="16Gi"; direct_memory="512m" ;;
        16Gi) heap="16g"; request="32Gi"; direct_memory="512m" ;;
        *) return 1 ;;
    esac
    OPENSEARCH_MEMORY_REQUEST="${request}"
    OPENSEARCH_JAVA_OPTS="-Xms${heap} -Xmx${heap} -XX:MaxDirectMemorySize=${direct_memory}"
}

set_opensearch_memory() {
    if [[ "$#" -ne 1 ]] || ! _opensearch_memory_settings_for_heap "${1:-}"; then
        log_error "Usage: deploy.sh opensearch set-memory <1Gi|2Gi|4Gi|8Gi|16Gi>"
        log_error "Supported values: 1Gi, 2Gi, 4Gi, 8Gi, 16Gi"
        return 2
    fi
    if ! is_helm_installed "${OPENSEARCH_RELEASE_NAME}" "${OPENSEARCH_NAMESPACE}"; then
        log_error "OpenSearch release ${OPENSEARCH_RELEASE_NAME} is not deployed in namespace ${OPENSEARCH_NAMESPACE}."
        return 1
    fi

    local chart_ref="opensearch/opensearch" use_local_chart="false"
    if [[ -f "${OPENSEARCH_CHART_TGZ}" ]]; then
        chart_ref="${OPENSEARCH_CHART_TGZ}"
        use_local_chart="true"
    elif [[ "${OFFLINE_MODE}" == "true" ]]; then
        log_error "Offline mode requires local OpenSearch chart: ${OPENSEARCH_CHART_TGZ}"
        return 1
    else
        helm repo add --force-update opensearch "${HELM_REPO_OPENSEARCH}"
        helm repo update
    fi

    local -a helm_args=(
        upgrade "${OPENSEARCH_RELEASE_NAME}" "${chart_ref}"
        --namespace "${OPENSEARCH_NAMESPACE}"
        --reset-values
        --set-string "opensearchJavaOpts=${OPENSEARCH_JAVA_OPTS}"
        --set "resources.requests.memory=${OPENSEARCH_MEMORY_REQUEST}"
        --wait --timeout=900s
    )
    [[ "${OPENSEARCH_HELM_ATOMIC}" == "true" ]] && helm_args+=(--atomic)
    [[ "${use_local_chart}" == "true" ]] || helm_args+=(--version "${OPENSEARCH_CHART_VERSION}")

    log_warn "Updating OpenSearch memory (heap=${1}, request=${OPENSEARCH_MEMORY_REQUEST}); the single-node service will restart."
    if ! bkn_helm_upgrade_without_resource_limits "${OPENSEARCH_RELEASE_NAME}" "${OPENSEARCH_NAMESPACE}" "${helm_args[@]}"; then
        log_error "OpenSearch memory update failed."
        return 1
    fi

    local statefulset_name="${OPENSEARCH_CLUSTER_NAME}-${OPENSEARCH_NODE_GROUP}"
    if ! kubectl rollout status statefulset "${statefulset_name}" -n "${OPENSEARCH_NAMESPACE}" --timeout=900s; then
        log_error "OpenSearch memory update did not become ready within 900s."
        return 1
    fi
    log_info "OpenSearch memory updated: heap=${1}, request=${OPENSEARCH_MEMORY_REQUEST}, JVM=${OPENSEARCH_JAVA_OPTS}, limit unset."
}

# Upgrade only the stock OpenSearch 2.19.4 image installed before the platform
# image became the default. Other image tags are intentionally left alone: they
# may be user-pinned builds and must not be overwritten by a regular install.
_opensearch_upgrade_legacy_image() {
    if [[ -n "${OPENSEARCH_IMAGE}" ]]; then
        log_info "OpenSearch uses an explicit OPENSEARCH_IMAGE; skipping automatic image upgrade."
        return 0
    fi

    local statefulset_name="${OPENSEARCH_CLUSTER_NAME}-${OPENSEARCH_NODE_GROUP}"
    local current_image
    current_image="$(kubectl get statefulset "${statefulset_name}" -n "${OPENSEARCH_NAMESPACE}" \
        -o jsonpath='{.spec.template.spec.containers[?(@.name=="opensearch")].image}' 2>/dev/null || true)"
    local current_tag="${current_image##*:}"
    if [[ -z "${current_image}" || "${current_tag}" == "${current_image}" ]]; then
        log_warn "Could not determine the image for existing OpenSearch release; skipping automatic image upgrade."
        return 0
    fi
    if [[ "${current_tag}" != "2.19.4" ]]; then
        log_info "OpenSearch is already installed with image ${current_image}; skipping automatic image upgrade."
        return 0
    fi

    _opensearch_resolve_image_defaults
    local os_image_repo="${OPENSEARCH_IMAGE%:*}"
    local os_image_tag="${OPENSEARCH_IMAGE##*:}"
    if [[ "${os_image_repo}" == "${OPENSEARCH_IMAGE}" || -z "${os_image_tag}" ]]; then
        log_error "Invalid OPENSEARCH_IMAGE (expected repo:tag): ${OPENSEARCH_IMAGE}"
        return 1
    fi

    log_info "Upgrading existing OpenSearch image from ${current_image} to ${OPENSEARCH_IMAGE}."
    # Keep the Helm release values in sync so a later Helm upgrade does not
    # restore the legacy image. --reuse-values preserves the existing release
    # configuration; the only value intentionally changed here is the image.
    local chart_ref="opensearch/opensearch"
    local use_local_chart="false"
    if [[ -f "${OPENSEARCH_CHART_TGZ}" ]]; then
        chart_ref="${OPENSEARCH_CHART_TGZ}"
        use_local_chart="true"
    elif [[ "${OFFLINE_MODE}" == "true" ]]; then
        log_error "Offline mode requires local OpenSearch chart: ${OPENSEARCH_CHART_TGZ}"
        return 1
    else
        helm repo add --force-update opensearch "${HELM_REPO_OPENSEARCH}"
        helm repo update
    fi

    local -a helm_args
    helm_args=(
        upgrade "${OPENSEARCH_RELEASE_NAME}" "${chart_ref}"
        --namespace "${OPENSEARCH_NAMESPACE}"
        --reuse-values
        --set image.repository="${os_image_repo}"
        --set image.tag="${os_image_tag}"
        --wait --timeout=900s
    )

    if [[ "${OPENSEARCH_HELM_ATOMIC}" == "true" ]]; then
        helm_args+=(--atomic)
    fi
    if [[ "${use_local_chart}" != "true" ]]; then
        helm_args+=(--version "${OPENSEARCH_CHART_VERSION}")
    fi
    if ! helm "${helm_args[@]}"; then
        log_error "OpenSearch image upgrade failed; the existing image remains in use."
        return 1
    fi

    if ! kubectl rollout status statefulset "${statefulset_name}" \
        -n "${OPENSEARCH_NAMESPACE}" --timeout=900s; then
        log_error "OpenSearch image upgrade did not become ready within 900s."
        return 1
    fi
}

install_opensearch() {
    log_info "Installing OpenSearch via Helm..."

    local fresh_install="true"

    if is_helm_installed "${OPENSEARCH_RELEASE_NAME}" "${OPENSEARCH_NAMESPACE}"; then
        local limit_status
        if bkn_helm_release_has_resource_limits "${OPENSEARCH_RELEASE_NAME}" "${OPENSEARCH_NAMESPACE}"; then
            log_info "OpenSearch has resource limits. Reconciling limits while retaining its existing memory request and JVM settings."
            local existing_values
            if ! existing_values="$(helm get values "${OPENSEARCH_RELEASE_NAME}" --namespace "${OPENSEARCH_NAMESPACE}" --all -o json)"; then
                log_error "Could not read existing OpenSearch values; skipping upgrade."
                return 1
            fi
            local existing_settings
            if ! existing_settings="$(printf '%s' "${existing_values}" | python3 "${HELM_JSON_HELPER}" opensearch-settings)"; then
                log_error "Could not determine existing OpenSearch request and JVM settings; skipping upgrade."
                return 1
            fi
            OPENSEARCH_JAVA_OPTS="${existing_settings%%$'\n'*}"
            OPENSEARCH_MEMORY_REQUEST="${existing_settings#*$'\n'}"
        else
            limit_status=$?
            if [[ "${limit_status}" == "1" ]]; then
                log_info "OpenSearch is already installed without resource limits. Skipping upgrade."
                log_info "Existing OpenSearch memory settings retained. To adjust them, run: deploy.sh opensearch set-memory <1Gi|2Gi|4Gi|8Gi|16Gi>"
                return 0
            else
                log_error "Could not inspect OpenSearch Helm manifest for resource limits; skipping upgrade."
                return 1
            fi
        fi
        fresh_install="false"
    fi

    # OpenSearch password handling
    local existing_pass=$(config_yaml_dep_field opensearch password)
    if [[ -n "${existing_pass}" ]]; then
        OPENSEARCH_INITIAL_ADMIN_PASSWORD="${existing_pass}"
        log_info "Using existing OpenSearch password from config.yaml"
    else
        OPENSEARCH_INITIAL_ADMIN_PASSWORD=$(generate_random_password 10)
        log_info "Generated random 10-character OpenSearch password"
    fi

    if [[ -z "${OPENSEARCH_INITIAL_ADMIN_PASSWORD}" ]]; then
        log_error "OPENSEARCH_INITIAL_ADMIN_PASSWORD is empty; OpenSearch will not start."
        return 1
    fi

    kubectl create namespace "${OPENSEARCH_NAMESPACE}" 2>/dev/null || true

    if [[ "${fresh_install}" == "true" ]]; then
        _opensearch_apply_fresh_install_memory_defaults
    fi
    _opensearch_resolve_image_defaults

    local persistence_enabled="${OPENSEARCH_PERSISTENCE_ENABLED}"
    if [[ "${persistence_enabled}" == "true" ]]; then
        if [[ -z "$(kubectl get storageclass --no-headers 2>/dev/null)" ]]; then
            log_warn "No StorageClass found. OpenSearch PVC will stay Pending."
            if [[ "${AUTO_INSTALL_LOCALPV}" == "true" ]]; then
                install_localpv
            fi

            if [[ -z "$(kubectl get storageclass --no-headers 2>/dev/null)" ]]; then
                log_warn "Still no StorageClass found. Disabling OpenSearch persistence to avoid Pending PVC."
                persistence_enabled="false"
            fi
        fi
    fi

    # Parse image repository/tag from OPENSEARCH_IMAGE (chart expects image.repository + image.tag)
    local os_image_repo="${OPENSEARCH_IMAGE%:*}"
    local os_image_tag="${OPENSEARCH_IMAGE##*:}"
    if [[ "${os_image_repo}" == "${OPENSEARCH_IMAGE}" || -z "${os_image_tag}" ]]; then
        log_error "Invalid OPENSEARCH_IMAGE (expected repo:tag): ${OPENSEARCH_IMAGE}"
        return 1
    fi

    # Parse init image repo/tag for busybox-based initContainers (chart expects persistence.image/imageTag + sysctlInit.image/imageTag)
    local init_image_repo="${OPENSEARCH_INIT_IMAGE%:*}"
    local init_image_tag="${OPENSEARCH_INIT_IMAGE##*:}"
    if [[ "${init_image_repo}" == "${OPENSEARCH_INIT_IMAGE}" || -z "${init_image_tag}" ]]; then
        log_error "Invalid OPENSEARCH_INIT_IMAGE (expected repo:tag): ${OPENSEARCH_INIT_IMAGE}"
        return 1
    fi

    local chart_ref="opensearch/opensearch"
    local use_local_chart="false"
    if [[ -f "${OPENSEARCH_CHART_TGZ}" ]]; then
        chart_ref="${OPENSEARCH_CHART_TGZ}"
        use_local_chart="true"
        log_info "Using local OpenSearch chart: ${chart_ref}"
    else
        log_info "Using remote OpenSearch chart: ${chart_ref} (version ${OPENSEARCH_CHART_VERSION})"
    fi

    # In offline mode, skip helm repo operations
    if [[ "${OFFLINE_MODE}" == "true" ]]; then
        if [[ ! -f "${OPENSEARCH_CHART_TGZ}" ]]; then
            log_error "Offline mode requires local OpenSearch chart: ${OPENSEARCH_CHART_TGZ}"
            log_error "Please prepare the chart in advance"
            return 1
        fi
    elif [[ "${use_local_chart}" != "true" ]]; then
        helm repo add --force-update opensearch "${HELM_REPO_OPENSEARCH}"
        helm repo update
    fi

    local disable_security="${OPENSEARCH_DISABLE_SECURITY}"
    if [[ -z "${disable_security}" ]]; then
        if [[ "${OPENSEARCH_PROTOCOL}" == "http" ]]; then
            disable_security="true"
        else
            disable_security="false"
        fi
    fi

    local tmp_os_yml
    tmp_os_yml="$(mktemp)"
    cat >"${tmp_os_yml}" <<EOF
cluster.name: ${OPENSEARCH_CLUSTER_NAME}
network.host: 0.0.0.0
EOF
    if [[ "${disable_security}" == "true" ]]; then
        cat >>"${tmp_os_yml}" <<EOF
plugins.security.disabled: true
EOF
    fi

    local -a helm_args
    helm_args=(
        upgrade --install "${OPENSEARCH_RELEASE_NAME}" "${chart_ref}"
        --namespace "${OPENSEARCH_NAMESPACE}"
        --set image.repository="${os_image_repo}"
        --set image.tag="${os_image_tag}"
        --set-file config.opensearch\\.yml="${tmp_os_yml}"
        --set persistence.image="${init_image_repo}"
        --set persistence.imageTag="${init_image_tag}"
        --set opensearchJavaOpts="${OPENSEARCH_JAVA_OPTS}"
        --set clusterName="${OPENSEARCH_CLUSTER_NAME}"
        --set nodeGroup="${OPENSEARCH_NODE_GROUP}"
        --set singleNode="${OPENSEARCH_SINGLE_NODE}"
        --set replicas=1
        --set sysctlInit.enabled="${OPENSEARCH_SYSCTL_INIT_ENABLED}"
        --set sysctlInit.image="${init_image_repo}"
        --set sysctlInit.imageTag="${init_image_tag}"
        --set sysctlVmMaxMapCount="${OPENSEARCH_SYSCTL_VM_MAX_MAP_COUNT}"
        --set-string extraEnvs[0].name=OPENSEARCH_INITIAL_ADMIN_PASSWORD
        --set-string extraEnvs[0].value="${OPENSEARCH_INITIAL_ADMIN_PASSWORD}"
        # Keep main, init, and sidecar container limits empty on new installs
        # and after upgrades using sanitized effective values.
        --set-json resources.limits=null
        --set-json initResources.limits=null
        --set-json sidecarResources.limits=null
        --wait --timeout=900s
    )

    if [[ "${OPENSEARCH_HELM_ATOMIC}" == "true" ]]; then
        helm_args+=(--atomic)
    fi

    if [[ "${use_local_chart}" != "true" ]]; then
        helm_args+=(--version "${OPENSEARCH_CHART_VERSION}")
    fi

    if [[ "${persistence_enabled}" == "true" ]]; then
        helm_args+=(
            --set persistence.enabled=true
            --set persistence.size="${OPENSEARCH_STORAGE_SIZE}"
        )
        if [[ -n "${OPENSEARCH_STORAGE_CLASS}" ]]; then
            helm_args+=(--set persistence.storageClass="${OPENSEARCH_STORAGE_CLASS}")
        fi
    else
        helm_args+=(--set persistence.enabled=false)
    fi

    if [[ -n "${OPENSEARCH_MEMORY_REQUEST}" ]]; then
        helm_args+=(--set resources.requests.memory="${OPENSEARCH_MEMORY_REQUEST}")
    fi
    bkn_helm_uninstall_if_not_deployed "${OPENSEARCH_RELEASE_NAME}" "${OPENSEARCH_NAMESPACE}"

    local helm_ec=0
    if [[ "${fresh_install}" == "true" ]]; then
        helm "${helm_args[@]}" || helm_ec=$?
    else
        bkn_helm_upgrade_without_resource_limits "${OPENSEARCH_RELEASE_NAME}" "${OPENSEARCH_NAMESPACE}" "${helm_args[@]}" || helm_ec=$?
    fi
    rm -f "${tmp_os_yml}" 2>/dev/null || true
    if [[ "${helm_ec}" -ne 0 ]]; then
        log_error "OpenSearch Helm install/upgrade failed (exit ${helm_ec})."
        return 1
    fi

    local service_name="${OPENSEARCH_CLUSTER_NAME}-${OPENSEARCH_NODE_GROUP}"
    log_info "OpenSearch installed successfully"
    log_info "OpenSearch connection info:"
    log_info "  Service: ${service_name}.${OPENSEARCH_NAMESPACE}.svc.cluster.local"
    log_info "  Port: 9200"
    log_info "  Username: admin"
    log_info "  Password: ${OPENSEARCH_INITIAL_ADMIN_PASSWORD}"
    if [[ "${disable_security}" == "true" ]]; then
        log_warn "  Security: disabled (HTTP, no auth)"
    else
        log_info "  Security: enabled (HTTPS + basic auth)"
    fi

    if [[ "${fresh_install}" == "true" && "${AUTO_GENERATE_CONFIG}" == "true" ]]; then
        log_info "Updating conf/config.yaml after OpenSearch fresh install..."
        generate_config_yaml
    fi
    if [[ "${fresh_install}" != "true" ]]; then
        log_info "Existing OpenSearch memory settings retained. To adjust them, run: deploy.sh opensearch set-memory <1Gi|2Gi|4Gi|8Gi|16Gi>"
    fi
}

uninstall_opensearch() {
    log_info "Uninstalling OpenSearch from namespace ${OPENSEARCH_NAMESPACE}..."

    helm uninstall "${OPENSEARCH_RELEASE_NAME}" -n "${OPENSEARCH_NAMESPACE}" 2>/dev/null || true

    # Delete PVCs (this will also trigger PV deletion if reclaim policy is Delete)
    if [[ "${OPENSEARCH_PURGE_PVC}" == "true" ]]; then
        log_warn "OPENSEARCH_PURGE_PVC=true: deleting OpenSearch PVCs (data loss!)"
        
        # Get all PVCs related to OpenSearch before deletion
        local pvc_names
        pvc_names=$(kubectl get pvc -n "${OPENSEARCH_NAMESPACE}" -l "app.kubernetes.io/instance=${OPENSEARCH_RELEASE_NAME}" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)
        
        # Delete PVCs by label
        kubectl delete pvc -n "${OPENSEARCH_NAMESPACE}" -l "app.kubernetes.io/instance=${OPENSEARCH_RELEASE_NAME}" 2>/dev/null || true
        # Also try to delete by name pattern (OpenSearch chart naming)
        kubectl delete pvc -n "${OPENSEARCH_NAMESPACE}" -l "app.kubernetes.io/name=opensearch" 2>/dev/null || true
        # Try to delete PVCs by name pattern
        local pvc_name="${OPENSEARCH_CLUSTER_NAME}-${OPENSEARCH_NODE_GROUP}-${OPENSEARCH_CLUSTER_NAME}-${OPENSEARCH_NODE_GROUP}-0"
        kubectl delete pvc -n "${OPENSEARCH_NAMESPACE}" "${pvc_name}" 2>/dev/null || true
        
        # Wait a bit for PVs to be released
        sleep 2
        
        # Try to delete PVs that are in Released state
        log_info "Cleaning up Released PVs..."
        local released_pvs
        released_pvs=$(kubectl get pv -o jsonpath='{.items[?(@.status.phase=="Released")].metadata.name}' 2>/dev/null || true)
        if [[ -n "${released_pvs}" ]]; then
            for pv in ${released_pvs}; do
                # Check if this PV was bound to one of the deleted PVCs
                local pv_claim
                pv_claim=$(kubectl get pv "${pv}" -o jsonpath='{.spec.claimRef.name}' 2>/dev/null || true)
                if [[ -n "${pv_claim}" ]] && echo "${pvc_names}" | grep -q "${pv_claim}"; then
                    log_info "Deleting Released PV: ${pv}"
                    kubectl delete pv "${pv}" 2>/dev/null || true
                fi
            done
        fi
    else
        # Even if PURGE_PVC is false, try to find and delete orphaned PVCs
        log_info "Checking for orphaned PVCs..."
        local orphaned_pvcs
        orphaned_pvcs=$(kubectl get pvc -n "${OPENSEARCH_NAMESPACE}" -l "app.kubernetes.io/instance=${OPENSEARCH_RELEASE_NAME}" -o name 2>/dev/null || true)
        if [[ -n "${orphaned_pvcs}" ]]; then
            log_warn "Found orphaned PVCs. Use OPENSEARCH_PURGE_PVC=true to delete them."
        fi
    fi

    log_info "OpenSearch uninstall done"
}
