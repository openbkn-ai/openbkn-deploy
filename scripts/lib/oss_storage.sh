#!/usr/bin/env bash
# Copyright openbkn.ai
#
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.

# Shared, best-effort post-install provisioning; no platform login is required.
# OSS_DEFAULT_BUCKET / OSS_DEFAULT_STORAGE_NAME override bkn-oss / minio-default.
# ONBOARD_SKIP_OSS_STORAGE=true retains the existing opt-out for both callers.
# OSS_STORAGE_READY_TIMEOUT bounds each Kubernetes readiness wait (default 120s).
oss_provision_default_storage() {
    local ns="${1:-${NAMESPACE:-openbkn}}"
    [[ "${ONBOARD_SKIP_OSS_STORAGE:-false}" == true ]] && return 0
    if ! _oss_provision_default_storage "${ns}"; then
        log_warn "Default OSS storage initialization failed; platform installation is not affected. File assets / skill registration may be unavailable."
        local retry
        printf -v retry 'sudo env NAMESPACE=%q CONFIG_YAML_PATH=%q bash %q -y --skip-test-user' \
            "${ns}" "${CONFIG_YAML_PATH:-${HOME}/.openbkn-ai/config.yaml}" "${SCRIPT_DIR}/onboard.sh"
        log_warn "After fixing the cause, retry: ${retry}"
    fi
    return 0
}

# A subshell confines cleanup traps, including when sourced by deploy.sh.
_oss_provision_default_storage() (
    local ns="$1" bucket="${OSS_DEFAULT_BUCKET:-bkn-oss}" sname="${OSS_DEFAULT_STORAGE_NAME:-minio-default}"
    local dep
    for dep in kubectl curl python3; do
        command -v "${dep}" >/dev/null 2>&1 || { log_warn "OSS storage: missing ${dep}"; return 1; }
    done
    # Missing services are allowed for installations without these components.
    local services
    services="$(kubectl get svc -n "${ns}" oss-gateway-backend minio --ignore-not-found -o name 2>/dev/null)" || {
        log_warn "OSS storage: cannot query services in ${ns}"; return 1;
    }
    if [[ "${services}" != *service/oss-gateway-backend* || "${services}" != *service/minio* ]]; then
        log_info "OSS storage: skipping (OSS Gateway or bundled MinIO is not installed in ${ns})"
        return 0
    fi
    kubectl rollout status deployment/oss-gateway-backend -n "${ns}" --timeout="${OSS_STORAGE_READY_TIMEOUT:-120s}" >/dev/null 2>&1 || {
        log_warn "OSS storage: OSS Gateway did not become ready"; return 1;
    }

    local pf="" pf_log="" lport="" base="" i resp storage_id
    pf_log="$(mktemp)" || return 1
    trap 'if [[ -n "${pf}" ]]; then kill "${pf}" 2>/dev/null || true; wait "${pf}" 2>/dev/null || true; fi; rm -f "${pf_log}"' EXIT
    # Let kubectl choose an available loopback port rather than reserving 18080.
    kubectl port-forward --address=127.0.0.1 -n "${ns}" svc/oss-gateway-backend :8080 >"${pf_log}" 2>&1 &
    pf=$!
    for ((i=0; i<15; i++)); do
        lport="$(sed -n 's/.*Forwarding from 127\.0\.0\.1:\([0-9]*\) .*/\1/p' "${pf_log}" | head -n 1)"
        [[ -n "${lport}" ]] && break
        kill -0 "${pf}" 2>/dev/null || break
        sleep 1
    done
    [[ -n "${lport}" ]] || { log_warn "OSS storage: port-forward failed to start"; return 1; }
    base="http://127.0.0.1:${lport}/api/v1"
    resp="$(curl -fsS --max-time 15 "${base}/storages?enabled=true&is_default=true" 2>/dev/null)" || {
        log_warn "OSS storage: default-storage query failed"; return 1;
    }
    storage_id="$(printf '%s' "${resp}" | _oss_storage_response default)" || {
        log_warn "OSS storage: invalid default-storage query response"; return 1;
    }
    if [[ -n "${storage_id}" ]]; then
        log_info "OSS default storage already configured; skipping"
        return 0
    fi

    kubectl rollout status deployment/minio -n "${ns}" --timeout="${OSS_STORAGE_READY_TIMEOUT:-120s}" >/dev/null 2>&1 || {
        log_warn "OSS storage: MinIO did not become ready"; return 1;
    }
    local mpod muser mpass body
    mpod="$(kubectl get pod -n "${ns}" -l component=minio --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)" || { log_warn "OSS storage: cannot locate MinIO pod"; return 1; }
    [[ -n "${mpod}" ]] || { log_warn "OSS storage: no running MinIO pod"; return 1; }
    muser="$(kubectl exec -n "${ns}" "${mpod}" -- printenv MINIO_ROOT_USER 2>/dev/null)" || { log_warn "OSS storage: cannot read MinIO user"; return 1; }
    mpass="$(kubectl exec -n "${ns}" "${mpod}" -- printenv MINIO_ROOT_PASSWORD 2>/dev/null)" || { log_warn "OSS storage: cannot read MinIO password"; return 1; }
    [[ -n "${muser}" && -n "${mpass}" ]] || { log_warn "OSS storage: missing MinIO credentials"; return 1; }
    # Pass the bucket as an argument; never interpolate it into shell source.
    # --ignore-existing makes retries safe while preserving actual failures.
    if ! kubectl exec -n "${ns}" "${mpod}" -- sh -c \
        'mc alias set kwoss http://127.0.0.1:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1 && mc mb --ignore-existing "kwoss/$1" >/dev/null 2>&1' sh "${bucket}" >/dev/null 2>&1; then
        log_warn "OSS storage: could not create MinIO bucket '${bucket}'; storage registration skipped"
        return 1
    fi
    body="$(MUSER="${muser}" MPASS="${mpass}" SNAME="${sname}" BUCKET="${bucket}" EP="http://minio.${ns}.svc.cluster.local:9000" python3 -c '
import json, os
print(json.dumps(dict(storage_name=os.environ["SNAME"], vendor_type="ECEPH",
    endpoint=os.environ["EP"], bucket_name=os.environ["BUCKET"],
    access_key_id=os.environ["MUSER"], access_key_secret=os.environ["MPASS"],
    region="us-east-1", is_default=True)))')" || return 1
    # stdin avoids putting credentials in curl argv or diagnostic output.
    resp="$(printf '%s' "${body}" | curl -fsS --max-time 15 -X POST "${base}/storages" -H 'Content-Type: application/json' --data-binary @- 2>/dev/null)" || {
        log_warn "OSS storage: registration request failed (check OSS Gateway logs for conflicts or service errors)"; return 1;
    }
    storage_id="$(printf '%s' "${resp}" | _oss_storage_response create)" || {
        log_warn "OSS storage: registration response did not confirm success"; return 1;
    }
    resp="$(curl -fsS --max-time 15 -X POST "${base}/storages/${storage_id}/check" 2>/dev/null)" || {
        log_warn "OSS storage: registered storage failed its connection check"; return 1;
    }
    printf '%s' "${resp}" | _oss_storage_response check || {
        log_warn "OSS storage: connection check did not confirm success"; return 1;
    }
    log_info "Default OSS storage ready: '${sname}' (MinIO bucket '${bucket}')"
)

_oss_storage_response() {
    python3 -c '
import json, sys
try:
    obj = json.load(sys.stdin)
    assert isinstance(obj, dict) and not obj.get("code") and not obj.get("res")
    data = obj.get("data")
    mode = sys.argv[1]
    if mode == "default":
        assert "data" in obj and (data is None or isinstance(data, list))
        rows = data or []
        assert all(isinstance(row, dict) and row.get("storage_id") for row in rows)
        print(rows[0]["storage_id"] if rows else "")
    elif mode == "create":
        assert data.get("status") == "ok" and data.get("id")
        print(data["id"])
    else:
        assert data.get("connected") is True
except (ValueError, AssertionError, AttributeError, TypeError, KeyError):
    sys.exit(1)
' "$1"
}
