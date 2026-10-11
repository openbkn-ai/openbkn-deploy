#!/usr/bin/env bash
# Copyright openbkn.ai
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.
# Real Helm client rendering; all cluster operations are mocked.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
command -v helm >/dev/null || { echo 'Helm 3 with client-only template rendering is required.' >&2; exit 1; }

run_case() (
    source "${ROOT}/scripts/services/openbkn.sh"
    SCRIPT_DIR="${ROOT}"
    CASE_DIR="$(mktemp -d)"
    trap '_openbkn_audit_cleanup; rm -rf "${CASE_DIR}"' EXIT
    CONFIG_YAML_PATH="${CASE_DIR}/config.yaml"
    export KUBECONFIG="${CASE_DIR}/nonexistent-kubeconfig"
    printf 'auditPublisher:\n  environment: test\n' >"${CONFIG_YAML_PATH}"
    : >"${CASE_DIR}/upgrades"
    : >"${CASE_DIR}/cluster-calls"
    export AUDIT_TEST_SECRET_STATE=ready
    KAFKA_IMAGE=kafka:test
    WAIT_STATUS=0
    SHOULD_SKIP=false
    RELEASE_ROWS='[{"name":"agent-observability","namespace":"openbkn","revision":"1","status":"deployed","chart":"agent-observability-0.1.4"}]'
    OLD_USER='{"core":{"store":"mariadb","mariadb":{"existingSecret":"core-db"}},"resources":{"requests":{"cpu":"50m"}},"image":{"repository":"ee-custom"},"affinity":{"nodeAffinity":{}},"enterpriseBusinessProvenance":{"enabled":true},"auditPublisher":{"enabled":false,"environment":""},"kafkaConsumers":{"audit":{"enabled":false,"brokers":[],"consumerGroup":"","existingSecret":{"name":""}}}}'
    CORE_RELEASE_EXTRA_SETS=(core.store=mariadb core.mariadb.existingSecret=core-db opensearch.endpoint=old)
    CORE_RELEASE_EXTRA_SET_STRINGS=()
    CORE_SET_VALUES=()
    chart="${ROOT}/scripts/testdata/audit-install"
    args=(upgrade --install agent-observability "${chart}" --namespace openbkn -f "${CONFIG_YAML_PATH}")
    for value in "${CORE_RELEASE_EXTRA_SETS[@]}"; do args+=(--set "${value}"); done
    log_info() { :; }
    log_warn() { :; }
    log_error() { printf '%s\n' "$*" >>"${CASE_DIR}/errors"; }
    config_yaml_dep_field() { case "$2" in mqHost) printf broker ;; mqPort) printf 9092 ;; mechanism) printf PLAIN ;; esac; }
    _openbkn_drop_literal_env_now_from_secret() { printf 'drop-env\n' >>"${CASE_DIR}/cluster-calls"; }
    _openbkn_adopt_unowned_resources() { return 1; }
    _openbkn_should_skip_upgrade() { [[ "${SHOULD_SKIP}" == true ]]; }
    # Patch only the kubectl subprocess in Secret validation. This test adapter
    # also works with native Windows Python; production never imports it.
    eval "$(declare -f python3 2>/dev/null | sed '1s/python3/audit_test_real_python3/')" || true
    if ! declare -F audit_test_real_python3 >/dev/null; then
        audit_test_real_python3() { command python3 "$@"; }
    fi
    python3() {
        if [[ "${1:-}" == "${ROOT}/scripts/lib/audit_consumer.py" && "${2:-}" == validate-system ]]; then
            audit_test_real_python3 "${ROOT}/scripts/testdata/audit-install/helper_runner.py" "$@"
        else
            audit_test_real_python3 "$@"
        fi
    }
    refresh_old() {
        printf '%s\n' "${OLD_USER}" >"${CASE_DIR}/old.json"
        OLD_CURRENT="$(python3 "${ROOT}/scripts/lib/audit_helm_render.py" render "${CASE_DIR}" actual upgrade --install agent-observability "${chart}" --namespace openbkn -f "${CASE_DIR}/old.json" |
            python3 -c 'import json,sys; sys.path.insert(0,sys.argv[1]); from audit_consumer import rendered_system_values; print(json.dumps(rendered_system_values(json.load(sys.stdin))))' "${ROOT}/scripts/lib")"
    }
    refresh_old
    helm() {
        case "$1" in
            list) printf '%s\n' "${RELEASE_ROWS}" ;;
            get) if [[ " $* " == *' --all '* ]]; then printf '%s\n' "${OLD_CURRENT}"; else printf '%s\n' "${OLD_USER}"; fi ;;
            install) command helm "$@" ;;
            status) printf '{"info":{"status":"deployed"}}' ;;
            upgrade)
                printf '%s\n' "$3" >>"${CASE_DIR}/upgrades"
                python3 "${ROOT}/scripts/lib/audit_helm_render.py" render "${CASE_DIR}" actual "$@" >"${CASE_DIR}/applied.json"
                ;;
            *) echo "unexpected helm call: $1" >&2; return 1 ;;
        esac
    }
    kubectl() {
        printf '%s\n' "$*" >>"${CASE_DIR}/cluster-calls"
        case "$1" in
            create) [[ "${2:-}" == namespace ]] && return 0; cp "$3" "${CASE_DIR}/job.json"; printf audit-test-job ;;
            wait) return "${WAIT_STATUS}" ;;
            delete) return 0 ;;
            *) return 1 ;;
        esac
    }
    prepare() { _openbkn_helm_upgrade_release agent-observability openbkn --prepare-only agent-observability-0.1.4 "${args[@]}" "$@" 2>>"${CASE_DIR}/errors"; }
    apply() { _openbkn_helm_upgrade_release agent-observability openbkn "${args[@]}"; }
    assert_no_upgrade() { [[ ! -s "${CASE_DIR}/upgrades" ]]; }
    "$1"
    echo "PASS: $1"
)

repair_and_preserve() {
    prepare
    # Mutating the original input must not change the prepared AO upgrade.
    printf 'auditPublisher:\n  environment: production\n' >"${CONFIG_YAML_PATH}"
    apply
    python3 - "${CASE_DIR}/applied.json" "${ROOT}/scripts/lib" <<'PY'
import json,sys
sys.path.insert(0, sys.argv[2])
from audit_consumer import rendered_system_values
v=rendered_system_values(json.load(open(sys.argv[1])))
assert v['kafkaConsumers']['audit']['enabled'] is True
assert v['kafkaConsumers']['audit']['consumerGroup']=='bkn-trace-audit-ledger-v1'
assert v['auditPublisher']['environment']=='test'
assert v['resources']['requests']=={'cpu':'50m','memory':'128Mi'}
assert v['image']['repository']=='ee-custom'
assert v['enterpriseBusinessProvenance']['enabled'] is True
assert v['affinity']=={'nodeAffinity':{}}
PY
    grep -q 'delete job audit-test-job' "${CASE_DIR}/cluster-calls"
}

explicit_override_and_idempotency() {
    prepare --set resources.requests.cpu=0m --set 'kafkaConsumers.audit.existingSecret.name=custom-client' \
        --set 'kafkaConsumers.audit.existingSecret.usernameKey=custom-user' --set 'kafkaConsumers.audit.existingSecret.passwordKey=custom-pass'
    apply
    OLD_USER="$(python3 -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1]))["config"]))' "${CASE_DIR}/applied.json")"
    refresh_old
    SHOULD_SKIP=true
    prepare --set resources.requests.cpu=0m
    apply
    [[ "$(wc -l <"${CASE_DIR}/upgrades")" -eq 1 ]]
    python3 - "${CASE_DIR}/job.json" <<'PY'
import json,sys
env={i['name']:i for i in json.load(open(sys.argv[1]))['spec']['template']['spec']['containers'][0]['env']}
assert env['TASK_KAFKA_PASSWORD']['valueFrom']['secretKeyRef']=={'name':'custom-client','key':'custom-pass'}
PY
}

non_audit_drift_is_blocked() {
    OLD_USER="$(python3 -c 'import json,sys; v=json.loads(sys.argv[1]); v["opensearch"]={"endpoint":"custom"}; print(json.dumps(v))' "${OLD_USER}")"
    refresh_old
    if prepare; then echo 'unexpected non-Audit reset accepted' >&2; exit 1; fi
    assert_no_upgrade
    grep -q opensearch.endpoint "${CASE_DIR}/errors"
    prepare --set opensearch.endpoint=old
    apply
}

current_file_override() {
    printf 'auditPublisher:\n  environment: staging\nresources:\n  requests:\n    cpu: 0m\n' >"${CONFIG_YAML_PATH}"
    prepare
    apply
    python3 - "${CASE_DIR}/applied.json" <<'PY'
import json,sys
v=json.load(open(sys.argv[1]))['config']
assert v['resources']['requests']['cpu']=='0m'
assert v['auditPublisher']['environment']=='staging'
PY
}

negative_configuration() {
    for flag in kafkaConsumers.audit.enabled=false auditPublisher.enabled=false core.autoMigrate=false; do
        if prepare --set "${flag}"; then echo "accepted $flag" >&2; exit 1; fi
    done
    printf '{}\n' >"${CONFIG_YAML_PATH}"
    if prepare; then echo 'guessed missing environment' >&2; exit 1; fi
    printf 'auditPublisher:\n  environment: test\n' >"${CONFIG_YAML_PATH}"
    for state in missing missing-key missing-dsn timeout; do
        export AUDIT_TEST_SECRET_STATE="${state}"
        if prepare; then echo "accepted $state Secret" >&2; exit 1; fi
    done
    assert_no_upgrade
    [[ ! -s "${CASE_DIR}/cluster-calls" ]]
}

pending_and_concurrent_release() {
    RELEASE_ROWS="${RELEASE_ROWS/deployed/pending-upgrade}"
    if prepare; then echo 'accepted pending release' >&2; exit 1; fi
    [[ ! -s "${CASE_DIR}/cluster-calls" ]]
    RELEASE_ROWS="${RELEASE_ROWS/pending-upgrade/deployed}"
    prepare
    RELEASE_ROWS="${RELEASE_ROWS/\"1\"/\"2\"}"
    if apply; then echo 'overwrote concurrent revision' >&2; exit 1; fi
    assert_no_upgrade
    ! grep -q drop-env "${CASE_DIR}/cluster-calls"
}

topic_failure_stops_and_cleans() {
    WAIT_STATUS=1
    if prepare; then echo 'accepted failed topic prerequisite' >&2; exit 1; fi
    assert_no_upgrade
    grep -q 'delete job audit-test-job' "${CASE_DIR}/cluster-calls"
    ! grep -q drop-env "${CASE_DIR}/cluster-calls"
}

fresh_install_and_chart_upgrade() {
    RELEASE_ROWS='[]'
    prepare
    apply
    RELEASE_ROWS='[{"name":"agent-observability","namespace":"openbkn","revision":"1","status":"deployed","chart":"agent-observability-0.1.3"}]'
    prepare
    apply
    [[ "$(wc -l <"${CASE_DIR}/upgrades")" -eq 2 ]]
}

preflight_precedes_application_loop() {
    OFFLINE_MODE=true
    OPENBKN_SKIP_PLATFORM_BOOTSTRAP=false
    _openbkn_resolve_latest_manifest() { :; }
    _openbkn_require_version_manifest() { :; }
    _openbkn_apply_default_set_values() { :; }
    ensure_platform_prerequisites() { :; }
    reconcile_openbkn_firewall() { :; }
    _openbkn_resolve_target_namespace() { printf openbkn; }
    _openbkn_resolve_charts_dir() { printf '%s' "${CASE_DIR}"; }
    _openbkn_release_exists() { return 1; }
    init_openbkn_databases() { :; }
    bkn_mapfile_compat() { local var="$1"; shift; mapfile -t "${var}" < <("$@"); }
    _openbkn_release_names() { printf '%s\n' unrelated-before agent-observability unrelated-after; }
    _openbkn_prepare_trace_profile() { :; }
    _openbkn_warn_unwired_evidence_producers() { :; }
    _openbkn_resolve_release_version() { printf 0.1.4; }
    _install_openbkn_release_local() {
        if [[ "${4:-}" == --prepare-only ]]; then prepare; else printf '%s\n' "$1" >>"${CASE_DIR}/upgrades"; fi
    }
    WAIT_STATUS=1
    if install_openbkn; then echo 'installer reported success after preflight failure' >&2; exit 1; fi
    assert_no_upgrade
    # A manifest not containing AO must not acquire an Audit gate.
    _openbkn_release_names() { printf '%s\n' unrelated-only; }
    _openbkn_uninstall_retired_releases() { :; }
    gen_install_status_json() { return 1; }
    if install_openbkn; then exit 1; fi
    grep -q unrelated-only "${CASE_DIR}/upgrades"
}

for case in repair_and_preserve explicit_override_and_idempotency non_audit_drift_is_blocked current_file_override negative_configuration pending_and_concurrent_release topic_failure_stops_and_cleans fresh_install_and_chart_upgrade preflight_precedes_application_loop; do
    run_case "${case}"
done

# The public CLI must propagate an installer failure even when called in an if.
(
    source "${ROOT}/deploy.sh"
    parse_openbkn_args() { :; }
    confirm_access_address_before_install() { :; }
    install_openbkn() { return 17; }
    result=0
    main openbkn install || result=$?
    [[ "${result}" -eq 17 ]]
)
echo 'openbkn_audit_consumer_test: PASS'
