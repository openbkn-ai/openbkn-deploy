#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TOPIC_TEST_DIR="$(mktemp -d)"
trap 'rm -rf "${TOPIC_TEST_DIR}"' EXIT
KAFKA_IMAGE=kafka:test
TOPIC_WAIT_STATUS=0
CORE_SET_VALUES=()
source "${SCRIPT_DIR}/scripts/services/openbkn.sh"
log_error() { printf '%s\n' "$*" >>"${TOPIC_TEST_DIR}/errors"; }
config_yaml_dep_field() {
    case "$2" in mqHost) printf broker.example ;; mqPort) printf 9092 ;; esac
}
kubectl() {
    printf '%s\n' "$*" >>"${TOPIC_TEST_DIR}/calls"
    case "$1" in
        create) cp "$3" "${TOPIC_TEST_DIR}/manifest.json"; printf topic-test-job ;;
        wait) return "${TOPIC_WAIT_STATUS}" ;;
        delete) return 0 ;;
        *) return 1 ;;
    esac
}

_openbkn_prepare_trace_evidence_topic test
grep -q 'delete job topic-test-job -n test' "${TOPIC_TEST_DIR}/calls"
python3 - "${TOPIC_TEST_DIR}/manifest.json" <<'PY'
import json,sys
x=json.load(open(sys.argv[1])); e={v['name']:v for v in x['spec']['template']['spec']['containers'][0]['env']}
assert e['TASK_KAFKA_BROKERS']['value']=='broker.example:9092'
assert e['TASK_KAFKA_PASSWORD']['valueFrom']['secretKeyRef']['name']=='bkn-trace-evidence-kafka'
PY

TOPIC_WAIT_STATUS=1
: >"${TOPIC_TEST_DIR}/calls"
if _openbkn_prepare_trace_evidence_topic test; then echo 'FAIL: wait failure must stop'; exit 1; fi
grep -q 'delete job topic-test-job -n test' "${TOPIC_TEST_DIR}/calls"
grep -q 'Existing CreateTime topics require an operator-reviewed migration' "${TOPIC_TEST_DIR}/errors"

CORE_SET_VALUES=(kafkaConsumers.evidence.enabled=false)
: >"${TOPIC_TEST_DIR}/calls"
_openbkn_prepare_trace_evidence_topic test
[[ ! -s "${TOPIC_TEST_DIR}/calls" ]]

TOPIC_WAIT_STATUS=0
CORE_SET_VALUES=('kafkaConsumers.evidence.brokers[0]=other.example:9092' 'kafkaConsumers.evidence.existingSecret.name=custom-secret' 'kafkaConsumers.evidence.saslMechanism=SCRAM-SHA-256')
_openbkn_prepare_trace_evidence_topic test
python3 - "${TOPIC_TEST_DIR}/manifest.json" <<'PY'
import json,sys
x=json.load(open(sys.argv[1])); e={v['name']:v for v in x['spec']['template']['spec']['containers'][0]['env']}
assert e['TASK_KAFKA_BROKERS']['value']=='other.example:9092'
assert e['TASK_KAFKA_MECHANISM']['value']=='SCRAM-SHA-256'
assert e['TASK_KAFKA_PASSWORD']['valueFrom']['secretKeyRef']['name']=='custom-secret'
PY

CORE_SET_VALUES=('kafkaConsumers.evidence.saslMechanism=invalid')
: >"${TOPIC_TEST_DIR}/calls"
if _openbkn_prepare_trace_evidence_topic test 2>/dev/null; then echo 'FAIL: invalid mechanism must stop'; exit 1; fi
[[ ! -s "${TOPIC_TEST_DIR}/calls" ]]

echo 'openbkn_trace_topic_test: PASS (bounded Job, cleanup, failed wait, overrides, invalid settings)'
