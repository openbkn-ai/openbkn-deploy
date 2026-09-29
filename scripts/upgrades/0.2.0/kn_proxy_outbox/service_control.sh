#!/usr/bin/env bash
# Copyright openbkn.ai
#
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.

set -euo pipefail

readonly STATE_FORMAT="openbkn-kn-proxy-outbox-workload-v1"
readonly WORKLOAD="bkn-backend"

usage() {
  cat <<EOF
Usage: $0 {stop|verify-stopped|start} [options]

Options:
  --namespace NAMESPACE       Kubernetes namespace (default: openbkn)
  --expected-context CONTEXT  Optional explicit kubectl context safety check
  --state-file PATH           Replica snapshot path
  --timeout-seconds SECONDS   Stop/start timeout (default: 300)
EOF
}

action=${1:-}
case "$action" in
  stop|verify-stopped|start) shift ;;
  -h|--help) usage; exit 0 ;;
  *) usage >&2; exit 2 ;;
esac

namespace=openbkn
expected_context=
state_file=/tmp/openbkn-kn-proxy-outbox-workload.tsv
timeout_seconds=300
while (( $# > 0 )); do
  case "$1" in
    --namespace|--expected-context|--state-file|--timeout-seconds)
      (( $# >= 2 )) || { echo "missing value for $1" >&2; exit 2; }
      option=$1
      value=$2
      case "$option" in
        --namespace) namespace=$value ;;
        --expected-context) expected_context=$value ;;
        --state-file) state_file=$value ;;
        --timeout-seconds) timeout_seconds=$value ;;
      esac
      shift 2
      ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

[[ $namespace =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || {
  echo "invalid Kubernetes namespace: $namespace" >&2
  exit 2
}
[[ $timeout_seconds =~ ^[1-9][0-9]*$ ]] || {
  echo "--timeout-seconds must be a positive integer" >&2
  exit 2
}
command -v kubectl >/dev/null || { echo "kubectl is required" >&2; exit 2; }
current_context=$(kubectl config current-context)
if [[ -n $expected_context && $current_context != "$expected_context" ]]; then
  echo "kubectl context mismatch: expected $expected_context, current $current_context" >&2
  exit 2
fi
expected_context=${expected_context:-$current_context}

kubectl_target() {
  kubectl --context "$expected_context" --namespace "$namespace" "$@"
}

wait_for_replicas() {
  local expected=$1 deadline status desired replicas ready
  deadline=$(( $(date +%s) + timeout_seconds ))
  while true; do
    status=$(kubectl_target get deployment "$WORKLOAD" -o jsonpath='{.spec.replicas}{" "}{.status.replicas}{" "}{.status.readyReplicas}')
    read -r desired replicas ready <<< "$status"
    desired=${desired:-0}
    replicas=${replicas:-0}
    ready=${ready:-0}
    if [[ $expected == 0 && $desired == 0 && $replicas == 0 && $ready == 0 ]]; then
      return 0
    fi
    if [[ $expected != 0 && $desired == "$expected" && $ready == "$expected" ]]; then
      return 0
    fi
    if (( $(date +%s) >= deadline )); then
      echo "Deployment did not reach $expected desired/ready replicas: $namespace/$WORKLOAD (desired=$desired replicas=$replicas ready=$ready)" >&2
      return 1
    fi
    sleep 2
  done
}

read_state() {
  local format saved_namespace saved_context saved_workload
  IFS=$'\t' read -r format saved_namespace saved_context saved_workload saved_replicas < "$state_file"
  [[ $format == "$STATE_FORMAT" && $saved_namespace == "$namespace" &&
     $saved_context == "$expected_context" && $saved_workload == "$WORKLOAD" &&
     $saved_replicas =~ ^[0-9]+$ ]] || {
    echo "invalid or mismatched workload state file: $state_file" >&2
    exit 1
  }
}

kubectl_target get deployment "$WORKLOAD" >/dev/null
case "$action" in
  stop)
    if [[ -e $state_file ]]; then
      read_state
      replicas=$saved_replicas
    else
      replicas=$(kubectl_target get deployment "$WORKLOAD" -o jsonpath='{.spec.replicas}')
      replicas=${replicas:-1}
      [[ $replicas =~ ^[0-9]+$ ]] || { echo "invalid replica count: $replicas" >&2; exit 1; }
      temporary_state=$(mktemp "${state_file}.tmp.XXXXXX")
      printf '%s\t%s\t%s\t%s\t%s\n' "$STATE_FORMAT" "$namespace" "$expected_context" "$WORKLOAD" "$replicas" > "$temporary_state"
      mv -- "$temporary_state" "$state_file"
    fi
    kubectl_target scale deployment "$WORKLOAD" --replicas=0 >/dev/null
    wait_for_replicas 0
    ;;
  verify-stopped)
    read_state
    wait_for_replicas 0
    ;;
  start)
    read_state
    kubectl_target scale deployment "$WORKLOAD" --replicas="$saved_replicas" >/dev/null
    wait_for_replicas "$saved_replicas"
    rm -f -- "$state_file"
    ;;
esac
