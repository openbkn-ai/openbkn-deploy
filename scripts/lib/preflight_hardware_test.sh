#!/usr/bin/env bash
set -euo pipefail

# Exercise the actual checks without reading host hardware or changing a host.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${PREFLIGHT_CHECKS_FILE:-${SCRIPT_DIR}/preflight_checks.sh}"

log_info() { :; }
_preflight_log_line() { :; }
preflight_skip() { [[ "${MOCK_SKIP:-false}" == true ]]; }
nproc() { echo "${MOCK_CPU}"; }
free() {
    local memory="${MOCK_MEMORY_BYTES}"
    case "$*" in
        -b) ;;
        -m) [[ "${memory}" =~ ^[0-9]+$ ]] && memory=$((memory / 1048576)) ;;
        *) echo 'unexpected free arguments' >&2; return 1 ;;
    esac
    printf 'Mem: %s 0 0 0 0 0\n' "${memory}"
}
df() {
    printf 'Filesystem 1024-blocks Used Available Capacity Mounted\n'
    printf 'mock 999999999 0 %s 0%% /\n' "${MOCK_DISK_KIB}"
}

GREEN='' YELLOW='' RED='' NC=''
PREFLIGHT_OUTPUT_JSON=true
PREFLIGHT_REPORT_FILE=''

check_case() {
    local name="$1" cpu="$2" memory="$3" disk="$4" ok="$5" warn="$6" fail="$7"
    MOCK_CPU="${cpu}" MOCK_MEMORY_BYTES="${memory}" MOCK_DISK_KIB="${disk}"
    preflight_reset_counters
    preflight_check_hardware
    preflight_check_containerd_disk
    if [[ "${PREFLIGHT_OK_COUNT}/${PREFLIGHT_WARN_COUNT}/${PREFLIGHT_FAIL_COUNT}" != "${ok}/${warn}/${fail}" ]]; then
        printf 'FAIL %s: expected OK/WARN/FAIL=%s/%s/%s, got %s/%s/%s\n' \
            "${name}" "${ok}" "${warn}" "${fail}" \
            "${PREFLIGHT_OK_COUNT}" "${PREFLIGHT_WARN_COUNT}" "${PREFLIGHT_FAIL_COUNT}" >&2
        printf '%s\n' "${PREFLIGHT_JSON_OK[@]}" "${PREFLIGHT_JSON_WARN[@]}" "${PREFLIGHT_JSON_FAIL[@]}" >&2
        exit 1
    fi
}

# Exact decimal GB boundaries, including the independent image-directory check.
check_case minimum 4 8000000000 48828125 0 5 0
check_case recommended 16 32000000000 488281250 5 0 0
check_case below-minimum-cpu 3 32000000000 488281250 4 0 1
check_case below-minimum-memory 16 7999999999 488281250 4 0 1
check_case below-minimum-disk 16 32000000000 48828124 2 0 3
check_case below-recommended-cpu 15 32000000000 488281250 4 1 0
check_case below-recommended-memory 16 31999999999 488281250 4 1 0
check_case below-recommended-disk 16 32000000000 488281249 2 3 0
check_case above-recommended 32 64000000000 976562500 5 0 0
check_case unreadable-memory 16 unknown 488281250 4 1 0
check_case unreadable-disk 16 32000000000 unknown 2 3 0
check_case zero-free-disk 16 32000000000 0 2 0 3
MOCK_SKIP=true
check_case skipped 1 1 0 0 0 0

echo 'OK preflight_hardware_test.sh'
