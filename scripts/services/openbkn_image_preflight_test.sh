#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/scripts/services/openbkn.sh"
log_info() { :; }
log_error() { :; }
log_warn() { :; }
work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
_openbkn_drop_literal_env_now_from_secret() { echo mutation >>"${work}/events"; }
_openbkn_preflight_images() { echo preflight >>"${work}/events"; return "${PULL_STATUS}"; }
helm() { echo upgrade >>"${work}/events"; }
PULL_STATUS=1
if _openbkn_helm_upgrade_release vega-backend openbkn upgrade --install vega-backend chart; then exit 1; fi
[[ "$(cat "${work}/events")" == preflight ]]
: >"${work}/events"
PULL_STATUS=0
_openbkn_helm_upgrade_release vega-backend openbkn upgrade --install vega-backend chart
[[ "$(cat "${work}/events")" == $'preflight\nmutation\nupgrade' ]]
printf '%s\n' 'PASS: image preflight precedes mutations and failed pull blocks Helm upgrade'
