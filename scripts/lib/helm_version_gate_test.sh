#!/usr/bin/env bash
# Copyright openbkn.ai
#
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.
# Helm 4 is refused before any release changes; Helm 3 passes. No cluster access.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${SCRIPT_DIR}/scripts/lib/common.sh"
MAC_DEV_ROOT="${SCRIPT_DIR}/dev"
source "${SCRIPT_DIR}/dev/lib/mac_common.sh"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }

fake_helm() {
    printf '#!/usr/bin/env bash\ncase "$*" in\n  "version --short"*) echo "v%s+gabc" ;;\n  *) echo "version.BuildInfo{Version:\\"v%s\\", GitCommit:\\"x\\"}" ;;\nesac\n' "$1" "$1" >"${work}/helm"
    chmod +x "${work}/helm"
}
PATH="${work}:${PATH}"
log_error() { echo "ERROR: $*" >>"${work}/errors"; }
log_warn() { :; }
install_helm() { echo installed >>"${work}/installs"; }

fake_helm 3.22.1
bkn_require_supported_helm || fail "Helm 3 must pass"
mac_helm_version_ok_for_charts 3.22.1 || fail "doctor must accept Helm 3"
ensure_helm_available || fail "ensure_helm_available must accept Helm 3"

fake_helm 4.3.0
if bkn_require_supported_helm; then fail "Helm 4 must be refused"; fi
grep -q 'Helm 4.3.0 is not supported: the installer needs Helm 3' "${work}/errors" || fail "refusal must name the version"
if mac_helm_version_ok_for_charts 4.3.0; then fail "doctor must flag Helm 4"; fi
[[ "$(mac_doctor_hint_for_tool helm)" == *"helm@3"* ]] || fail "doctor hint must install helm@3"

OPENBKN_SKIP_PLATFORM_BOOTSTRAP=true
if ensure_helm_available; then fail "bring-your-own cluster must not continue on Helm 4"; fi
[[ ! -e "${work}/installs" ]] || fail "the operator's Helm must not be replaced on a bring-your-own cluster"

# The real entry point stops before any release work on a bring-your-own cluster.
printf '#!/usr/bin/env bash\necho "$*" >>"%s/helm-calls"\necho "v4.3.0+gabc"; echo "version.BuildInfo{Version:\\"v4.3.0\\"}"\n' "${work}" >"${work}/helm"
chmod +x "${work}/helm"
printf '#!/usr/bin/env bash\necho "$*" >>"%s/kubectl-calls"\nexit 1\n' "${work}" >"${work}/kubectl"
chmod +x "${work}/kubectl"
: >"${work}/kubeconfig"
set +e
KUBECONFIG="${work}/kubeconfig" OPENBKN_SKIP_PLATFORM_BOOTSTRAP=true CONFIG_YAML_PATH="${work}/none.yaml" \
    bash "${SCRIPT_DIR}/deploy.sh" data-services install >"${work}/deploy-out" 2>&1
rc=$?
set -e
[[ "${rc}" -ne 0 ]] || fail "deploy.sh must refuse Helm 4"
grep -q 'Helm 4.3.0 is not supported' "${work}/deploy-out" || fail "deploy.sh must say why: $(tail -3 "${work}/deploy-out")"
if grep -qv '^version' "${work}/helm-calls"; then fail "only 'helm version' may run before the refusal: $(cat "${work}/helm-calls")"; fi

echo "helm_version_gate_test: passed"
