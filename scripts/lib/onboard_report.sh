#!/usr/bin/env bash
# Completion report for deploy/onboard.sh. Source after onboard libs; call onboard_print_completion_report on success.
# Opt out: ONBOARD_NO_COMPLETION_REPORT=1
# shellcheck source=/dev/null

# Optional state (set by probe steps):
#   ONBOARD_REPORT_MAIN_MODE   interactive | bkn-only | config-yaml
#   ONBOARD_REPORT_TEST_USER  human-readable status
#   ONBOARD_REPORT_MODELS         human-readable status (e.g. "skipped — N LLM, M small/embedding already registered")
#   ONBOARD_REPORT_BKN_CM         human-readable status (e.g. "skipped — already patched (defaultSmallModelName=…)")

# onboard_bkn_safe_detected — true when the bkn-safe auth stack is present
# (helm release "bkn-safe" or a bkn-safe Deployment in any namespace). bkn-safe
# installs into the shared openbkn namespace, so match the release/deploy name
# rather than a dedicated namespace. Lives here so the report has it in every
# mode. The listing is read whole first: `grep -q` closing a pipe early makes
# the writer fail with SIGPIPE, which pipefail turns into "not detected".
onboard_bkn_safe_detected() {
    local _names=""
    if command -v helm &>/dev/null; then
        _names="$(helm list -A -q 2>/dev/null || true)"
        grep -qx 'bkn-safe' <<<"${_names}" && return 0
    fi
    _names="$(kubectl get deploy -A -o custom-columns=NAME:.metadata.name --no-headers 2>/dev/null || true)"
    grep -qx 'bkn-safe' <<<"${_names}"
}

onboard_print_completion_report() {
    if [[ "${ONBOARD_NO_COMPLETION_REPORT:-}" == "1" || "${ONBOARD_NO_COMPLETION_REPORT:-}" == "true" ]]; then
        return 0
    fi

    local _isfu _line _kwh _kctx _acurl _isf _isf_styled _adm_pwd _tpw
    _isfu="${ONBOARD_REPORT_TEST_USER:-}"
    _line="--------------------------------------------"
    # Platform account credentials (NOT database passwords): only where they are
    # kept. The report ends up in logs, so it never carries a password.
    _adm_pwd="$(config_yaml_top_field bknSafe initialPassword 2>/dev/null || true)"
    if type onboard_test_user_password_hint &>/dev/null; then
        _tpw="$(onboard_test_user_password_hint)"
    else
        _tpw="see ONBOARD_TEST_USER_PASSWORD_FILE"
    fi

    if type onboard_bkn_safe_detected &>/dev/null && onboard_bkn_safe_detected 2>/dev/null; then
        _isf="bkn-safe"
        _isf_styled="${GREEN}${_isf}${NC}"
    else
        _isf="bkn-safe missing"
        _isf_styled="${YELLOW}${_isf}${NC}"
    fi

    if command -v openbkn &>/dev/null; then
        _kwh="$(openbkn --version 2>/dev/null | head -1 || true)"
    else
        _kwh="(openbkn not on PATH)"
    fi

    if command -v kubectl &>/dev/null; then
        _kctx="$(kubectl config current-context 2>/dev/null || echo "(kubectl context not set)")"
    else
        _kctx="(kubectl not on PATH or not configured)"
    fi

    if type onboard_default_access_base_url &>/dev/null; then
        _acurl="$(onboard_default_access_base_url 2>/dev/null || true)"
    else
        _acurl="${ONBOARD_DEFAULT_ACCESS_BASE:-(set ONBOARD_DEFAULT_ACCESS_BASE or use default host IP)}"
    fi

    {
        echo ""
        echo "============================================"
        echo "  BKN Foundry Onboard — completion report"
        echo "  Time (UTC)  $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
        echo "  Mode        ${ONBOARD_REPORT_MAIN_MODE:-interactive}"
        echo "${_line}"
        echo "  Environment host=$(hostname 2>/dev/null || echo '?')"
        echo "  Node          $(command -v node &>/dev/null && node -v || echo '—')"
        echo "  openbkn       ${_kwh}"
        echo "  kubectl       ctx=${_kctx}  namespace=${NAMESPACE:-bkn}"
        echo "  Default base  ${_acurl}"
        echo "${_line}"
        echo -e "  Install type   ${_isf_styled}"
        echo "  User [test]    ${_isfu:-(not run or not recorded)}"
        echo "  Models         ${ONBOARD_REPORT_MODELS:-(not run or not recorded)}"
        echo "  BKN ConfigMap  ${ONBOARD_REPORT_BKN_CM:-(not run or not recorded)}"
        echo "${_line}"
        case "${_isfu}" in
            created*)
                echo -e "  ${GREEN}✓ User [test] was created for the first time on this platform.${NC}"
                echo "${_line}"
                ;;
            ready*)
                echo -e "  ${GREEN}✓ User [test] is ready on this platform (already existed; roles re-synced).${NC}"
                echo "${_line}"
                ;;
        esac
        echo "  Platform accounts (initial passwords, NOT database credentials)"
        if [[ -n "${_adm_pwd}" ]]; then
            echo "   • admin:  initial password is bknSafe.initialPassword in ${CONFIG_YAML_PATH:-config.yaml} (a change is forced on first login)"
        else
            echo "   • admin:  initial password not recorded here; see bknSafe.initialPassword in ${CONFIG_YAML_PATH:-config.yaml}"
        fi
        case "${_isfu}" in
            created*)
                echo "   • test:   password ${_tpw}  (business user created by onboard)"
                ;;
            ready*)
                echo "   • test:   already existed — password unchanged"
                ;;
        esac
        echo "${_line}"
        echo "  Next steps"
        case "${_isfu}" in
            created*)
                echo "   • User test:  sign-in:  openbkn auth login ${_acurl} -u test -p '<password>' -k"
                ;;
            ready*)
                echo "   • User test:  sign-in:  openbkn auth login ${_acurl} -u test -p '<password>' -k"
                ;;
        esac
        # (no business-domain flag: the old ${_bd} was never set, which under
        # onboard.sh's set -u cut the report short.)
        echo "   • Verify:    openbkn bkn list"
        echo "   • Tools:     openbkn context info   (Context Loader tool catalog, served over MCP)"
        echo "   • Docs:      https://github.com/openbkn-ai/bkn-foundry/blob/main/help/README.md"
        echo "                https://github.com/openbkn-ai/bkn-foundry/blob/main/help/en/README.md  (EN)"
        echo "                https://github.com/openbkn-ai/bkn-foundry/blob/main/help/zh/README.md  (中文)"
        echo "============================================"
        echo ""
    } 2>/dev/null || {
        echo ""
        echo "============================================"
        echo "  BKN Foundry Onboard — done"
        echo "============================================"
        echo ""
    }
}
