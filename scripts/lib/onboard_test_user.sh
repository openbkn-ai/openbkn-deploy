#!/usr/bin/env bash
# Copyright openbkn.ai
#
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.

# Optional: first business test user (login: test) with all roles from openbkn admin "role list"
# (on typical full stacks this is three business admin roles, e.g. 数据/AI/应用 管理员) + openbkn re-login
# for ADP impex and other business operations (built-in  admin  often lacks  CommonAdd  on
# agent-operator;  test  with roles does).
# shellcheck source=/dev/null

# ── bkn-safe test user ──────────────────────────────────────────────────────
# bkn-safe seeds only the admin; this provisions the business "test" account
# (login: test) with the three business-admin roles (数据/AI/应用管理员;
# source=business — NOT super-admin). bkn-safe's admin create+reset forces a
# password change, so we complete it once via the self-service change-password
# endpoint to leave test directly loginable. Idempotent. Response shapes are
# bkn-safe's ({"users":[...]}, {"roles":[...]}).

# Print a user's id by login (empty if absent). bkn-safe's role-binding API
# validates accessor ids and rejects logins, so callers must bind by UUID.
onboard_bkn_safe_user_id() {
    openbkn admin --json user list --keyword "${1:-test}" --limit 200 2>/dev/null \
        | _OB_LOGIN="${1:-test}" python3 -c "
import json, os, sys
login = os.environ.get('_OB_LOGIN', 'test')
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(1)
for e in d.get('users', []):
    if (e.get('account') or '') == login:
        print(e.get('id') or '')
        sys.exit(0)
sys.exit(1)"
}

# Print test's user id (empty if absent).
onboard_bkn_safe_test_user_id() {
    onboard_bkn_safe_user_id test
}

# Assign every source=business role to <login> (idempotent; duplicates no-op).
onboard_bkn_safe_assign_business_roles() {
    local _login="${1:-test}" rjson rids _rid _uid _ok=0 _n=0
    if ! rjson="$(openbkn admin --json role list --limit 1000 2>/dev/null)"; then
        log_warn "openbkn admin role list failed; cannot assign roles to ${_login}"
        return 1
    fi
    # bkn-safe role-binding validates the accessor id and rejects logins —
    # resolve the login to its UUID first (fall back to the login for older
    # bkn-safe builds that still accept it).
    _uid="$(onboard_bkn_safe_user_id "${_login}" || true)"
    if [[ -z "${_uid}" ]]; then
        log_warn "Could not resolve user id for [${_login}]; passing the login as accessor id"
        _uid="${_login}"
    fi
    rids="$(echo "${rjson}" | python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(1)
for e in d.get('roles', []):
    if (e.get('source') or '') == 'business' and e.get('id'):
        print(e['id'])
" 2>/dev/null)" || true
    if [[ -z "${rids// }" ]]; then
        log_warn "No business roles found; none assigned to ${_login}"
        return 0
    fi
    while IFS= read -r _rid; do
        [[ -n "${_rid}" ]] || continue
        _n=$((_n + 1))
        openbkn admin user assign-role "${_uid}" "${_rid}" >/dev/null 2>&1 && _ok=$((_ok + 1))
    done <<< "${rids}"
    if [[ "${_ok}" -lt "${_n}" ]]; then
        log_warn "User [${_login}]: only ${_ok}/${_n} business roles assigned (check: openbkn admin user roles ${_login})"
    else
        log_info "User [${_login}]: ${_ok}/${_n} business roles assigned (check: openbkn admin user roles ${_login})"
    fi
    return 0
}

# Where onboard keeps the test user's password: a 0600 file, never the log.
onboard_test_user_password_file() {
    printf '%s' "${ONBOARD_TEST_USER_PASSWORD_FILE:-${HOME}/.openbkn-ai/test-user-password}"
}

# The test user's password: ONBOARD_TEST_USER_PASSWORD (or the older
# ONBOARD_DEFAULT_TEST_USER_PASSWORD) when set, else the one kept from an earlier
# run, else a new random one, kept in onboard_test_user_password_file.
onboard_test_user_password() {
    local _pw="${ONBOARD_TEST_USER_PASSWORD:-${ONBOARD_DEFAULT_TEST_USER_PASSWORD:-}}" _file
    if [[ -n "${_pw}" ]]; then
        printf '%s' "${_pw}"
        return 0
    fi
    _file="$(onboard_test_user_password_file)"
    if [[ -s "${_file}" ]]; then
        cat "${_file}"
        return 0
    fi
    _pw="$(generate_random_password 16)"
    [[ -n "${_pw}" ]] || return 1
    mkdir -p "$(dirname "${_file}")" && (umask 077 && printf '%s' "${_pw}" >"${_file}") || return 1
    printf '%s' "${_pw}"
}

# Self-service change-password for [test]; the passwords go in the request body
# on stdin, never in argv or the log. Prints the HTTP status.
onboard_bkn_safe_test_change_password() {
    local _kurl="$1" _old="$2" _new="$3"
    _OB_OLD="${_old}" _OB_NEW="${_new}" python3 -c '
import json, os
print(json.dumps({"account": "test", "old_password": os.environ["_OB_OLD"], "new_password": os.environ["_OB_NEW"]}))' |
        curl -sk -o /dev/null -w '%{http_code}' -X POST "${_kurl%/}/api/safe/v1/auth/change-password" \
            -H 'Content-Type: application/json' --data-binary @- 2>/dev/null || true
}

# Clear a pending must-change-password flag on [test] without changing the
# effective password: bounce final -> final.heal -> final through the
# self-service change-password endpoint (a successful self-service change
# clears the flag; admin reset would re-arm it). First hop 401 = the password
# is not the expected one (operator changed it) — leave the account alone.
# Heals users created by older onboard runs that left the flag set.
onboard_bkn_safe_heal_test_password_flag() {
    local _final="$1" _kurl="$2" _tmp _c1 _c2
    [[ -z "${_kurl}" ]] && return 0
    _tmp="${_final}.heal"
    _c1="$(onboard_bkn_safe_test_change_password "${_kurl}" "${_final}" "${_tmp}")"
    if [[ "${_c1}" != 20* ]]; then
        [[ "${_c1}" == "401" ]] || log_warn "test password-flag heal probe got http ${_c1}; if first login still demands a password change, reset manually: openbkn admin user reset-password <id> --password '<tmp>' then change-password to the final one."
        return 0
    fi
    _c2="$(onboard_bkn_safe_test_change_password "${_kurl}" "${_tmp}" "${_final}")"
    if [[ "${_c2}" != 20* ]]; then
        log_warn "test password is now its stored password with '.heal' appended (restore hop got http ${_c2}); change it back with the self-service change-password endpoint."
        return 1
    fi
    log_info "User [test]: cleared pending must-change-password (password unchanged)"
    return 0
}

# Create + activate the business test user for bkn-safe. Gated on bkn-safe + an
# authenticated openbkn admin session. Honors ONBOARD_SKIP_TEST_USER and
# ONBOARD_TEST_USER_PASSWORD; otherwise a random password is kept in
# onboard_test_user_password_file.
onboard_provision_bkn_safe_test_user() {
    if [[ "${ONBOARD_SKIP_TEST_USER:-false}" == "true" ]]; then
        ONBOARD_REPORT_TEST_USER="skipped: ONBOARD_SKIP_TEST_USER / --skip-test-user"
        return 0
    fi
    type onboard_bkn_safe_detected &>/dev/null && onboard_bkn_safe_detected 2>/dev/null || {
        ONBOARD_REPORT_TEST_USER="n/a (bkn-safe not detected)"
        return 0
    }
    command -v openbkn &>/dev/null || return 0
    if ! openbkn admin --json user list --limit 1 &>/dev/null; then
        onboard_log_warn "openbkn admin not signed in; skipping test-user provisioning. Run: openbkn auth login <url> -u admin -p '<pw>' -k, then re-run: $0"
        ONBOARD_REPORT_TEST_USER="skipped: openbkn admin not signed in"
        return 0
    fi

    # Let the operator choose the test password (Enter = default). Skipped for
    # -y, non-TTY, or when ONBOARD_TEST_USER_PASSWORD is preset.
    if [[ -z "${ONBOARD_TEST_USER_PASSWORD:-}" && -t 0 && "${ONBOARD_ASSUME_YES:-false}" != "true" ]]; then
        local _p1 _p2
        read -r -s -p "  Password for business user [test] [Enter = generate one]: " _p1
        echo ""
        if [[ -n "${_p1}" ]]; then
            read -r -s -p "  Confirm password: " _p2
            echo ""
            if [[ "${_p1}" == "${_p2}" ]]; then
                ONBOARD_TEST_USER_PASSWORD="${_p1}"
            else
                onboard_log_warn "Passwords do not match — generating one instead."
            fi
        fi
    fi

    local _final _tmp _kurl _id _known
    _kurl="$(onboard_default_access_base_url 2>/dev/null || true)"

    # `|| true`: the lookup exits 1 when [test] does not exist yet — the normal
    # fresh-install case — and a bare command-substitution assignment would kill
    # the whole onboard run under `set -e` with no error output.
    _id="$(onboard_bkn_safe_test_user_id || true)"
    if [[ -n "${_id}" ]]; then
        log_info "User [test] already exists (id ${_id}); re-syncing business roles…"
        onboard_bkn_safe_assign_business_roles test || true
        # Older onboard runs could leave must-change-password set (the heal is a
        # no-op when the password was changed away from the known one). Only a
        # password set or stored earlier is tried; none is generated for it.
        _known="${ONBOARD_TEST_USER_PASSWORD:-${ONBOARD_DEFAULT_TEST_USER_PASSWORD:-}}"
        [[ -n "${_known}" || ! -s "$(onboard_test_user_password_file)" ]] || _known="$(cat "$(onboard_test_user_password_file)")"
        [[ -z "${_known}" ]] || onboard_bkn_safe_heal_test_password_flag "${_known}" "${_kurl}" || true
        ONBOARD_REPORT_TEST_USER="ready (existed; business roles re-synced; password unchanged)"
        return 0
    fi

    if ! _final="$(onboard_test_user_password)"; then
        log_warn "Could not generate or store a password for user test"
        ONBOARD_REPORT_TEST_USER="error: no password for test"
        return 0
    fi
    _tmp="${_final}.init"   # admin-set temp; must differ from final

    log_info "Creating business user [test] (bkn-safe)…"
    # The SDK requires an initial password; give the temporary one by environment
    # (not argv). `|| true`: a refused create must reach the lookup below, not
    # end onboard under set -e.
    _id="$(BKN_NEW_USER_PASSWORD="${_tmp}" openbkn admin --json user create --login test --display-name test 2>/dev/null \
        | python3 -c "import json,sys
try: print(json.load(sys.stdin).get('id',''))
except Exception: pass" 2>/dev/null || true)"
    if [[ -z "${_id}" ]]; then
        _id="$(onboard_bkn_safe_test_user_id || true)"
    fi
    if [[ -z "${_id}" ]]; then
        log_warn "Could not create or find user test"
        ONBOARD_REPORT_TEST_USER="error: create user test failed"
        return 0
    fi

    # admin reset -> temp (forces must-change-password), then self-service
    # change-password temp -> final clears the flag so test is directly loginable.
    if ! openbkn admin user reset-password "${_id}" --password "${_tmp}" >/dev/null 2>&1; then
        log_warn "openbkn admin reset-password for test failed"
        ONBOARD_REPORT_TEST_USER="error: reset-password test failed"
        return 0
    fi
    local _code
    _code="$(onboard_bkn_safe_test_change_password "${_kurl}" "${_tmp}" "${_final}")"
    if [[ "${_code}" != 20* ]]; then
        log_warn "Completing test's forced password change failed (http ${_code}); test may need a first-login change."
    fi
    onboard_bkn_safe_assign_business_roles test || true
    ONBOARD_REPORT_TEST_USER="created (bkn-safe; account test, business roles assigned)"
    log_info "User [test] ready — password: $(onboard_test_user_password_hint)"
    return 0
}

# Where to find the test password, for messages (never the value itself).
onboard_test_user_password_hint() {
    if [[ -n "${ONBOARD_TEST_USER_PASSWORD:-${ONBOARD_DEFAULT_TEST_USER_PASSWORD:-}}" ]]; then
        printf 'the one set in ONBOARD_TEST_USER_PASSWORD'
    else
        printf 'stored in %s' "$(onboard_test_user_password_file)"
    fi
}
