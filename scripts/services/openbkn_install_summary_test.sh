#!/usr/bin/env bash
# Regression coverage for the post-install browser entry point.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source_file="${script_dir}/openbkn.sh"

summary_block="$(sed -n '/Open Studio to verify your installation:/,/Platform account credentials/p' "${source_file}")"

rg -q 'Open Studio to verify your installation:' <<<"${summary_block}"
rg -q 'administrators can open Service Operations from the user menu' <<<"${summary_block}"
rg -Fq '${_scheme}://${_host}/studio' <<<"${summary_block}"
rg -Fq '${_scheme}://${_host}:${_port}/studio' <<<"${summary_block}"
if rg -q '/install-status' <<<"${summary_block}"; then
    echo 'post-install summary must direct users to Studio, not the token-gated install-status endpoint' >&2
    exit 1
fi

# Install output is kept and shared: it names where secrets are stored, never their values.
for f in openbkn.sh mariadb.sh redis.sh kafka.sh opensearch.sh; do
    if rg -n '(echo|log_[a-z]+)[^|#]*\$\{(_initial_pwd|MARIADB_PASSWORD|MARIADB_ROOT_PASSWORD|redis_password|KAFKA_CLIENT_PASSWORD|OPENSEARCH_INITIAL_ADMIN_PASSWORD)\}[^|]*$' "${script_dir}/${f}"; then
        echo "${f} must not print secret values" >&2
        exit 1
    fi
done
rg -Fq 'password: bknSafe.initialPassword in ${CONFIG_YAML_PATH}' "${source_file}"

echo 'openbkn install summary tests passed'
