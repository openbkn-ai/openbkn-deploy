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

echo 'openbkn install summary tests passed'
