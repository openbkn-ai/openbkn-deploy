#!/usr/bin/env bash
# Regression coverage for main/release chart selection and --latest branch detection.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

python3 - "${script_dir}/gen-dev-manifest.sh" <<'PY'
import ast
import re
import sys

source = open(sys.argv[1]).read()
match = re.search(r"MAIN_BUILD=re\.compile\(r'([^']+)'\)", source)
assert match, "MAIN_BUILD pattern not found"
pattern = re.compile(match.group(1))
assert pattern.fullmatch("0.1.5-main.20260918003916.sha1643ce0")
assert not pattern.fullmatch("0.1.5-feature-main.20260918003916.sha1643ce0")

embedded = re.search(r"ORG=.*?python3 - <<'PY'\n(.*)\nPY\n", source, re.DOTALL)
assert embedded, "embedded Python resolver not found"
tree = ast.parse(embedded.group(1))
functions = [
    node for node in tree.body
    if isinstance(node, ast.FunctionDef)
    and node.name in {"sanitize", "branch_channel", "newest_branch_build"}
]
namespace = {"re": re}
exec(compile(ast.fix_missing_locations(ast.Module(body=functions, type_ignores=[])),
             "<resolver>", "exec"), namespace)
branch_channel = namespace["branch_channel"]
newest_branch_build = namespace["newest_branch_build"]

assert branch_channel("release/2.0.0") == ("release", "2.0.0")
assert branch_channel("main") == ("main", None)
tags = [
    "2.0.0-release.20261009100000.sha1111111",
    "2.0.0-release.20261009120000.sha2222222",
    "2.0.0-release-2.0.0.20261009130000.sha3333333",
    "0.2.0-release.20261010140000.sha4444444",
]
assert newest_branch_build(tags, "release", "2.0.0") == tags[2]
assert newest_branch_build(tags[:2], "release", "2.0.0") == tags[1]
assert newest_branch_build(tags, "release", "2.0.1") is None
assert newest_branch_build(tags, "main") is None
PY

test_bin="$(mktemp -d)"
trap 'rm -rf "${test_bin}"' EXIT
cat > "${test_bin}/git" <<'SH'
#!/usr/bin/env bash
if [[ "$1" == "-C" && "$3 $4 $5 $6" == "symbolic-ref --quiet --short HEAD" && -n "${TEST_GIT_BRANCH:-}" ]]; then
    printf '%s\n' "${TEST_GIT_BRANCH}"
    exit 0
fi
exit 1
SH
chmod +x "${test_bin}/git"

assert_latest_branch() {
    local branch="$1" expected="$2" output status
    set +e
    output="$(cd "${test_bin}" && PATH="${test_bin}:${PATH}" TEST_GIT_BRANCH="${branch}" \
        bash "${script_dir}/gen-dev-manifest.sh" --latest --template=/missing 2>&1)"
    status=$?
    set -e
    [[ ${status} -ne 0 ]] || { echo "--latest unexpectedly succeeded for ${branch}" >&2; exit 1; }
    [[ "${output}" == *"${expected}"* ]] || { echo "unexpected --latest output for ${branch}: ${output}" >&2; exit 1; }
}

assert_latest_branch "main" "current branch is main; resolving newest main builds"
assert_latest_branch "release/2.0.0" "current branch is release/2.0.0; resolving its newest builds"
assert_latest_branch "feature/my-work" "only supports main or release/X.Y.Z"
assert_latest_branch "release/2.0" "only supports main or release/X.Y.Z"
assert_latest_branch "" "detached HEAD is not supported"
