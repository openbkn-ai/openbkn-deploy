#!/usr/bin/env bash
# Copyright openbkn.ai
# Licensed under the Apache License, Version 2.0. See LICENSE in the project root.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/config.sh"
test_dir="$(mktemp -d)"
trap 'rm -rf "${test_dir}"' EXIT

_config_observability_block "${test_dir}/missing.yaml" > "${test_dir}/defaults.yaml"
[[ "$(<"${test_dir}/defaults.yaml")" == *'collectorMetricsEndpoint: http://otelcol-contrib:8888/metrics'* ]]
[[ "$(<"${test_dir}/defaults.yaml")" == *'opensearchCapacityThreshold: ""'* ]]

cat > "${test_dir}/expected.yaml" <<'EOF'
observability:
  sourceTimeout: 2s
  admissionBudget:
    profile: production
    collectorMetricsEndpoint: https://collector.example/custom-metrics
    opensearchCapacityThreshold: 0.91
    opensearchHeapThreshold: 0.72
    collectorQueueThreshold: 0.63
    storagePoolThreshold: 0.54
EOF
{
    printf 'namespace: other-namespace\n'
    cat "${test_dir}/expected.yaml"
    printf 'depServices:\n  rds:\n    password: do-not-copy\n'
} > "${test_dir}/existing.yaml"
_config_observability_block "${test_dir}/existing.yaml" > "${test_dir}/actual.yaml"
diff -u "${test_dir}/expected.yaml" "${test_dir}/actual.yaml"

# A subsequent regeneration must keep the same values, including other
# observability settings and a nonstandard Collector deployment topology.
_config_observability_block "${test_dir}/actual.yaml" > "${test_dir}/again.yaml"
diff -u "${test_dir}/expected.yaml" "${test_dir}/again.yaml"

printf 'observability: {sourceTimeout: 2s}\nnamespace: other\n' > "${test_dir}/inline.yaml"
[[ "$(_config_observability_block "${test_dir}/inline.yaml")" == 'observability: {sourceTimeout: 2s}' ]]
printf 'PASS: observability config defaults and preservation\n'
