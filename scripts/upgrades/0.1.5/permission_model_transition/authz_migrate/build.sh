#!/usr/bin/env bash
# Copyright openbkn.ai
#
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.

# Builds authz-migrate-<os>-<arch> for each <os>/<arch> argument. Without
# arguments it builds every executable the release bundles.

set -euo pipefail

script_directory=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$script_directory"

targets=("$@")
if [[ ${#targets[@]} -eq 0 ]]; then
  targets=(linux/amd64 linux/arm64 darwin/arm64)
fi

for target in "${targets[@]}"; do
  goos=${target%/*}
  goarch=${target#*/}
  GOOS=$goos GOARCH=$goarch CGO_ENABLED=0 \
    go build -trimpath -o "authz-migrate-$goos-$goarch" .
done
