#!/usr/bin/env bash
# Copyright openbkn.ai
#
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.

# Compatibility entry point; deploy and onboard share the same implementation.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/oss_storage.sh"

onboard_provision_oss_default_storage() {
    oss_provision_default_storage "$@"
}
