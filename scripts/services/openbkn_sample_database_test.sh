#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${SCRIPT_DIR}/scripts/services/openbkn.sh"
resolve_openbkn_image_registry() { printf '%s' "${1:-offline.example:5000}"; }
compose_image_ref() { printf '%s/%s:%s' "$1" "$2" "$3"; }
_openbkn_sample_gateway_base_url() { printf '%s' 'http://internal-gateway'; }
MARIADB_IMAGE=''
MARIADB_IMAGE_TAG=11.4.7
MARIADB_IMAGE_REPOSITORY=mariadb
MARIADB_STORAGE_CLASS=customer-storage
_openbkn_release_extra_sets bkn-sample-studio customer
[[ " ${CORE_RELEASE_EXTRA_SET_STRINGS[*]} " == *' sampleDatabase.image.repository=offline.example:5000/mariadb '* ]]
[[ " ${CORE_RELEASE_EXTRA_SET_STRINGS[*]} " == *' sampleDatabase.image.tag=11.4.7 '* ]]
[[ " ${CORE_RELEASE_EXTRA_SET_STRINGS[*]} " == *' sampleDatabase.persistence.storageClass=customer-storage '* ]]
MARIADB_IMAGE='private.example:5000/product/mariadb@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
_openbkn_release_extra_sets bkn-sample-studio customer
[[ " ${CORE_RELEASE_EXTRA_SET_STRINGS[*]} " == *' sampleDatabase.image.repository=private.example:5000/product/mariadb '* ]]
[[ " ${CORE_RELEASE_EXTRA_SET_STRINGS[*]} " == *' sampleDatabase.image.digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa '* ]]
[[ " ${CORE_RELEASE_EXTRA_SET_STRINGS[*]} " != *' sampleDatabase.image.tag='* ]]
printf '%s\n' 'PASS: shared sample database product image, digest, offline registry and storage selection'
