#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/k8s.sh"
API_SERVER_ADVERTISE_ADDRESS=10.0.0.1
IMAGE_REPOSITORY=example.invalid/k8s
POD_CIDR=192.169.0.0/16
SERVICE_CIDR=10.96.0.0/12

config="$(write_kubeadm_config "$API_SERVER_ADVERTISE_ADDRESS")"
[[ "$config" == *$'kind: InitConfiguration\nlocalAPIEndpoint:\n  advertiseAddress: 10.0.0.1\nnodeRegistration:'* ]]
[[ "$config" == *$'kind: ClusterConfiguration\nkubernetesVersion: stable-1.28\ncontrolPlaneEndpoint: 10.0.0.1:6443'* ]]

config="$(write_kubeadm_config '')"
[[ "$config" != *localAPIEndpoint* ]]
[[ "$config" == *'controlPlaneEndpoint: 10.0.0.1:6443'* ]]
echo 'kubeadm configuration tests passed'
