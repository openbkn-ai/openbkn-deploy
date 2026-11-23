#!/usr/bin/env python3
# Copyright openbkn.ai
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.

"""Resolve sandbox REST gateway defaults without using the MCP endpoint."""

import ipaddress
import json
import sys
from urllib.parse import urlsplit


def preserve(values):
    """Keep operator overrides, but rediscover installer-owned defaults on upgrade."""
    values = values or {}
    result = {}
    base = values.get("controlPlane", {}).get("env", {}).get("BKN_BASE_URL", "")
    if base and base.strip():
        result["controlPlane"] = {"env": {"BKN_BASE_URL": base}}
    policy = values.get("networkPolicy", {}).get("gateway")
    generated = values.get("controlPlane", {}).get("gateway", {}).get("networkPolicy", {})
    if isinstance(policy, dict):
        # Previous discovery output is not an operator override. Keep only
        # changed keys so a moved controller is rediscovered on upgrade.
        policy = {key: value for key, value in policy.items() if generated.get(key) != value}
        if policy:
            result["networkPolicy"] = {"gateway": policy}
    return result


def base_url(render):
    """Read the actual rendered ConfigMap, including chart defaults and all overrides."""
    # Helm's manifest is YAML, but each ConfigMap value is rendered as a quoted
    # string. Inspect only our exact key, without loading or logging Secrets.
    for line in render.get("manifest", "").splitlines():
        if line.startswith("  BKN_BASE_URL:"):
            value = json.loads(line.split(":", 1)[1].strip())
            if value:
                parsed = urlsplit(value)
                if (
                    parsed.scheme not in ("http", "https")
                    or not parsed.hostname
                    or parsed.username
                    or parsed.password
                    or parsed.query
                    or parsed.fragment
                    or parsed.path not in ("", "/")
                ):
                    raise ValueError(
                        "BKN_BASE_URL must be an HTTP(S) origin without credentials or path"
                    )
                if parsed.port is not None and not 1 <= parsed.port <= 65535:
                    raise ValueError("BKN_BASE_URL has an invalid port")
            return value
    raise ValueError("Sandbox ConfigMap BKN_BASE_URL is missing from Helm rendering")


def _controller_pods(items, namespace, selector):
    """Return the live Pods behind the discovered Service."""
    pods = []
    for item in items:
        metadata = item.get("metadata", {})
        labels = metadata.get("labels") or {}
        if (
            item.get("kind") != "Pod"
            or metadata.get("namespace") != namespace
            or any(labels.get(key) != value for key, value in selector.items())
            or item.get("status", {}).get("phase") in ("Succeeded", "Failed")
        ):
            continue
        pods.append(item)
    return pods


def _container_port(pods, target_port):
    """Resolve a Service target port to the number a hostNetwork Pod listens on."""
    if not isinstance(target_port, str):
        return int(target_port)
    numbers = set()
    for pod in pods:
        for container in pod.get("spec", {}).get("containers", []):
            for port in container.get("ports") or []:
                if port.get("name") == target_port and port.get("protocol", "TCP") == "TCP":
                    numbers.add(int(port["containerPort"]))
    if len(numbers) != 1:
        raise ValueError(
            "Cannot resolve the hostNetwork gateway target port {}; configure "
            "networkPolicy.gateway explicitly".format(target_port)
        )
    return numbers.pop()


def _node_ip_blocks(nodes):
    """Every node InternalIP, so a rescheduled controller stays reachable."""
    networks = set()
    for node in (nodes or {}).get("items", []):
        for address in node.get("status", {}).get("addresses", []):
            if address.get("type") == "InternalIP":
                networks.add(ipaddress.ip_network(address["address"]))
    if not networks:
        raise ValueError(
            "A hostNetwork gateway needs node InternalIP addresses; configure "
            "networkPolicy.gateway.ipBlocks explicitly"
        )
    ordered = sorted(networks, key=lambda network: (network.version, network))
    return [{"cidr": str(network)} for network in ordered]


def discover(services, nodes=None):
    """Discover one ingress controller using its Service port, selector and target port.

    The input list may also carry the controller Pods. A hostNetwork controller
    answers on node addresses, and NetworkPolicy implementations such as the
    kube-router built into k3s never select hostNetwork Pods by label, so the
    rule then also lists node addresses on the numeric port.
    """
    items = services.get("items", [])
    candidates = []
    for service in items:
        if service.get("kind", "Service") != "Service":
            continue
        spec = service.get("spec", {})
        for port in spec.get("ports", []):
            if port.get("name") != "http" or port.get("protocol", "TCP") != "TCP":
                continue
            if not spec.get("selector") or not 1 <= int(port["port"]) <= 65535:
                continue
            candidates.append((service, port))
    if len(candidates) != 1:
        raise ValueError(
            "Expected one HTTP ingress controller Service; configure "
            "controlPlane.env.BKN_BASE_URL and networkPolicy.gateway explicitly"
        )
    service, port = candidates[0]
    metadata, selector = service["metadata"], service["spec"]["selector"]
    target_port = port.get("targetPort", port["port"])
    policy = {
        "enabled": True,
        "namespace": metadata["namespace"],
        "podSelector": selector,
        "port": target_port,
        "ipBlocks": [],
    }
    pods = _controller_pods(items, metadata["namespace"], selector)
    if not pods:
        print(
            "Sandbox gateway: no running controller Pod found; the egress rule "
            "assumes Pod networking",
            file=sys.stderr,
        )
    if any(pod.get("spec", {}).get("hostNetwork") for pod in pods):
        policy["port"] = _container_port(pods, target_port)
        policy["ipBlocks"] = _node_ip_blocks(nodes)
    return {
        "controlPlane": {
            "gateway": {
                "baseURL": "http://{}.{}.svc:{}".format(
                    metadata["name"], metadata["namespace"], port["port"]
                ),
                "networkPolicy": policy,
            }
        },
        "networkPolicy": {"gateway": policy},
    }


def main():
    action = sys.argv[1]
    value = json.load(sys.stdin)
    if action == "preserve":
        json.dump(preserve(value), sys.stdout)
    elif action == "discover":
        nodes = None
        if len(sys.argv) > 2:
            with open(sys.argv[2], encoding="utf-8") as handle:
                nodes = json.load(handle)
        json.dump(discover(value, nodes), sys.stdout)
    elif action == "base-url":
        print(base_url(value))
    elif action == "validate":
        if not base_url(value):
            raise ValueError("Sandbox requires an explicit or discovered BKN_BASE_URL")
    else:
        raise ValueError("Unknown sandbox gateway action")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, TypeError, OSError) as error:
        print("Sandbox gateway: {}".format(error), file=sys.stderr)
        sys.exit(1)
