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


# Discovery reruns whenever BKN_BASE_URL is empty, and an explicit origin skips
# it entirely, so a manual fix always needs both settings.
REMEDY = (
    "set controlPlane.env.BKN_BASE_URL and configure networkPolicy.gateway "
    "explicitly (a hostNetwork gateway needs ipBlocks and a numeric port)"
)


def _controller_pods(items, namespace, selector):
    """Return the live Pods behind the discovered Service."""
    pods = []
    for item in items:
        metadata = item.get("metadata", {})
        labels = metadata.get("labels") or {}
        if (
            item.get("kind", "Pod") != "Pod"
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
            "Cannot resolve the hostNetwork gateway target port {}; {}".format(target_port, REMEDY)
        )
    return numbers.pop()


def _node_ip_blocks(nodes):
    """Every node InternalIP, so a rescheduled controller stays reachable."""
    if "items" not in (nodes or {}):
        raise ValueError("Cannot read cluster nodes for the hostNetwork gateway; " + REMEDY)
    networks = set()
    for node in nodes["items"]:
        for address in node.get("status", {}).get("addresses", []):
            if address.get("type") == "InternalIP":
                networks.add(ipaddress.ip_network(address["address"]))
    if not networks:
        raise ValueError("Cluster nodes report no InternalIP for the hostNetwork gateway; " + REMEDY)
    ordered = sorted(networks, key=lambda network: (network.version, network))
    return [{"cidr": str(network)} for network in ordered]


def discover(services, nodes=None, pods=None):
    """Discover one ingress controller using its Service port, selector and target port.

    A hostNetwork controller answers on node addresses, and NetworkPolicy
    implementations such as the kube-router built into k3s never select
    hostNetwork Pods by label, so the rule then also lists node addresses on the
    numeric port. Without readable Pods the rule assumes Pod networking, as
    before; without readable nodes only a hostNetwork controller fails.
    """
    candidates = []
    for service in services.get("items", []):
        spec = service.get("spec", {})
        for port in spec.get("ports", []):
            if port.get("name") != "http" or port.get("protocol", "TCP") != "TCP":
                continue
            if not spec.get("selector") or not 1 <= int(port["port"]) <= 65535:
                continue
            candidates.append((service, port))
    if len(candidates) != 1:
        raise ValueError("Expected one HTTP ingress controller Service; " + REMEDY)
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
    if "items" not in (pods or {}):
        print(
            "Sandbox gateway: cannot read controller Pods; the egress rule assumes "
            "Pod networking, which a hostNetwork controller never matches",
            file=sys.stderr,
        )
    controllers = _controller_pods((pods or {}).get("items", []), metadata["namespace"], selector)
    if "items" in (pods or {}) and not controllers:
        print(
            "Sandbox gateway: no running controller Pod found; the egress rule "
            "assumes Pod networking",
            file=sys.stderr,
        )
    if any(pod.get("spec", {}).get("hostNetwork") for pod in controllers):
        policy["port"] = _container_port(controllers, target_port)
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
        # Optional files: nodes, then controller Pods; each may hold {} when unreadable.
        documents = []
        for path in sys.argv[2:4]:
            with open(path, encoding="utf-8") as handle:
                documents.append(json.load(handle))
        json.dump(discover(value, *documents), sys.stdout)
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
