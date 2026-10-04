#!/usr/bin/env python3
"""Small JSON operations used by deployment shell scripts.

Keeps normal install and upgrade paths independent of the optional jq binary.
"""

import json
import sys


WORKLOAD_KINDS = {
    "Deployment",
    "StatefulSet",
    "DaemonSet",
    "Job",
    "ReplicaSet",
    "ReplicationController",
}
RESOURCE_VALUE_KEYS = {"resources", "initResources", "sidecarResources"}


def get_path(value, *keys):
    for key in keys:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def pod_spec(resource):
    kind = resource.get("kind")
    if kind == "CronJob":
        return get_path(resource, "spec", "jobTemplate", "spec", "template", "spec")
    if kind in WORKLOAD_KINDS:
        return get_path(resource, "spec", "template", "spec")
    if kind == "Pod":
        return resource.get("spec")
    return None


def read_json_documents(raw):
    decoder = json.JSONDecoder()
    position = 0
    documents = []
    while position < len(raw):
        while position < len(raw) and raw[position].isspace():
            position += 1
        if position == len(raw):
            break
        document, position = decoder.raw_decode(raw, position)
        if isinstance(document, dict) and document.get("kind") == "List":
            documents.extend(document.get("items", []))
        else:
            documents.append(document)
    return documents


def has_resource_limits():
    for resource in read_json_documents(sys.stdin.read()):
        if not isinstance(resource, dict):
            continue
        spec = pod_spec(resource)
        if not isinstance(spec, dict):
            continue
        for container_type in ("containers", "initContainers", "ephemeralContainers"):
            for container in spec.get(container_type, []) or []:
                limits = get_path(container, "resources", "limits")
                if isinstance(limits, dict) and limits:
                    print("true")
                    return
    print("false")


def remove_resource_limits(value):
    if isinstance(value, dict):
        for key, child in value.items():
            if key in RESOURCE_VALUE_KEYS and isinstance(child, dict):
                child.pop("limits", None)
            remove_resource_limits(child)
    elif isinstance(value, list):
        for child in value:
            remove_resource_limits(child)


def sanitize_values():
    values = json.load(sys.stdin)
    if not isinstance(values, dict):
        raise ValueError("Helm values must be a JSON object")
    remove_resource_limits(values)
    json.dump(values, sys.stdout, separators=(",", ":"))
    sys.stdout.write("\n")


def opensearch_settings():
    values = json.load(sys.stdin)
    java_opts = values.get("opensearchJavaOpts")
    memory_request = values.get("resources", {}).get("requests", {}).get("memory")
    if not isinstance(java_opts, str) or not java_opts:
        raise ValueError("opensearchJavaOpts must be a non-empty string")
    if not isinstance(memory_request, str) or not memory_request:
        raise ValueError("resources.requests.memory must be a non-empty string")
    print(java_opts)
    print(memory_request)


def main():
    operations = {
        "has-resource-limits": has_resource_limits,
        "sanitize-values": sanitize_values,
        "opensearch-settings": opensearch_settings,
    }
    if len(sys.argv) != 2 or sys.argv[1] not in operations:
        raise SystemExit(f"Usage: {sys.argv[0]} <{'|'.join(operations)}>")
    operations[sys.argv[1]]()


if __name__ == "__main__":
    main()
