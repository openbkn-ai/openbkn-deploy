#!/usr/bin/env python3
# Copyright openbkn.ai
#
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.

"""Carry the default small model chosen by onboard across installer upgrades.

onboard.sh writes `server.defaultSmallModelName` / `defaultSmallModelEnabled`
into the live bkn-backend-cm / ontology-query-cm. Those keys are Chart values
(`config.server.*`), so a later `helm upgrade` renders the Chart default back.
Reads the live ConfigMap JSON on stdin and prints Helm values restoring the
choice, or `{}` when none was made; with --name, only the model name (or
nothing). The installer passes the choice as --set ahead of every other --set:
it wins over values files (config.yaml and the commercial runtime values, which
carry the value of the previous release), and an operator's --set still wins.

Standard library only: the install host need not have PyYAML.
"""
import json
import re
import sys


def _scalar(raw):
    value = raw.split(" #", 1)[0].strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
        value = value[1:-1]
    return value


def preserved(configmap):
    data = (configmap or {}).get("data") or {}
    key = next((k for k in data if k.endswith("-config.yaml")), None)
    if not key:
        return {}
    lines = str(data[key]).splitlines()
    server, inside = {}, False
    for line in lines:
        if re.match(r"^server:\s*$", line):
            inside = True
            continue
        if inside and line.strip() and not line[0].isspace():
            break
        match = inside and re.match(r"^\s+(defaultSmallModelName|defaultSmallModelEnabled):\s*(.*)$", line)
        if match:
            server[match.group(1)] = _scalar(match.group(2))
    name = server.get("defaultSmallModelName", "")
    if server.get("defaultSmallModelEnabled", "").lower() != "true" or not name:
        return {}
    return {"config": {"server": {"defaultSmallModelName": name, "defaultSmallModelEnabled": True}}}


def main():
    raw = sys.stdin.read()
    try:
        configmap = json.loads(raw) if raw.strip() else {}
    except ValueError:
        configmap = {}
    values = preserved(configmap)
    if sys.argv[1:] == ["--name"]:
        print(values["config"]["server"]["defaultSmallModelName"] if values else "")
    else:
        print(json.dumps(values))
    return 0


if __name__ == "__main__":
    sys.exit(main())
