#!/usr/bin/env python3
"""Client-only Helm rendering; no handwritten YAML/--set merge implementation.

Stage one immutable Chart in the private Audit plan. A temporary values probe
is removed before the staged Chart is handed to helm upgrade. The raw-input
probe has no defaults, allowing explicit user changes to be distinguished from
installer-generated flags. Rendered manifests/Secrets are never logged.
"""
import json
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import sys
import tarfile

PROBE = '''apiVersion: v1
kind: ConfigMap
metadata:
  name: openbkn-audit-values-probe
data:
  payload: {{ dict "config" .Values "chart" (dict "metadata" (dict "name" .Chart.Name "version" .Chart.Version) "values" (dict)) | toJson | quote }}
'''


def run_helm(args):
    result = subprocess.run(["helm", *args], capture_output=True, text=True, timeout=120)
    if result.returncode:
        # Errors can include template values; never forward raw tool output.
        raise ValueError("Helm client rendering/Chart resolution failed; review Chart and values without logging credentials")
    return result.stdout


def extract(archive, destination):
    destination.mkdir()
    with tarfile.open(archive) as package:
        members = package.getmembers()
        for member in members:
            path = PurePosixPath(member.name)
            if (path.is_absolute() or ".." in path.parts or "\\" in member.name
                    or ":" in member.name or not (member.isdir() or member.isfile())):
                raise ValueError("Audit Chart archive contains an unsafe path or link")
        # Paths and member types were checked, including on Python < 3.12.
        if hasattr(tarfile, "data_filter"):
            package.extractall(destination, members=members, filter="data")
        else:
            package.extractall(destination, members=members)
    roots = [path.parent for path in destination.glob("*/Chart.yaml")]
    if len(roots) != 1:
        raise ValueError("Audit Chart archive must contain one root Chart")
    return roots[0]


def stage(directory, args):
    directory = Path(directory)
    source = Path(args[3])
    if source.is_dir():
        target = directory / "chart"
        shutil.copytree(source, target)
    else:
        if source.is_file():
            archive = source
        else:
            options = []
            for i, arg in enumerate(args[4:], start=4):
                if arg == "--version":
                    options += [arg, args[i + 1]]
                elif arg == "--devel":
                    options.append(arg)
            run_helm(["pull", args[3], "--destination", str(directory), *options])
            packages = list(directory.glob("*.tgz"))
            if len(packages) != 1:
                raise ValueError("Audit Chart resolution did not produce one pinned archive")
            archive = packages[0]
        target = extract(archive, directory / "chart")
    print(str(target))


def render(directory, mode, args):
    if mode == "explicit":
        target = Path(directory) / "input-probe"
        target.mkdir(exist_ok=True)
        (target / "Chart.yaml").write_text(json.dumps({"apiVersion": "v2", "name": "audit-input-values", "version": "0.0.0"}), encoding="utf-8")
        args[3] = str(target)
    else:
        target = Path(args[3])
    templates = target / "templates"
    templates.mkdir(exist_ok=True)
    probe = templates / "openbkn-audit-values-probe.yaml"
    if probe.exists():
        raise ValueError("Audit probe collides with an existing Chart template")
    probe.write_text(PROBE, encoding="utf-8")
    try:
        output = run_helm(["template", *args[2:], "--show-only", "templates/openbkn-audit-values-probe.yaml"])
        payloads = [line.strip()[len("payload: "):] for line in output.splitlines() if line.strip().startswith("payload: ")]
        if len(payloads) != 1:
            raise ValueError("Helm values probe did not produce exactly one payload")
        json.dump(json.loads(json.loads(payloads[0])), sys.stdout)
    finally:
        probe.unlink()


def cleanup(directory):
    target = Path(directory)
    if not target.exists():
        return
    if target.is_symlink() or not target.name.startswith("openbkn-audit.") or not (target / ".audit-owner").is_file():
        raise ValueError("refusing to clean an unowned Audit plan directory")
    shutil.rmtree(target)


def main():
    try:
        action, directory = sys.argv[1:3]
        if action == "stage":
            stage(directory, sys.argv[3:])
        elif action == "render":
            render(directory, sys.argv[3], sys.argv[4:])
        elif action == "cleanup":
            cleanup(directory)
        else:
            raise ValueError("unknown Audit Helm action")
    except (ValueError, OSError, subprocess.TimeoutExpired, tarfile.TarError):
        print("Audit Helm preparation failed (Chart resolution, rendering or private-plan cleanup); no release was upgraded by this helper.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
