#!/usr/bin/env python3
# Copyright openbkn.ai
#
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.

"""Declarative runtime delivery profiles; no product-specific tools or policies."""
import argparse
import base64
import copy
import json
from pathlib import Path
import re
import secrets
import subprocess
import sys

STATE_NAME = "openbkn-runtime-profile"
API = "deploy.openbkn.ai/runtime-profile/v1"
NAME = re.compile(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?\Z")
MISSING = object()


def run(args, payload=None):
    result = subprocess.run(args, input=payload, capture_output=True, text=True, timeout=180)
    if result.returncode:
        # Helm/Kubernetes diagnostics can include rendered credentials.
        raise ValueError(f"{args[0]} {args[1]} failed; inspect the resource without logging credentials")
    return result.stdout


def read(path):
    text = Path(path).read_text(encoding="utf-8")
    if len(text) > 384 * 1024:
        raise ValueError("Runtime profile exceeds the size limit")
    return json.loads(text)


def write(path, value):
    path = Path(path)
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
    path.chmod(0o600)


def get(kind, name, namespace):
    raw = run(["kubectl", "get", kind, name, "-n", namespace, "--ignore-not-found", "-o", "json"])
    return json.loads(raw) if raw.strip() else None


def value_at(values, path, default=MISSING):
    for part in path.split("."):
        if not isinstance(values, dict) or part not in values:
            return default
        values = values[part]
    return values


def set_value(values, path, value):
    parts = path.split(".")
    for part in parts[:-1]:
        values = values.setdefault(part, {})
        if not isinstance(values, dict):
            raise ValueError("Runtime value path conflicts with an existing non-object field")
    values[parts[-1]] = copy.deepcopy(value)


def validate(profile, version, releases):
    if not isinstance(profile, dict) or profile.get("apiVersion") != API:
        raise ValueError("Unsupported runtime profile")
    if profile.get("platformVersion") != version:
        raise ValueError("Runtime profile does not match the platform version; provide the matching delivery profile")
    if not NAME.fullmatch(profile.get("name", "")):
        raise ValueError("Invalid runtime profile name")
    entries = profile.get("releases")
    if not isinstance(entries, dict) or not entries or set(entries) - set(releases):
        raise ValueError("Runtime profile requires releases absent from the platform manifest")
    for name, entry in entries.items():
        if not NAME.fullmatch(name) or not isinstance(entry, dict) or not isinstance(entry.get("values"), dict):
            raise ValueError("Invalid runtime release values")
        if "workload" in entry:
            workload = entry["workload"]
            if not isinstance(workload, dict) or not all(NAME.fullmatch(workload.get(k, "")) for k in ("name", "container")):
                raise ValueError("Invalid runtime workload identity")
        for field in ("removeEnv", "requiredManifestStrings", "exclusiveValues"):
            if not isinstance(entry.get(field, []), list) or any(not isinstance(x, str) or not x for x in entry.get(field, [])):
                raise ValueError("Invalid runtime release requirements")
        envs(entry["values"].get("extraEnv", []))
        image = entry["values"].get("image", {})
        if not isinstance(image, dict):
            raise ValueError("Invalid runtime image values")
        for field in ("registry", "repository", "tag"):
            if field in image and (not isinstance(image[field], str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/@+-]*", image[field])):
                raise ValueError("Invalid runtime image coordinate")
        mapping = entry.get("valueEnv", {})
        if not isinstance(mapping, dict) or any(not isinstance(k, str) or not all(k.split(".")) or not isinstance(v, str) or not v for k, v in mapping.items()):
            raise ValueError("Invalid runtime environment mapping")
    declared = profile.get("secrets", [])
    if not isinstance(declared, list) or len(declared) > 16:
        raise ValueError("Invalid runtime Secret declarations")
    names = set()
    for secret in declared:
        name = secret.get("name", "")
        if not NAME.fullmatch(name) or name in names or not isinstance(secret.get("keys"), dict) or not secret["keys"]:
            raise ValueError("Invalid or duplicate runtime Secret declaration")
        names.add(name)
        for key, spec in secret["keys"].items():
            if not NAME.fullmatch(key) or spec != {"bytes": 32, "encoding": "base64"}:
                raise ValueError("Unsupported runtime key contract")


def envs(values):
    if not isinstance(values, list):
        raise ValueError("Runtime extraEnv must be an array")
    result = {}
    for item in values:
        if not isinstance(item, dict) or not isinstance(item.get("name"), str) or item["name"] in result:
            raise ValueError("Invalid or duplicate runtime environment entry")
        result[item["name"]] = item
    return result


def equivalent(a, b):
    if isinstance(a, str) and isinstance(b, str):
        try:
            return json.loads(a) == json.loads(b)
        except ValueError:
            pass
    return a == b


def decode_key(encoded):
    try:
        value = base64.b64decode(encoded, validate=True)
        raw = base64.b64decode(value, validate=True)
        if len(raw) != 32 or base64.b64encode(raw) != value:
            raise ValueError()
        return raw
    except (ValueError, TypeError):
        raise ValueError("Runtime keys must be valid Base64-encoded 32-byte keys; restore the existing Secret") from None


def validate_secret(resource, spec):
    data = resource.get("data", {})
    if any(key not in data for key in spec["keys"]):
        raise ValueError(f"Existing Secret {spec['name']} is incomplete; restore it instead of rotating keys")
    decoded = [decode_key(data[key]) for key in spec["keys"]]
    if len(set(decoded)) != len(decoded):
        raise ValueError(f"Secret {spec['name']} keys must differ; restore the existing Secret")


def prepare(args):
    directory = Path(args.state)
    saved = get("secret", STATE_NAME, args.namespace)
    old = json.loads(base64.b64decode(saved["data"]["profile.json"], validate=True)) if saved else None
    profile = read(args.profile) if args.profile else old
    if profile is None:
        return
    validate(profile, args.version, args.releases)
    if old:
        validate(old, old.get("platformVersion"), args.releases)
        if old["name"] != profile["name"] or set(old["releases"]) - set(profile["releases"]):
            raise ValueError("Another runtime profile already owns installed releases; refusing to discard its configuration")
        declarations = {item["name"]: item for item in profile.get("secrets", [])}
        if any(declarations.get(item["name"]) != item for item in old.get("secrets", [])):
            raise ValueError("Runtime key ownership changed; retain the existing key contract instead of replacing keys")
    if saved:
        write(directory / "previous-state.json", saved)
    write(directory / "profile.json", profile)
    secret_cache = {}

    def lookup(name):
        if name not in secret_cache:
            secret_cache[name] = get("secret", name, args.namespace)
        return secret_cache[name]

    observed = {}
    for release, entry in profile["releases"].items():
        listed = run(["helm", "list", "--all", "-q", "-n", args.namespace, "--filter", "^" + re.escape(release) + "$"])
        current = json.loads(run(["helm", "get", "values", release, "-n", args.namespace, "-o", "json"])) if listed.strip() else {}
        current = current or {}
        active = envs(current.get("extraEnv", []))
        workload = entry.get("workload")
        if workload:
            deployment = get("deployment", workload["name"], args.namespace)
            if deployment:
                containers = deployment["spec"]["template"]["spec"]["containers"]
                selected = [c for c in containers if c["name"] == workload["container"]]
                if len(selected) != 1:
                    raise ValueError("Runtime workload container identity changed")
                live = envs(selected[0].get("env", []))
                # Capture manual runtime patches for managed names only; do not
                # turn every chart-generated environment variable into an override.
                for name in envs(entry["values"].get("extraEnv", [])):
                    if name in live:
                        active[name] = live[name]
                for field, env_name in entry.get("valueEnv", {}).items():
                    if env_name in live:
                        if "value" not in live[env_name]:
                            raise ValueError("Cannot normalize a non-literal runtime environment mapping")
                        set_value(current, field, live[env_name]["value"])
                write(directory / f"{release}.deployment.json", deployment)
        previous = (old or {}).get("releases", {}).get(release, {}).get("values", {})
        for field in entry.get("exclusiveValues", []):
            value = current.get(field)
            if value not in (None, "") and not equivalent(value, previous.get(field)) and not equivalent(value, entry["values"].get(field)):
                raise ValueError(f"{release} already has another extension provider; refusing to overwrite {field}")
        observed[release] = copy.deepcopy(active)
        desired = copy.deepcopy(entry["values"])
        desired["namespace"] = args.namespace
        for field in entry.get("valueEnv", {}):
            if field in entry.get("exclusiveValues", []):
                continue
            existing = value_at(current, field)
            previous_value = value_at(previous, field)
            if existing is not MISSING and (previous_value is MISSING or not equivalent(existing, previous_value)):
                set_value(desired, field, existing)
        previous_env = envs(previous.get("extraEnv", []))
        for name, default in envs(desired.get("extraEnv", [])).items():
            existing = active.get(name)
            reference = default.get("valueFrom", {}).get("secretKeyRef")
            if reference:
                # The Secret provisioning phase retains any existing key
                # material before normalizing these fields to durable references.
                active[name] = default
            elif existing is None or existing == previous_env.get(name):
                active[name] = default
        for name in entry.get("removeEnv", []):
            active.pop(name, None)
        if "extraEnv" in desired:
            desired["extraEnv"] = list(active.values())
        write(directory / f"{release}.values.json", desired)

    pending = []
    for spec in profile.get("secrets", []):
        name = spec["name"]
        existing = lookup(name)
        supplied = {}
        expected = bool(old and any(s["name"] == name for s in old.get("secrets", [])))
        for release, entry in profile["releases"].items():
            for env_name, default in envs(entry["values"].get("extraEnv", [])).items():
                target = default.get("valueFrom", {}).get("secretKeyRef", {})
                if target.get("name") != name or target.get("key") not in spec["keys"]:
                    continue
                current = observed[release].get(env_name)
                if not current:
                    continue
                ref = current.get("valueFrom", {}).get("secretKeyRef")
                if ref:
                    expected = expected or ref.get("name") == name
                    source = lookup(ref["name"])
                    if not source or ref["key"] not in source.get("data", {}):
                        raise ValueError("A previously used runtime key is missing; restore its Secret")
                    supplied[target["key"]] = source["data"][ref["key"]]
                elif "value" in current:
                    supplied[target["key"]] = base64.b64encode(current["value"].encode()).decode()
                else:
                    raise ValueError("Cannot recover the existing runtime key source")
        if existing:
            validate_secret(existing, spec)
            if any(existing["data"][key] != value for key, value in supplied.items()):
                raise ValueError("Existing workload keys conflict with the persistent Secret; restore consistent key material")
            continue
        if expected:
            raise ValueError(f"Previously used Secret {name} is missing; restore it instead of generating replacement keys")
        if supplied and set(supplied) != set(spec["keys"]):
            raise ValueError("Existing runtime key material is incomplete; restore all keys")
        data = supplied or {key: base64.b64encode(base64.b64encode(secrets.token_bytes(32))).decode() for key in spec["keys"]}
        resource = {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": name, "namespace": args.namespace, "annotations": {"helm.sh/resource-policy": "keep"}}, "type": "Opaque", "data": data}
        validate_secret(resource, spec)
        pending.append(resource)
    write(directory / "pending-secrets.json", pending)


def apply(args):
    directory = Path(args.state)
    profile = read(directory / "profile.json")
    for resource in read(directory / "pending-secrets.json"):
        name = resource["metadata"]["name"]
        existing = get("secret", name, args.namespace)
        if existing:
            if existing.get("data") != resource["data"]:
                raise ValueError("Runtime Secret was concurrently created; rerun installation to reuse it")
            continue
        run(["kubectl", "create", "-f", "-"], json.dumps(resource))
    previous_path = directory / "previous-state.json"
    current = get("secret", STATE_NAME, args.namespace)
    previous = read(previous_path) if previous_path.exists() else None
    if (current or {}).get("metadata", {}).get("resourceVersion") != (previous or {}).get("metadata", {}).get("resourceVersion"):
        raise ValueError("Runtime delivery profile changed concurrently; rerun installation")
    state = copy.deepcopy(current) if current else {"apiVersion": "v1", "kind": "Secret", "type": "Opaque", "metadata": {"name": STATE_NAME, "namespace": args.namespace}, "data": {}}
    state.setdefault("data", {})["profile.json"] = base64.b64encode(json.dumps(profile, ensure_ascii=False).encode()).decode()
    run(["kubectl", "replace" if current else "create", "-f", "-"], json.dumps(state))


def helm_options(arguments):
    # The caller supplies the exact planned upgrade arguments. Only readiness
    # flags absent from `helm template` are dropped for the client-only render.
    result = []
    i = 2
    while i < len(arguments):
        arg = arguments[i]
        if arg in ("--wait", "--atomic", "--cleanup-on-fail") or arg.startswith(("--wait=", "--timeout=")):
            i += 1
        elif arg == "--timeout":
            i += 2
        else:
            result.append(arg)
            i += 1
    return result


def render(args):
    entry = read(Path(args.state) / "profile.json")["releases"][args.release]
    manifest = run(["helm", "template", *helm_options(args.helm_args)])
    if any(marker not in manifest for marker in entry.get("requiredManifestStrings", [])):
        raise ValueError(f"{args.release} Chart does not implement the required runtime configuration; update the platform Chart")


def patch_env(args):
    directory = Path(args.state)
    entry = read(directory / "profile.json")["releases"][args.release]
    path = directory / f"{args.release}.deployment.json"
    if not path.exists() or not entry.get("workload"):
        return
    original = read(path)
    desired = envs(read(directory / f"{args.release}.values.json").get("extraEnv", []))
    containers = original["spec"]["template"]["spec"]["containers"]
    index = next(i for i, c in enumerate(containers) if c["name"] == entry["workload"]["container"])
    current = envs(containers[index].get("env", []))
    for name in entry.get("removeEnv", []):
        current.pop(name, None)
    current.update(desired)
    updated = list(current.values())
    if updated == containers[index].get("env", []):
        return
    patch = [{"op": "test", "path": "/metadata/resourceVersion", "value": original["metadata"]["resourceVersion"]}, {"op": "add", "path": f"/spec/template/spec/containers/{index}/env", "value": updated}]
    patch_file = directory / f"{args.release}.patch.json"
    write(patch_file, patch)
    run(["kubectl", "patch", "deployment", entry["workload"]["name"], "-n", args.namespace, "--type=json", "--patch-file", str(patch_file)])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("prepare", "apply", "render", "patch-env"):
        command = commands.add_parser(name)
        command.add_argument("--state", required=True)
        command.add_argument("--namespace", default="openbkn")
        if name == "prepare":
            command.add_argument("--profile", default="")
            command.add_argument("--version", required=True)
            command.add_argument("--releases", nargs="+", required=True)
        if name in ("render", "patch-env"):
            command.add_argument("--release", required=True)
        if name == "render":
            command.add_argument("helm_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if hasattr(args, "helm_args") and args.helm_args[:1] == ["--"]:
        args.helm_args = args.helm_args[1:]
    try:
        {"prepare": prepare, "apply": apply, "render": render, "patch-env": patch_env}[args.command](args)
    except (ValueError, KeyError, TypeError, OSError, subprocess.TimeoutExpired):
        # Deliberately do not include exception payloads: JSON/parser/transport
        # errors may contain key material. Known validation messages are safe.
        error = sys.exc_info()[1]
        if type(error) is ValueError and not isinstance(error, json.JSONDecodeError):
            print(str(error), file=sys.stderr)
        else:
            print("Runtime profile validation failed; inspect its configuration and restore existing keys", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
