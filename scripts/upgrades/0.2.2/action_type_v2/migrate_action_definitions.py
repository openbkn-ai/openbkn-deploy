# Copyright openbkn.ai

"""Plan or apply the one-time action model conversion before runtime cutover.

The default is a dry run. --input converts a JSON network/export without a DB.
Database mode discovers the installed bkn-backend Helm release by default;
explicit --host/--port/--user/--password/--database or BKN_DB_* override it.
--apply is database-only and requires the 0.2.2/02-action-operation.sql schema
step first. Stop action writes, keep the generated rollback snapshot, apply the
conversion, verify it, then switch all callers and services to the new model.
No compatibility runtime or executable fallback is installed by this script.
"""

from __future__ import annotations

import argparse
import copy
import json
import hashlib
import math
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import time
from typing import Any


class MigrationError(ValueError):
    """An explicit conversion refusal; messages never include parameter values."""


def decoded(value: Any, default: Any) -> Any:
    if value is None or value == "":
        return copy.deepcopy(default)
    if isinstance(value, (str, bytes)):
        try:
            return json.loads(value)
        except (ValueError, UnicodeError) as error:
            raise MigrationError("stored JSON is invalid") from error
    return copy.deepcopy(value)


def parameters_from_schema(schema: Any) -> list[dict[str, Any]]:
    if not isinstance(schema, dict) or schema.get("type") != "object" or not isinstance(schema.get("properties"), dict):
        raise MigrationError("a resolved object input_schema snapshot is required")
    required = schema.get("required", [])
    if not isinstance(required, list) or any(not isinstance(name, str) or name not in schema["properties"] for name in required):
        raise MigrationError("input_schema required fields do not match its properties")
    parameters = []
    for name, field in schema["properties"].items():
        if not isinstance(field, dict) or "$ref" in field or not isinstance(field.get("type"), str) or field["type"] not in {"string", "integer", "number", "boolean", "array", "object"}:
            raise MigrationError("resolve input_schema references and ambiguous types before conversion")
        if name in required:
            parameters.append({"name": name, "type": field["type"], "value_from": "input"})
        elif "default" in field:
            parameters.append({"name": name, "type": field["type"], "value_from": "const", "value": copy.deepcopy(field["default"])})
    return parameters


def parameter_type_from_schema(schema: Any, name: str) -> str:
    """Resolve a saved parameter path without guessing a former tool input type."""
    current = schema
    for segment in name.split("."):
        if not isinstance(current, dict) or "$ref" in current:
            raise MigrationError(f"parameter {name} needs a resolved tool_input_schema snapshot")
        properties = current.get("properties")
        if not isinstance(properties, dict) or segment not in properties:
            raise MigrationError(f"parameter {name} has no type; provide its tool_input_schema snapshot")
        current = properties[segment]
    if not isinstance(current, dict) or "$ref" in current or not isinstance(current.get("type"), str) or current["type"] not in {"string", "integer", "number", "boolean", "array", "object"}:
        raise MigrationError(f"parameter {name} has an ambiguous tool schema type")
    return current["type"]


def migrate_action(action: dict[str, Any]) -> dict[str, Any]:
    """Convert a stored old definition once; preserve an already converted one."""
    result = copy.deepcopy(action)
    existing = result.get("operation_config")
    if existing is not None:
        allowed = {"type", "relation_type_id", "relation_operation", "anchor_role", "other_object_condition"}
        if not isinstance(existing, dict) or set(existing) - allowed or not isinstance(existing.get("type"), str) or existing["type"] not in {"object", "relation", "tool"}:
            raise MigrationError("operation_config is invalid; explicit review is required")
        kind = existing["type"]
        if kind in {"object", "relation"} and not result.get("object_type_id"):
            raise MigrationError("object and relation operations require an object type")
        if kind == "tool" and (result.get("object_type_id") or result.get("condition") is not None):
            raise MigrationError("tool operations cannot bind object context")
        if kind == "relation" and (not existing.get("relation_type_id") or existing.get("relation_operation") not in ("create", "delete") or existing.get("anchor_role") not in ("source", "target")):
            raise MigrationError("relation operation metadata is incomplete")
        if kind != "relation" and set(existing) != {"type"}:
            raise MigrationError("relation metadata is only valid on relation operations")
        return result
    operation = result.get("action_type") or result.get("action_intent")
    if not isinstance(operation, str) or operation not in {"add", "modify", "delete"}:
        raise MigrationError("action_type must be add, modify or delete")
    if result.get("action_intent") not in (None, "", operation):
        raise MigrationError("action_type and action_intent conflict")
    result["action_type"] = result["action_intent"] = operation
    bound = bool(result.get("object_type_id"))
    if bound and result.get("schedule"):
        raise MigrationError("scheduled bound actions require an explicit target plan before migration")
    result["operation_config"] = {"type": "object" if bound else "tool"}
    parameters = result.get("parameters") or []
    tool_schema = result.pop("tool_input_schema", None)
    action_source = result.get("action_source") or {}
    if not isinstance(action_source, dict):
        raise MigrationError("action_source must be an object")
    if action_source.get("type") == "mcp" and not parameters:
        parameters = parameters_from_schema(tool_schema)
    if not isinstance(parameters, list):
        raise MigrationError("parameters must be an array")
    inputs = set()
    names = set()
    for parameter in parameters:
        if not isinstance(parameter, dict) or not isinstance(parameter.get("name"), str) or not parameter["name"]:
            raise MigrationError("a parameter is missing its name")
        name = parameter["name"]
        if name in names:
            raise MigrationError(f"duplicate parameter name: {name}")
        if any(part in {"", "__proto__", "constructor", "prototype"} for part in name.split(".")) or any(name.startswith(other + ".") or other.startswith(name + ".") for other in names):
            raise MigrationError("parameter paths are invalid or overlapping")
        names.add(name)
        source = parameter.get("value_from")
        if not isinstance(source, str) or source not in {"const", "input", "property"}:
            raise MigrationError(f"parameter {name} has an unsupported value source")
        if source == "property":
            if not bound or not isinstance(parameter.get("value"), str) or not parameter["value"]:
                raise MigrationError(f"parameter {name} requires a bound object property")
        if not parameter.get("type") or parameter["type"] == "unknown":
            parameter["type"] = parameter_type_from_schema(tool_schema, name)
        if not isinstance(parameter["type"], str):
            raise MigrationError(f"parameter {name} has an invalid type")
        if source == "const" and isinstance(parameter.get("value"), str):
            kind = str(parameter.get("type", "")).lower()
            if kind in {"boolean", "bool", "integer", "int", "int32", "int64", "long", "number", "double", "float", "array", "object"}:
                try:
                    parameter["value"] = json.loads(parameter["value"])
                except ValueError as error:
                    raise MigrationError(f"parameter {name} has an invalid typed constant") from error
        if source == "const":
            kind = str(parameter.get("type", "")).lower()
            value = parameter.get("value")
            valid = True
            if value is not None:
                if kind in {"bool", "boolean"}: valid = type(value) is bool
                elif kind in {"integer", "int", "int32", "int64", "long"}: valid = type(value) is int
                elif kind in {"number", "double", "float"}: valid = type(value) in {int, float} and (type(value) is int or math.isfinite(value))
                elif kind == "array": valid = isinstance(value, list)
                elif kind == "object": valid = isinstance(value, dict)
            if not valid: raise MigrationError(f"parameter {name} has an incompatible typed constant")
        if source == "input":
            inputs.add(name)
    result["parameters"] = parameters
    # An object creation has no existing instance to filter. Legacy definitions
    # sometimes stored an applicability condition here; it has no create-time
    # meaning in the new model and must not be carried forward.
    if bound and operation == "add":
        result["condition"] = None
    condition = result.get("condition")
    if condition is not None and not bound:
        raise MigrationError("unbound tool definitions cannot carry object conditions")

    def normalize_condition(config: dict[str, Any]) -> None:
        if not isinstance(config, dict):
            raise MigrationError("condition contains a non-object node")
        if config.get("object_type_id") not in (None, "", result.get("object_type_id")):
            raise MigrationError("condition references a different object type")
        children = config.get("sub_conditions")
        if config.get("operation") in ("and", "or"):
            if not isinstance(children, list) or not children:
                raise MigrationError("logical conditions require nonempty children")
            for child in children:
                normalize_condition(child)
            return
        if not config.get("field") or not config.get("operation"):
            raise MigrationError("condition leaf is missing a field or operator")
        source = config.setdefault("value_from", "const") or "const"
        config["value_from"] = source
        if not isinstance(source, str) or source not in {"const", "property", "input"}:
            raise MigrationError("condition has an unsupported value source")
        if source == "input" and config.get("value") not in inputs:
            raise MigrationError("condition references an unknown dynamic input")
        if source == "property" and not isinstance(config.get("value"), str):
            raise MigrationError("condition property reference is not a name")

    if condition is not None:
        normalize_condition(condition)
    return result


def convert_document(document: Any) -> tuple[Any, dict[str, Any]]:
    converted = copy.deepcopy(document)
    if isinstance(converted, list):
        actions = converted
    elif isinstance(converted, dict):
        actions = converted.get("action_types", converted.get("entries"))
    else:
        actions = None
    if not isinstance(actions, list):
        raise MigrationError("input must be an action array, an entries response, or a network with action_types")
    errors, changed, unchanged = [], [], []
    for index, action in enumerate(actions):
        identifier = action.get("id", f"row-{index}") if isinstance(action, dict) else f"row-{index}"
        try:
            if not isinstance(action, dict):
                raise MigrationError("action is not an object")
            migrated = migrate_action(action)
            actions[index] = migrated
            (changed if migrated != action else unchanged).append(identifier)
        except MigrationError as error:
            errors.append({"id": identifier, "reason": str(error)})
    return converted, {"changed": changed, "unchanged": unchanged, "errors": errors}


DB_COLUMNS = (
    "f_id", "f_name", "f_kn_id", "f_branch", "f_action_type", "f_action_intent",
    "f_object_type_id", "f_operation_config", "f_parameters", "f_condition",
    "f_bkn_raw_content", "f_update_time", "f_action_source",
)


def plan_rows(rows: list[dict[str, Any]], tool_schemas: dict[tuple[str, str, str], Any] | None = None) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    changes, errors = [], []
    for row in rows:
        identifier = str(row["f_id"])
        try:
            action = {
                "id": identifier, "action_type": row["f_action_type"],
                "action_intent": row["f_action_intent"], "object_type_id": row["f_object_type_id"],
                "action_source": decoded(row.get("f_action_source"), {}),
                "operation_config": decoded(row["f_operation_config"], None),
                "parameters": decoded(row["f_parameters"], []), "condition": decoded(row["f_condition"], None),
            }
            lookup_key = (row["f_kn_id"], row["f_branch"], identifier)
            if tool_schemas and lookup_key in tool_schemas:
                action["tool_input_schema"] = tool_schemas[lookup_key]
            if row.get("_unsafe_schedule_ids"):
                raise MigrationError("scheduled execution lacks explicit targets; review schedules before migration")
            migrated = migrate_action(action)
            if migrated == action:
                continue
            raw = row.get("f_bkn_raw_content") or ""
            if "### Operation Config" in raw:
                raise MigrationError("raw BKN contains operation metadata missing from storage; explicit review is required")
            if migrated["operation_config"] == {"type": "object"} and migrated["action_type"] == "add":
                lines = raw.splitlines(keepends=True)
                headings = [i for i, line in enumerate(lines) if line.strip() == "### Trigger Condition"]
                if len(headings) > 1 or (action["condition"] is not None and len(headings) != 1):
                    raise MigrationError("legacy create condition has no unique raw BKN section to remove")
                if headings:
                    start = headings[0]
                    end = next((i for i in range(start + 1, len(lines)) if lines[i].startswith(("### ", "## "))), len(lines))
                    raw = "".join(lines[:start] + lines[end:])
            config = json.dumps(migrated["operation_config"], ensure_ascii=False, separators=(",", ":"))
            raw = raw.rstrip() + "\n\n### Operation Config\n\n```yaml\noperation_config: " + config + "\n```\n"
            after = {
                "f_action_type": migrated["action_type"], "f_action_intent": migrated["action_intent"],
                "f_operation_config": config,
                "f_parameters": json.dumps(migrated["parameters"], ensure_ascii=False),
                "f_condition": json.dumps(migrated["condition"], ensure_ascii=False),
                "f_bkn_raw_content": raw,
            }
            changes.append({"id": identifier, "kn_id": row["f_kn_id"], "branch": row["f_branch"], "before": copy.deepcopy(row), "after": after})
        except MigrationError as error:
            try:
                source = decoded(row.get("f_action_source"), {})
            except MigrationError:
                source = {}
            if not isinstance(source, dict):
                source = {}
            errors.append({
                "id": identifier, "name": row.get("f_name") or identifier,
                "kn_id": row["f_kn_id"], "branch": row["f_branch"],
                "tool_source": {key: source[key] for key in ("type", "tool_name", "box_id", "tool_id", "mcp_id") if key in source},
                "reason": str(error),
            })
    return changes, errors


def migration_report(changes: list[dict[str, Any]], errors: list[dict[str, Any]], *, applied: bool) -> dict[str, Any]:
    return {
        "planned": [item["id"] for item in changes],
        "applied_count": len(changes) if applied else 0,
        "partial": applied and bool(errors),
        "removed_create_conditions": [
            {"kn_id": item["kn_id"], "branch": item["branch"], "id": item["id"], "name": item["before"].get("f_name", "")}
            for item in changes
            if item["before"].get("f_condition") not in (None, "", "null") and item["after"]["f_condition"] == "null"
        ],
        "errors": errors,
        "skipped_actions": errors,
        "applied": applied,
    }


MIGRATED_FIELDS = ("f_action_type", "f_action_intent", "f_operation_config", "f_parameters", "f_condition", "f_bkn_raw_content")


def apply_plan(connection: Any, changes: list[dict[str, Any]], *, rollback: bool = False) -> None:
    """Apply one atomic plan with optimistic checks; never overwrite later edits."""
    connection.begin()
    try:
        with connection.cursor() as cursor:
            for change in changes:
                source = change["after"] if rollback else change["before"]
                target = change["before"] if rollback else change["after"]
                assignments = ", ".join(f"{field} = %s" for field in MIGRATED_FIELDS)
                # The definition timestamp plus every migrated field protects both
                # forward apply and rollback from replacing a concurrent edit.
                predicate = " AND ".join(f"{field} <=> %s" for field in MIGRATED_FIELDS)
                sql = f"UPDATE t_action_type SET {assignments} WHERE f_id = %s AND f_kn_id = %s AND f_branch = %s AND f_update_time = %s AND {predicate}"
                values = [target.get(field) for field in MIGRATED_FIELDS]
                values += [change["id"], change["kn_id"], change["branch"], change["before"]["f_update_time"]]
                values += [source.get(field) for field in MIGRATED_FIELDS]
                cursor.execute(sql, values)
                if cursor.rowcount != 1:
                    raise MigrationError(f"action {change['id']} changed since the snapshot; no rows were committed")
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def private_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        os.fchmod(handle.fileno(), 0o600)
        json.dump(value, handle, ensure_ascii=False, indent=2)
        handle.write("\n")


def write_migration_snapshot(path: Path, changes: list[dict[str, Any]]) -> None:
    """Keep the original rollback evidence across identical and empty reruns."""
    snapshot = {"version": 1, "changes": changes}
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except (ValueError, OSError) as error:
            raise MigrationError("existing rollback snapshot is unreadable; choose a new --output path") from error
        valid = isinstance(existing, dict) and existing.get("version") == 1 and isinstance(existing.get("changes"), list)
        if valid and (not changes or existing == snapshot):
            return
        raise MigrationError("--output already contains a different migration plan; its rollback snapshot was preserved")
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(snapshot, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def apply_snapshot_path(args: argparse.Namespace, changes: list[dict[str, Any]], errors: list[dict[str, Any]]) -> Path:
    """Keep each applied batch independently recoverable without asking for a path."""
    if args.output_explicit or (not errors and not args.output.exists()):
        return args.output
    digest = hashlib.sha256(json.dumps(changes, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16]
    return args.output.with_name(f"rollback-{digest}.json")


def rollback_snapshot_paths(directory: Path) -> list[Path]:
    paths = [directory / "rollback.json", *sorted(
        path for path in directory.glob("rollback-*.json")
        if re.fullmatch(r"rollback-[0-9a-f]{16}\.json", path.name)
    )]
    found = [path for path in paths if path.is_file()]
    if not found:
        raise MigrationError(f"no applied rollback snapshots found in {directory}")
    return found


def load_rollback_snapshots(paths: list[Path]) -> list[dict[str, Any]]:
    changes = []
    seen = set()
    for path in paths:
        try:
            snapshot = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as error:
            raise MigrationError(f"rollback snapshot is unreadable: {path}") from error
        if not isinstance(snapshot, dict) or snapshot.get("version") != 1 or not isinstance(snapshot.get("changes"), list):
            raise MigrationError(f"rollback snapshot has an invalid format: {path}")
        for change in snapshot["changes"]:
            if not isinstance(change, dict) or not all(key in change for key in ("kn_id", "branch", "id", "before", "after")):
                raise MigrationError(f"rollback snapshot has an invalid change: {path}")
            key = (change["kn_id"], change["branch"], change["id"])
            if key in seen:
                raise MigrationError(f"action {change['id']} appears in multiple rollback snapshots")
            seen.add(key)
            changes.append(change)
    return changes


def select_rollback_changes(changes: list[dict[str, Any]], current_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
    """Ignore batches never committed or already restored; refuse later edits."""
    current = {(row["f_kn_id"], row["f_branch"], row["f_id"]): row for row in current_rows}
    selected, already = [], []
    for change in changes:
        row = current.get((change["kn_id"], change["branch"], change["id"]))
        if row is None:
            raise MigrationError(f"action {change['id']} is missing; rollback stopped")
        before = change["before"]
        after = {**before, **change["after"]}
        fields = (*MIGRATED_FIELDS, "f_update_time")
        if all(row.get(field) == after.get(field) for field in fields):
            selected.append(change)
        elif all(row.get(field) == before.get(field) for field in fields):
            already.append(change["id"])
        else:
            raise MigrationError(f"action {change['id']} changed after migration; rollback stopped")
    return selected, already


def validate_artifact_paths(output: Path, report: Path, sources: list[Path]) -> None:
    """A report or converted export must never overwrite an input or snapshot."""
    def aliases(left: Path, right: Path) -> bool:
        return left.resolve() == right.resolve() or (left.exists() and right.exists() and left.samefile(right))

    if aliases(output, report):
        raise MigrationError("--output and --report must use different files")
    for source in sources:
        if aliases(output, source) or aliases(report, source):
            raise MigrationError("output artifacts must not overwrite migration inputs or rollback snapshots")


DATABASE_ARGUMENTS = {
    "host": "BKN_DB_HOST", "port": "BKN_DB_PORT", "user": "BKN_DB_USER",
    "password": "BKN_DB_PASSWORD", "database": "BKN_DB_NAME",
}
ROLLBACK_DEFAULT = object()


def configure_artifact_paths(args: argparse.Namespace) -> None:
    """Separate deployment scopes and keep dry runs away from rollback evidence."""
    args.output_explicit = args.output is not None
    args.rollback_default = args.rollback is ROLLBACK_DEFAULT
    base = Path(os.getenv("OPENBKN_MIGRATION_WORKDIR", "~/.openbkn-ai/migrations")).expanduser()
    if args.input:
        identity = f"offline\0{args.input.resolve()}"
        mode = "offline"
    else:
        identity = "\0".join((
            args.host, str(args.port), args.database, args.namespace,
            args.release, args.kube_context, args.kn_id or "*",
        ))
        mode = "rollback" if args.rollback is not None else "apply" if args.apply else "dry-run"
    key = hashlib.sha256(identity.encode()).hexdigest()[:16]
    directory = base / "0.2.2" / "action-type-v2" / key
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(directory, 0o700)
    if args.rollback_default:
        args.rollback = directory / "rollback.json"
    if args.output is None:
        args.output = directory / {
            "offline": "converted.json", "dry-run": "dry-run-plan.json",
            "apply": "rollback.json", "rollback": "rollback-result.json",
        }[mode]
    if args.report is None:
        args.report = directory / f"{mode}-report.json"


def helm_database_values(args: argparse.Namespace, required: list[str]) -> dict[str, str]:
    """Resolve missing DB fields from the installed release without logging credentials."""
    if not shutil.which("helm"):
        raise MigrationError("database settings are incomplete and helm is unavailable; provide all BKN_DB_* fields")
    command = ["helm", "get", "values", args.release, "--namespace", args.namespace, "--output", "json"]
    if args.kube_context:
        command += ["--kube-context", args.kube_context]
    try:
        result = subprocess.run(command, check=False, capture_output=True, text=True)
    except OSError as error:
        raise MigrationError("failed to read database settings from Helm") from error
    if result.returncode != 0:
        raise MigrationError("failed to read bkn-backend Helm values; check access or provide all BKN_DB_* fields")
    try:
        values = json.loads(result.stdout)
    except (TypeError, ValueError) as error:
        raise MigrationError("Helm returned invalid JSON while reading database settings") from error
    dep_services = values.get("depServices") if isinstance(values, dict) else None
    rds = dep_services.get("rds") if isinstance(dep_services, dict) else None
    if not isinstance(rds, dict):
        raise MigrationError("Helm values have no depServices.rds; provide all BKN_DB_* fields")
    resolved = {}
    for field in required:
        value = rds.get(field)
        if value is None or not str(value).strip():
            raise MigrationError(f"Helm values have no usable depServices.rds.{field}")
        resolved[field] = str(value).strip()
    return resolved


def resolve_database_config(args: argparse.Namespace) -> None:
    """Apply deploy's CLI > BKN_DB_* > Helm connection precedence."""
    resolved = {}
    for field, environment in DATABASE_ARGUMENTS.items():
        selected = getattr(args, field)
        resolved[field] = str(selected) if selected is not None else os.getenv(environment) or None
    missing = [field for field, value in resolved.items() if value is None]
    if missing:
        resolved.update(helm_database_values(args, missing))
    args.host = str(resolved["host"])
    try:
        args.port = int(str(resolved["port"]))
    except ValueError as error:
        raise MigrationError("database port must be an integer") from error
    if not 1 <= args.port <= 65535:
        raise MigrationError("database port is outside the valid range")
    args.user = str(resolved["user"])
    args.password = str(resolved["password"])
    args.database = str(resolved["database"])


def cluster_database_service(host: str) -> tuple[str, str] | None:
    match = re.fullmatch(
        r"([a-z0-9]([-a-z0-9]*[a-z0-9])?)\."
        r"([a-z0-9]([-a-z0-9]*[a-z0-9])?)\.svc"
        r"(?:\.cluster\.local)?", host,
    )
    return (match.group(1), match.group(3)) if match else None


def start_database_port_forward(service: str, namespace: str, remote_port: int, context: str) -> tuple[subprocess.Popen[str], int]:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        local_port = int(listener.getsockname()[1])
    command = ["kubectl"]
    if context:
        command += ["--context", context]
    command += ["--namespace", namespace, "port-forward", f"service/{service}", f"{local_port}:{remote_port}"]
    try:
        process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    except OSError as error:
        raise MigrationError("kubectl is required to reach the cluster database service") from error
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise MigrationError("kubectl database port-forward exited before becoming ready")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.settimeout(0.2)
            if client.connect_ex(("127.0.0.1", local_port)) == 0:
                return process, local_port
        time.sleep(0.1)
    stop_database_port_forward(process)
    raise MigrationError("timed out opening a database port-forward")


def stop_database_port_forward(process: subprocess.Popen[str] | None) -> None:
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def connect_database(args: argparse.Namespace, pymysql: Any) -> tuple[Any, subprocess.Popen[str] | None]:
    host, port, tunnel = args.host, args.port, None
    service = cluster_database_service(host)
    if service:
        tunnel, port = start_database_port_forward(*service, port, args.kube_context)
        host = "127.0.0.1"
    try:
        connection = pymysql.connect(
            host=host, port=port, user=args.user, password=args.password, database=args.database,
            charset="utf8mb4", cursorclass=pymysql.cursors.DictCursor, autocommit=False,
            connect_timeout=10,
        )
    except Exception as error:
        stop_database_port_forward(tunnel)
        raise MigrationError("database connection failed; verify the selected cluster and credentials") from error
    return connection, tunnel


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, help="Offline action/network JSON export")
    parser.add_argument("--output", type=Path, help="Optional override for the generated plan or rollback snapshot path")
    parser.add_argument("--report", type=Path, help="Optional override for the generated report path")
    parser.add_argument("--tool-schemas", type=Path, help="Resolved tool schemas for implicit MCP inputs or parameters missing their types")
    parser.add_argument("--kn-id", help="Restrict a DB plan to one knowledge network")
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--user")
    parser.add_argument("--password")
    parser.add_argument("--database")
    parser.add_argument("--namespace", default=os.getenv("BKN_NAMESPACE", "openbkn"))
    parser.add_argument("--release", default=os.getenv("BKN_HELM_RELEASE", "bkn-backend"))
    parser.add_argument("--kube-context", "--context", dest="kube_context", default=os.getenv("KUBECTL_CONTEXT", ""))
    parser.add_argument("--apply", action="store_true", help="Apply the database plan atomically")
    parser.add_argument("--allow-partial", action="store_true", help="With --apply, migrate convertible actions and report skipped actions; exit 2 while any remain")
    parser.add_argument("--rollback", nargs="?", const=ROLLBACK_DEFAULT, type=Path,
                        help="Restore the default or a specified snapshot; requires --apply")
    args = parser.parse_args()
    if args.rollback is not None and not args.apply:
        parser.error("--rollback requires --apply")
    if args.allow_partial and (not args.apply or args.rollback is not None):
        parser.error("--allow-partial requires --apply without --rollback")
    if args.input:
        if args.apply or args.rollback:
            parser.error("offline conversion cannot apply database changes")
        configure_artifact_paths(args)
        validate_artifact_paths(args.output, args.report, [path for path in (args.input, args.tool_schemas) if path])
        converted, report = convert_document(json.loads(args.input.read_text()))
        private_json(args.report, report)
        if report["errors"]:
            return 2
        private_json(args.output, converted)
        print(f"converted: {args.output}\nreport: {args.report}")
        return 0
    try:
        import pymysql  # Optional: only needed for the explicit database mode.
    except ImportError:
        parser.error("database mode requires PyMySQL; offline conversion uses only Python's standard library")
    resolve_database_config(args)
    configure_artifact_paths(args)
    validate_artifact_paths(args.output, args.report, [path for path in (args.tool_schemas, args.rollback) if isinstance(path, Path)])
    print(f"report: {args.report}")
    connection, tunnel = connect_database(args, pymysql)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SHOW COLUMNS FROM t_action_type LIKE 'f_operation_config'")
            if not cursor.fetchone():
                raise MigrationError("apply the 0.2.2 action-operation schema migration before converting data")
        if args.rollback:
            paths = rollback_snapshot_paths(args.rollback.parent) if args.rollback_default else [args.rollback]
            changes = load_rollback_snapshots(paths)
            with connection.cursor() as cursor:
                cursor.execute("SELECT f_id, f_kn_id, f_branch, f_update_time, " + ", ".join(MIGRATED_FIELDS) + " FROM t_action_type")
                current_rows = cursor.fetchall()
            selected, already = select_rollback_changes(changes, current_rows)
            if selected:
                apply_plan(connection, selected, rollback=True)
            private_json(args.report, {
                "restored": [item["id"] for item in selected],
                "already_restored_or_not_applied": already,
                "snapshots": [str(path) for path in paths],
            })
            return 0
        with connection.cursor() as cursor:
            statement = "SELECT " + ", ".join(DB_COLUMNS) + " FROM t_action_type"
            values = []
            if args.kn_id:
                statement += " WHERE f_kn_id = %s"
                values.append(args.kn_id)
            cursor.execute(statement, values)
            rows = cursor.fetchall()
            cursor.execute("SELECT f_id, f_kn_id, f_branch, f_action_type_id, f_instance_identities FROM t_action_schedule")
            schedules = cursor.fetchall()
        for row in rows:
            related = [item for item in schedules if (item["f_kn_id"], item["f_branch"], item["f_action_type_id"]) == (row["f_kn_id"], row["f_branch"], row["f_id"])]
            unsafe = []
            for schedule in related:
                identities = decoded(schedule["f_instance_identities"], [])
                bound = bool(row["f_object_type_id"])
                if (bound and (row["f_action_type"] == "add" or not identities)) or (not bound and identities):
                    unsafe.append(schedule["f_id"])
            if unsafe: row["_unsafe_schedule_ids"] = unsafe
        tool_schemas = {}
        if args.tool_schemas:
            snapshots = json.loads(args.tool_schemas.read_text())
            if not isinstance(snapshots,list): raise MigrationError("tool schemas must be an array")
            for snapshot in snapshots:
                key = (snapshot["kn_id"], snapshot.get("branch", "main"), snapshot["action_type_id"])
                if key in tool_schemas: raise MigrationError("duplicate tool schema snapshot")
                tool_schemas[key] = snapshot["input_schema"]
        changes, errors = plan_rows(rows, tool_schemas)
        if not args.apply:
            if args.output_explicit:
                write_migration_snapshot(args.output, changes)
            else:
                private_json(args.output, {"version": 1, "changes": changes})
            private_json(args.report, migration_report(changes, errors, applied=False))
            print(f"plan: {args.output}")
            return 2 if errors else 0
        private_json(args.report, migration_report(changes, errors, applied=False))
        if errors and not args.allow_partial:
            return 2
        if changes:
            args.output = apply_snapshot_path(args, changes, errors)
            validate_artifact_paths(args.output, args.report, [path for path in (args.tool_schemas, args.rollback) if isinstance(path, Path)])
            write_migration_snapshot(args.output, changes)
            print(f"rollback snapshot: {args.output}")
            apply_plan(connection, changes)
        private_json(args.report, migration_report(changes, errors, applied=not errors or bool(changes)))
        return 2 if errors else 0
    finally:
        connection.close()
        stop_database_port_forward(tunnel)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except MigrationError as error:
        raise SystemExit(str(error)) from None
