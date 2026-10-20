#!/usr/bin/env python3
"""Safely migrate unqualified legacy atomic metrics to object-metric V1.

This deployment upgrade is report-only unless --apply is supplied. Metrics that contain a
business condition, fixed grouping, order clauses, or non-atomic logic are deliberately
left for explicit business review. Legacy HAVING is treated as query-time logic and omitted.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import shutil
import socket
import sys
import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import pymysql


@dataclass
class Decision:
    metric_id: str
    action: str
    reason: str
    definition: dict[str, Any] | None = None
    definitions: list[dict[str, Any]] = field(default_factory=list)
    lost_capabilities: list[str] = field(default_factory=list)
    normalizations: list[str] = field(default_factory=list)


# Keep decimal defaults aligned with the Studio Object Metrics form defaults.
DEFAULT_DECIMAL_PRECISION = 18
DEFAULT_DECIMAL_SCALE = 2
DEFAULT_DECIMAL_ROUNDING = "half_up"
COUNT_AGGREGATIONS = {"count_rows", "count", "count_distinct"}
AGGREGATION_MAP = {
    "==": "eq", "=": "eq", "!=": "neq", "<>": "neq",
    ">": "gt", ">=": "gte", "<": "lt", "<=": "lte",
    "in": "in", "not_in": "not_in", "between": "between",
    "not_between": "not_between", "contains": "contains",
    "not_contains": "not_contains", "starts_with": "starts_with",
    "ends_with": "ends_with", "is_null": "is_null",
    "is_not_null": "is_not_null", "is_empty": "is_empty",
    "is_not_empty": "is_not_empty", "is_true": "is_true", "is_false": "is_false",
}
INTEGER_PROPERTY_TYPES = {"byte", "int", "int8", "int16", "int32", "int64", "integer", "long", "short", "uint", "uint8", "uint16", "uint32", "uint64"}
DECIMAL_PROPERTY_TYPES = {"decimal", "double", "float", "float32", "float64", "number"}
NUMERIC_PROPERTY_TYPES = INTEGER_PROPERTY_TYPES | DECIMAL_PROPERTY_TYPES


def parse_json(value: Any, default: Any) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    return json.loads(value)


def stable_code(metric_id: str) -> str:
    return "legacy_" + re.sub(r"[^a-z0-9_]+", "_", metric_id.lower()).strip("_")


def published_at(row: dict[str, Any]) -> str:
    milliseconds = int(row.get("f_update_time") or row.get("f_create_time") or 0)
    return datetime.fromtimestamp(milliseconds / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def unit_spec(row: dict[str, Any]) -> dict[str, Any] | None:
    unit = str(row.get("f_unit") or "").strip()
    if not unit or unit == "none":
        return None
    if unit == "1":
        return {"system": "UCUM", "quantity_kind": "count", "code": "1", "scale_factor": 1}
    # Legacy countUnit values (e.g. piece, item, transaction) are not UCUM
    # codes. Preserve their original label as a registered count unit instead
    # of collapsing distinct business units to the dimensionless code "1".
    return {"system": "registered", "quantity_kind": "count", "code": unit, "scale_factor": 1}


def stable_helper_id(kn_id: str, branch: str, metric_id: str) -> str:
    identity = "\0".join((kn_id, branch, metric_id)).encode("utf-8")
    return "legacy_base_" + hashlib.sha256(identity).hexdigest()[:20]


def result_unit_spec(row: dict[str, Any], aggregation: str) -> tuple[dict[str, Any] | None, bool, str | None]:
    unit_type = str(row.get("f_unit_type") or "").strip()
    unit = str(row.get("f_unit") or "").strip()
    if aggregation in COUNT_AGGREGATIONS:
        if unit_type in {"", "countUnit", "numUnit"} and unit in {"", "none", "1"}:
            return {"system": "UCUM", "quantity_kind": "count", "code": "1", "scale_factor": 1}, unit != "1", None
        if unit_type != "countUnit":
            return None, False, "count result has an incompatible legacy unit"
        return unit_spec(row), False, None
    if unit_type in {"", "numUnit"} and unit in {"", "none"}:
        return None, False, None
    if unit_type in {"", "numUnit"} and unit == "1":
        return {"system": "UCUM", "quantity_kind": "dimensionless", "code": "1", "scale_factor": 1}, False, None
    if unit_type == "numUnit":
        return {"system": "registered", "quantity_kind": "custom", "code": unit, "scale_factor": 1}, False, None
    if unit_type == "countUnit":
        return {"system": "registered", "quantity_kind": "count", "code": unit, "scale_factor": 1}, False, None
    if unit_type == "currencyUnit" and re.fullmatch(r"[A-Z]{3}", unit):
        return {"system": "ISO_4217", "quantity_kind": "currency", "code": unit, "scale_factor": 1}, False, None
    if unit_type == "timeUnit" and unit in {"second", "minute", "hour", "day", "week", "month", "year"}:
        return {"system": "registered", "quantity_kind": "duration", "code": unit, "scale_factor": 1}, False, None
    return None, False, f"legacy unit is incompatible with aggregation result: {unit_type or '<empty>'}/{unit or '<empty>'}"


def property_physical_type(property_type: str) -> str:
    normalized = property_type.lower()
    if normalized in INTEGER_PROPERTY_TYPES:
        return "int64"
    if normalized in DECIMAL_PROPERTY_TYPES:
        return "decimal"
    if normalized in {"bool", "boolean"}:
        return "boolean"
    if normalized in {"date"}:
        return "date"
    if normalized in {"datetime", "timestamp"}:
        return "datetime"
    if normalized in {"string", "text", "varchar", "char", "category"}:
        return "string"
    return ""


def result_value_semantics(
    aggregation: str, property_type: str, unit_type: str, unit: dict[str, Any] | None,
) -> tuple[dict[str, Any] | None, str | None]:
    if aggregation in COUNT_AGGREGATIONS:
        value: dict[str, Any] = {"physical_type": "int64", "semantic_type": "count"}
    elif unit_type == "timeUnit":
        value = {"physical_type": "duration", "semantic_type": "duration"}
    else:
        normalized_type = property_type.lower()
        semantic_type = "amount" if unit_type == "currencyUnit" else "count" if unit_type == "countUnit" else "measure"
        if unit_type == "currencyUnit":
            physical_type = "decimal"
        elif aggregation in {"sum", "min", "max"} and normalized_type in INTEGER_PROPERTY_TYPES:
            physical_type = "int64"
        elif aggregation in {"min", "max"} and normalized_type in {"date"}:
            physical_type = "date"
        elif aggregation in {"min", "max"} and normalized_type in {"datetime", "timestamp"}:
            physical_type = "datetime"
        elif aggregation in {"min", "max"} and normalized_type in {"string", "text", "varchar", "char", "category"}:
            physical_type = "string"
        elif normalized_type in NUMERIC_PROPERTY_TYPES:
            physical_type = "decimal"
        else:
            return None, f"cannot determine result type for {aggregation} over {property_type or '<unknown>'}"
        value = {"physical_type": physical_type, "semantic_type": semantic_type}
        if physical_type == "decimal":
            value.update({
                "precision": DEFAULT_DECIMAL_PRECISION,
                "scale": DEFAULT_DECIMAL_SCALE,
                "rounding_mode": DEFAULT_DECIMAL_ROUNDING,
            })
    if unit:
        value["unit"] = unit
    return value, None


def literal_operand(value: Any, physical_type: str) -> dict[str, Any] | None:
    try:
        if physical_type == "int64":
            converted: Any = int(value)
        elif physical_type == "decimal":
            converted = float(value)
            if converted != converted or converted in {float("inf"), float("-inf")}:
                return None
        elif physical_type == "boolean":
            if isinstance(value, bool):
                converted = value
            elif str(value).lower() in {"true", "1"}:
                converted = True
            elif str(value).lower() in {"false", "0"}:
                converted = False
            else:
                return None
        elif physical_type in {"string", "date", "datetime"}:
            converted = str(value)
        else:
            return None
    except (TypeError, ValueError, OverflowError):
        return None
    return {"source": "literal", "value_type": physical_type, "value": converted}


def legacy_condition_to_v1(
    condition: Any, owner_id: str, object_properties: dict[str, str] | None,
) -> tuple[dict[str, Any] | None, str | None]:
    if not isinstance(condition, dict):
        return None, "legacy condition is not a predicate object"
    allowed = {"field", "operation", "value_from", "value", "RemainCfg"}
    if set(condition) - allowed:
        return None, "legacy condition contains unknown fields"
    if condition.get("value_from") != "const" or condition.get("RemainCfg") not in (None, ""):
        return None, "legacy condition is not a constant predicate"
    property_name = condition.get("field")
    operation = AGGREGATION_MAP.get(str(condition.get("operation") or "").lower())
    if not isinstance(property_name, str) or not property_name:
        return None, "legacy condition property is missing"
    if not operation:
        return None, f"unsupported legacy condition operator: {condition.get('operation')!r}"
    property_type = object_properties.get(property_name) if object_properties is not None else None
    if object_properties is not None and property_type is None:
        return None, f"legacy condition property does not exist: {property_name}"
    physical_type = property_physical_type(property_type or "string")
    if not physical_type:
        return None, f"unsupported condition property type: {property_type!r}"
    result: dict[str, Any] = {
        "node_type": "predicate",
        "comparison_operator": operation,
        "left": {
            "source": "property",
            "property_ref": {"object_type_id": owner_id, "property_name": property_name},
        },
    }
    if operation in {"is_null", "is_not_null", "is_empty", "is_not_empty", "is_true", "is_false"}:
        result["right"] = []
        return result, None
    raw_value = condition.get("value")
    if operation in {"in", "not_in"}:
        values = raw_value if isinstance(raw_value, list) else [raw_value]
    elif operation in {"between", "not_between"}:
        values = raw_value if isinstance(raw_value, list) else []
        if len(values) != 2:
            return None, "legacy range condition must contain exactly two values"
    else:
        values = [raw_value]
    if raw_value is None and operation not in {"is_null", "is_not_null"}:
        return None, "legacy condition value is missing"
    operands = [literal_operand(value, physical_type) for value in values]
    if not operands or any(operand is None for operand in operands):
        return None, f"legacy condition value cannot be converted to {physical_type}"
    result["right"] = operands
    return result, None


def legacy_metric_bindings(object_rows: list[dict[str, Any]]) -> tuple[dict[tuple[str, str, str], list[str]], set[tuple[str, str]]]:
    """Find legacy metric-backed properties that cannot use object-set metrics."""
    bindings: dict[tuple[str, str, str], list[str]] = {}
    malformed: set[tuple[str, str]] = set()
    for row in object_rows:
        kn_branch = (row["f_kn_id"], row["f_branch"])
        try:
            properties = parse_json(row.get("f_logic_properties"), [])
        except (TypeError, ValueError):
            malformed.add(kn_branch)
            continue
        # Some legacy rows store JSON `null` rather than an empty list. It means
        # "no logic properties", not a malformed object type. Treating it as a
        # schema error incorrectly blocks every legacy metric in the network.
        if properties is None:
            properties = []
        if not isinstance(properties, list):
            malformed.add(kn_branch)
            continue
        for prop in properties:
            if not isinstance(prop, dict):
                malformed.add(kn_branch)
                continue
            source = prop.get("data_source")
            if prop.get("type") != "metric" or not isinstance(source, dict):
                continue
            metric_id = source.get("id")
            if not isinstance(metric_id, str) or not metric_id:
                malformed.add(kn_branch)
                continue
            key = (*kn_branch, metric_id)
            bindings.setdefault(key, []).append(f"{row['f_id']}.{prop.get('name') or '<unnamed>'}")
    return bindings, malformed


def cluster_database_service(host: str) -> tuple[str, str] | None:
    """Return service and namespace for an in-cluster Kubernetes DNS name."""
    match = re.fullmatch(
        r"([a-z0-9]([-a-z0-9]*[a-z0-9])?)\."
        r"([a-z0-9]([-a-z0-9]*[a-z0-9])?)\.svc"
        r"(?:\.cluster\.local)?",
        host,
    )
    return (match.group(1), match.group(3)) if match else None


def start_database_port_forward(
    service: str, namespace: str, remote_port: int, context: str = "",
) -> tuple[subprocess.Popen[str], int]:
    """Expose an in-cluster DB service on a temporary loopback port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        local_port = int(listener.getsockname()[1])
    command = ["kubectl"]
    if context:
        command.extend(["--context", context])
    command.extend([
        "--namespace", namespace, "port-forward", f"service/{service}",
        f"{local_port}:{remote_port}",
    ])
    process = subprocess.Popen(
        command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
    )
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if process.poll() is not None:
            detail = process.stderr.read().strip() if process.stderr else ""
            raise RuntimeError(f"kubectl port-forward failed: {detail or 'process exited'}")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.settimeout(0.2)
            if client.connect_ex(("127.0.0.1", local_port)) == 0:
                return process, local_port
        time.sleep(0.1)
    stop_database_port_forward(process)
    raise RuntimeError(f"timed out opening port-forward to service/{service} in {namespace}")


def stop_database_port_forward(process: subprocess.Popen[str] | None) -> None:
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


DATABASE_ARGUMENTS = {
    "host": "BKN_DB_HOST",
    "port": "BKN_DB_PORT",
    "user": "BKN_DB_USER",
    "password": "BKN_DB_PASSWORD",
    "database": "BKN_DB_NAME",
}


def helm_context_arguments(context: str) -> list[str]:
    return ["--kube-context", context] if context else []


def helm_database_values(args: argparse.Namespace, required: list[str]) -> dict[str, str]:
    """Read database settings from the installed backend Helm release."""
    if not shutil.which("helm"):
        raise RuntimeError(
            "database settings are incomplete and helm is unavailable; provide all of "
            "--host, --port, --user, --password, and --database or set the BKN_DB_* variables"
        )
    command = [
        "helm", "get", "values", args.release,
        "--namespace", args.namespace,
        "--output", "json",
        *helm_context_arguments(args.kube_context),
    ]
    try:
        result = subprocess.run(command, check=False, capture_output=True, text=True)
    except OSError as exc:
        raise RuntimeError("failed to read database settings from Helm") from exc
    if result.returncode != 0:
        raise RuntimeError(
            f"failed to read Helm values for release {args.release!r} "
            f"in namespace {args.namespace!r} (exit code {result.returncode}); "
            "check Helm access or provide all database connection parameters explicitly"
        )
    try:
        values = json.loads(result.stdout)
    except (TypeError, json.JSONDecodeError) as exc:
        raise RuntimeError("Helm returned invalid JSON while reading database settings") from exc
    dependencies = values.get("depServices") if isinstance(values, dict) else None
    rds = dependencies.get("rds") if isinstance(dependencies, dict) else None
    if not isinstance(rds, dict):
        raise RuntimeError(
            f"Helm values for release {args.release!r} have no depServices.rds; "
            "provide all database connection parameters explicitly"
        )
    resolved: dict[str, str] = {}
    for key in required:
        value = rds.get(key)
        if value is None or not str(value).strip():
            raise RuntimeError(
                f"Helm values for release {args.release!r} have no usable depServices.rds.{key}"
            )
        resolved[key] = str(value).strip()
    return resolved


def resolve_database_config(args: argparse.Namespace) -> None:
    """Resolve connection settings with CLI > environment > Helm precedence."""
    resolved: dict[str, str | None] = {}
    for name, environment_name in DATABASE_ARGUMENTS.items():
        cli_value = getattr(args, name)
        environment_value = os.getenv(environment_name)
        resolved[name] = (
            str(cli_value) if cli_value is not None
            else environment_value if environment_value not in (None, "")
            else None
        )

    missing = [name for name, value in resolved.items() if value is None]
    if missing:
        helm_values = helm_database_values(args, missing)
        for name in missing:
            resolved[name] = helm_values[name]

    args.host = str(resolved["host"])
    args.port = int(str(resolved["port"]))
    args.user = str(resolved["user"])
    args.password = str(resolved["password"])
    args.database = str(resolved["database"])


def connect_database(args: argparse.Namespace) -> tuple[Any, subprocess.Popen[str] | None]:
    host, port, tunnel = args.host, args.port, None
    service = cluster_database_service(host)
    if service:
        tunnel, port = start_database_port_forward(*service, port, args.kube_context)
        host = "127.0.0.1"
    try:
        connection = pymysql.connect(
            host=host,
            port=port,
            user=args.user,
            password=args.password,
            database=args.database,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
            connect_timeout=10,
        )
    except Exception:
        stop_database_port_forward(tunnel)
        raise
    return connection, tunnel


def translate(
    row: dict[str, Any], object_properties: dict[str, str] | None = None,
) -> Decision:
    metric_id = row["f_id"]
    if row.get("f_metric_type") != "atomic":
        return Decision(metric_id, "manual_review", "only legacy atomic metrics are supported")
    if row.get("f_scope_type") != "object_type" or not row.get("f_scope_ref"):
        return Decision(metric_id, "manual_review", "metric is not owned by one object type")
    if not str(row.get("f_name") or "").strip():
        return Decision(metric_id, "manual_review", "metric name is missing")
    try:
        formula = parse_json(row.get("f_calculation_formula"), {})
    except (TypeError, ValueError):
        return Decision(metric_id, "manual_review", "calculation formula is not valid JSON")
    if not isinstance(formula, dict):
        return Decision(metric_id, "manual_review", "calculation formula is not an object")
    if set(formula) - {"aggregation", "condition", "group_by", "order_by", "having"}:
        return Decision(metric_id, "manual_review", "calculation formula contains unknown fields")

    lost_capabilities: list[str] = []
    if formula.get("having"):
        lost_capabilities.append("having omitted as query-time condition")
    if formula.get("group_by"):
        lost_capabilities.append("group_by omitted as query-time analysis setting")
    if formula.get("order_by"):
        lost_capabilities.append("order_by omitted as query-time analysis setting")

    try:
        time_dimension = parse_json(row.get("f_time_dimension"), {})
    except (TypeError, ValueError):
        return Decision(metric_id, "manual_review", "time dimension is not valid JSON")
    if time_dimension is None:
        time_dimension = {}
    if (
        isinstance(time_dimension, dict)
        and not (time_dimension.get("property") or time_dimension.get("field"))
        and str(time_dimension.get("default_range_policy") or "").strip() in {"", "none"}
    ):
        time_dimension = {}
    if not isinstance(time_dimension, dict):
        return Decision(metric_id, "manual_review", "time dimension does not use the expected object shape")
    if set(time_dimension) - {"property", "field", "default_range_policy"}:
        return Decision(metric_id, "manual_review", "time dimension contains unknown fields")
    time_property = str(time_dimension.get("property") or time_dimension.get("field") or "").strip()
    default_range_policy = str(time_dimension.get("default_range_policy") or "none").strip() or "none"
    if default_range_policy not in {"none", "last_24h"}:
        return Decision(
            metric_id, "manual_review",
            f"time dimension default range policy cannot be migrated losslessly: {default_range_policy!r}",
        )
    if time_dimension and not time_property and default_range_policy != "none":
        return Decision(metric_id, "manual_review", "time dimension property is required by its default range")
    if time_dimension and not time_property and default_range_policy == "none":
        return Decision(metric_id, "manual_review", "time dimension property is missing")
    if object_properties is not None and time_property:
        property_type = object_properties.get(time_property)
        if property_type is None or property_physical_type(property_type) not in {"date", "datetime"}:
            return Decision(metric_id, "manual_review", f"time dimension property is missing or not temporal: {time_property}")

    aggregation = formula.get("aggregation") or {}
    if not isinstance(aggregation, dict) or set(aggregation) - {"aggr", "property"}:
        return Decision(metric_id, "manual_review", "aggregation does not use the expected shape")
    aggregation_name = aggregation.get("aggr")
    property_name = aggregation.get("property")
    if aggregation_name not in {"count", "count_distinct", "sum", "avg", "min", "max"}:
        return Decision(metric_id, "manual_review", f"unsupported aggregation: {aggregation_name!r}")
    if aggregation_name != "count" and (not isinstance(property_name, str) or not property_name.strip()):
        return Decision(metric_id, "manual_review", "aggregation property is missing")
    if object_properties is not None and property_name and property_name not in object_properties:
        return Decision(metric_id, "manual_review", f"aggregation property does not exist: {property_name}")
    source_property_type = object_properties.get(property_name, "") if object_properties is not None and property_name else ""
    if aggregation_name in {"sum", "avg"} and source_property_type and source_property_type.lower() not in NUMERIC_PROPERTY_TYPES:
        return Decision(metric_id, "manual_review", f"{aggregation_name} requires a numeric property: {property_name}")
    if aggregation_name in {"min", "max"} and source_property_type and source_property_type.lower() not in NUMERIC_PROPERTY_TYPES:
        return Decision(metric_id, "manual_review", f"{aggregation_name} requires a numeric property: {property_name}")

    migrated_aggregation = "count_rows" if aggregation_name == "count" and not property_name else aggregation_name
    migrated_unit, unit_defaulted, unit_error = result_unit_spec(row, migrated_aggregation)
    if unit_error:
        return Decision(metric_id, "manual_review", unit_error)
    unit_type = str(row.get("f_unit_type") or "").strip()
    value, value_error = result_value_semantics(
        migrated_aggregation, source_property_type, unit_type, migrated_unit,
    )
    if value_error:
        return Decision(metric_id, "manual_review", value_error)

    try:
        analysis = parse_json(row.get("f_analysis_dimensions"), [])
    except (TypeError, ValueError):
        return Decision(metric_id, "manual_review", "analysis dimensions are not valid JSON")
    if analysis is None:
        analysis = []
    if not isinstance(analysis, list) or any(
        not isinstance(item, dict)
        or set(item) - {"name", "display_name"}
        or not isinstance(item.get("name"), str)
        or not item["name"].strip()
        for item in analysis
    ):
        return Decision(metric_id, "manual_review", "analysis dimensions do not use the expected structured list")
    if object_properties is not None:
        missing_dimensions = [item["name"] for item in analysis if item["name"] not in object_properties]
        if missing_dimensions:
            return Decision(metric_id, "manual_review", "analysis dimensions do not exist: " + ", ".join(missing_dimensions))

    converted_condition = None
    if formula.get("condition"):
        converted_condition, condition_error = legacy_condition_to_v1(
            formula["condition"], row["f_scope_ref"], object_properties,
        )
        if condition_error:
            return Decision(metric_id, "manual_review", condition_error)

    expression = None
    if migrated_aggregation != "count_rows":
        expression = {
            "node_type": "property",
            "property_ref": {
                "object_type_id": row["f_scope_ref"],
                "property_name": property_name,
            },
        }
    if migrated_aggregation in COUNT_AGGREGATIONS:
        value["physical_type"] = "int64"
        value["semantic_type"] = "count"
        value.pop("precision", None)
        value.pop("scale", None)
        value.pop("rounding_mode", None)

    analysis_dimensions = [
        {"object_type_id": row["f_scope_ref"], "property_name": item["name"]}
        for item in analysis if item.get("name")
    ]
    time_dimensions = [
        {"object_type_id": row["f_scope_ref"], "property_name": time_property}
    ] if time_property else []
    lifecycle = {
        "status": "published",
        "published_at": published_at(row),
        "ontology_version": "migration-snapshot",
        "expression_language_version": "1.0",
        "function_registry_version": "1.0",
        "aggregation_registry_version": "1.0",
        "unit_registry_version": "1.0",
    }
    result_semantics = {
        "statistical_grain": {"group_by_dimensions": []},
        "value": value,
        "no_data_policy": "zero",
        "partial_data_policy": "reject",
    }
    query_capabilities = {
        "analysis_dimensions": analysis_dimensions,
        "time_dimensions": time_dimensions,
    }
    base_id = stable_helper_id(row["f_kn_id"], row["f_branch"], metric_id)
    base_definition = {
        "id": base_id,
        "version": 1,
        "code": stable_code(base_id),
        "name": f"{row['f_name']}（迁移基础指标）",
        "description": f"Generated base atomic metric for migrated metric {row['f_name']}",
        "owner_object_type_id": row["f_scope_ref"],
        "metric_type": "atomic",
        "calculation_scope": "object_set",
        "result_semantics": copy.deepcopy(result_semantics),
        "specification": {
            "kind": "atomic",
            "measure": {"aggregation": migrated_aggregation, **({"expression": expression} if expression else {})},
        },
        "query_capabilities": copy.deepcopy(query_capabilities),
        "lifecycle": copy.deepcopy(lifecycle),
    }
    final_definition = copy.deepcopy(base_definition)
    final_definition.update({"id": metric_id, "code": stable_code(metric_id), "name": row["f_name"]})
    final_definition["description"] = row.get("f_comment") or f"Migrated from legacy metric {row['f_name']}"

    needs_derived = converted_condition is not None or default_range_policy == "last_24h"
    definitions = [final_definition]
    if needs_derived:
        qualifiers: dict[str, Any] = {}
        if converted_condition is not None:
            qualifiers["filters"] = converted_condition
        if default_range_policy == "last_24h":
            qualifiers["event_time"] = {"object_type_id": row["f_scope_ref"], "property_name": time_property}
            qualifiers["time_window"] = {"mode": "rolling", "length": 24, "unit": "hour"}
        final_definition["metric_type"] = "derived"
        final_definition["specification"] = {
            "kind": "derived",
            "base_metric": {"metric_id": base_id, "version": 1},
            "business_qualifiers": qualifiers,
        }
        definitions = [base_definition, final_definition]

    normalizations: list[str] = []
    if unit_defaulted:
        normalizations.append("missing count unit defaulted to UCUM 1")
    if value.get("physical_type") == "decimal":
        normalizations.append(
            f"decimal default applied: precision={DEFAULT_DECIMAL_PRECISION}, "
            f"scale={DEFAULT_DECIMAL_SCALE}, rounding={DEFAULT_DECIMAL_ROUNDING}"
        )
    if default_range_policy == "last_24h":
        normalizations.append("legacy last_24h default preserved as a rolling 24-hour derived metric")
    conversion = "atomic plus derived qualifier metric" if needs_derived else "atomic metric"
    reason = conversion + ("; query-time clauses omitted" if lost_capabilities else "")
    return Decision(
        metric_id, "migrate", reason, final_definition, definitions,
        lost_capabilities, normalizations,
    )


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--user")
    parser.add_argument("--password")
    parser.add_argument("--database")
    parser.add_argument("--namespace", default=os.getenv("BKN_NAMESPACE", "openbkn"))
    parser.add_argument("--release", default=os.getenv("BKN_HELM_RELEASE", "bkn-backend"))
    parser.add_argument(
        "--kube-context", "--context", dest="kube_context",
        default=os.getenv("KUBECTL_CONTEXT", ""),
    )
    parser.add_argument("--kn-id")
    parser.add_argument("--branch", default="main")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--report", default="object-metric-migration-report.json")
    return parser.parse_args()


def main() -> int:
    args = arguments()
    resolve_database_config(args)
    connection, tunnel = connect_database(args)
    where = ["f_branch = %s"]
    params: list[Any] = [args.branch]
    if args.kn_id:
        where.append("f_kn_id = %s")
        params.append(args.kn_id)
    select = """
        SELECT f_id, f_kn_id, f_branch, f_name, f_comment, f_unit_type, f_unit,
               f_metric_type, f_scope_type, f_scope_ref, f_calculation_formula,
               f_analysis_dimensions, f_time_dimension, f_creator, f_creator_type, f_create_time,
               f_updater, f_updater_type, f_update_time
          FROM t_metric_definition
         WHERE """ + " AND ".join(where)
    decisions: list[Decision] = []
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT f_kn_id, f_branch, f_id, f_version, f_code
                  FROM t_object_metric_definition
                 WHERE f_branch = %s
                """,
                (args.branch,),
            )
            existing_rows = cursor.fetchall()
            existing_ids = {
                (item["f_kn_id"], item["f_branch"], item["f_id"], item["f_version"])
                for item in existing_rows
            }
            code_owners: dict[tuple[str, str, str], set[str]] = {}
            for item in existing_rows:
                code_key = (item["f_kn_id"], item["f_branch"], item["f_code"])
                code_owners.setdefault(code_key, set()).add(item["f_id"])
            planned_codes: dict[tuple[str, str, str], str] = {}
            cursor.execute(
                "SELECT f_kn_id, f_branch, f_id, f_logic_properties, f_data_properties "
                "FROM t_object_type WHERE f_branch = %s",
                (args.branch,),
            )
            object_rows = cursor.fetchall()
            owner_ids = {
                (item["f_kn_id"], item["f_branch"], item["f_id"])
                for item in object_rows
            }
            property_types: dict[tuple[str, str, str], dict[str, str]] = {}
            for item in object_rows:
                try:
                    properties = parse_json(item.get("f_data_properties"), [])
                except (TypeError, ValueError):
                    properties = None
                if not isinstance(properties, list):
                    properties = []
                property_types[(item["f_kn_id"], item["f_branch"], item["f_id"])] = {
                    prop["name"]: str(prop.get("type") or "")
                    for prop in properties
                    if isinstance(prop, dict) and isinstance(prop.get("name"), str)
                }
            bindings, _malformed_bindings = legacy_metric_bindings(object_rows)
            cursor.execute(select, params)
            rows = cursor.fetchall()
            planned_metric_ids: dict[tuple[str, str, str], str] = {}
            for row in rows:
                owner_key = (row["f_kn_id"], row["f_branch"], row["f_scope_ref"])
                translated = translate(row, property_types.get(owner_key, {}))
                decision = translated
                binding_locations = bindings.get((row["f_kn_id"], row["f_branch"], row["f_id"]), [])
                if binding_locations:
                    decision.lost_capabilities.append(
                        "logic-property bindings omitted: " + ", ".join(binding_locations)
                    )
                definitions = decision.definitions or ([decision.definition] if decision.definition else [])
                if decision.action == "migrate" and definitions:
                    if any(
                        (row["f_kn_id"], row["f_branch"], definition["owner_object_type_id"]) not in owner_ids
                        for definition in definitions
                    ):
                        decision = Decision(row["f_id"], "manual_review", "owner object type does not exist")
                        definitions = []
                    else:
                        target_keys = [
                            (row["f_kn_id"], row["f_branch"], definition["id"], definition["version"])
                            for definition in definitions
                        ]
                        existing_count = sum(key in existing_ids for key in target_keys)
                        if existing_count == len(target_keys):
                            decision = Decision(row["f_id"], "already_exists", "all target metric definitions already exist")
                            definitions = []
                        elif existing_count:
                            decision = Decision(
                                row["f_id"], "manual_review",
                                "only part of the target metric chain already exists; review before retrying",
                            )
                            definitions = []
                if decision.action == "migrate" and definitions:
                    for definition in definitions:
                        metric_id_key = (row["f_kn_id"], row["f_branch"], definition["id"])
                        planned_metric_owner = planned_metric_ids.get(metric_id_key)
                        if planned_metric_owner and planned_metric_owner != row["f_id"]:
                            decision = Decision(
                                row["f_id"], "manual_review",
                                f"target metric id conflicts with migration of {planned_metric_owner}",
                            )
                            break
                        code_key = (row["f_kn_id"], row["f_branch"], definition["code"])
                        conflicting_owners = code_owners.get(code_key, set()) - {definition["id"]}
                        planned_owner = planned_codes.get(code_key)
                        if planned_owner and planned_owner != definition["id"]:
                            conflicting_owners.add(planned_owner)
                        if conflicting_owners:
                            decision = Decision(
                                row["f_id"], "manual_review",
                                f"stable code conflicts with metric {sorted(conflicting_owners)[0]}: {definition['code']}",
                            )
                            break
                    if decision.action == "migrate":
                        for definition in definitions:
                            metric_id_key = (row["f_kn_id"], row["f_branch"], definition["id"])
                            planned_metric_ids[metric_id_key] = row["f_id"]
                            code_key = (row["f_kn_id"], row["f_branch"], definition["code"])
                            planned_codes[code_key] = definition["id"]
                decisions.append(decision)
                if not args.apply or decision.action != "migrate" or not definitions:
                    continue
                for definition in definitions:
                    cursor.execute(
                        """
                        INSERT INTO t_object_metric_definition
                        (f_kn_id, f_branch, f_id, f_version, f_code, f_name, f_description,
                         f_owner_object_type_id, f_metric_type, f_status, f_definition,
                         f_creator, f_creator_type, f_create_time, f_updater, f_updater_type, f_update_time)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'published', %s,
                                %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            row["f_kn_id"], row["f_branch"], definition["id"], definition["version"],
                            definition["code"], definition["name"], definition["description"],
                            definition["owner_object_type_id"], definition["metric_type"],
                            json.dumps(definition, ensure_ascii=False, separators=(",", ":")),
                            row["f_creator"], row["f_creator_type"], row["f_create_time"],
                            row["f_updater"], row["f_updater_type"], row["f_update_time"],
                        ),
                    )
        if args.apply:
            connection.commit()
        else:
            connection.rollback()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
        stop_database_port_forward(tunnel)
    report = {
        "mode": "apply" if args.apply else "dry-run",
        "summary": {
            "total": len(decisions),
            "migrate": sum(item.action == "migrate" for item in decisions),
            "manual_review": sum(item.action == "manual_review" for item in decisions),
            "failed": sum(item.action == "manual_review" for item in decisions),
            "already_exists": sum(item.action == "already_exists" for item in decisions),
            "target_definitions": sum(
                len(item.definitions or ([item.definition] if item.definition else []))
                for item in decisions if item.action == "migrate"
            ),
        },
        "entries": [
            {
                "kn_id": row["f_kn_id"],
                "branch": row["f_branch"],
                "legacy_metric_id": row["f_id"],
                "legacy_name": row["f_name"],
                "legacy_metric_type": row["f_metric_type"],
                "action": item.action,
                "migration_status": (
                    "migrated_with_loss" if item.action == "migrate" and item.lost_capabilities
                    else "migrated" if item.action == "migrate"
                    else "already_exists" if item.action == "already_exists"
                    else "failed"
                ),
                "reason": item.reason,
                "target_metrics": [
                    {"id": definition["id"], "name": definition["name"], "metric_type": definition["metric_type"]}
                    for definition in (item.definitions or ([item.definition] if item.definition else []))
                ],
                "lost_capabilities": item.lost_capabilities,
                "normalizations": item.normalizations,
                "definitions": item.definitions or ([item.definition] if item.definition else []),
            }
            for row, item in zip(rows, decisions)
        ],
    }
    with open(args.report, "w", encoding="utf-8") as output:
        json.dump(report, output, ensure_ascii=False, indent=2)
    print(json.dumps(report["summary"], ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
