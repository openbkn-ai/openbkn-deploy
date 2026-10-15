#!/usr/bin/env python3
"""Safely migrate unqualified legacy atomic metrics to object-metric V1.

This deployment upgrade is report-only unless --apply is supplied. Metrics that contain a
business condition, fixed grouping, having/order clauses, or non-atomic logic
are deliberately left for explicit business review instead of being guessed.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pymysql


@dataclass
class Decision:
    metric_id: str
    action: str
    reason: str
    definition: dict[str, Any] | None = None


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
    return {"system": "UCUM", "quantity_kind": "count", "code": "1", "scale_factor": 1}


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


def review_bound_metric(
    row: dict[str, Any], decision: Decision,
    bindings: dict[tuple[str, str, str], list[str]], malformed: set[tuple[str, str]],
) -> Decision:
    kn_branch = (row["f_kn_id"], row["f_branch"])
    if kn_branch in malformed:
        return Decision(row["f_id"], "manual_review", "object type logic properties are malformed in this knowledge network")
    locations = bindings.get((*kn_branch, row["f_id"]))
    if locations:
        return Decision(row["f_id"], "manual_review", "legacy metric backs object-type logic properties: " + ", ".join(locations))
    return decision


def translate(row: dict[str, Any]) -> Decision:
    metric_id = row["f_id"]
    if row.get("f_metric_type") != "atomic":
        return Decision(metric_id, "manual_review", "only legacy atomic metrics are losslessly auto-migrated")
    if row.get("f_scope_type") != "object_type" or not row.get("f_scope_ref"):
        return Decision(metric_id, "manual_review", "metric is not owned by one object type")
    if not str(row.get("f_name") or "").strip():
        return Decision(metric_id, "manual_review", "metric name is missing")
    if row.get("f_unit_type") == "transmissionRate":
        return Decision(
            metric_id,
            "manual_review",
            "legacy rate unit has no explicit numerator and denominator contract",
        )
    try:
        formula = parse_json(row.get("f_calculation_formula"), {})
    except (TypeError, ValueError):
        return Decision(metric_id, "manual_review", "calculation formula is not valid JSON")
    if not isinstance(formula, dict):
        return Decision(metric_id, "manual_review", "calculation formula is not an object")
    if set(formula) - {"aggregation", "condition", "group_by", "order_by", "having"}:
        return Decision(metric_id, "manual_review", "calculation formula contains unknown fields")
    if formula.get("condition") or formula.get("group_by") or formula.get("order_by") or formula.get("having"):
        return Decision(metric_id, "manual_review", "legacy business qualifiers must be separated into a derived metric")
    try:
        time_dimension = parse_json(row.get("f_time_dimension"), {})
    except (TypeError, ValueError):
        return Decision(metric_id, "manual_review", "time dimension is not valid JSON")
    if not isinstance(time_dimension, dict):
        return Decision(metric_id, "manual_review", "time dimension does not use the expected object shape")
    if set(time_dimension) - {"property", "field", "default_range_policy"}:
        return Decision(metric_id, "manual_review", "time dimension contains unknown fields")
    time_property = str(time_dimension.get("property") or time_dimension.get("field") or "").strip()
    default_range_policy = str(time_dimension.get("default_range_policy") or "").strip()
    if default_range_policy not in {"", "none"}:
        return Decision(
            metric_id,
            "manual_review",
            f"time dimension default range policy cannot be migrated losslessly: {default_range_policy!r}",
        )
    if time_dimension and not time_property:
        return Decision(metric_id, "manual_review", "time dimension property is missing")
    aggregation = formula.get("aggregation") or {}
    if not isinstance(aggregation, dict) or set(aggregation) - {"aggr", "property"}:
        return Decision(metric_id, "manual_review", "aggregation does not use the expected shape")
    aggr = aggregation.get("aggr")
    prop = aggregation.get("property")
    if aggr not in {"count", "count_distinct", "sum", "avg", "min", "max"}:
        return Decision(metric_id, "manual_review", f"unsupported aggregation: {aggr!r}")
    if aggr != "count" and (not isinstance(prop, str) or not prop.strip()):
        return Decision(metric_id, "manual_review", "aggregation property is missing")
    migrated_aggr = "count_rows" if aggr == "count" and not prop else aggr
    if migrated_aggr not in {"count_rows", "count", "count_distinct"}:
        return Decision(metric_id, "manual_review", "decimal precision, scale, and rounding require business review")
    unit = str(row.get("f_unit") or "").strip()
    if unit not in {"", "none", "1"} or (unit == "1" and row.get("f_unit_type") != "countUnit"):
        return Decision(metric_id, "manual_review", "legacy count unit requires explicit unit review")
    if row.get("f_unit_type") not in {None, "", "countUnit"}:
        return Decision(metric_id, "manual_review", "legacy unit type is incompatible with a count result")
    expression = None
    if migrated_aggr != "count_rows":
        expression = {
            "node_type": "property",
            "property_ref": {
                "object_type_id": row["f_scope_ref"],
                "property_name": prop,
            },
        }
    value = {
        "physical_type": "int64",
        "semantic_type": "count",
    }
    unit = unit_spec(row)
    if unit:
        value["unit"] = unit
    try:
        analysis = parse_json(row.get("f_analysis_dimensions"), [])
    except (TypeError, ValueError):
        return Decision(metric_id, "manual_review", "analysis dimensions are not valid JSON")
    if not isinstance(analysis, list) or any(
        not isinstance(item, dict)
        or set(item) - {"name", "display_name"}
        or not isinstance(item.get("name"), str)
        or not item["name"].strip()
        for item in analysis
    ):
        return Decision(metric_id, "manual_review", "analysis dimensions do not use the expected structured list")
    definition = {
        "id": metric_id,
        "version": 1,
        "code": stable_code(metric_id),
        "name": row["f_name"],
        "description": row.get("f_comment") or f"Migrated from legacy metric {row['f_name']}",
        "owner_object_type_id": row["f_scope_ref"],
        "metric_type": "atomic",
        "calculation_scope": "object_set",
        "result_semantics": {
            "statistical_grain": {"group_by_dimensions": []},
            "value": value,
            "no_data_policy": "zero",
            "partial_data_policy": "reject",
        },
        "specification": {
            "kind": "atomic",
            "measure": {"aggregation": migrated_aggr, **({"expression": expression} if expression else {})},
        },
        "query_capabilities": {
            "analysis_dimensions": [
                {"object_type_id": row["f_scope_ref"], "property_name": item["name"]}
                for item in analysis
                if item.get("name")
            ],
            "time_dimensions": [
                {"object_type_id": row["f_scope_ref"], "property_name": time_property}
            ] if time_property else [],
        },
        "lifecycle": {
            "status": "published",
            "published_at": published_at(row),
            "ontology_version": "migration-snapshot",
            "expression_language_version": "1.0",
            "function_registry_version": "1.0",
            "aggregation_registry_version": "1.0",
            "unit_registry_version": "1.0",
        },
    }
    return Decision(metric_id, "migrate", "lossless atomic metric", definition)


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=os.getenv("BKN_DB_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("BKN_DB_PORT", "3306")))
    parser.add_argument("--user", default=os.getenv("BKN_DB_USER", "root"))
    parser.add_argument("--password", default=os.getenv("BKN_DB_PASSWORD", ""))
    parser.add_argument("--database", default=os.getenv("BKN_DB_NAME", "openbkn"))
    parser.add_argument("--kn-id")
    parser.add_argument("--branch", default="main")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--report", default="object-metric-migration-report.json")
    return parser.parse_args()


def main() -> int:
    args = arguments()
    connection = pymysql.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )
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
                "SELECT f_kn_id, f_branch, f_id, f_logic_properties FROM t_object_type WHERE f_branch = %s",
                (args.branch,),
            )
            object_rows = cursor.fetchall()
            owner_ids = {
                (item["f_kn_id"], item["f_branch"], item["f_id"])
                for item in object_rows
            }
            bindings, malformed_bindings = legacy_metric_bindings(object_rows)
            cursor.execute(select, params)
            rows = cursor.fetchall()
            for row in rows:
                translated = translate(row)
                decision = review_bound_metric(row, translated, bindings, malformed_bindings)
                target_key = (row["f_kn_id"], row["f_branch"], row["f_id"], 1)
                if decision is translated and target_key in existing_ids:
                    decision = Decision(row["f_id"], "already_exists", "object metric version already exists")
                elif decision.action == "migrate" and decision.definition is not None:
                    owner_key = (row["f_kn_id"], row["f_branch"], decision.definition["owner_object_type_id"])
                    if owner_key not in owner_ids:
                        decision = Decision(row["f_id"], "manual_review", "owner object type does not exist")
                if decision.action == "migrate" and decision.definition is not None:
                    code_key = (
                        row["f_kn_id"],
                        row["f_branch"],
                        decision.definition["code"],
                    )
                    conflicting_owners = code_owners.get(code_key, set()) - {row["f_id"]}
                    planned_owner = planned_codes.get(code_key)
                    if planned_owner and planned_owner != row["f_id"]:
                        conflicting_owners.add(planned_owner)
                    if conflicting_owners:
                        decision = Decision(
                            row["f_id"],
                            "manual_review",
                            f"stable code conflicts with metric {sorted(conflicting_owners)[0]}: {decision.definition['code']}",
                        )
                    else:
                        planned_codes[code_key] = row["f_id"]
                decisions.append(decision)
                if not args.apply or decision.action != "migrate" or decision.definition is None:
                    continue
                definition = decision.definition
                cursor.execute(
                    """
                    INSERT INTO t_object_metric_definition
                    (f_kn_id, f_branch, f_id, f_version, f_code, f_name, f_description,
                     f_owner_object_type_id, f_metric_type, f_status, f_definition,
                     f_creator, f_creator_type, f_create_time, f_updater, f_updater_type, f_update_time)
                    VALUES (%s, %s, %s, 1, %s, %s, %s, %s, 'atomic', 'published', %s,
                            %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        row["f_kn_id"], row["f_branch"], row["f_id"], definition["code"],
                        definition["name"], definition["description"], definition["owner_object_type_id"],
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
    report = {
        "mode": "apply" if args.apply else "dry-run",
        "summary": {
            "total": len(decisions),
            "migrate": sum(item.action == "migrate" for item in decisions),
            "manual_review": sum(item.action == "manual_review" for item in decisions),
            "already_exists": sum(item.action == "already_exists" for item in decisions),
        },
        "entries": [
            {
                "kn_id": row["f_kn_id"],
                "branch": row["f_branch"],
                "legacy_name": row["f_name"],
                "legacy_metric_type": row["f_metric_type"],
                **item.__dict__,
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
