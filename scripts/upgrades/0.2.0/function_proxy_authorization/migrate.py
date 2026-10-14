#!/usr/bin/env python3
# Copyright openbkn.ai
#
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.

"""Repair historical Function proxy grants projected as ``tool_box``.

The 0.1.5 permission transition predated Function's independent authorization
domain and projected every toolbox-backed binding as ``tool_box``. 0.2.0 keeps
that snapshot durable, so a ready knowledge-network proxy may still carry the
old type while Execution Factory authorizes the same box as ``function``.

This migration is deliberately offline. It rewrites one knowledge network at
a time across BKN and bkn-safe, verifies both sides before committing, and is
idempotent. Dry-run is the default operational entry point.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import traceback
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence


SCRIPT_DIR = Path(__file__).resolve().parent
PERMISSION_TRANSITION = (
    SCRIPT_DIR.parents[1] / "0.1.5" / "permission_model_transition" / "bkn_data.py"
)


def _load_shared_migration():
    """Load the audited 0.1.5 proxy materialization helpers by file path."""
    spec = importlib.util.spec_from_file_location(
        "openbkn_permission_transition_bkn_data", PERMISSION_TRANSITION
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load migration helpers from {PERMISSION_TRANSITION}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


shared = _load_shared_migration()

PLANNED_TABLE = "t_kn_proxy_planned_grant_source"
PUBLISHED_TABLE = "t_kn_proxy_published_grant_source"
SOURCE_TYPE = "kn_proxy_binding"
OLD_RESOURCE_TYPE = "tool_box"
FUNCTION_RESOURCE_TYPE = "function"
EXECUTE_OPERATION = "execute"
ACTIVE_OUTBOX_STATUSES = (
    "pending",
    "processing",
    "retrying",
    "completing",
    "dead",
)


class MigrationError(RuntimeError):
    """Raised when a repair cannot be proven safe."""


@dataclass(frozen=True)
class ProxyMapping:
    kn_id: str
    proxy_account_id: str
    sync_status: str
    published_model_version: str
    synced_model_version: str
    sync_generation: int
    published_generation: int
    last_grantor_id: str
    lock_owner: str
    lock_until: int


@dataclass(frozen=True)
class NetworkRepair:
    mapping: ProxyMapping
    grantor_id: str
    old_version: str
    new_version: str
    new_generation: int
    old_sources: tuple[Any, ...]
    new_sources: tuple[Any, ...]
    preserved_derived_sources: tuple[Any, ...]
    replacements: int

    @property
    def kn_id(self) -> str:
        return self.mapping.kn_id


@dataclass(frozen=True)
class PlanFailure:
    kn_id: str
    detail: str


@dataclass
class RepairPlan:
    function_boxes: int
    scanned_networks: int
    repairs: list[NetworkRepair]
    failures: list[PlanFailure]


def normalize(value: Any) -> str:
    return "" if value is None else str(value)


def source_key(source: Any) -> tuple[str, ...]:
    return (
        source.binding_type,
        source.binding_id,
        source.resource_type,
        source.resource_id,
        source.operation,
        source.source_id,
        source.kn_id,
    )


def classify_sources(
    sources: Sequence[Any], function_box_ids: set[str]
) -> tuple[tuple[Any, ...], int]:
    """Return a canonical source set with only real Function boxes retyped."""
    repaired: list[Any] = []
    replacements = 0
    for source in sources:
        if (
            source.resource_type == OLD_RESOURCE_TYPE
            and source.resource_id in function_box_ids
            and source.operation == EXECUTE_OPERATION
        ):
            source = replace(source, resource_type=FUNCTION_RESOURCE_TYPE)
            replacements += 1
        repaired.append(source)
    repaired.sort(key=source_key)
    if len({source_key(item) for item in repaired}) != len(repaired):
        raise MigrationError("resource-type conversion produces duplicate proxy sources")
    return tuple(repaired), replacements


def load_function_box_ids(connection) -> set[str]:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT f_box_id FROM t_toolbox WHERE f_metadata_type = %s",
            (FUNCTION_RESOURCE_TYPE,),
        )
        values = {normalize(row["f_box_id"]).strip() for row in cursor.fetchall()}
    values.discard("")
    return values


def load_mappings(connection, kn_id: str = "") -> list[ProxyMapping]:
    query = (
        "SELECT f_kn_id, f_proxy_account_id, f_sync_status, "
        "f_published_model_version, f_synced_model_version, f_sync_generation, "
        "f_published_generation, f_last_grantor_id, f_lock_owner, f_lock_until "
        "FROM t_kn_proxy_account WHERE f_lifecycle_status = 'active'"
    )
    parameters: tuple[Any, ...] = ()
    if kn_id:
        query += " AND f_kn_id = %s"
        parameters = (kn_id,)
    query += " ORDER BY f_kn_id"
    with connection.cursor() as cursor:
        cursor.execute(query, parameters)
        rows = list(cursor.fetchall())
    return [
        ProxyMapping(
            kn_id=normalize(row["f_kn_id"]),
            proxy_account_id=normalize(row["f_proxy_account_id"]),
            sync_status=normalize(row["f_sync_status"]),
            published_model_version=normalize(row["f_published_model_version"]),
            synced_model_version=normalize(row["f_synced_model_version"]),
            sync_generation=int(row["f_sync_generation"] or 0),
            published_generation=int(row["f_published_generation"] or 0),
            last_grantor_id=normalize(row["f_last_grantor_id"]),
            lock_owner=normalize(row["f_lock_owner"]),
            lock_until=int(row["f_lock_until"] or 0),
        )
        for row in rows
    ]


def load_snapshot(connection, table: str, kn_id: str) -> tuple[Any, ...]:
    if table not in {PLANNED_TABLE, PUBLISHED_TABLE}:
        raise ValueError(f"unsupported snapshot table: {table}")
    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT f_resource_type, f_resource_id, f_operation, f_source_type, "
            f"f_source_id, f_kn_id, f_binding_type, f_binding_id FROM {table} "
            "WHERE f_kn_id = %s",
            (kn_id,),
        )
        rows = list(cursor.fetchall())
    sources = []
    for row in rows:
        source_type = normalize(row["f_source_type"])
        if source_type != SOURCE_TYPE:
            raise MigrationError(
                f"{table} contains unsupported source type {source_type!r} for {kn_id}"
            )
        sources.append(
            shared.ProxySource(
                resource_type=normalize(row["f_resource_type"]),
                resource_id=normalize(row["f_resource_id"]),
                operation=normalize(row["f_operation"]),
                source_id=normalize(row["f_source_id"]),
                kn_id=normalize(row["f_kn_id"]),
                binding_type=normalize(row["f_binding_type"]),
                binding_id=normalize(row["f_binding_id"]),
            )
        )
    return tuple(sorted(sources, key=source_key))


def load_safe_proxy(connection, proxy_account_id: str) -> Optional[Mapping[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT proxy_account_id, managed_resource_id, lifecycle_status, "
            "grant_sync_generation, grant_snapshot_version "
            "FROM managed_proxy_accounts WHERE proxy_account_id = %s",
            (proxy_account_id,),
        )
        return cursor.fetchone()


def load_safe_sources(
    connection, proxy_account_id: str, requirement_derived: bool
) -> tuple[Any, ...]:
    """Load active explicit or prerequisite-derived proxy sources."""
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT resource_type, resource_id, operation, source_id, kn_id, "
            "binding_type, binding_id FROM proxy_grant_source "
            "WHERE proxy_account_id = %s AND source_type = %s "
            "AND lifecycle_status = 'active' AND requirement_derived = %s",
            (proxy_account_id, SOURCE_TYPE, int(requirement_derived)),
        )
        rows = list(cursor.fetchall())
    return tuple(
        sorted(
            (
                shared.ProxySource(
                    resource_type=normalize(row["resource_type"]),
                    resource_id=normalize(row["resource_id"]),
                    operation=normalize(row["operation"]),
                    source_id=normalize(row["source_id"]),
                    kn_id=normalize(row["kn_id"]),
                    binding_type=normalize(row["binding_type"]),
                    binding_id=normalize(row["binding_id"]),
                )
                for row in rows
            ),
            key=source_key,
        )
    )


def has_active_outbox(connection, kn_id: str) -> bool:
    placeholders = ",".join(["%s"] * len(ACTIVE_OUTBOX_STATUSES))
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) AS count FROM t_kn_proxy_sync_outbox "
            f"WHERE f_kn_id = %s AND f_status IN ({placeholders})",
            (kn_id, *ACTIVE_OUTBOX_STATUSES),
        )
        return int(cursor.fetchone()["count"]) > 0


def validate_mapping(mapping: ProxyMapping, now_ms: int) -> None:
    if mapping.sync_status != "ready":
        raise MigrationError(f"proxy mapping is {mapping.sync_status!r}, not ready")
    if not mapping.published_model_version or (
        mapping.published_model_version != mapping.synced_model_version
    ):
        raise MigrationError("published and synchronized model versions differ")
    if mapping.sync_generation != mapping.published_generation:
        raise MigrationError("published and synchronized generations differ")
    if mapping.lock_owner and mapping.lock_until > now_ms:
        raise MigrationError("proxy mapping has an active publication lock")


def build_plan(
    bkn_connection,
    safe_connection,
    operator_connection,
    kn_id: str = "",
    grantor_override: str = "",
    now: Optional[datetime] = None,
) -> RepairPlan:
    now = now or datetime.now(timezone.utc)
    now_ms = int(now.timestamp() * 1000)
    function_boxes = load_function_box_ids(operator_connection)
    mappings = load_mappings(bkn_connection, kn_id)
    if kn_id and not mappings:
        return RepairPlan(
            function_boxes=len(function_boxes),
            scanned_networks=0,
            repairs=[],
            failures=[PlanFailure(kn_id, "active proxy mapping not found")],
        )

    repairs: list[NetworkRepair] = []
    failures: list[PlanFailure] = []
    authority_by_grantor: dict[str, Any] = {}
    for mapping in mappings:
        try:
            planned = load_snapshot(bkn_connection, PLANNED_TABLE, mapping.kn_id)
            published = load_snapshot(bkn_connection, PUBLISHED_TABLE, mapping.kn_id)
            if planned != published:
                raise MigrationError("planned and published snapshots differ")
            desired, replacements = classify_sources(planned, function_boxes)
            if replacements == 0:
                continue
            validate_mapping(mapping, now_ms)
            if has_active_outbox(bkn_connection, mapping.kn_id):
                raise MigrationError("proxy synchronization outbox is not drained")

            old_version = shared.proxy_grant_snapshot_version(planned)
            new_version = shared.proxy_grant_snapshot_version(desired)
            safe_proxy = load_safe_proxy(safe_connection, mapping.proxy_account_id)
            if safe_proxy is None:
                raise MigrationError("managed proxy account is missing in bkn-safe")
            if (
                normalize(safe_proxy["managed_resource_id"]) != mapping.kn_id
                or normalize(safe_proxy["lifecycle_status"]) != "active"
            ):
                raise MigrationError("managed proxy ownership or lifecycle is invalid")
            safe_version = normalize(safe_proxy["grant_snapshot_version"])
            if safe_version not in {
                "",
                mapping.synced_model_version,
                old_version,
                new_version,
            }:
                raise MigrationError(
                    "bkn-safe snapshot version conflicts with the BKN snapshot"
                )
            safe_sources = load_safe_sources(
                safe_connection, mapping.proxy_account_id, False
            )
            safe_already_repaired = (
                safe_sources == desired and safe_version == new_version
            )
            if safe_sources != planned and not safe_already_repaired:
                raise MigrationError(
                    "bkn-safe active source ledger differs from the BKN snapshot"
                )
            derived_sources = load_safe_sources(
                safe_connection, mapping.proxy_account_id, True
            )

            grantor_id = grantor_override or mapping.last_grantor_id
            if not grantor_id:
                raise MigrationError(
                    "no grantor is available; pass --grantor-id explicitly"
                )
            authority = authority_by_grantor.get(grantor_id)
            if authority is None:
                authority = shared.load_grant_index(safe_connection, grantor_id)
                authority_by_grantor[grantor_id] = authority
            network_sources, skipped = shared.partition_granted_sources(
                authority, grantor_id, desired
            )
            if skipped:
                # A historical ready snapshot may already omit unauthorized Skills.
                # Do not silently change its unrelated source set in this repair.
                raise MigrationError(
                    "grantor cannot authorize every source in the existing snapshot"
                )
            preserved_derived_sources, skipped_derived = (
                shared.partition_granted_sources(
                    authority, grantor_id, derived_sources
                )
            )
            if skipped_derived:
                raise MigrationError(
                    "grantor cannot authorize prerequisite-derived proxy sources"
                )
            safe_generation = int(safe_proxy["grant_sync_generation"] or 0)
            repairs.append(
                NetworkRepair(
                    mapping=mapping,
                    grantor_id=grantor_id,
                    old_version=old_version,
                    new_version=new_version,
                    new_generation=max(
                        mapping.sync_generation,
                        mapping.published_generation,
                        safe_generation,
                    )
                    + 1,
                    old_sources=planned,
                    new_sources=tuple(network_sources),
                    preserved_derived_sources=tuple(preserved_derived_sources),
                    replacements=replacements,
                )
            )
        except Exception as error:
            failures.append(PlanFailure(mapping.kn_id, str(error)))

    return RepairPlan(
        function_boxes=len(function_boxes),
        scanned_networks=len(mappings),
        repairs=repairs,
        failures=failures,
    )


def replace_snapshot(cursor, table: str, repair: NetworkRepair, epoch_ms: int) -> None:
    if table not in {PLANNED_TABLE, PUBLISHED_TABLE}:
        raise ValueError(f"unsupported snapshot table: {table}")
    cursor.execute(f"DELETE FROM {table} WHERE f_kn_id = %s", (repair.kn_id,))
    if not repair.new_sources:
        return
    cursor.executemany(
        f"INSERT INTO {table} "
        "(f_kn_id, f_binding_type, f_binding_id, f_resource_type, f_resource_id, "
        "f_operation, f_source_type, f_source_id, f_created_at, f_updated_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        [
            (
                source.kn_id,
                source.binding_type,
                source.binding_id,
                source.resource_type,
                source.resource_id,
                source.operation,
                SOURCE_TYPE,
                source.source_id,
                epoch_ms,
                epoch_ms,
            )
            for source in repair.new_sources
        ],
    )


def verify_applied(
    bkn_connection, safe_connection, repair: NetworkRepair
) -> None:
    planned = load_snapshot(bkn_connection, PLANNED_TABLE, repair.kn_id)
    published = load_snapshot(bkn_connection, PUBLISHED_TABLE, repair.kn_id)
    if planned != repair.new_sources or published != repair.new_sources:
        raise MigrationError("BKN snapshot verification failed")
    mapping = load_mappings(bkn_connection, repair.kn_id)
    if len(mapping) != 1:
        raise MigrationError("BKN mapping verification failed")
    current = mapping[0]
    if (
        current.sync_status != "ready"
        or current.published_model_version != repair.new_version
        or current.synced_model_version != repair.new_version
        or current.sync_generation != repair.new_generation
        or current.published_generation != repair.new_generation
    ):
        raise MigrationError("BKN mapping verification failed")
    safe_proxy = load_safe_proxy(safe_connection, repair.mapping.proxy_account_id)
    if safe_proxy is None or (
        int(safe_proxy["grant_sync_generation"] or 0) != repair.new_generation
        or normalize(safe_proxy["grant_snapshot_version"]) != repair.new_version
    ):
        raise MigrationError("bkn-safe mapping verification failed")
    explicit_sources = load_safe_sources(
        safe_connection, repair.mapping.proxy_account_id, False
    )
    if explicit_sources != repair.new_sources:
        raise MigrationError("bkn-safe explicit source verification failed")
    derived_sources = load_safe_sources(
        safe_connection, repair.mapping.proxy_account_id, True
    )
    if derived_sources != repair.preserved_derived_sources:
        raise MigrationError("bkn-safe derived source verification failed")


def apply_repair(
    bkn_connection, safe_connection, repair: NetworkRepair, now: Optional[datetime] = None
) -> dict[str, int]:
    now = now or datetime.now(timezone.utc)
    timestamp = now.astimezone(timezone.utc).replace(tzinfo=None)
    epoch_ms = int(now.timestamp() * 1000)
    network = shared.ProxyNetworkPlan(
        kn_id=repair.kn_id,
        kn_name=repair.kn_id,
        proxy_account_id=repair.mapping.proxy_account_id,
        model_version=repair.new_version,
        # The 0.1.5 helper predates requirement_derived. Supplying the current
        # derived rows keeps them active and preserves their flag while the
        # explicit Function source is replaced.
        sources=list(repair.new_sources + repair.preserved_derived_sources),
        create_account=False,
    )
    try:
        with safe_connection.cursor() as safe_cursor, bkn_connection.cursor() as bkn_cursor:
            result = shared.sync_proxy_sources(
                safe_cursor, network, repair.grantor_id, timestamp
            )
            safe_cursor.execute(
                "UPDATE managed_proxy_accounts SET grant_sync_generation = %s, "
                "grant_snapshot_version = %s, updated_at = %s "
                "WHERE proxy_account_id = %s AND managed_resource_id = %s "
                "AND lifecycle_status = 'active'",
                (
                    repair.new_generation,
                    repair.new_version,
                    timestamp,
                    repair.mapping.proxy_account_id,
                    repair.kn_id,
                ),
            )
            if safe_cursor.rowcount != 1:
                raise MigrationError("bkn-safe proxy generation update was not unique")

            replace_snapshot(bkn_cursor, PLANNED_TABLE, repair, epoch_ms)
            replace_snapshot(bkn_cursor, PUBLISHED_TABLE, repair, epoch_ms)
            bkn_cursor.execute(
                "UPDATE t_kn_proxy_account SET f_sync_status = 'ready', "
                "f_published_model_version = %s, f_synced_model_version = %s, "
                "f_pending_model_version = '', f_sync_generation = %s, "
                "f_published_generation = %s, f_last_sync_error = '', "
                "f_last_grantor_id = %s, f_lock_owner = '', f_lock_until = 0, "
                "f_last_sync_started_at = %s, f_last_sync_succeeded_at = %s, "
                "f_updated_at = %s WHERE f_kn_id = %s AND f_sync_status = 'ready' "
                "AND f_sync_generation = %s AND f_published_generation = %s",
                (
                    repair.new_version,
                    repair.new_version,
                    repair.new_generation,
                    repair.new_generation,
                    repair.grantor_id,
                    epoch_ms,
                    epoch_ms,
                    epoch_ms,
                    repair.kn_id,
                    repair.mapping.sync_generation,
                    repair.mapping.published_generation,
                ),
            )
            if bkn_cursor.rowcount != 1:
                raise MigrationError("BKN mapping changed after dry-run")
        verify_applied(bkn_connection, safe_connection, repair)
        # Safe is committed first. If the BKN commit then fails, the next run
        # sees the old BKN snapshot and idempotently replays the Safe repair.
        safe_connection.commit()
        bkn_connection.commit()
        return result
    except Exception:
        safe_connection.rollback()
        bkn_connection.rollback()
        raise


def failure_text(failures: Iterable[PlanFailure]) -> str:
    return "\n".join(f"- {item.kn_id}: {item.detail}" for item in failures)


def report_for(
    mode: str,
    scope: str,
    plan: RepairPlan,
    results: Optional[Mapping[str, Mapping[str, int]]] = None,
    backup: Optional[Any] = None,
    backup_skipped: bool = False,
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "migration": "0.2.0-function-proxy-authorization",
        "mode": mode,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": scope,
        "function_boxes": plan.function_boxes,
        "scanned_networks": plan.scanned_networks,
        "affected_networks": [
            {
                "knowledge_network_id": item.kn_id,
                "proxy_account_id": item.mapping.proxy_account_id,
                "grantor_id": item.grantor_id,
                "retyped_sources": item.replacements,
                "old_version": item.old_version,
                "new_version": item.new_version,
                "new_generation": item.new_generation,
                "result": dict((results or {}).get(item.kn_id, {})),
            }
            for item in plan.repairs
        ],
        "failures": [
            {"knowledge_network_id": item.kn_id, "detail": item.detail}
            for item in plan.failures
        ],
    }
    if backup is not None:
        report["backup"] = {
            "status": "created",
            "path": str(backup.path),
            "restore_commands": list(backup.restore_commands),
        }
    elif backup_skipped:
        report["backup"] = {"status": "skipped"}
    return report


def write_report(report: Mapping[str, Any], output_path: str = "") -> None:
    content = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if output_path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    print(content, end="")


def database_configs():
    bkn_config, safe_config = shared.database_configs()
    operator_config = shared.database_config(
        "OPERATOR_DB", "openbkn", fallback=bkn_config
    )
    return bkn_config, safe_config, operator_config


def run(args: argparse.Namespace) -> int:
    bkn_connection = None
    safe_connection = None
    operator_connection = None
    try:
        bkn_config, safe_config, operator_config = database_configs()
        bkn_connection = shared.connect_database(bkn_config)
        safe_connection = shared.connect_database(safe_config)
        if operator_config == bkn_config:
            operator_connection = bkn_connection
        else:
            operator_connection = shared.connect_database(operator_config)

        plan = build_plan(
            bkn_connection,
            safe_connection,
            operator_connection,
            kn_id=args.kn_id,
            grantor_override=args.grantor_id,
        )
        scope = args.kn_id or "all"
        if plan.failures:
            write_report(report_for(args.mode, scope, plan), args.report)
            raise MigrationError(
                "migration preflight failed:\n" + failure_text(plan.failures)
            )
        if args.mode == "dry-run":
            write_report(report_for(args.mode, scope, plan), args.report)
            return 0
        if not plan.repairs:
            write_report(report_for(args.mode, scope, plan), args.report)
            return 0
        if not args.confirm_services_stopped:
            raise MigrationError("apply requires --confirm-services-stopped")

        backup = None
        dump_executable = shared.find_dump_executable()
        if dump_executable is None and not args.allow_no_backup:
            raise MigrationError(
                "mariadb-dump or mysqldump is required; use --allow-no-backup only "
                "when an external snapshot/PITR is verified"
            )
        if dump_executable is not None and plan.repairs:
            backup = shared.create_pre_migration_backup(
                {"bkn": bkn_config, "safe": safe_config},
                dump_executable=dump_executable,
            )

        results: dict[str, Mapping[str, int]] = {}
        for offset in range(0, len(plan.repairs), args.batch_size):
            for repair in plan.repairs[offset : offset + args.batch_size]:
                try:
                    results[repair.kn_id] = apply_repair(
                        bkn_connection, safe_connection, repair
                    )
                except Exception as error:
                    plan.failures.append(PlanFailure(repair.kn_id, str(error)))
                    write_report(
                        report_for(
                            args.mode,
                            scope,
                            plan,
                            results,
                            backup,
                            backup_skipped=dump_executable is None,
                        ),
                        args.report,
                    )
                    raise
        write_report(
            report_for(
                args.mode,
                scope,
                plan,
                results,
                backup,
                backup_skipped=dump_executable is None and bool(plan.repairs),
            ),
            args.report,
        )
        return 0
    finally:
        if operator_connection is not None and operator_connection is not bkn_connection:
            operator_connection.close()
        if safe_connection is not None:
            safe_connection.close()
        if bkn_connection is not None:
            bkn_connection.close()


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Repair historical Function proxy grant sources carried into OpenBKN 0.2.0."
        )
    )
    parser.add_argument("--mode", choices=("dry-run", "apply"), default="dry-run")
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument("--all", action="store_true", help="scan every active proxy mapping")
    scope.add_argument("--kn-id", default="", help="repair one knowledge network")
    parser.add_argument(
        "--grantor-id",
        default="",
        help="override each mapping's last grantor after authority validation",
    )
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--report", default="")
    parser.add_argument("--confirm-services-stopped", action="store_true")
    parser.add_argument("--allow-no-backup", action="store_true")
    args = parser.parse_args(argv)
    if args.batch_size < 1:
        parser.error("--batch-size must be positive")
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    try:
        return run(parse_args(argv))
    except KeyboardInterrupt:
        print("Migration interrupted.", file=sys.stderr)
        return 130
    except Exception:
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
