#!/usr/bin/env python3
# Copyright openbkn.ai
#
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.

"""Validate and initialize the BKN proxy outbox stopped upgrade."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Mapping, Optional, Protocol, Sequence


class MigrationError(RuntimeError):
    """Raised when the proxy outbox migration cannot proceed safely."""


class Cursor(Protocol):
    def execute(self, query: str, args: Sequence[Any] = ()) -> int: ...
    def fetchone(self) -> Optional[Mapping[str, Any]]: ...


@dataclass(frozen=True)
class DBConfig:
    host: str
    port: int
    user: str
    password: str
    database: str

    @classmethod
    def from_environment(cls) -> "DBConfig":
        required = ("BKN_DB_HOST", "BKN_DB_USER", "BKN_DB_PASSWORD")
        missing = [name for name in required if not os.getenv(name)]
        if missing:
            raise MigrationError(
                "missing deployment-provided database settings: " + ", ".join(missing)
            )
        return cls(
            host=os.environ["BKN_DB_HOST"],
            port=int(os.getenv("BKN_DB_PORT", "3306")),
            user=os.environ["BKN_DB_USER"],
            password=os.environ["BKN_DB_PASSWORD"],
            database=os.getenv("BKN_DB_NAME", "openbkn"),
        )


PRECHECK_SQL = """
SELECT
  COUNT(*) AS mapping_count,
  SUM(CASE WHEN f_sync_status <> 'ready' THEN 1 ELSE 0 END) AS non_ready_count,
  SUM(CASE WHEN f_lock_owner <> '' AND f_lock_until > %s THEN 1 ELSE 0 END) AS active_lock_count
FROM t_kn_proxy_account
"""

VERIFY_SQL = """
SELECT
  (SELECT COUNT(*) FROM t_kn_proxy_sync_outbox) AS outbox_count,
  (SELECT COUNT(*) FROM t_kn_proxy_account
     WHERE f_sync_status <> 'ready' OR f_published_generation <> f_sync_generation) AS generation_mismatch_count,
  ((SELECT COUNT(*) FROM t_kn_proxy_published_grant_source p
      LEFT JOIN t_kn_proxy_planned_grant_source q
        ON q.f_kn_id = p.f_kn_id AND q.f_binding_type = p.f_binding_type
       AND q.f_binding_id = p.f_binding_id AND q.f_resource_type = p.f_resource_type
       AND q.f_resource_id = p.f_resource_id AND q.f_operation = p.f_operation
       AND q.f_source_type = p.f_source_type AND q.f_source_id = p.f_source_id
      WHERE q.f_kn_id IS NULL)
   +
   (SELECT COUNT(*) FROM t_kn_proxy_planned_grant_source q
      LEFT JOIN t_kn_proxy_published_grant_source p
        ON p.f_kn_id = q.f_kn_id AND p.f_binding_type = q.f_binding_type
       AND p.f_binding_id = q.f_binding_id AND p.f_resource_type = q.f_resource_type
       AND p.f_resource_id = q.f_resource_id AND p.f_operation = q.f_operation
       AND p.f_source_type = q.f_source_type AND p.f_source_id = q.f_source_id
      WHERE p.f_kn_id IS NULL)) AS snapshot_mismatch_count
"""

INITIALIZE_SQL = (
    """UPDATE t_kn_proxy_account
       SET f_published_generation = f_sync_generation
       WHERE f_sync_status = 'ready'""",
    """INSERT IGNORE INTO t_kn_proxy_planned_grant_source (
         f_kn_id, f_binding_type, f_binding_id, f_resource_type, f_resource_id,
         f_operation, f_source_type, f_source_id, f_created_at, f_updated_at)
       SELECT p.f_kn_id, p.f_binding_type, p.f_binding_id, p.f_resource_type,
         p.f_resource_id, p.f_operation, p.f_source_type, p.f_source_id,
         p.f_created_at, p.f_updated_at
       FROM t_kn_proxy_published_grant_source p
       JOIN t_kn_proxy_account m ON m.f_kn_id = p.f_kn_id
       WHERE m.f_sync_status = 'ready'""",
)


def normalize_counts(row: Optional[Mapping[str, Any]], names: Sequence[str]) -> dict[str, int]:
    """Normalize aggregate output without leaking row contents into reports."""
    if row is None:
        raise MigrationError("database aggregate returned no row")
    return {name: int(row.get(name) or 0) for name in names}


def precheck(cursor: Cursor, now_millis: int) -> dict[str, int]:
    """Require a quiescent, fully synchronized legacy proxy state."""
    cursor.execute(PRECHECK_SQL, (now_millis,))
    counts = normalize_counts(
        cursor.fetchone(), ("mapping_count", "non_ready_count", "active_lock_count")
    )
    if counts["non_ready_count"]:
        raise MigrationError(
            f"{counts['non_ready_count']} proxy mappings are not ready; repair them before upgrading"
        )
    if counts["active_lock_count"]:
        raise MigrationError(
            f"{counts['active_lock_count']} publication locks are active; BKN requests are not drained"
        )
    return counts


def initialize(cursor: Cursor) -> dict[str, int]:
    """Idempotently seed planned state after the repository Schema has run."""
    updated = cursor.execute(INITIALIZE_SQL[0])
    inserted = cursor.execute(INITIALIZE_SQL[1])
    return {"generation_rows_updated": updated, "planned_rows_inserted": inserted}


def verify(cursor: Cursor) -> dict[str, int]:
    """Require an empty queue and exact published/planned baseline parity."""
    cursor.execute(VERIFY_SQL)
    counts = normalize_counts(
        cursor.fetchone(),
        ("outbox_count", "generation_mismatch_count", "snapshot_mismatch_count"),
    )
    if any(counts.values()):
        raise MigrationError(
            "proxy outbox baseline verification failed: "
            + ", ".join(f"{name}={value}" for name, value in counts.items())
        )
    return counts


def connect(config: DBConfig):
    """Open a transaction-capable connection without logging credentials."""
    try:
        import pymysql
    except ImportError as exc:
        raise MigrationError("PyMySQL 1.1.0+ is required") from exc
    return pymysql.connect(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        database=config.database,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )
