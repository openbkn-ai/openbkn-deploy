# Copyright openbkn.ai
#
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.

import unittest

import kn_proxy_outbox


class FakeCursor:
    def __init__(self, rows, affected=None):
        self.rows = iter(rows)
        self.affected = iter(affected or [])
        self.queries = []

    def execute(self, query, args=()):
        self.queries.append((query, args))
        return next(self.affected, 0)

    def fetchone(self):
        return next(self.rows)


class ProxyOutboxMigrationTest(unittest.TestCase):
    def test_precheck_rejects_non_ready_networks(self):
        cursor = FakeCursor(
            [{"mapping_count": 3, "non_ready_count": 1, "active_lock_count": 0}]
        )
        with self.assertRaisesRegex(kn_proxy_outbox.MigrationError, "not ready"):
            kn_proxy_outbox.precheck(cursor, 1000)

    def test_precheck_rejects_active_publication_locks(self):
        cursor = FakeCursor(
            [{"mapping_count": 3, "non_ready_count": 0, "active_lock_count": 1}]
        )
        with self.assertRaisesRegex(kn_proxy_outbox.MigrationError, "locks are active"):
            kn_proxy_outbox.precheck(cursor, 1000)

    def test_initialize_is_idempotent_sql(self):
        cursor = FakeCursor([], [3, 20])
        self.assertEqual(
            {"generation_rows_updated": 3, "planned_rows_inserted": 20},
            kn_proxy_outbox.initialize(cursor),
        )
        self.assertIn("INSERT IGNORE", cursor.queries[1][0])

    def test_verify_requires_empty_queue_and_snapshot_parity(self):
        cursor = FakeCursor(
            [{"outbox_count": 0, "generation_mismatch_count": 0, "snapshot_mismatch_count": 2}]
        )
        with self.assertRaisesRegex(kn_proxy_outbox.MigrationError, "snapshot_mismatch_count=2"):
            kn_proxy_outbox.verify(cursor)

    def test_verify_compares_source_identity_not_only_resource_identity(self):
        self.assertIn("q.f_source_type = p.f_source_type", kn_proxy_outbox.VERIFY_SQL)
        self.assertIn("q.f_source_id = p.f_source_id", kn_proxy_outbox.VERIFY_SQL)
        self.assertIn("p.f_source_type = q.f_source_type", kn_proxy_outbox.VERIFY_SQL)
        self.assertIn("p.f_source_id = q.f_source_id", kn_proxy_outbox.VERIFY_SQL)


if __name__ == "__main__":
    unittest.main()
