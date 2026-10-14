# Copyright openbkn.ai
#
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.

import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import migrate


def source(resource_type, resource_id, binding_id="action-1"):
    return migrate.shared.ProxySource(
        resource_type=resource_type,
        resource_id=resource_id,
        operation="execute",
        source_id=f"source-{binding_id}",
        kn_id="kn-1",
        binding_type="action_type",
        binding_id=binding_id,
    )


class ClassifySourcesTest(unittest.TestCase):
    def test_retypes_only_toolbox_sources_whose_box_is_a_function(self):
        repaired, count = migrate.classify_sources(
            [
                source("tool_box", "function-box", "function-action"),
                source("tool_box", "api-box", "api-action"),
                source("function", "already-fixed", "fixed-action"),
            ],
            {"function-box", "already-fixed"},
        )

        self.assertEqual(1, count)
        self.assertEqual(
            {
                ("function", "function-box"),
                ("tool_box", "api-box"),
                ("function", "already-fixed"),
            },
            {(item.resource_type, item.resource_id) for item in repaired},
        )

    def test_refuses_a_conversion_that_collides_with_an_existing_source(self):
        with self.assertRaisesRegex(migrate.MigrationError, "duplicate"):
            migrate.classify_sources(
                [
                    source("tool_box", "function-box"),
                    source("function", "function-box"),
                ],
                {"function-box"},
            )


class BuildPlanTest(unittest.TestCase):
    def setUp(self):
        self.mapping = migrate.ProxyMapping(
            kn_id="kn-1",
            proxy_account_id="proxy-1",
            sync_status="ready",
            published_model_version="",
            synced_model_version="",
            sync_generation=7,
            published_generation=7,
            last_grantor_id="grantor-1",
            lock_owner="",
            lock_until=0,
        )
        self.old_sources = (source("tool_box", "function-box"),)
        old_version = migrate.shared.proxy_grant_snapshot_version(self.old_sources)
        self.mapping = migrate.replace(
            self.mapping,
            published_model_version=old_version,
            synced_model_version=old_version,
        )

    @patch.object(migrate.shared, "partition_granted_sources")
    @patch.object(migrate.shared, "load_grant_index", return_value=object())
    @patch.object(migrate, "has_active_outbox", return_value=False)
    @patch.object(migrate, "load_safe_sources")
    @patch.object(migrate, "load_safe_proxy")
    @patch.object(migrate, "load_snapshot")
    @patch.object(migrate, "load_mappings")
    @patch.object(migrate, "load_function_box_ids", return_value={"function-box"})
    def test_builds_one_fenced_network_repair(
        self,
        _boxes,
        mappings,
        snapshots,
        safe_proxy,
        safe_sources,
        _outbox,
        _authority,
        partition,
    ):
        mappings.return_value = [self.mapping]
        snapshots.side_effect = [self.old_sources, self.old_sources]
        desired, _ = migrate.classify_sources(self.old_sources, {"function-box"})
        partition.side_effect = [(list(desired), []), ([], [])]
        safe_proxy.return_value = {
            "managed_resource_id": "kn-1",
            "lifecycle_status": "active",
            "grant_sync_generation": 9,
            "grant_snapshot_version": self.mapping.synced_model_version,
        }
        safe_sources.side_effect = [self.old_sources, ()]

        plan = migrate.build_plan(
            MagicMock(),
            MagicMock(),
            MagicMock(),
            now=datetime(2026, 10, 14, tzinfo=timezone.utc),
        )

        self.assertEqual([], plan.failures)
        self.assertEqual(1, len(plan.repairs))
        self.assertEqual(10, plan.repairs[0].new_generation)
        self.assertEqual("function", plan.repairs[0].new_sources[0].resource_type)
        self.assertEqual((), plan.repairs[0].preserved_derived_sources)

    @patch.object(migrate.shared, "partition_granted_sources")
    @patch.object(migrate.shared, "load_grant_index", return_value=object())
    @patch.object(migrate, "has_active_outbox", return_value=False)
    @patch.object(migrate, "load_safe_sources")
    @patch.object(migrate, "load_safe_proxy")
    @patch.object(migrate, "load_snapshot")
    @patch.object(migrate, "load_mappings")
    @patch.object(migrate, "load_function_box_ids", return_value={"function-box"})
    def test_repairs_a_snapshot_even_when_its_historical_mapping_hash_is_stale(
        self,
        _boxes,
        mappings,
        snapshots,
        safe_proxy,
        safe_sources,
        _outbox,
        _authority,
        partition,
    ):
        stale_mapping = migrate.replace(
            self.mapping,
            published_model_version="sha256:historical",
            synced_model_version="sha256:historical",
        )
        mappings.return_value = [stale_mapping]
        snapshots.side_effect = [self.old_sources, self.old_sources]
        desired, _ = migrate.classify_sources(self.old_sources, {"function-box"})
        partition.side_effect = [(list(desired), []), ([], [])]
        safe_sources.side_effect = [self.old_sources, ()]
        safe_proxy.return_value = {
            "managed_resource_id": "kn-1",
            "lifecycle_status": "active",
            "grant_sync_generation": 7,
            "grant_snapshot_version": "sha256:historical",
        }

        plan = migrate.build_plan(MagicMock(), MagicMock(), MagicMock())

        self.assertEqual([], plan.failures)
        self.assertEqual(1, len(plan.repairs))

    @patch.object(migrate.shared, "partition_granted_sources")
    @patch.object(migrate.shared, "load_grant_index", return_value=object())
    @patch.object(migrate, "has_active_outbox", return_value=False)
    @patch.object(migrate, "load_safe_sources")
    @patch.object(migrate, "load_safe_proxy")
    @patch.object(migrate, "load_snapshot")
    @patch.object(migrate, "load_mappings")
    @patch.object(migrate, "load_function_box_ids", return_value={"function-box"})
    def test_accepts_safe_first_commit_and_preserves_derived_sources(
        self,
        _boxes,
        mappings,
        snapshots,
        safe_proxy,
        safe_sources,
        _outbox,
        _authority,
        partition,
    ):
        mappings.return_value = [self.mapping]
        snapshots.side_effect = [self.old_sources, self.old_sources]
        desired, _ = migrate.classify_sources(self.old_sources, {"function-box"})
        derived = (source("resource", "dataset-1", "derived-source"),)
        derived = (migrate.replace(derived[0], operation="view_detail"),)
        safe_sources.side_effect = [desired, derived]
        safe_proxy.return_value = {
            "managed_resource_id": "kn-1",
            "lifecycle_status": "active",
            "grant_sync_generation": 8,
            "grant_snapshot_version": migrate.shared.proxy_grant_snapshot_version(
                desired
            ),
        }
        partition.side_effect = [(list(desired), []), (list(derived), [])]

        plan = migrate.build_plan(MagicMock(), MagicMock(), MagicMock())

        self.assertEqual([], plan.failures)
        self.assertEqual(1, len(plan.repairs))
        self.assertEqual(derived, plan.repairs[0].preserved_derived_sources)
        self.assertEqual(9, plan.repairs[0].new_generation)

    @patch.object(migrate, "load_snapshot")
    @patch.object(migrate, "load_mappings")
    @patch.object(migrate, "load_function_box_ids", return_value={"function-box"})
    def test_refuses_divergent_planned_and_published_snapshots(
        self, _boxes, mappings, snapshots
    ):
        mappings.return_value = [self.mapping]
        snapshots.side_effect = [
            self.old_sources,
            (source("tool_box", "other-box"),),
        ]

        plan = migrate.build_plan(MagicMock(), MagicMock(), MagicMock())

        self.assertEqual([], plan.repairs)
        self.assertIn("snapshots differ", plan.failures[0].detail)

    @patch.object(migrate, "load_snapshot")
    @patch.object(migrate, "load_mappings")
    @patch.object(migrate, "load_function_box_ids", return_value={"function-box"})
    def test_noop_network_does_not_require_safe_or_outbox_access(
        self, _boxes, mappings, snapshots
    ):
        mappings.return_value = [self.mapping]
        fixed = (source("function", "function-box"),)
        snapshots.side_effect = [fixed, fixed]

        plan = migrate.build_plan(MagicMock(), MagicMock(), MagicMock())

        self.assertEqual([], plan.repairs)
        self.assertEqual([], plan.failures)


class ApplyRepairTest(unittest.TestCase):
    def repair(self):
        old_sources = (source("tool_box", "function-box"),)
        new_sources, _ = migrate.classify_sources(old_sources, {"function-box"})
        derived = source("resource", "dataset-1", "derived-source")
        derived = migrate.replace(derived, operation="view_detail")
        mapping = migrate.ProxyMapping(
            kn_id="kn-1",
            proxy_account_id="proxy-1",
            sync_status="ready",
            published_model_version="old-version",
            synced_model_version="old-version",
            sync_generation=7,
            published_generation=7,
            last_grantor_id="grantor-1",
            lock_owner="",
            lock_until=0,
        )
        return migrate.NetworkRepair(
            mapping=mapping,
            grantor_id="grantor-1",
            old_version="old-version",
            new_version="new-version",
            new_generation=8,
            old_sources=old_sources,
            new_sources=new_sources,
            preserved_derived_sources=(derived,),
            replacements=1,
        )

    @patch.object(migrate, "verify_applied")
    @patch.object(migrate, "replace_snapshot")
    @patch.object(migrate.shared, "sync_proxy_sources", return_value={"added": 1})
    def test_apply_keeps_derived_sources_and_commits_safe_first(
        self, sync_sources, _replace_snapshot, _verify
    ):
        events = []
        safe = MagicMock()
        bkn = MagicMock()
        safe_cursor = safe.cursor.return_value.__enter__.return_value
        bkn_cursor = bkn.cursor.return_value.__enter__.return_value
        safe_cursor.rowcount = 1
        bkn_cursor.rowcount = 1
        safe.commit.side_effect = lambda: events.append("safe")
        bkn.commit.side_effect = lambda: events.append("bkn")
        repair = self.repair()

        result = migrate.apply_repair(safe_connection=safe, bkn_connection=bkn, repair=repair)

        self.assertEqual({"added": 1}, result)
        network = sync_sources.call_args.args[1]
        self.assertEqual(
            list(repair.new_sources + repair.preserved_derived_sources),
            network.sources,
        )
        self.assertEqual(["safe", "bkn"], events)


class CommandTest(unittest.TestCase):
    def test_dry_run_is_default_and_requires_a_scope(self):
        args = migrate.parse_args(["--kn-id", "kn-1"])
        self.assertEqual("dry-run", args.mode)
        self.assertEqual("kn-1", args.kn_id)

        with self.assertRaises(SystemExit):
            migrate.parse_args([])

    def test_apply_requires_positive_batch_size(self):
        with self.assertRaises(SystemExit):
            migrate.parse_args(["--all", "--batch-size", "0"])


if __name__ == "__main__":
    unittest.main()
