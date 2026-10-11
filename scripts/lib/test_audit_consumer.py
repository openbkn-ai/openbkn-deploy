#!/usr/bin/env python3
# Copyright openbkn.ai
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.

import copy
import json
import subprocess
import os
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

import audit_consumer as audit


class SystemAuditTests(unittest.TestCase):
    def values(self):
        values = audit.defaults("broker:9092", "PLAIN", "existing-client")
        values["auditPublisher"]["environment"] = "test"
        values["core"] = {"store": "mariadb", "autoMigrate": True,
                          "mariadb": {"existingSecret": "core-db", "dsnKey": "dsn"}}
        return values

    def test_defaults_complete_both_sides_without_credentials(self):
        values = self.values()
        self.assertTrue(values["kafkaConsumers"]["audit"]["enabled"])
        self.assertTrue(values["auditPublisher"]["enabled"])
        self.assertEqual(values["kafkaConsumers"]["audit"]["consumerGroup"], "bkn-trace-audit-ledger-v1")
        self.assertNotIn("password", values["auditPublisher"])

    def test_environment_is_not_guessed(self):
        self.assertNotIn("environment", audit.defaults("broker:9092", "PLAIN", "existing-client")["auditPublisher"])

    def test_historical_disabled_empty_settings_do_not_block_defaults(self):
        values = self.values()
        for section in (values["kafkaConsumers"]["audit"], values["auditPublisher"]):
            section.update(enabled=False, brokers=[], saslMechanism="", existingSecret={"name": ""})
        saved = audit.preserved_system_values(values)
        self.assertNotIn("enabled", saved["kafkaConsumers"]["audit"])
        self.assertNotIn("brokers", saved["auditPublisher"])
        self.assertNotIn("existingSecret", saved["auditPublisher"])
        self.assertEqual(saved["auditPublisher"]["environment"], "test")

    def test_preserves_operator_choices_without_stale_derived_connections(self):
        values = self.values()
        values["auditPublisher"]["password"] = "do-not-copy"
        saved = audit.preserved_system_values(values)
        for section in (saved["auditPublisher"], saved["kafkaConsumers"]["audit"]):
            self.assertNotIn("brokers", section)
            self.assertNotIn("saslMechanism", section)
            self.assertEqual(section["existingSecret"]["name"], "existing-client")
        self.assertEqual(saved["auditPublisher"]["environment"], "test")
        self.assertEqual(saved["kafkaConsumers"]["audit"]["consumerGroup"], "bkn-trace-audit-ledger-v1")
        self.assertNotIn("password", saved["auditPublisher"])
        self.assertNotIn("core", saved)

    @patch.object(audit, "check_secret")
    def test_validates_both_sides_and_database(self, check):
        audit.validate_system(self.values(), "openbkn")
        self.assertEqual(check.call_count, 3)

    @patch.object(audit, "check_secret")
    def test_current_disable_or_string_enablement_rejected(self, check):
        for component in ("consumer", "publisher"):
            for enabled in (False, "true"):
                values = self.values()
                section = values["auditPublisher"] if component == "publisher" else values["kafkaConsumers"]["audit"]
                section["enabled"] = enabled
                with self.subTest(component=component, enabled=enabled), self.assertRaises(ValueError):
                    audit.validate_system(values, "openbkn")

    @patch.object(audit, "check_secret")
    def test_missing_environment_unsupported_mechanism_and_core_rejected(self, check):
        for path, value in ((["auditPublisher", "environment"], ""),
                            (["auditPublisher", "saslMechanism"], "SCRAM-SHA-256"),
                            (["core", "autoMigrate"], False),
                            (["core", "store"], "memory")):
            values = self.values()
            values[path[0]][path[1]] = value
            with self.subTest(path=path), self.assertRaises(ValueError):
                audit.validate_system(values, "openbkn")

    @patch.object(audit, "check_secret")
    def test_production_does_not_assume_consumer_identity_is_publisher_identity(self, check):
        values = self.values()
        values["auditPublisher"]["environment"] = "production"
        with self.assertRaisesRegex(ValueError, "production.*independent"):
            audit.validate_system(values, "openbkn")
        values["auditPublisher"]["existingSecret"]["name"] = "approved-publisher"
        audit.validate_system(values, "openbkn")

    def test_rendered_defaults_and_partial_maps_use_helm_result(self):
        values = self.values()
        rendered = {"chart": {"values": values}, "config": {"auditPublisher": {"existingSecret": {"name": "custom"}}}}
        actual = audit.rendered_system_values(rendered)
        self.assertEqual(actual["auditPublisher"]["existingSecret"],
                         {"name": "custom", "usernameKey": "username", "passwordKey": "password"})

    def test_same_version_compares_all_effective_audit_fields(self):
        values = self.values()
        self.assertTrue(audit.same_system_config(values, copy.deepcopy(values)))
        for component, field, value in (("publisher", "environment", "staging"),
                                        ("consumer", "consumerGroup", "other"),
                                        ("publisher", "brokers", ["new:9092"])):
            changed = copy.deepcopy(values)
            section = changed["auditPublisher"] if component == "publisher" else changed["kafkaConsumers"]["audit"]
            section[field] = value
            self.assertFalse(audit.same_system_config(values, changed))
        values["auditPublisher"]["enabled"] = False
        self.assertFalse(audit.same_system_config(self.values(), values))

    def test_changed_ledger_secret_requires_reconciliation(self):
        values = self.values()
        changed = copy.deepcopy(values)
        changed["core"]["mariadb"]["existingSecret"] = "other-core-db"
        self.assertFalse(audit.same_system_config(values, changed))

    @patch.object(audit, "check_secret")
    def test_independent_group_and_inline_credentials(self, check):
        values = self.values()
        values["kafkaConsumers"]["evidence"] = {"consumerGroup": values["kafkaConsumers"]["audit"]["consumerGroup"]}
        with self.assertRaises(ValueError):
            audit.validate_system(values, "test")
        values = self.values()
        values["auditPublisher"]["password"] = "secret-must-not-appear"
        with self.assertRaises(ValueError) as error:
            audit.validate_system(values, "test")
        self.assertNotIn("secret-must-not-appear", str(error.exception))

    def test_non_audit_user_values_preserved_but_audit_and_unknown_removed(self):
        values = self.values()
        values.update(resources={"requests": {"cpu": "50m"}}, affinity={"nodeAffinity": {}},
                      enterpriseBusinessProvenance={"enabled": True}, unrelated="not-a-chart-value")
        values["kafkaConsumers"]["evidence"] = {"consumerGroup": "evidence-custom"}
        saved = audit.non_audit_values(values)
        self.assertEqual(saved["resources"], values["resources"])
        self.assertEqual(saved["affinity"], values["affinity"])
        self.assertEqual(saved["enterpriseBusinessProvenance"], values["enterpriseBusinessProvenance"])
        self.assertEqual(saved["kafkaConsumers"]["evidence"]["consumerGroup"], "evidence-custom")
        self.assertNotIn("audit", saved["kafkaConsumers"])
        self.assertNotIn("auditPublisher", saved)
        self.assertNotIn("unrelated", saved)
        self.assertNotIn("autoMigrate", saved["core"])

    def test_non_audit_differences_fail_without_values_leaking(self):
        before = self.values()
        before["opensearch"] = {"endpoint": "old"}
        after = copy.deepcopy(before)
        after["opensearch"]["endpoint"] = "sensitive-new-value"
        with self.assertRaises(ValueError) as error:
            audit.check_non_audit(after, before)
        self.assertIn("opensearch.endpoint", str(error.exception))
        self.assertNotIn("sensitive-new-value", str(error.exception))
        audit.check_non_audit(after, before, {"opensearch": {"endpoint": "sensitive-new-value"}})
        with self.assertRaises(ValueError):
            audit.check_non_audit(after, before, {"opensearch": {"endpoint": "different"}})

    @patch.object(audit.subprocess, "run")
    def test_secret_lookup_failure_and_missing_keys(self, run):
        for result in (subprocess.CompletedProcess([], 1, "", "private-error"),
                       subprocess.CompletedProcess([], 0, '{"data":{"username":"dGVzdA=="}}', "")):
            run.return_value = result
            with self.assertRaises(ValueError) as error:
                audit.check_secret("test", "existing", ["username", "password"])
            self.assertNotIn("private-error", str(error.exception))
        run.side_effect = subprocess.TimeoutExpired("kubectl", 15)
        with self.assertRaises(subprocess.TimeoutExpired):
            audit.check_secret("test", "existing", ["username", "password"])

    def test_release_state_detects_revision_changes_and_rejects_ambiguous_rows(self):
        row = {"name": "agent-observability", "namespace": "test", "revision": 1,
               "status": "deployed", "chart": "agent-observability-1.0", "updated": "irrelevant"}
        self.assertNotIn("updated", audit.release_state([row]))
        changed = dict(row, revision=2)
        self.assertNotEqual(audit.release_state([row]), audit.release_state([changed]))
        self.assertEqual(audit.release_state([]), {})
        with self.assertRaises(ValueError):
            audit.release_state([row, row])


if __name__ == "__main__":
    unittest.main()
