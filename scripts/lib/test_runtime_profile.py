#!/usr/bin/env python3
# Copyright openbkn.ai
#
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.

import argparse
import base64
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("runtime_profile", Path(__file__).with_name("runtime_profile.py"))
runtime = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runtime)


def stored(value):
    return base64.b64encode(base64.b64encode(value)).decode()


def profile():
    return {"apiVersion": runtime.API, "name": "sample-provider", "version": "1.0", "platformVersion": "1.0",
            "secrets": [{"name": "provider-keys", "keys": {key: {"bytes": 32, "encoding": "base64"} for key in ("one", "two")}}],
            "releases": {
                "provider": {"workload": {"name": "provider", "container": "provider"}, "removeEnv": ["RETIRED_SETTING"],
                             "requiredManifestStrings": ["PROVIDER_ENABLED", "provider-keys"],
                             "values": {"image": {"registry": "registry.example/private", "repository": "provider", "tag": "pinned"},
                                        "extraEnv": [{"name": "PROVIDER_ENABLED", "value": "true"}, {"name": "SOURCE_URL", "value": "http://source"},
                                                     *[{"name": env, "valueFrom": {"secretKeyRef": {"name": "provider-keys", "key": key}}} for env, key in (("KEY_ONE", "one"), ("KEY_TWO", "two"))]]}},
                "loader": {"values": {"extensionURL": "http://provider/tools", "extensionJSON": '{"tools":[{"name":"sample"}]}', "upstream": {"url": "http://shared-service"}},
                           "workload": {"name": "loader", "container": "loader"}, "valueEnv": {"upstream.url": "UPSTREAM_URL"},
                           "exclusiveValues": ["extensionURL", "extensionJSON"], "requiredManifestStrings": ["sample"]}}}


class Cluster:
    def __init__(self):
        self.objects = {}
        self.values = {}
        self.calls = []
        self.render = "PROVIDER_ENABLED provider-keys sample"
        self.denied = False

    def run(self, args, payload=None):
        self.calls.append((args, payload))
        if args[:2] == ["kubectl", "get"]:
            if self.denied:
                raise ValueError("Kubernetes access denied")
            value = self.objects.get((args[2], args[3]))
            return json.dumps(value) if value else ""
        if args[:2] == ["helm", "list"]:
            release = args[-1].strip("^$")
            return release if release in self.values else ""
        if args[:3] == ["helm", "get", "values"]:
            return json.dumps(self.values[args[3]])
        if args[:2] == ["helm", "template"]:
            return self.render
        if args[0] == "kubectl" and args[1] in ("create", "replace"):
            resource = json.loads(payload)
            kind = resource["kind"].lower()
            resource["metadata"]["resourceVersion"] = "2"
            self.objects[(kind, resource["metadata"]["name"])] = resource
            return ""
        if args[:2] == ["kubectl", "patch"]:
            return ""
        raise AssertionError(args)


class RuntimeProfileTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.input = self.root / "input.json"
        self.desired = profile()
        runtime.write(self.input, self.desired)
        self.cluster = Cluster()
        self.mock = patch.object(runtime, "run", self.cluster.run)
        self.mock.start()
        self.addCleanup(self.mock.stop)
        self.args = argparse.Namespace(state=str(self.root), namespace="isolated", profile=str(self.input), version="1.0", releases=["provider", "loader"])

    def secret(self, data=None):
        return {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "provider-keys", "namespace": "isolated"},
                "data": data or {"one": stored(b"a" * 32), "two": stored(b"b" * 32)}}

    def prepare(self):
        runtime.prepare(self.args)
        return runtime.read(self.root / "provider.values.json")

    def test_community_install_without_provider_does_not_generate_runtime_keys(self):
        self.args.profile = ""
        runtime.prepare(self.args)
        self.assertFalse((self.root / "profile.json").exists())
        self.assertFalse(any(args[1] in ("create", "replace", "patch") for args, _ in self.cluster.calls))

    def test_new_install_stages_keys_without_cluster_writes_then_persists_profile(self):
        values = self.prepare()
        self.assertEqual(values["namespace"], "isolated")
        self.assertFalse(any(args[1] in ("create", "replace", "patch") for args, _ in self.cluster.calls))
        keys = runtime.read(self.root / "pending-secrets.json")[0]["data"]
        self.assertNotEqual(keys["one"], keys["two"])
        self.assertTrue(all(len(runtime.decode_key(value)) == 32 for value in keys.values()))
        runtime.apply(self.args)
        self.assertEqual(self.cluster.objects[("secret", "provider-keys")]["data"], keys)
        self.assertIn(("secret", runtime.STATE_NAME), self.cluster.objects)

    def test_upgrade_reuses_keys_and_preserves_operator_environment(self):
        existing = self.secret()
        self.cluster.objects[("secret", "provider-keys")] = copy.deepcopy(existing)
        self.cluster.values["provider"] = {"extraEnv": [{"name": "PROVIDER_ENABLED", "value": "false"}, {"name": "SOURCE_URL", "value": "http://custom"}, {"name": "OTHER", "value": "keep"}, {"name": "RETIRED_SETTING", "value": "old"}]}
        values = self.prepare()
        env = runtime.envs(values["extraEnv"])
        self.assertEqual(env["PROVIDER_ENABLED"]["value"], "false")
        self.assertEqual(env["SOURCE_URL"]["value"], "http://custom")
        self.assertIn("OTHER", env)
        self.assertNotIn("RETIRED_SETTING", env)
        self.assertEqual(runtime.read(self.root / "pending-secrets.json"), [])
        runtime.apply(self.args)
        self.assertEqual(existing["data"], self.cluster.objects[("secret", "provider-keys")]["data"])
        # Later ordinary deploy.sh invocations reuse the selected delivery profile.
        self.args.profile = ""
        self.prepare()
        self.assertEqual(runtime.read(self.root / "pending-secrets.json"), [])

    def test_live_manual_keys_are_retained_and_normalized_to_secret_references(self):
        self.cluster.objects[("deployment", "provider")] = {
            "metadata": {"resourceVersion": "7"}, "spec": {"template": {"spec": {"containers": [{"name": "provider", "env": [
                {"name": "KEY_ONE", "value": base64.b64encode(b"a" * 32).decode()},
                {"name": "KEY_TWO", "value": base64.b64encode(b"b" * 32).decode()},
                {"name": "SOURCE_URL", "value": "http://patched"}, {"name": "RETIRED_SETTING", "value": "old"}]}]}}}}
        values = self.prepare()
        self.assertEqual(runtime.envs(values["extraEnv"])["SOURCE_URL"]["value"], "http://patched")
        self.assertNotIn("value", runtime.envs(values["extraEnv"])["KEY_ONE"])
        staged = runtime.read(self.root / "pending-secrets.json")[0]
        self.assertEqual(staged["data"], self.secret()["data"])
        runtime.patch_env(argparse.Namespace(state=str(self.root), namespace="isolated", release="provider"))
        patch_args, patch_payload = self.cluster.calls[-1]
        self.assertEqual(patch_args[:2], ["kubectl", "patch"])
        patch_data = runtime.read(patch_args[-1])
        self.assertEqual(patch_data[0], {"op": "test", "path": "/metadata/resourceVersion", "value": "7"})
        self.assertFalse(any(e["name"] == "RETIRED_SETTING" for e in patch_data[1]["value"]))

    def test_existing_missing_corrupt_and_equal_keys_fail_without_mutations(self):
        for data in ({"one": stored(b"a" * 32)}, {"one": "invalid", "two": stored(b"b" * 32)}, {"one": stored(b"a" * 32), "two": stored(b"a" * 32)}):
            self.cluster.objects[("secret", "provider-keys")] = self.secret(data)
            with self.assertRaisesRegex(ValueError, "restore"):
                self.prepare()
        self.assertFalse(any(args[1] in ("create", "replace", "patch") for args, _ in self.cluster.calls))

    def test_previously_used_missing_secret_is_never_regenerated(self):
        self.cluster.objects[("secret", runtime.STATE_NAME)] = {"metadata": {"resourceVersion": "3"}, "data": {"profile.json": base64.b64encode(json.dumps(self.desired).encode()).decode()}}
        with self.assertRaisesRegex(ValueError, "restore"):
            self.prepare()

    def test_conflicting_runtime_keys_and_other_extension_providers_are_rejected(self):
        self.cluster.objects[("secret", "provider-keys")] = self.secret()
        self.cluster.values["provider"] = {"extraEnv": [{"name": "KEY_ONE", "value": base64.b64encode(b"x" * 32).decode()}]}
        with self.assertRaisesRegex(ValueError, "conflict"):
            self.prepare()
        self.cluster.values = {"loader": {"extensionURL": "http://another-provider"}}
        with self.assertRaisesRegex(ValueError, "another extension provider"):
            self.prepare()

    def test_render_preflight_checks_real_template_before_any_write(self):
        self.prepare()
        args = argparse.Namespace(state=str(self.root), release="provider", helm_args=["upgrade", "--install", "provider", "chart", "--namespace", "isolated", "--wait", "--timeout=60s", "-f", "values.json"])
        runtime.render(args)
        self.assertEqual(self.cluster.calls[-1][0], ["helm", "template", "provider", "chart", "--namespace", "isolated", "-f", "values.json"])
        self.cluster.render = "old chart ignores runtime settings"
        with self.assertRaisesRegex(ValueError, "update the platform Chart"):
            runtime.render(args)
        self.assertFalse(any(call[0][1] in ("create", "replace", "patch") for call in self.cluster.calls))

    def test_denied_reads_and_version_mismatch_do_not_create_replacement_keys(self):
        self.cluster.denied = True
        with self.assertRaisesRegex(ValueError, "denied"):
            self.prepare()
        self.cluster.denied = False
        self.args.version = "2.0"
        with self.assertRaisesRegex(ValueError, "matching delivery profile"):
            self.prepare()

    def test_profile_update_cannot_discard_existing_key_ownership(self):
        self.cluster.objects[("secret", runtime.STATE_NAME)] = {"metadata": {"resourceVersion": "3"}, "data": {"profile.json": base64.b64encode(json.dumps(self.desired).encode()).decode()}}
        modified = copy.deepcopy(self.desired)
        modified["secrets"][0]["name"] = "replacement-keys"
        runtime.write(self.input, modified)
        with self.assertRaisesRegex(ValueError, "key ownership changed"):
            self.prepare()
        self.assertFalse(any(args[1] in ("create", "replace", "patch") for args, _ in self.cluster.calls))

    def test_concurrent_profile_update_is_not_overwritten(self):
        self.prepare()
        self.cluster.objects[("secret", runtime.STATE_NAME)] = {"metadata": {"resourceVersion": "10"}, "data": {"profile.json": base64.b64encode(json.dumps(self.desired).encode()).decode()}}
        with self.assertRaisesRegex(ValueError, "concurrently"):
            runtime.apply(self.args)
        self.assertEqual(self.cluster.objects[("secret", runtime.STATE_NAME)]["metadata"]["resourceVersion"], "10")

    def test_mapped_nested_values_preserve_explicit_helm_overrides(self):
        for address in ("https://custom-service:3443", "http://custom-service:3000"):
            with self.subTest(address=address):
                self.cluster.values["loader"] = {"upstream": {"url": address}}
                self.prepare()
                self.assertEqual(runtime.read(self.root / "loader.values.json")["upstream"]["url"], address)

    def test_empty_mapped_values_use_provider_default(self):
        for address in (None, ""):
            with self.subTest(address=address):
                self.cluster.values["loader"] = {"upstream": {"url": address}}
                self.prepare()
                self.assertEqual(runtime.read(self.root / "loader.values.json")["upstream"]["url"], "http://shared-service")

    def test_empty_live_mapping_does_not_erase_custom_helm_value(self):
        self.cluster.values["loader"] = {"upstream": {"url": "https://helm-service"}}
        self.cluster.objects[("deployment", "loader")] = {
            "metadata": {"resourceVersion": "7"}, "spec": {"template": {"spec": {"containers": [
                {"name": "loader", "env": [{"name": "UPSTREAM_URL", "value": ""}]}]}}}}
        self.prepare()
        self.assertEqual(runtime.read(self.root / "loader.values.json")["upstream"]["url"], "https://helm-service")

    def test_live_mapped_value_wins_over_helm_and_survives_ordinary_reinstall(self):
        self.cluster.values["loader"] = {"upstream": {"url": "http://helm-service"}}
        self.cluster.objects[("deployment", "loader")] = {
            "metadata": {"resourceVersion": "7"}, "spec": {"template": {"spec": {"containers": [
                {"name": "loader", "env": [{"name": "UPSTREAM_URL", "value": "https://patched-service"}]}]}}}}
        self.prepare()
        self.assertEqual(runtime.read(self.root / "loader.values.json")["upstream"]["url"], "https://patched-service")
        runtime.apply(self.args)
        self.args.profile = ""
        self.prepare()
        self.assertEqual(runtime.read(self.root / "loader.values.json")["upstream"]["url"], "https://patched-service")

    def test_profile_default_can_change_without_replacing_operator_override(self):
        self.prepare()
        runtime.apply(self.args)
        self.cluster.values["loader"] = {"upstream": {"url": "http://shared-service"}}
        self.desired["releases"]["loader"]["values"]["upstream"]["url"] = "http://new-default"
        runtime.write(self.input, self.desired)
        self.prepare()
        self.assertEqual(runtime.read(self.root / "loader.values.json")["upstream"]["url"], "http://new-default")
        self.cluster.values["loader"] = {"upstream": {"url": "http://operator-service"}}
        self.prepare()
        self.assertEqual(runtime.read(self.root / "loader.values.json")["upstream"]["url"], "http://operator-service")

    def test_nested_mapping_does_not_replace_other_values_or_scalar_parents(self):
        self.cluster.values["loader"] = {"upstream": {"url": "http://custom", "unmanaged": "not-owned"}}
        self.prepare()
        values = runtime.read(self.root / "loader.values.json")
        self.assertEqual(values["upstream"]["url"], "http://custom")
        self.assertNotIn("upstream.url", values)
        nested = {"upstream": {"url": "http://old", "unmanaged": "keep"}}
        runtime.set_value(nested, "upstream.url", "http://new")
        self.assertEqual(nested["upstream"], {"url": "http://new", "unmanaged": "keep"})
        with self.assertRaisesRegex(ValueError, "non-object"):
            runtime.set_value({"upstream": "existing-scalar"}, "upstream.url", "http://custom")

    def test_invalid_mapping_paths_fail_preflight(self):
        for path in ("", ".url", "upstream..url", "upstream."):
            with self.subTest(path=path):
                self.desired["releases"]["loader"]["valueEnv"] = {path: "UPSTREAM_URL"}
                with self.assertRaisesRegex(ValueError, "environment mapping"):
                    runtime.validate(self.desired, "1.0", ["provider", "loader"])

    def test_new_mapping_adopts_old_manual_address_without_changing_keys(self):
        old = copy.deepcopy(self.desired)
        del old["releases"]["loader"]["valueEnv"]
        del old["releases"]["loader"]["values"]["upstream"]
        self.cluster.objects[("secret", runtime.STATE_NAME)] = {"metadata": {"resourceVersion": "3"}, "data": {"profile.json": base64.b64encode(json.dumps(old).encode()).decode()}}
        self.cluster.objects[("secret", "provider-keys")] = self.secret()
        self.cluster.objects[("deployment", "loader")] = {"metadata": {"resourceVersion": "7"}, "spec": {"template": {"spec": {"containers": [
            {"name": "loader", "env": [{"name": "UPSTREAM_URL", "value": "http://old-patch"}]}]}}}}
        self.prepare()
        self.assertEqual(runtime.read(self.root / "loader.values.json")["upstream"]["url"], "http://old-patch")
        self.assertEqual(runtime.read(self.root / "pending-secrets.json"), [])


if __name__ == "__main__":
    unittest.main()
