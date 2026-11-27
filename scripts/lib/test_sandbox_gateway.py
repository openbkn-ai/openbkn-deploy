"""Focused regression tests for sandbox installer gateway ownership."""

import unittest
from unittest.mock import patch
import builtins

from sandbox_gateway import base_url, discover, preserve


class SandboxGatewayTests(unittest.TestCase):
    def service(self, name="edge", namespace="gateway"):
        return {
            "metadata": {"name": name, "namespace": namespace},
            "spec": {
                "selector": {"app": name},
                "ports": [{"name": "http", "port": 8080, "targetPort": "web", "nodePort": 32080}],
            },
        }

    def test_rediscovery_does_not_preserve_previous_system_defaults(self):
        old = discover({"items": [self.service()]})
        self.assertEqual(preserve(old), {})
        new = discover({"items": [self.service("new", "other")]})
        self.assertEqual(new["controlPlane"]["gateway"]["baseURL"], "http://new.other.svc:8080")

    def test_preserve_only_operator_changes_to_generated_policy(self):
        old = discover({"items": [self.service()]})
        # Simulate Helm JSON serialization; these are independent objects after storage.
        import json

        old = json.loads(json.dumps(old))
        old["networkPolicy"]["gateway"]["port"] = 8081
        old["controlPlane"]["env"] = {"BKN_BASE_URL": "http://explicit:80"}
        kept = preserve(old)
        self.assertEqual(kept["networkPolicy"]["gateway"], {"port": 8081})
        self.assertEqual(kept["controlPlane"]["env"]["BKN_BASE_URL"], "http://explicit:80")

    def test_admission_service_is_not_an_http_gateway(self):
        service = self.service()
        service["spec"]["ports"][0]["name"] = "https-webhook"
        with self.assertRaises(ValueError):
            discover({"items": [service]})

    def test_invalid_origin_is_rejected_without_echoing_credentials(self):
        with self.assertRaisesRegex(ValueError, "without credentials or path"):
            base_url({"manifest": '  BKN_BASE_URL: "http://user:secret@gateway"\n'})

    def test_post_rendered_yaml_scalar_formats(self):
        for scalar in ("'http://gateway:8080'", '"http://gateway:8080"', 'http://gateway:8080'):
            self.assertEqual(base_url({'manifest': '  BKN_BASE_URL: ' + scalar + '\n'}), 'http://gateway:8080')
        self.assertEqual(base_url({'manifest': "  BKN_BASE_URL: ''\n"}), '')

    def test_community_quoted_value_does_not_require_pyyaml(self):
        original = builtins.__import__
        def no_yaml(name, *args, **kwargs):
            if name == 'yaml':
                raise ImportError('PyYAML is absent')
            return original(name, *args, **kwargs)
        with patch('builtins.__import__', side_effect=no_yaml):
            self.assertEqual(base_url({'manifest': '  BKN_BASE_URL: "http://gateway:8080"\n'}), 'http://gateway:8080')

    def pod(self, host_network=True, phase="Running", namespace="gateway", app="edge"):
        return {
            "kind": "Pod",
            "metadata": {"namespace": namespace, "labels": {"app": app, "extra": "x"}},
            "spec": {
                "hostNetwork": host_network,
                "containers": [
                    {"ports": [{"name": "web", "containerPort": 80, "protocol": "TCP"}]}
                ],
            },
            "status": {"phase": phase},
        }

    def nodes(self, *addresses):
        return {
            "items": [
                {"status": {"addresses": [{"type": kind, "address": value} for kind, value in node]}}
                for node in addresses
            ]
        }

    def gateway(self, result):
        return result["networkPolicy"]["gateway"]

    def test_pod_network_gateway_keeps_selector_and_named_port(self):
        result = discover({"items": [self.service()]}, None, {"items": [self.pod(host_network=False)]})
        self.assertEqual(self.gateway(result)["port"], "web")
        self.assertEqual(self.gateway(result)["ipBlocks"], [])
        self.assertEqual(self.gateway(result), result["controlPlane"]["gateway"]["networkPolicy"])

    def test_host_network_gateway_allows_every_node_on_numeric_port(self):
        # kube-router never selects hostNetwork Pods by label, so without node
        # addresses the rule allowed nothing and executors got connection refused.
        nodes = self.nodes(
            [("InternalIP", "192.0.2.11"), ("Hostname", "node-b"), ("ExternalIP", "198.51.100.7")],
            [("InternalIP", "192.0.2.10"), ("InternalIP", "2001:db8::10")],
        )
        result = discover({"items": [self.service()]}, nodes, {"items": [self.pod()]})
        policy = self.gateway(result)
        self.assertEqual(policy["port"], 80)
        self.assertEqual(
            policy["ipBlocks"],
            [{"cidr": "192.0.2.10/32"}, {"cidr": "192.0.2.11/32"}, {"cidr": "2001:db8::10/128"}],
        )
        self.assertEqual(policy["podSelector"], {"app": "edge"})
        self.assertEqual(result["controlPlane"]["gateway"]["baseURL"], "http://edge.gateway.svc:8080")

    def test_finished_and_unrelated_pods_do_not_decide_the_mode(self):
        pods = [
            self.pod(phase="Succeeded"),
            self.pod(namespace="other"),
            self.pod(app="other"),
            self.pod(host_network=False),
        ]
        result = discover({"items": [self.service()]}, None, {"items": pods})
        self.assertEqual(self.gateway(result)["ipBlocks"], [])

    def test_unreadable_pods_and_nodes_keep_pod_network_discovery(self):
        # Reading Pods and nodes is optional, so installs that worked before still do.
        result = discover({"items": [self.service()]}, {}, {})
        self.assertEqual(self.gateway(result)["port"], "web")
        self.assertEqual(self.gateway(result)["ipBlocks"], [])

    def test_host_network_gateway_without_node_addresses_fails(self):
        pods = {"items": [self.pod()]}
        with self.assertRaisesRegex(ValueError, "Cannot read cluster nodes"):
            discover({"items": [self.service()]}, {}, pods)
        with self.assertRaisesRegex(ValueError, "no InternalIP"):
            discover({"items": [self.service()]}, self.nodes([("Hostname", "node-a")]), pods)

    def test_failures_name_both_settings_a_manual_fix_needs(self):
        # An explicit origin skips discovery, so the gateway rule must be set with it.
        for services, nodes in (({"items": []}, None), ({"items": [self.service()]}, {})):
            with self.assertRaises(ValueError) as caught:
                discover(services, nodes, {"items": [self.pod()]})
            self.assertIn("controlPlane.env.BKN_BASE_URL", str(caught.exception))
            self.assertIn("networkPolicy.gateway", str(caught.exception))

    def test_unresolvable_host_network_target_port_fails(self):
        pod = self.pod()
        pod["spec"]["containers"][0]["ports"][0]["name"] = "other"
        with self.assertRaisesRegex(ValueError, "target port web"):
            discover({"items": [self.service()]}, self.nodes([("InternalIP", "192.0.2.10")]), {"items": [pod]})

    def test_numeric_target_port_needs_no_container_lookup(self):
        service = self.service()
        service["spec"]["ports"][0]["targetPort"] = 8443
        pod = self.pod()
        pod["spec"]["containers"] = []
        result = discover({"items": [service]}, self.nodes([("InternalIP", "192.0.2.10")]), {"items": [pod]})
        self.assertEqual(self.gateway(result)["port"], 8443)

    def test_changed_node_addresses_are_rediscovered(self):
        import json

        nodes = self.nodes([("InternalIP", "192.0.2.10")])
        old = discover({"items": [self.service()]}, nodes, {"items": [self.pod()]})
        self.assertEqual(preserve(json.loads(json.dumps(old))), {})
        # Values saved by an installer without hostNetwork support are system defaults too.
        legacy = json.loads(json.dumps(discover({"items": [self.service()]})))
        for policy in (legacy["networkPolicy"]["gateway"], legacy["controlPlane"]["gateway"]["networkPolicy"]):
            del policy["ipBlocks"]
        self.assertEqual(preserve(legacy), {})

    def test_operator_ip_blocks_survive_rediscovery(self):
        import json

        old = json.loads(json.dumps(discover({"items": [self.service()]}, None, {"items": [self.pod(host_network=False)]})))
        old["networkPolicy"]["gateway"]["ipBlocks"] = [{"cidr": "192.0.2.50/32"}]
        self.assertEqual(preserve(old), {"networkPolicy": {"gateway": {"ipBlocks": [{"cidr": "192.0.2.50/32"}]}}})

    def test_empty_legacy_values_are_supported(self):
        self.assertEqual(preserve(None), {})
        self.assertEqual(preserve({"controlPlane": {"env": {"BKN_BASE_URL": " "}}}), {})


if __name__ == "__main__":
    unittest.main()
