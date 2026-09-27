"""Local HTTP checks for the loopback-only log reader."""

import io
import json
import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import urlopen

import log_server as srv


class LogServerTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.pods = Path(self.temp.name) / "pods.json"
        self.pods.write_text(json.dumps({"items": [{
            "metadata": {"name": "api-abc"},
            "status": {
                "initContainerStatuses": [{"name": "setup"}],
                "containerStatuses": [{"name": "server"}],
            },
        }]}))
        self.pod_patch = patch.object(srv, "PODS", self.pods)
        self.pod_patch.start()
        self.server = srv.ThreadingHTTPServer(("127.0.0.1", 0), srv.Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = "http://127.0.0.1:{}".format(self.server.server_port)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.pod_patch.stop()
        self.temp.cleanup()

    def get(self, path, headers=None):
        try:
            request = srv.Request(self.base + path, headers=headers or {})
            with urlopen(request) as response:
                return response.status, response.read(), response.headers
        except HTTPError as error:
            return error.code, error.read(), error.headers

    def test_no_general_routes_and_explicitly_disabled_logs(self):
        self.assertEqual(self.get("/healthz")[0], 404)
        self.assertEqual(self.get("/install-status-ops/api/components")[0], 404)
        with patch.dict(os.environ, {"INSTALL_STATUS_LOGS_ENABLED": "false"}):
            self.assertEqual(self.get("/install-status/logs?pod=api-abc&container=server")[0], 403)

    def test_logs_enabled_by_default(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertTrue(srv.logs_enabled())

    def test_admin_auth_subrequest_requires_studio_admin_token(self):
        self.assertEqual(self.get("/auth")[0], 401)
        with patch.object(srv, "verify_studio_admin", return_value=(204, "admin-id")) as verify:
            status, _, headers = self.get("/auth", {"Authorization": "Bearer valid-token"})
            self.assertEqual(status, 204)
            self.assertEqual(headers["X-Authenticated-User"], "admin-id")
            verify.assert_called_once_with("valid-token")
        with patch.object(srv, "verify_studio_admin", return_value=(403, "")):
            self.assertEqual(self.get("/auth", {"Authorization": "Bearer valid-token"})[0], 403)
        with patch.object(srv, "verify_studio_admin", side_effect=OSError("bkn-safe unavailable")):
            self.assertEqual(self.get("/auth", {"Authorization": "Bearer valid-token"})[0], 503)

    def test_studio_admin_rule_and_audit_identity(self):
        with patch.object(srv, "safe_api_json", side_effect=[{"is_admin": True}, {"id": "admin-1"}]) as api:
            self.assertEqual(srv.verify_studio_admin("valid-token"), (204, "admin-1"))
            self.assertEqual(api.call_args_list[0].args, ("/api/safe/v1/me/permissions?scope=type", "valid-token"))
            self.assertEqual(api.call_args_list[1].args, ("/api/safe/v1/me", "valid-token"))
        with patch.object(srv, "safe_api_json", return_value={"is_admin": False}) as api:
            self.assertEqual(srv.verify_studio_admin("regular-user-token"), (403, ""))
            api.assert_called_once()

    def test_known_target_and_bounds(self):
        self.assertTrue(srv.known_target("api-abc", "setup"))
        with patch.dict(os.environ, {"INSTALL_STATUS_LOGS_ENABLED": "true"}), \
             patch.object(srv, "kube_log", return_value=b"last crash\n") as read_log:
            path = "/install-status/logs?pod=api-abc&container=server&previous=true&tail=100"
            status, body, headers = self.get(path)
            self.assertEqual((status, body), (200, b"last crash\n"))
            self.assertEqual(headers["Cache-Control"], "no-store")
            read_log.assert_called_once_with("api-abc", "server", True, 100)
            status, body, _ = self.get("/install-status/logs?pod=api-abc&container=setup&tail=100")
            self.assertEqual((status, body), (200, b"last crash\n"))
            for suffix, expected in (
                ("pod=other&container=server", 404),
                ("pod=api-abc&container=other", 404),
                ("pod=api-abc&container=server&tail=201", 400),
                ("pod=api-abc&container=server&previous=maybe", 400),
                ("pod=api-abc&container=server&container=other", 400),
            ):
                self.assertEqual(self.get("/install-status/logs?" + suffix)[0], expected)
            self.assertEqual(read_log.call_count, 2)

    def test_missing_snapshot_fails_closed(self):
        self.pods.unlink()
        with patch.dict(os.environ, {"INSTALL_STATUS_LOGS_ENABLED": "true"}), \
             patch.object(srv, "kube_log") as read_log:
            self.assertEqual(self.get("/install-status/logs?pod=api-abc&container=server")[0], 503)
            read_log.assert_not_called()

    def test_kubernetes_request_is_namespaced_and_limited(self):
        sa = Path(self.temp.name) / "serviceaccount"
        sa.mkdir()
        (sa / "token").write_text("test-token")
        (sa / "ca.crt").write_text("test-ca")
        with patch.object(srv, "SA_DIR", sa), \
             patch.object(srv.ssl, "create_default_context"), \
             patch.object(srv, "urlopen", return_value=io.BytesIO(b"x" * 100000)) as fetch, \
             patch.dict(os.environ, {"POD_NAMESPACE": "openbkn",
                                   "KUBERNETES_SERVICE_HOST": "kubernetes.default.svc",
                                   "KUBERNETES_SERVICE_PORT": "443"}):
            data = srv.kube_log("api-abc", "server", True, 200)
        self.assertEqual(len(data), srv.MAX_LOG_BYTES)
        request = fetch.call_args.args[0]
        self.assertIn("/namespaces/openbkn/pods/api-abc/log?", request.full_url)
        self.assertIn("tailLines=200", request.full_url)
        self.assertIn("previous=true", request.full_url)
        self.assertIn("limitBytes=65536", request.full_url)
        self.assertEqual(request.get_header("Authorization"), "Bearer test-token")


if __name__ == "__main__":
    unittest.main()
