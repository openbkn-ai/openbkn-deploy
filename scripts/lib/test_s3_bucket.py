# Copyright openbkn.ai
#
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.

import http.server
import os
import subprocess
import sys
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import s3_bucket  # noqa: E402

SCRIPT = Path(__file__).with_name("s3_bucket.py")


class Handler(http.server.BaseHTTPRequestHandler):
    replies = {}
    seen = []

    def do_PUT(self):
        self.seen.append((self.path, dict(self.headers)))
        status, body = self.replies.get(self.path, (200, ""))
        self.send_response(status)
        self.end_headers()
        self.wfile.write(body.encode())

    def log_message(self, *args):
        pass


class S3BucketTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()
        cls.endpoint = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    def setUp(self):
        Handler.seen.clear()
        Handler.replies = {
            "/owned": (409, "<Error><Code>BucketAlreadyOwnedByYou</Code></Error>"),
            "/taken": (409, "<Error><Code>BucketAlreadyExists</Code></Error>"),
            "/denied": (403, "<Error><Code>AccessDenied</Code></Error>"),
        }

    def test_created_and_signed(self):
        self.assertEqual(s3_bucket.ensure_bucket(self.endpoint, "new", "key-id", "secret"), "created")
        path, headers = Handler.seen[0]
        self.assertEqual(path, "/new")
        auth = headers["Authorization"]
        self.assertTrue(auth.startswith("AWS4-HMAC-SHA256 Credential=key-id/"), auth)
        self.assertIn("/us-east-1/s3/aws4_request", auth)
        self.assertIn("SignedHeaders=host;x-amz-content-sha256;x-amz-date", auth)
        self.assertNotIn("secret", auth)

    def test_owned_bucket_is_success(self):
        self.assertEqual(s3_bucket.ensure_bucket(self.endpoint, "owned", "k", "s"), "exists")

    def test_refusals_fail(self):
        for bucket, code in (("taken", "BucketAlreadyExists"), ("denied", "AccessDenied")):
            with self.assertRaisesRegex(RuntimeError, code):
                s3_bucket.ensure_bucket(self.endpoint, bucket, "k", "s")

    def test_cli_keeps_credentials_out_of_output(self):
        env = {**os.environ, "S3_ACCESS_KEY": "visible-key", "S3_SECRET_KEY": "hidden-secret"}
        result = subprocess.run([sys.executable, str(SCRIPT), self.endpoint, "denied"],
                                env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 1)
        self.assertIn("AccessDenied", result.stderr)
        self.assertNotIn("hidden-secret", result.stdout + result.stderr)
        result = subprocess.run([sys.executable, str(SCRIPT), self.endpoint, "x"],
                                env={k: v for k, v in env.items() if not k.startswith("S3_")},
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)


if __name__ == "__main__":
    unittest.main()
