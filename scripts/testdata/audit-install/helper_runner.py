"""Test-only kubectl subprocess adapter; never imported by production code."""
import json
import os
from pathlib import Path
import subprocess
import sys
from unittest.mock import patch

sys.path.insert(0, str(Path(sys.argv[1]).resolve().parent))
import audit_consumer  # noqa: E402


def fake_kubectl(args, **kwargs):
    assert args[:3] == ["kubectl", "get", "secret"], args
    state = os.environ.get("AUDIT_TEST_SECRET_STATE", "ready")
    if state == "missing":
        return subprocess.CompletedProcess(args, 1, "", "not exposed")
    if state == "timeout":
        raise subprocess.TimeoutExpired(args, 15)
    data = {"username": "dGVzdA==", "password": "dGVzdA==", "dsn": "dGVzdA==",
            "custom-user": "dGVzdA==", "custom-pass": "dGVzdA=="}
    data.pop({"missing-key": "password", "missing-dsn": "dsn"}.get(state, "absent"), None)
    return subprocess.CompletedProcess(args, 0, json.dumps({"data": data}), "")


sys.argv = sys.argv[1:]
with patch.object(audit_consumer.subprocess, "run", side_effect=fake_kubectl):
    raise SystemExit(audit_consumer.main())
