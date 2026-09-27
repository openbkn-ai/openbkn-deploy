#!/usr/bin/env python3
"""Loopback-only admin checker and bounded Kubernetes log reader.

Nginx calls the admin check as an internal auth subrequest before serving any
dashboard data or logs. This process does not serve the status page or a general
Kubernetes API.
"""

import json
import os
import re
import secrets
import ssl
import sys
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit, parse_qs
from urllib.request import ProxyHandler, Request, build_opener, urlopen


PODS = Path(os.getenv("STATUS_PODS", "/live/pods.public.json"))
SA_DIR = Path(os.getenv("STATUS_SA_DIR", "/var/run/secrets/kubernetes.io/serviceaccount"))
NAME = re.compile(r"^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$")
MAX_LOG_LINES = 200
MAX_LOG_BYTES = 65536
MAX_SNAPSHOT_BYTES = 4 * 1024 * 1024
MAX_AUTH_RESPONSE_BYTES = 1024 * 1024


def logs_enabled():
    return os.getenv("INSTALL_STATUS_LOGS_ENABLED", "true").lower() == "true"


def known_target(pod_name, container_name):
    with PODS.open("rb") as file:
        raw = file.read(MAX_SNAPSHOT_BYTES + 1)
    if len(raw) > MAX_SNAPSHOT_BYTES:
        raise ValueError("Pod snapshot is too large")
    for pod in json.loads(raw).get("items", []):
        if pod.get("metadata", {}).get("name") != pod_name:
            continue
        status = pod.get("status", {})
        containers = status.get("initContainerStatuses", []) + status.get("containerStatuses", [])
        return any(item.get("name") == container_name for item in containers)
    return False


def kube_log(pod_name, container_name, previous, lines):
    namespace = os.getenv("POD_NAMESPACE", "")
    host = os.getenv("KUBERNETES_SERVICE_HOST", "")
    port = os.getenv("KUBERNETES_SERVICE_PORT", "443")
    if not namespace or not host or not NAME.fullmatch(namespace):
        raise ValueError("Kubernetes connection is not configured")
    token = (SA_DIR / "token").read_text().strip()
    context = ssl.create_default_context(cafile=str(SA_DIR / "ca.crt"))
    host = "[{}]".format(host) if ":" in host and not host.startswith("[") else host
    query = urlencode({"container": container_name, "tailLines": lines,
                       "limitBytes": MAX_LOG_BYTES, "previous": str(previous).lower(),
                       "timestamps": "true"})
    url = "https://{}:{}/api/v1/namespaces/{}/pods/{}/log?{}".format(
        host, port, quote(namespace), quote(pod_name), query)
    request = Request(url, headers={"Authorization": "Bearer " + token})
    with urlopen(request, context=context, timeout=10) as response:
        return response.read(MAX_LOG_BYTES + 1)[:MAX_LOG_BYTES]


def safe_api_json(path, token):
    base = os.getenv("BKN_SAFE_URL", "http://bkn-safe:3000").rstrip("/")
    request = Request(base + path, headers={"Authorization": "Bearer " + token,
                                             "Accept": "application/json"})
    opener = build_opener(ProxyHandler({}))
    with opener.open(request, timeout=5) as response:
        raw = response.read(MAX_AUTH_RESPONSE_BYTES + 1)
        if len(raw) > MAX_AUTH_RESPONSE_BYTES:
            raise ValueError("bkn-safe response is too large")
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError("bkn-safe response is invalid")
        return data


def verify_studio_admin(token):
    """Match Studio's isAdmin rule from its token-gated /me endpoints."""
    permissions = safe_api_json("/api/safe/v1/me/permissions?scope=type", token)
    if permissions.get("is_admin") is not True:
        return 403, ""
    identity = safe_api_json("/api/safe/v1/me", token)
    subject = identity.get("id") or identity.get("account") or "studio-admin"
    if not re.fullmatch(r"[A-Za-z0-9_.@-]{1,128}", str(subject)):
        subject = "studio-admin"
    return 204, str(subject)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format_string, *args):
        # Log queries and response bodies can contain sensitive information.
        pass

    def send_body(self, status, body, content_type):
        if urlsplit(self.path).path == "/install-status/logs":
            self.audit(status)
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.end_headers()
        self.wfile.write(body)

    def audit(self, status):
        query = parse_qs(urlsplit(self.path).query)
        user = self.headers.get("X-Authenticated-User", "unknown")
        if not re.fullmatch(r"[A-Za-z0-9_.@-]{1,64}", user):
            user = "invalid"
        entry = {
            "event": "install_status_log_access",
            "at": datetime.now(timezone.utc).isoformat(),
            "user": user,
            "pod": query.get("pod", [""])[0][:253],
            "container": query.get("container", [""])[0][:128],
            "previous": query.get("previous", ["false"])[0] == "true",
            "tail": query.get("tail", ["100"])[0][:4],
            "status": status,
        }
        print(json.dumps(entry, separators=(",", ":")), file=sys.stderr, flush=True)

    def error(self, status, code, message):
        trace_id = secrets.token_hex(8)
        body = json.dumps({"error_code": code, "message": message,
                           "trace_id": trace_id}).encode("utf-8")
        self.send_body(status, body, "application/json; charset=utf-8")

    def authenticate(self):
        header = self.headers.get("Authorization", "")
        scheme, separator, token = header.partition(" ")
        if (scheme.lower() != "bearer" or not separator or len(token) > 8192
                or not re.fullmatch(r"[A-Za-z0-9._~+/=-]+", token)):
            self.send_auth_result(401)
            return
        try:
            status, subject = verify_studio_admin(token)
        except HTTPError as exc:
            status, subject = (exc.code, "") if exc.code in (401, 403) else (503, "")
        except (OSError, URLError, ValueError, json.JSONDecodeError):
            status, subject = 503, ""
        self.send_auth_result(status, subject)

    def send_auth_result(self, status, subject=""):
        self.send_response(status)
        self.send_header("Cache-Control", "no-store")
        if subject:
            self.send_header("X-Authenticated-User", subject)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        parsed = urlsplit(self.path)
        if parsed.path == "/auth":
            self.authenticate()
            return
        if parsed.path != "/install-status/logs":
            self.error(404, "RESOURCE_NOT_FOUND", "Not found")
            return
        if not logs_enabled():
            self.error(403, "FORBIDDEN", "Log access is disabled")
            return
        query = parse_qs(parsed.query, keep_blank_values=True)
        if set(query) - {"pod", "container", "previous", "tail"} or any(
                len(values) != 1 for values in query.values()):
            self.error(400, "INVALID_PARAMETER", "Invalid log parameters")
            return
        pod_name = query.get("pod", [""])[0]
        container_name = query.get("container", [""])[0]
        previous = query.get("previous", ["false"])[0]
        try:
            lines = int(query.get("tail", ["100"])[0])
        except ValueError:
            lines = 0
        if (not NAME.fullmatch(pod_name) or not NAME.fullmatch(container_name)
                or previous not in ("true", "false") or not 1 <= lines <= MAX_LOG_LINES):
            self.error(400, "INVALID_PARAMETER", "Invalid log parameters")
            return
        try:
            if not known_target(pod_name, container_name):
                self.error(404, "RESOURCE_NOT_FOUND", "Pod or container not found")
                return
        except (OSError, ValueError, json.JSONDecodeError):
            self.error(503, "SNAPSHOT_UNAVAILABLE", "Pod snapshot unavailable")
            return
        try:
            data = kube_log(pod_name, container_name, previous == "true", lines)
        except HTTPError as exc:
            self.error(404 if exc.code == 404 else 502,
                       "RESOURCE_NOT_FOUND" if exc.code == 404 else "KUBERNETES_UNAVAILABLE",
                       "Pod logs unavailable")
            return
        except (OSError, URLError, ValueError):
            self.error(502, "KUBERNETES_UNAVAILABLE", "Pod logs unavailable")
            return
        self.send_body(200, data, "text/plain; charset=utf-8")


if __name__ == "__main__":
    ThreadingHTTPServer(("127.0.0.1", 8081), Handler).serve_forever()
