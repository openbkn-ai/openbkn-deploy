#!/usr/bin/env python3
# Copyright openbkn.ai
#
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.

"""Single operator entry for the OpenBKN 0.1.5 permission transition."""

from __future__ import annotations

import argparse
import base64
import binascii
import contextlib
import json
import os
import platform
import re
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Optional, Sequence


SCRIPT_DIRECTORY = Path(__file__).resolve().parent
GO_ARCHITECTURES = {
    "x86_64": "amd64",
    "amd64": "amd64",
    "aarch64": "arm64",
    "arm64": "arm64",
}


def host_machine(goos: str) -> str:
    """Return the host CPU architecture, seeing through Rosetta on macOS.

    An x86_64 Python on Apple silicon reports x86_64, but the host can run the
    native arm64 executable, and only that one is bundled for macOS.
    """
    machine = platform.machine()
    if goos != "darwin" or machine != "x86_64":
        return machine
    try:
        result = subprocess.run(
            ["/usr/sbin/sysctl", "-n", "hw.optional.arm64"],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return machine
    return "arm64" if result.stdout.strip() == "1" else machine


def bundled_authz_migrator(system: str = "", machine: str = "") -> Path:
    """Return the checked-in authorization executable for the running host.

    The executable runs where this script runs, not inside the cluster, so it
    must match the operator host's platform rather than the node images.
    """
    goos = (system or platform.system()).lower()
    machine = (machine or host_machine(goos)).lower()
    goarch = GO_ARCHITECTURES.get(machine, machine)
    executable = f"authz-migrate-{goos}-{goarch}"
    return SCRIPT_DIRECTORY / "authz_migrate" / executable


DEFAULT_AUTHZ_MIGRATOR = str(bundled_authz_migrator())
DEFAULT_STATE_FILE = "/tmp/openbkn-permission-model-transition-workloads.tsv"
DEFAULT_RUN_ROOT = Path.home() / ".openbkn-ai" / "migrations"
TARGET_VERSION = "0.1.5"


class OrchestrationError(RuntimeError):
    """Raised when a release migration step cannot be executed safely."""


@dataclass(frozen=True)
class Step:
    name: str
    command: tuple[str, ...]
    report_path: Path


def build_parser() -> argparse.ArgumentParser:
    """Build the release-specific command line."""
    parser = argparse.ArgumentParser(
        description="Run the one-time OpenBKN 0.1.5 permission-model transition."
    )
    commands = parser.add_subparsers(dest="command", required=True)

    for action in ("stop", "start"):
        control = commands.add_parser(action, help=f"{action} migration workloads")
        control.add_argument("--namespace", default="openbkn")
        control.add_argument("--expected-context", default="")
        control.add_argument("--state-file", default=DEFAULT_STATE_FILE)
        control.add_argument("--timeout-seconds", type=int, default=300)

    for mode in ("dry-run", "apply"):
        migration = commands.add_parser(mode, help=f"{mode} all registered steps")
        migration.add_argument("--report-dir", required=True)
        migration.add_argument("--state-file", default=DEFAULT_STATE_FILE)
        migration.add_argument("--namespace", default="openbkn")
        migration.add_argument("--expected-context", default="")
        migration.add_argument(
            "--authz-migrator",
            default=os.getenv("OPENBKN_AUTHZ_MIGRATOR", DEFAULT_AUTHZ_MIGRATOR),
            help=argparse.SUPPRESS,
        )

    upgrade = commands.add_parser(
        "upgrade",
        help="run the complete post-install 0.1.5 permission transition",
    )
    upgrade.add_argument("--namespace", default="", help=argparse.SUPPRESS)
    upgrade.add_argument("--expected-context", default="", help=argparse.SUPPRESS)
    upgrade.add_argument(
        "--authz-migrator",
        default=os.getenv("OPENBKN_AUTHZ_MIGRATOR", DEFAULT_AUTHZ_MIGRATOR),
        help=argparse.SUPPRESS,
    )

    return parser


def control_services(args: argparse.Namespace) -> int:
    """Delegate workload control through the versioned release script."""
    command = [
        str(SCRIPT_DIRECTORY / "service_control.sh"),
        args.command,
        "--namespace",
        args.namespace,
        "--state-file",
        args.state_file,
        "--timeout-seconds",
        str(args.timeout_seconds),
    ]
    if args.expected_context:
        command.extend(["--expected-context", args.expected_context])
    return subprocess.run(command, check=False).returncode


def prepare_report_directory(path: str) -> Path:
    """Create an empty report directory without overwriting prior evidence."""
    report_dir = Path(path).resolve()
    if report_dir.exists() and any(report_dir.iterdir()):
        raise OrchestrationError(f"report directory is not empty: {report_dir}")
    report_dir.mkdir(parents=True, exist_ok=True)
    return report_dir


def run_json_command(command: Sequence[str], purpose: str) -> Any:
    """Run a cluster read and decode JSON without exposing sensitive output."""
    try:
        result = subprocess.run(
            list(command), check=False, capture_output=True, text=True
        )
    except FileNotFoundError as exc:
        raise OrchestrationError(f"{command[0]} is required for {purpose}") from exc
    if result.returncode != 0:
        detail = result.stderr.strip() or f"{command[0]} returned a non-zero exit code"
        raise OrchestrationError(f"cannot {purpose}: {detail}")
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise OrchestrationError(f"{command[0]} returned invalid JSON for {purpose}") from exc


def helm_context_arguments(expected_context: str) -> list[str]:
    """Return a Helm context override when the caller requested one."""
    if not expected_context:
        return []
    return ["--kube-context", expected_context]


def kubectl_context_arguments(expected_context: str) -> list[str]:
    """Return a kubectl context override when the caller requested one."""
    if not expected_context:
        return []
    return ["--context", expected_context]


def discover_target_namespace(requested: str, expected_context: str = "") -> str:
    """Find the unique namespace containing the installed bkn-safe release."""
    releases = run_json_command(
        [
            "helm",
            "list",
            "--all-namespaces",
            "--output",
            "json",
            *helm_context_arguments(expected_context),
        ],
        "discover the installed bkn-safe release",
    )
    if not isinstance(releases, list):
        raise OrchestrationError("helm returned an invalid release inventory")
    candidates = [
        item
        for item in releases
        if isinstance(item, dict)
        and item.get("name") == "bkn-safe"
        and (not requested or item.get("namespace") == requested)
    ]
    if not candidates:
        location = f" in namespace {requested!r}" if requested else ""
        raise OrchestrationError(f"cannot find the installed bkn-safe release{location}")
    namespaces = sorted(
        {str(item.get("namespace", "")).strip() for item in candidates}
    )
    if len(namespaces) != 1 or not namespaces[0]:
        raise OrchestrationError(
            "multiple bkn-safe releases were found; select one with --namespace"
        )
    return namespaces[0]


def installed_target_version(namespace: str, expected_context: str = "") -> str:
    """Require the target bkn-safe chart before modifying its data."""
    releases = run_json_command(
        [
            "helm",
            "list",
            "--namespace",
            namespace,
            "--output",
            "json",
            *helm_context_arguments(expected_context),
        ],
        "identify the installed OpenBKN release",
    )
    if not isinstance(releases, list):
        raise OrchestrationError("helm returned an invalid release inventory")
    chart = next(
        (item.get("chart", "") for item in releases if item.get("name") == "bkn-safe"),
        "",
    )
    match = re.fullmatch(r"bkn-safe-(\d+\.\d+\.\d+)", chart)
    if match is None:
        raise OrchestrationError(
            "cannot identify the installed bkn-safe chart version; expected release bkn-safe"
        )
    version = match.group(1)
    if version != TARGET_VERSION:
        raise OrchestrationError(
            f"unsupported installed version {version!r}; install "
            f"bkn-safe {TARGET_VERSION} before running this post-install migration"
        )
    return version


def helm_release_values(
    namespace: str, release: str, expected_context: str = ""
) -> Mapping[str, Any]:
    """Read the effective user values retained by an installed Helm release."""
    values = run_json_command(
        [
            "helm",
            "get",
            "values",
            release,
            "--namespace",
            namespace,
            "--output",
            "json",
            *helm_context_arguments(expected_context),
        ],
        f"read Helm values for {release}",
    )
    if not isinstance(values, dict):
        raise OrchestrationError(f"Helm values for {release} are not an object")
    return values


def kubernetes_resource(
    namespace: str, kind: str, name: str, expected_context: str = ""
) -> Mapping[str, Any]:
    """Read one namespaced Kubernetes resource as JSON."""
    resource = run_json_command(
        [
            "kubectl",
            *kubectl_context_arguments(expected_context),
            "--namespace",
            namespace,
            "get",
            kind,
            name,
            "--output",
            "json",
        ],
        f"read {kind}/{name}",
    )
    if not isinstance(resource, dict):
        raise OrchestrationError(f"{kind}/{name} is not a JSON object")
    return resource


def required_text(values: Mapping[str, Any], key: str, source: str) -> str:
    """Read one required non-empty configuration value."""
    value = values.get(key)
    if not isinstance(value, (str, int)) or not str(value).strip():
        raise OrchestrationError(f"{source} is missing required value {key}")
    return str(value).strip()


def rds_environment(
    prefix: str, release: str, values: Mapping[str, Any]
) -> dict[str, str]:
    """Translate an installed release's shared RDS values for a migration step."""
    dependencies = values.get("depServices")
    if not isinstance(dependencies, dict):
        raise OrchestrationError(f"Helm values for {release} have no depServices")
    rds = dependencies.get("rds")
    if not isinstance(rds, dict):
        raise OrchestrationError(f"Helm values for {release} have no depServices.rds")
    source = f"Helm values for {release}.depServices.rds"
    return {
        f"{prefix}_HOST": required_text(rds, "host", source),
        f"{prefix}_PORT": required_text(rds, "port", source),
        f"{prefix}_USER": required_text(rds, "user", source),
        f"{prefix}_PASSWORD": required_text(rds, "password", source),
        f"{prefix}_NAME": required_text(rds, "database", source),
    }


def decode_secret_value(secret: Mapping[str, Any], key: str, source: str) -> str:
    """Decode one required Kubernetes Secret value without logging it."""
    encoded = secret.get(key)
    if not isinstance(encoded, str) or not encoded:
        raise OrchestrationError(f"{source} is missing required key {key}")
    try:
        value = base64.b64decode(encoded, validate=True).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError) as exc:
        raise OrchestrationError(f"{source} contains invalid data for {key}") from exc
    if not value:
        raise OrchestrationError(f"{source} contains an empty value for {key}")
    return value


def discover_database_environment(
    namespace: str, expected_context: str = ""
) -> dict[str, str]:
    """Resolve database settings from the installed releases and Safe resources."""
    environment: dict[str, str] = {}
    for prefix, release in (
        ("BKN_DB", "bkn-backend"),
        ("VEGA_DB", "vega-backend"),
    ):
        environment.update(
            rds_environment(
                prefix,
                release,
                helm_release_values(namespace, release, expected_context),
            )
        )

    config_map = kubernetes_resource(
        namespace, "configmap", "bkn-safe-config", expected_context
    )
    config_data = config_map.get("data")
    if not isinstance(config_data, dict):
        raise OrchestrationError("configmap/bkn-safe-config has no data")
    safe_source = "configmap/bkn-safe-config"
    for source_key, target_key in (
        ("SAFE_DB_TYPE", "SAFE_DB_TYPE"),
        ("SAFE_DB_HOST", "SAFE_DB_HOST"),
        ("SAFE_DB_PORT", "SAFE_DB_PORT"),
        ("SAFE_DB_USER", "SAFE_DB_USER"),
        ("SAFE_DB_NAME", "SAFE_DB_NAME"),
    ):
        environment[target_key] = required_text(config_data, source_key, safe_source)

    secret = kubernetes_resource(
        namespace, "secret", "bkn-safe-secrets", expected_context
    )
    secret_data = secret.get("data")
    if not isinstance(secret_data, dict):
        raise OrchestrationError("secret/bkn-safe-secrets has no data")
    environment["SAFE_DB_PASSWORD"] = decode_secret_value(
        secret_data, "SAFE_DB_PASSWORD", "secret/bkn-safe-secrets"
    )
    return environment


def install_database_environment(environment: Mapping[str, str]) -> None:
    """Install authoritative discovered values for all migration subprocesses."""
    os.environ.pop("SAFE_CONFIG", None)
    for name, value in environment.items():
        os.environ[name] = value


def cluster_service(host: str) -> Optional[tuple[str, str]]:
    """Parse service.namespace.svc hosts that require in-cluster access."""
    match = re.fullmatch(
        r"([a-z0-9]([-a-z0-9]*[a-z0-9])?)\."
        r"([a-z0-9]([-a-z0-9]*[a-z0-9])?)\.svc"
        r"(?:\.cluster\.local)?",
        host,
    )
    if match is None:
        return None
    return match.group(1), match.group(3)


def available_local_port() -> int:
    """Reserve an ephemeral loopback port for a short-lived port-forward."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def wait_for_port_forward(
    process: subprocess.Popen[str], local_port: int, description: str
) -> None:
    """Wait until kubectl accepts connections or exits with an error."""
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if process.poll() is not None:
            detail = process.stderr.read().strip() if process.stderr else ""
            raise OrchestrationError(
                f"cannot establish {description}: {detail or 'kubectl port-forward exited'}"
            )
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.settimeout(0.2)
            if client.connect_ex(("127.0.0.1", local_port)) == 0:
                return
        time.sleep(0.1)
    raise OrchestrationError(f"timed out establishing {description}")


def stop_port_forward(process: subprocess.Popen[str]) -> None:
    """Stop one temporary kubectl port-forward process."""
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


@contextlib.contextmanager
def database_access_environment(
    environment: Mapping[str, str], expected_context: str = ""
) -> Iterator[dict[str, str]]:
    """Make cluster-internal databases reachable from the operator machine."""
    effective = dict(environment)
    tunnels: list[subprocess.Popen[str]] = []
    forwarded: dict[tuple[str, str], int] = {}
    try:
        for prefix in ("BKN_DB", "VEGA_DB", "SAFE_DB"):
            host_key = f"{prefix}_HOST"
            port_key = f"{prefix}_PORT"
            host = effective[host_key]
            service = cluster_service(host)
            if service is None:
                continue
            remote_port = effective[port_key]
            tunnel_key = (host, remote_port)
            local_port = forwarded.get(tunnel_key)
            if local_port is None:
                service_name, service_namespace = service
                local_port = available_local_port()
                command = [
                    "kubectl",
                    *kubectl_context_arguments(expected_context),
                    "--namespace",
                    service_namespace,
                    "port-forward",
                    f"service/{service_name}",
                    f"{local_port}:{remote_port}",
                ]
                process = subprocess.Popen(
                    command,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                tunnels.append(process)
                wait_for_port_forward(
                    process,
                    local_port,
                    f"database tunnel for service/{service_name} in {service_namespace}",
                )
                forwarded[tunnel_key] = local_port
            effective[host_key] = "127.0.0.1"
            effective[port_key] = str(local_port)
        yield effective
    finally:
        for process in reversed(tunnels):
            stop_port_forward(process)


def build_steps(args: argparse.Namespace, report_dir: Path) -> list[Step]:
    """Return the fixed step order for this target release.

    Future Vega work for the same target release is added here as another
    explicit step. It must not introduce another operator-facing entry point.
    """
    authz_migrator = Path(args.authz_migrator)
    if not authz_migrator.is_file() or not os.access(authz_migrator, os.X_OK):
        raise OrchestrationError(
            f"authorization migration executable is missing or not executable: "
            f"{authz_migrator}; "
            "the release bundles linux/amd64, linux/arm64 and darwin/arm64, "
            "build another platform with authz_migrate/build.sh <os>/<arch>"
        )

    bkn_report = report_dir / "01-bkn-data.json"
    vega_report = report_dir / "02-vega-data.json"
    authz_report = report_dir / "03-authorization.json"
    authz_command = [str(authz_migrator), "--mode", args.command]
    return [
        Step(
            "bkn-data",
            (
                sys.executable,
                str(SCRIPT_DIRECTORY / "bkn_data.py"),
                "--mode",
                args.command,
            ),
            bkn_report,
        ),
        Step(
            "vega-data",
            (
                sys.executable,
                str(SCRIPT_DIRECTORY / "vega" / "vega_data.py"),
                "--mode",
                args.command,
            ),
            vega_report,
        ),
        Step("authorization", tuple(authz_command), authz_report),
    ]


def require_stopped_workloads(args: argparse.Namespace) -> None:
    """Require persisted and live evidence that workloads are stopped."""
    state = Path(args.state_file)
    if not state.is_file() or state.stat().st_size == 0:
        raise OrchestrationError(
            "apply requires a workload replica snapshot; run the unified stop command first"
        )
    command = [
        str(SCRIPT_DIRECTORY / "service_control.sh"),
        "verify-stopped",
        "--namespace",
        args.namespace,
        "--state-file",
        args.state_file,
    ]
    if args.expected_context:
        command.extend(["--expected-context", args.expected_context])
    result = subprocess.run(command, check=False)
    if result.returncode != 0:
        raise OrchestrationError("migration workloads are not fully stopped")


def run_step(step: Step) -> None:
    """Run one step and preserve its machine-readable standard output."""
    with step.report_path.open("w", encoding="utf-8") as output:
        result = subprocess.run(step.command, stdout=output, check=False)
    if result.returncode != 0:
        raise OrchestrationError(
            f"migration step {step.name} failed with exit code {result.returncode}"
        )
    if not step.report_path.is_file() or step.report_path.stat().st_size == 0:
        raise OrchestrationError(f"migration step {step.name} produced no report")


def write_summary(
    report_dir: Path,
    mode: str,
    steps: Sequence[Step],
    installed_version: str,
    error: str = "",
) -> None:
    """Write a release-level report that links every module result."""
    summary = {
        "target_version": "0.1.5",
        "installed_version": installed_version,
        "migration": "permission_model_transition",
        "mode": mode,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "steps": [
            {
                "name": step.name,
                "report": str(step.report_path),
                "completed": step.report_path.is_file()
                and step.report_path.stat().st_size > 0,
            }
            for step in steps
        ],
    }
    if error:
        summary["error"] = error
    (report_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def run_migration(args: argparse.Namespace) -> int:
    """Run BKN first and authorization second, stopping on the first failure."""
    installed_version = installed_target_version(
        args.namespace, args.expected_context
    )
    if args.command == "apply":
        require_stopped_workloads(args)
    report_dir = prepare_report_directory(args.report_dir)
    steps = build_steps(args, report_dir)
    try:
        for step in steps:
            run_step(step)
    except Exception as exc:
        write_summary(report_dir, args.command, steps, installed_version, str(exc))
        raise
    write_summary(report_dir, args.command, steps, installed_version)
    return 0


def automatic_run_directory() -> Path:
    """Allocate persistent evidence paths for the one-command workflow."""
    root = Path(os.getenv("OPENBKN_MIGRATION_WORKDIR", DEFAULT_RUN_ROOT))
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    for suffix in range(1000):
        name = timestamp if suffix == 0 else f"{timestamp}-{suffix:02d}"
        candidate = root / f"permission-model-transition-{name}"
        try:
            candidate.mkdir(parents=True)
            return candidate
        except FileExistsError:
            continue
        except OSError as exc:
            raise OrchestrationError(
                f"cannot create migration work directory {candidate}: {exc}"
            ) from exc
    raise OrchestrationError(f"cannot allocate a migration work directory under {root}")


def run_upgrade(args: argparse.Namespace) -> int:
    """Run the post-install transition without operator-supplied data inputs."""
    # Do not mutate data or workloads unless the target release is already
    # installed. This package is intentionally a one-time post-install step.
    namespace = discover_target_namespace(args.namespace, args.expected_context)
    args.namespace = namespace
    installed_target_version(namespace, args.expected_context)
    discovered_environment = discover_database_environment(
        namespace, args.expected_context
    )
    with database_access_environment(
        discovered_environment, args.expected_context
    ) as effective_environment:
        install_database_environment(effective_environment)
        run_dir = automatic_run_directory()
        state_file = run_dir / "workloads.tsv"
        base = {
            "namespace": args.namespace,
            "expected_context": args.expected_context,
            "authz_migrator": args.authz_migrator,
            "state_file": str(state_file),
        }
        dry_run = argparse.Namespace(
            **base, command="dry-run", report_dir=str(run_dir / "dry-run")
        )
        run_migration(dry_run)

        control = argparse.Namespace(
            **base, command="stop", timeout_seconds=300
        )
        if control_services(control) != 0:
            raise OrchestrationError("could not stop migration workloads")

        apply = argparse.Namespace(
            **base, command="apply", report_dir=str(run_dir / "apply")
        )
        run_migration(apply)

        restore = argparse.Namespace(
            **base, command="start", timeout_seconds=300
        )
        if control_services(restore) != 0:
            raise OrchestrationError(
                "migration completed but workloads could not be restored; "
                f"restore them with: {SCRIPT_DIRECTORY / 'migrate.py'} start "
                f"--state-file {state_file}"
            )

        print(
            f"Post-install permission transition for {TARGET_VERSION} completed: "
            f"{run_dir}"
        )
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Run the selected release migration action."""
    args = build_parser().parse_args(argv)
    try:
        if args.command in {"stop", "start"}:
            return control_services(args)
        if args.command == "upgrade":
            return run_upgrade(args)
        return run_migration(args)
    except KeyboardInterrupt:
        print("Migration interrupted; keep all workloads stopped.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"Migration failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
