#!/usr/bin/env python3
# Copyright openbkn.ai
#
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.

"""Single operator entry for the OpenBKN 0.1.5 permission transition."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Sequence


SCRIPT_DIRECTORY = Path(__file__).resolve().parent
DEFAULT_AUTHZ_MIGRATOR = str(
    SCRIPT_DIRECTORY / "authz_migrate" / "authz-migrate"
)
DEFAULT_STATE_FILE = "/tmp/openbkn-permission-model-transition-workloads.tsv"
DEFAULT_RUN_ROOT = "/var/lib/openbkn/migrations"
TARGET_VERSION = "0.1.5"
SOURCE_VERSION = "0.1.4"


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
        help="run the complete 0.1.4 to 0.1.5 permission transition",
    )
    upgrade.add_argument("--namespace", default="openbkn", help=argparse.SUPPRESS)
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


def installed_source_version(namespace: str) -> str:
    """Read the installed bkn-safe chart version instead of trusting user input."""
    try:
        result = subprocess.run(
            ["helm", "list", "--namespace", namespace, "--output", "json"],
            check=False,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        raise OrchestrationError("helm is required to identify the installed release") from exc
    if result.returncode != 0:
        detail = result.stderr.strip() or "helm list failed"
        raise OrchestrationError(f"cannot identify installed OpenBKN release: {detail}")
    try:
        releases = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise OrchestrationError("helm returned an invalid release inventory") from exc
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
    if version != SOURCE_VERSION:
        raise OrchestrationError(
            f"unsupported installed version {version!r}; this package only migrates "
            f"{SOURCE_VERSION} to {TARGET_VERSION}"
        )
    return version


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
            "run authz_migrate/build.sh first"
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
    source_version: str,
    error: str = "",
) -> None:
    """Write a release-level report that links every module result."""
    summary = {
        "target_version": "0.1.5",
        "source_version": source_version,
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
    source_version = installed_source_version(args.namespace)
    if args.command == "apply":
        require_stopped_workloads(args)
    report_dir = prepare_report_directory(args.report_dir)
    steps = build_steps(args, report_dir)
    try:
        for step in steps:
            run_step(step)
    except Exception as exc:
        write_summary(report_dir, args.command, steps, source_version, str(exc))
        raise
    write_summary(report_dir, args.command, steps, source_version)
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
    """Run the release-owned transition without operator-supplied data inputs."""
    # Fail before allocating a state directory or scaling anything when this is
    # not the source release reviewed by this one-time package.
    installed_source_version(args.namespace)
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

    print(
        f"Permission transition to {TARGET_VERSION} applied: {run_dir}\n"
        f"Workloads remain stopped. Deploy the {TARGET_VERSION} release, then restore "
        f"them with: {SCRIPT_DIRECTORY / 'migrate.py'} start --state-file {state_file}"
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
