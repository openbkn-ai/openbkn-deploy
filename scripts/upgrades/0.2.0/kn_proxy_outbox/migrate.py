#!/usr/bin/env python3
# Copyright openbkn.ai
#
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.

"""Operator entry for the OpenBKN 0.2.0 BKN proxy outbox transition."""

from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import kn_proxy_outbox


DEFAULT_NAMESPACE = "openbkn"
DEFAULT_STATE_FILE = "/tmp/openbkn-kn-proxy-outbox-workload.tsv"
DEFAULT_TIMEOUT_SECONDS = 300
DEFAULT_REPORT_DIRECTORY = (
    Path.home() / ".openbkn-ai" / "migrations" / "0.2.0" / "kn_proxy_outbox"
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate or initialize the stopped BKN proxy outbox upgrade."
    )
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("dry-run", "verify"):
        command = commands.add_parser(name)
        command.add_argument("--report", required=True)
    commands.add_parser("apply")
    for name in ("stop", "verify-stopped", "start"):
        control = commands.add_parser(name)
        control.add_argument("--namespace", default=DEFAULT_NAMESPACE)
        control.add_argument("--expected-context", default="")
        control.add_argument("--state-file", default=DEFAULT_STATE_FILE)
        control.add_argument(
            "--timeout-seconds", type=int, default=DEFAULT_TIMEOUT_SECONDS
        )
    return parser


def control_services(args: argparse.Namespace) -> int:
    command = [
        str(Path(__file__).resolve().parent / "service_control.sh"),
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


def standard_control_args(command: str) -> argparse.Namespace:
    return argparse.Namespace(
        command=command,
        namespace=DEFAULT_NAMESPACE,
        expected_context="",
        state_file=DEFAULT_STATE_FILE,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


def verify_bkn_stopped() -> None:
    """Verify the standard stopped-upgrade state without operator-supplied flags."""
    if control_services(standard_control_args("verify-stopped")) != 0:
        raise kn_proxy_outbox.MigrationError(
            "BKN did not remain in the recorded stopped state"
        )


def stop_bkn() -> None:
    """Stop BKN or resume from an already recorded stopped state."""
    if Path(DEFAULT_STATE_FILE).exists():
        return
    if control_services(standard_control_args("stop")) != 0:
        raise kn_proxy_outbox.MigrationError("failed to stop BKN workloads")


def start_bkn() -> None:
    """Restore the replica count recorded by the stop step."""
    if control_services(standard_control_args("start")) != 0:
        raise kn_proxy_outbox.MigrationError(
            "migration committed, but BKN workloads could not be restarted"
        )


def write_report(path: str, report: dict[str, object]) -> None:
    target = Path(path).resolve()
    if target.exists():
        raise kn_proxy_outbox.MigrationError(f"refusing to overwrite report: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def prepare_report_destination(path: str) -> None:
    """Fail before database work when the report destination is not writable."""
    target = Path(path).resolve()
    if target.exists():
        raise kn_proxy_outbox.MigrationError(f"refusing to overwrite report: {target}")
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=target.parent):
            pass
    except OSError as exc:
        raise kn_proxy_outbox.MigrationError(
            f"report directory is not writable: {target.parent}: {exc}"
        ) from exc


def default_report_path(command: str, now: Optional[datetime] = None) -> str:
    moment = now or datetime.now(timezone.utc)
    timestamp = moment.strftime("%Y%m%dT%H%M%S%fZ")
    return str(DEFAULT_REPORT_DIRECTORY / f"proxy-outbox-{command}-{timestamp}.json")


def run(args: argparse.Namespace) -> dict[str, object]:
    connection = kn_proxy_outbox.connect(kn_proxy_outbox.DBConfig.from_environment())
    try:
        with connection.cursor() as cursor:
            report: dict[str, object] = {
                "command": args.command,
                "precheck": kn_proxy_outbox.precheck(cursor, int(time.time() * 1000)),
            }
            if args.command == "apply":
                report["initialize"] = kn_proxy_outbox.initialize(cursor)
                report["verify"] = kn_proxy_outbox.verify(cursor)
                connection.commit()
            elif args.command == "verify":
                report["verify"] = kn_proxy_outbox.verify(cursor)
                connection.rollback()
            else:
                connection.rollback()
            return report
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def main() -> int:
    args = build_parser().parse_args()
    if args.command in {"stop", "verify-stopped", "start"}:
        return control_services(args)
    report_path = getattr(args, "report", "") or default_report_path(args.command)
    try:
        prepare_report_destination(report_path)
        if args.command == "apply":
            preflight = run(argparse.Namespace(command="dry-run"))
            stop_bkn()
            verify_bkn_stopped()
        report = run(args)
        if args.command == "apply":
            report["preflight"] = preflight
    except kn_proxy_outbox.MigrationError as exc:
        print(f"migration refused: {exc}")
        return 1
    report_write_failed = False
    try:
        write_report(report_path, report)
        print(f"migration report: {Path(report_path).resolve()}")
    except (kn_proxy_outbox.MigrationError, OSError) as exc:
        if args.command == "apply":
            print(f"migration completed, but the report could not be written: {exc}")
            report_write_failed = True
        else:
            print(f"migration report failed: {exc}")
            return 1
    if args.command == "apply":
        try:
            start_bkn()
        except kn_proxy_outbox.MigrationError as exc:
            print(f"migration completed: {exc}")
            return 1
        if report_write_failed:
            return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
