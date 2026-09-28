#!/usr/bin/env python3
# Copyright openbkn.ai
#
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.

"""Operator entry for the OpenBKN 0.2.0 BKN proxy outbox transition."""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path

import kn_proxy_outbox


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate or initialize the stopped BKN proxy outbox upgrade."
    )
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("dry-run", "verify"):
        command = commands.add_parser(name)
        command.add_argument("--report", required=True)
    apply = commands.add_parser("apply")
    apply.add_argument("--report", required=True)
    apply.add_argument(
        "--confirm-bkn-stopped",
        action="store_true",
        help="confirm BKN workloads are stopped and requests are drained",
    )
    for name in ("stop", "verify-stopped", "start"):
        control = commands.add_parser(name)
        control.add_argument("--namespace", default="openbkn")
        control.add_argument("--expected-context", default="")
        control.add_argument(
            "--state-file", default="/tmp/openbkn-kn-proxy-outbox-workload.tsv"
        )
        control.add_argument("--timeout-seconds", type=int, default=300)
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


def write_report(path: str, report: dict[str, object]) -> None:
    target = Path(path).resolve()
    if target.exists():
        raise kn_proxy_outbox.MigrationError(f"refusing to overwrite report: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, object]:
    if args.command == "apply" and not args.confirm_bkn_stopped:
        raise kn_proxy_outbox.MigrationError(
            "apply requires --confirm-bkn-stopped after deployment workloads are stopped"
        )
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
    try:
        report = run(args)
        write_report(args.report, report)
    except kn_proxy_outbox.MigrationError as exc:
        print(f"migration refused: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
