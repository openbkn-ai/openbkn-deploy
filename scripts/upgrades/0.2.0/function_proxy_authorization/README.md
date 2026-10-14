# OpenBKN 0.2.0 Function proxy authorization repair

This offline migration repairs historical knowledge-network proxy snapshots
that classified Function boxes as `tool_box/<box-id>:execute`. It scans the
platform but repairs and commits one knowledge network at a time. Only boxes
whose authoritative `t_toolbox.f_metadata_type` is `function` are converted.

The migration updates both BKN snapshots and mapping versions as well as the
bkn-safe source ledger, policies, grants, and managed-proxy generation. It is
dry-run first, idempotent, and refuses inconsistent snapshots, active outbox
work, active publication locks, or an unauthorized grantor. Existing
prerequisite-derived sources are preserved, and a Safe-first partial commit can
be resumed safely.

See [README.zh.md](README.zh.md) for configuration, execution, recovery, and
verification instructions.
