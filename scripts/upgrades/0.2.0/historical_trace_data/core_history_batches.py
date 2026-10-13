# Copyright (c) 2026 OpenBKN
# SPDX-License-Identifier: LicenseRef-OpenBKN
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt.
"""Offline batches retaining original Core identities and every import category."""
from collections import defaultdict
from snapshot import canonical

HISTORY_CORE_FIELDS = (
    "conversations",
    "interactions",
    "operations",
    "receipts",
    "call_facts",
    "idempotency_records",
    "assembly_revisions",
)


def _identity(key, row):
    if key == "call_facts":
        return canonical([row["operation_id"], row["attempt"]])
    if key == "idempotency_records":
        return canonical(
            [
                row["scope"],
                row["owner"],
                row["external_conversation_key"],
                row["idempotency_key"],
            ]
        )
    field = {
        "conversations": "conversation_id",
        "interactions": "interaction_id",
        "operations": "operation_id",
        "receipts": "receipt_id",
        "assembly_revisions": "revision_id",
    }[key]
    return row[field]


def core_history_import_batches(plan, max_bytes=64 << 20, max_interactions=100):
    """Yield bounded UTF-8 JSON with complete per-interaction dependencies.

    Standalone conversations and interactions are retained. Conversation-scoped
    idempotency rows accompany their original conversation and may repeat between
    batches, just as native idempotent readback allows. A single interaction that
    cannot fit fails explicitly instead of silently dropping any of its facts.
    """
    if max_bytes <= 0 or max_interactions <= 0:
        raise ValueError("invalid historical batch limit")
    if set(plan) - set(HISTORY_CORE_FIELDS):
        raise ValueError("unsupported historical core category")
    indexed = {}
    for key in HISTORY_CORE_FIELDS:
        indexed[key] = {}
        for row in plan.get(key, []):
            identity = _identity(key, row)
            if not identity or identity in indexed[key]:
                raise ValueError("duplicate historical core identity:" + key)
            indexed[key][identity] = row
    convs = indexed["conversations"]
    ints = indexed["interactions"]
    ops = indexed["operations"]
    receipts = indexed["receipts"]
    grouped = {key: defaultdict(list) for key in HISTORY_CORE_FIELDS[2:]}
    conv_idempotency = defaultdict(list)
    for row in ints.values():
        if row["conversation_id"] not in convs:
            raise ValueError("historical interaction conversation missing")
    for key in ("operations", "receipts", "call_facts", "assembly_revisions"):
        for row in indexed[key].values():
            iid = row["interaction_id"]
            if iid not in ints:
                raise ValueError("historical row interaction missing:" + key)
            if (
                key != "assembly_revisions"
                and row["conversation_id"] != ints[iid]["conversation_id"]
            ):
                raise ValueError("historical row conversation mismatch:" + key)
            if key == "receipts":
                op = ops.get(row["operation_id"])
                if op is None or op["interaction_id"] != iid:
                    raise ValueError("historical receipt operation missing")
            if key == "call_facts":
                receipt = receipts.get(row["receipt_id"])
                if (
                    receipt is None
                    or receipt["operation_id"] != row["operation_id"]
                    or receipt["attempt"] != row["attempt"]
                    or receipt["interaction_id"] != iid
                ):
                    raise ValueError("historical call receipt mismatch")
            grouped[key][iid].append(row)
    for row in indexed["idempotency_records"].values():
        if row["resource_type"] == "interaction":
            if row["resource_id"] not in ints:
                raise ValueError("historical idempotency interaction missing")
            grouped["idempotency_records"][row["resource_id"]].append(row)
        else:
            if row["resource_id"] not in convs:
                raise ValueError("historical idempotency conversation missing")
            conv_idempotency[row["resource_id"]].append(row)
    def units():
        seen_convs = set()
        for iid, interaction in ints.items():
            cid = interaction["conversation_id"]
            seen_convs.add(cid)
            unit = {key: [] for key in HISTORY_CORE_FIELDS}
            unit.update(conversations=[convs[cid]], interactions=[interaction])
            for key in HISTORY_CORE_FIELDS[2:]:
                unit[key] = grouped[key][iid]
            unit["idempotency_records"] = (
                unit["idempotency_records"] + conv_idempotency[cid]
            )
            yield unit
        for cid, conv in convs.items():
            if cid not in seen_convs:
                unit = {key: [] for key in HISTORY_CORE_FIELDS}
                unit.update(conversations=[conv], idempotency_records=conv_idempotency[cid])
                yield unit
    def encoded_size(unit):
        return len(canonical({key: unit[key] for key in HISTORY_CORE_FIELDS}).encode())

    def split_oversized_interaction(unit):
        """Split one large Interaction on Operation boundaries only.

        Conversation/Interaction identity and their idempotency/revision context
        repeat in every fragment.  An Operation, its Receipt(s), and Call Fact(s)
        stay together, so a native Core request never observes a partial call.
        """
        if encoded_size(unit) <= max_bytes or not unit['interactions']:
            yield unit
            return
        if len(unit['interactions']) != 1:
            raise ValueError('historical oversized core interaction grouping invalid')
        by_operation = defaultdict(lambda: {key: [] for key in HISTORY_CORE_FIELDS})
        for row in unit['operations']:
            by_operation[row['operation_id']]['operations'].append(row)
        for key in ('receipts', 'call_facts'):
            for row in unit[key]:
                by_operation[row['operation_id']][key].append(row)
        if not by_operation:
            raise ValueError('historical core interaction exceeds batch limit')
        shared = {key: list(unit[key]) for key in HISTORY_CORE_FIELDS}
        shared['operations'], shared['receipts'], shared['call_facts'] = [], [], []
        fragment = {key: list(shared[key]) for key in HISTORY_CORE_FIELDS}
        for operation_id in sorted(by_operation):
            candidate = {key: list(fragment[key]) for key in HISTORY_CORE_FIELDS}
            for key in ('operations', 'receipts', 'call_facts'):
                candidate[key].extend(by_operation[operation_id][key])
            if encoded_size(candidate) > max_bytes:
                if fragment['operations']:
                    yield fragment
                    fragment = {key: list(shared[key]) for key in HISTORY_CORE_FIELDS}
                    for key in ('operations', 'receipts', 'call_facts'):
                        fragment[key].extend(by_operation[operation_id][key])
                    if encoded_size(fragment) > max_bytes:
                        raise ValueError('historical core operation exceeds batch limit')
                else:
                    raise ValueError('historical core operation exceeds batch limit')
            else:
                fragment = candidate
        if fragment['operations']:
            yield fragment

    # The caller stages and validates every batch on disk before importing.
    batch = {key: {} for key in HISTORY_CORE_FIELDS}
    overhead = len(canonical({key: [] for key in HISTORY_CORE_FIELDS}).encode())
    size = overhead
    for raw_unit in units():
        for unit in split_oversized_interaction(raw_unit):
            additions = {
                key: {
                    _identity(key, row): row
                    for row in unit[key]
                    if _identity(key, row) not in batch[key]
                }
                for key in HISTORY_CORE_FIELDS
            }
            added_size = sum(
                len(canonical(row).encode()) + (bool(batch[key]) or position > 0)
                for key in HISTORY_CORE_FIELDS
                for position, row in enumerate(additions[key].values())
            )
            if any(batch.values()) and (
                size + added_size > max_bytes
                or len(batch["interactions"]) + len(additions["interactions"])
                > max_interactions
            ):
                yield (
                    canonical(
                        {key: list(batch[key].values()) for key in HISTORY_CORE_FIELDS}
                    ).encode()
                )
                batch = {key: {} for key in HISTORY_CORE_FIELDS}
                size = overhead
                additions = {
                    key: {_identity(key, row): row for row in unit[key]}
                    for key in HISTORY_CORE_FIELDS
                }
                added_size = sum(
                    len(canonical(row).encode()) + (position > 0)
                    for key in HISTORY_CORE_FIELDS
                    for position, row in enumerate(additions[key].values())
                )
            if size + added_size > max_bytes:
                raise ValueError("historical core interaction exceeds batch limit")
            for key in HISTORY_CORE_FIELDS:
                batch[key].update(additions[key])
            size += added_size
    if any(batch.values()):
        yield (
            canonical(
                {key: list(batch[key].values()) for key in HISTORY_CORE_FIELDS}
            ).encode()
        )
