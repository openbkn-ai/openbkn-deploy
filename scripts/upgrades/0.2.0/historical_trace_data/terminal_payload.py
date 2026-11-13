# Copyright (c) 2026 OpenBKN
# SPDX-License-Identifier: LicenseRef-OpenBKN
"""Restore storage-only completion metadata; never edit business inline JSON.

Mirrors Core sessionstore/terminal_payload_codec.go and
sessionvo/evidence_completion.go. Strict native plan validation remains required.
"""
import copy
import re

KEY = '_trace_evidence_completion'


def _text(value, key):
    item = value.get(key, '')
    if item is None:
        return ''  # Go's stored string fields decode JSON null as their zero value.
    if not isinstance(item, str):
        raise ValueError('terminal_completion_invalid:string:' + key)
    return item


def _expectation_valid(value):
    if not isinstance(value, dict) or type(value.get('version')) is not int or value['version'] != 1 or value.get('closed') is not True:
        return False
    events = value.get('events')
    if events is None:
        events = []
    if not isinstance(events, list) or len(events) > 128:
        return False
    ids = set()
    for event in events:
        if not isinstance(event, dict):
            return False
        eid, kind, producer, reason, sha, disposition = (
            _text(event, key) for key in ('event_id', 'event_type', 'producer_id',
                                         'drop_reason', 'payload_hash', 'publish_disposition'))
        if not eid.strip() or not kind.strip() or eid in ids or any(
                len(text.encode('utf-8')) > limit for text, limit in
                [(eid, 192), (kind, 192), (producer, 128), (reason, 128)]):
            return False
        ids.add(eid)
        if disposition == 'accepted':
            if not re.fullmatch('[0-9a-f]{64}', sha) or not producer.strip() or reason:
                return False
        elif disposition == 'dropped':
            if not reason.strip() or len(sha.encode('utf-8')) > 64:
                return False
        else:
            return False
    return True


def _validate_completion(value):
    if not isinstance(value, dict) or value.get('original_durability') not in ('pending', 'durable', 'failed'):
        raise ValueError('terminal_completion_invalid:original_durability')
    for key in ('original_observed_refs', 'original_partial_reasons'):
        items = value.get(key)
        if items is not None and (not isinstance(items, list) or any(item is not None and not isinstance(item, str) for item in items)):
            raise ValueError('terminal_completion_invalid:' + key)
    reason, sha = _text(value, 'rejection_reason'), _text(value, 'rejected_expectation_hash')
    if reason:
        if not re.fullmatch('[0-9a-fA-F]{64}', sha) or value.get('expectation') is not None:
            raise ValueError('terminal_completion_invalid:rejected_expectation')
    elif not _expectation_valid(value.get('expectation')):
        raise ValueError('terminal_completion_invalid:evidence_expectation')


def restore_completion(call):
    """Mutate only the copied native CallFact, not source records or hashes."""
    for field in ('output', 'error'):
        payload = call.get(field)
        if not isinstance(payload, dict) or KEY not in payload:
            continue
        metadata = payload[KEY]
        _validate_completion(metadata)
        if 'evidence_completion' in call:
            raise ValueError('terminal_completion_conflict:output_and_error')
        call['evidence_completion'] = copy.deepcopy(metadata)
        del payload[KEY]
