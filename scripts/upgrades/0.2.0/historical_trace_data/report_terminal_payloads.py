#!/usr/bin/env python3
"""Offline private inventory of completion wrappers in a frozen source snapshot.

No database, Pod, import or model invocation. Does not establish live data loss.
"""
import argparse
import hashlib
from pathlib import Path

from core_history import _json
from snapshot import canonical, private_write, read_snapshot, strict_loads
from terminal_payload import KEY, restore_completion


def inspect(records):
    entries = []
    counts = {'call_facts': 0, 'wrapped': 0, 'invalid': 0}
    for ordinal, record in enumerate(records):
        if record.get('locator', '').split('.')[-1] != 'bkn_trace_operation_call_facts':
            continue
        counts['call_facts'] += 1
        row = record['row']
        fields, reason = [], ''
        try:
            call = {field: _json(row.get(field + '_payload'), loads=strict_loads)
                    for field in ('output', 'error')}
            fields = [field for field, value in call.items() if isinstance(value, dict) and KEY in value]
            restore_completion(call)
        except (ValueError, TypeError, UnicodeError) as error:
            detail = str(error)
            reason = detail if detail.startswith(('terminal_completion_invalid:', 'terminal_completion_conflict:')) else 'terminal_payload_json_invalid'
        if fields or reason:
            counts['wrapped'] += bool(fields)
            counts['invalid'] += bool(reason)
            entries.append({'source_ordinal': ordinal, 'operation_id': row.get('operation_id'),
                            'attempt': row.get('attempt_no'), 'interaction_id': row.get('interaction_id'),
                            'fields': fields, 'reason': reason or 'completion_wrapper_requires_restore'})
    return {'counts': counts, 'entries': entries,
            'live_presence_verified': False, 'native_plan_verified': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError('report output already exists')
    report = inspect(read_snapshot(args.source))
    manifest = strict_loads((args.source / 'snapshot.json').read_text())
    report['records_sha256'] = manifest['records_sha256']
    report['converter_sha256'] = {name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
                                  for name in ('core_history.py', 'terminal_payload.py')}
    private_write(args.output, (canonical(report) + '\n').encode())
    print(canonical(report['counts']))  # IDs stay in the private report.


if __name__ == '__main__':
    main()
