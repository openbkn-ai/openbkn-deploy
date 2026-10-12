"""Bounded loop batches over frozen JSONL; no database or resumable scheduler.

Small reference/offset files retain complete candidates without retaining source
bodies. The files are disposable and rebuilt for each run. Each dependency group
is indivisible; independent groups are packed to the configured count/byte caps.
"""
import hashlib
import os
import struct
from pathlib import Path

from snapshot import bounded_lines, canonical, strict_loads

ENTRY = struct.Struct('!IQQ')
BUCKETS = 64


def limits():
    count = int(os.environ.get('BKN_HISTORY_BATCH_RECORDS', '5000'))
    size = int(os.environ.get('BKN_HISTORY_BATCH_BYTES', str(32 << 20)))
    if not 1 <= count <= 10000 or not 0 < size <= 64 << 20:
        raise ValueError('invalid_history_batch_limits')
    return count, size


def references(record, deployment):
    kind, row = record['kind'], record['row']
    if kind == 'audit':
        from logs import convert_log
        converted = convert_log(record['source_id'], row, deployment['environment'], deployment['instance'])
        return {'audit:' + converted['event']['event_id']} if converted['disposition'] == 'convert' else set()
    if kind == 'agent_definition':
        return set()
    keys = set()
    fields = {'conversation_id': 'conv', 'interaction_id': 'int', 'operation_id': 'op',
              'parent_operation_id': 'op', 'receipt_id': 'receipt', 'trace_id': 'trace',
              'request_id': 'request', 'bkn.request.id': 'request', 'event_id': 'event',
              'thread_id': 'thread', 'f_thread_id': 'thread', 'f_parent_thread_id': 'thread',
              'revision_id': 'revision', 'parent_revision_id': 'revision',
              'artifact_id': 'artifact', 'question_artifact_ref': 'artifact',
              'answer_artifact_ref': 'artifact', 'result_artifact_ref': 'artifact',
              'f_task_id': 'task'}
    arrays = {'included_receipt_ids': 'receipt', 'included_event_ids': 'event', 'causation_event_ids': 'event'}
    def visit(value):
        if isinstance(value, str) and value.startswith(('{', '[')):
            try:
                value = strict_loads(value)
            except ValueError:
                return
        if isinstance(value, dict):
            for key, item in value.items():
                if key in fields and isinstance(item, str) and item:
                    keys.add(fields[key] + ':' + item)
                elif key in arrays:
                    entries = strict_loads(item) if isinstance(item, str) else item
                    if isinstance(entries, list):
                        keys.update(arrays[key] + ':' + v for v in entries if isinstance(v, str) and v)
                else:
                    visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)
    visit(row)
    if record.get('locator', '').endswith('bkn_trace_evidence_event_ledger'):
        if row.get('producer_stream_id') and row.get('producer_epoch') and row.get('producer_sequence'):
            keys.add('ledger-stream:' + canonical([row['producer_stream_id'], row['producer_epoch'], row['producer_sequence']]))
    if record.get('locator', '').endswith('bkn_trace_idempotency_records'):
        keys.add('idempotency:' + canonical([row.get('scope'),
            [row.get(k) or '' for k in ('application_principal_id', 'effective_subject_type', 'effective_subject_id', 'delegation_id')],
            row.get('external_conversation_key'), row.get('idempotency_key')]))
    if row.get('resource_type') in ('conversation', 'interaction') and row.get('resource_id'):
        keys.add(('conv:' if row['resource_type'] == 'conversation' else 'int:') + row['resource_id'])
    if row.get('external_conversation_key'):
        keys.add('external:' + row['external_conversation_key'])
    if kind == 'agent_thread':
        tid = row['f_thread_id']
        keys.update({'external:' + tid, 'external:mcp-host:' + hashlib.sha256(tid.encode()).hexdigest()})
    if kind == 'agent_task':
        from agent_history import extract_business_inputs
        from business_snapshot_links import _operation_refs
        for body in extract_business_inputs([record]):
            keys.update('op:' + ref for ref in _operation_refs(body))
    if kind == 'agent_blob':
        from agent_history import decode_messages
        visit(decode_messages(row['blob_hex']))
    return keys


class LoopSource:
    def __init__(self, root, snapshots, deployment):
        self.root = Path(root)
        self.root.mkdir(mode=0o700)
        self.snapshots, self.deployment = snapshots, deployment
        self.count_limit, self.byte_limit = limits()
        self.count = 0
        self.files = []

    def __enter__(self):
        try:
            self.catalog = (self.root / 'offsets.bin').open('w+b')
            self.done = (self.root / 'visited.bin').open('w+b')
            self.files.extend([self.catalog, self.done])
            buckets = [(self.root / ('refs-%02d.jsonl' % n)).open('wb') for n in range(BUCKETS)]
            self.files.extend(buckets)
            definitions = (self.root / 'definitions.jsonl').open('wb')
            self.files.append(definitions)
            actors = (self.root / 'actors.jsonl').open('wb')
            self.files.append(actors)
            for slot, snapshot in enumerate(self.snapshots):
                # Validate fully before using source offsets.
                from snapshot import read_snapshot
                read_snapshot(snapshot)
                with (Path(snapshot) / 'records.jsonl').open('rb') as stream:
                    offset = 0
                    for line in bounded_lines(stream):
                        record = strict_loads(line)
                        self.catalog.write(ENTRY.pack(slot, offset, len(line)))
                        offset += len(line)
                        ordinal = self.count
                        self.count += 1
                        if record['kind'] == 'agent_definition':
                            definitions.write((canonical([record['row']['f_agent_id'], ordinal]) + '\n').encode())
                        if record['kind'] == 'audit':
                            row = record['row']
                            actor = row.get('actor_id') or row.get('user_id') or row.get('operator_id')
                            name = row.get('actor_name_snapshot') or row.get('actor_name') or row.get('user_name') or row.get('operator_name')
                            if actor and name:
                                actors.write((canonical([str(actor), str(name)]) + '\n').encode())
                        for key in references(record, self.deployment):
                            bucket = int(hashlib.sha256(key.encode()).hexdigest()[:8], 16) % BUCKETS
                            buckets[bucket].write((canonical([key, ordinal]) + '\n').encode())
                        observer = getattr(self, 'observer', None)
                        if observer and self.count % 1000 == 0:
                            observer.update(indexed=self.count)
            for stream in buckets + [definitions, actors]:
                stream.close()
                self.files.remove(stream)
            self.catalog.flush()
            self.done.truncate(self.count)
            self.done.flush()
            return self
        except BaseException:
            self.close()
            raise

    def __exit__(self, *args):
        self.close()

    def close(self):
        for stream in self.files:
            stream.close()
        self.files.clear()

    def record(self, ordinal):
        self.catalog.seek(ordinal * ENTRY.size)
        slot, offset, length = ENTRY.unpack(self.catalog.read(ENTRY.size))
        with (Path(self.snapshots[slot]) / 'records.jsonl').open('rb') as stream:
            stream.seek(offset)
            return strict_loads(stream.read(length)), length

    def matches(self, keys):
        by_bucket = {}
        for key in keys:
            bucket = int(hashlib.sha256(key.encode()).hexdigest()[:8], 16) % BUCKETS
            by_bucket.setdefault(bucket, set()).add(key)
        for bucket, wanted in by_bucket.items():
            with (self.root / ('refs-%02d.jsonl' % bucket)).open('rb') as stream:
                for line in bounded_lines(stream):
                    key, ordinal = strict_loads(line)
                    if key in wanted:
                        yield ordinal

    def group(self, seeds):
        records, pending, searched, size = {}, set(seeds), set(), 0
        while pending:
            new_keys = set()
            for ordinal in pending:
                if ordinal in records:
                    continue
                record, length = self.record(ordinal)
                records[ordinal] = record
                size += length
                if len(records) > self.count_limit or size > self.byte_limit:
                    raise ValueError('dependency_group_exceeds_batch_limit')
                new_keys.update(references(record, self.deployment))
            new_keys.difference_update(searched)
            searched.update(new_keys)
            # References themselves are bounded by source bytes; candidate rows
            # are consumed incrementally, never collected as an unbounded list.
            pending = set()
            for ordinal in self.matches(new_keys):
                if ordinal not in records:
                    pending.add(ordinal)
                    if len(records) + len(pending) > self.count_limit:
                        raise ValueError('dependency_group_exceeds_batch_limit')
        # Definitions are lookup-only: a shared Agent must not merge all threads.
        agents = {r['row'].get('f_agent_id') for r in records.values() if r['kind'].startswith('agent_')}
        with (self.root / 'definitions.jsonl').open('rb') as stream:
            for line in bounded_lines(stream):
                agent, ordinal = strict_loads(line)
                if agent in agents and ordinal not in records:
                    record, length = self.record(ordinal)
                    size += length
                    records[ordinal] = record
                    if len(records) > self.count_limit or size > self.byte_limit:
                        raise ValueError('dependency_group_exceeds_batch_limit')
        return records, size

    def actor_names(self, records):
        wanted = {r['row'].get('f_account_id') for r in records if r['kind'].startswith('agent_')}
        names = {}
        with (self.root / 'actors.jsonl').open('rb') as stream:
            for line in bounded_lines(stream):
                actor, name = strict_loads(line)
                if actor in wanted:
                    names[actor] = name
        return names

    def batches(self):
        cursor = 0
        while cursor < self.count:
            seeds, size = [], 0
            for seed in range(cursor, self.count):
                self.done.seek(seed)
                if self.done.read(1) == b'1':
                    continue
                record, length = self.record(seed)
                if record['kind'] == 'agent_definition':
                    continue
                if seeds and (len(seeds) == self.count_limit or size + length > self.byte_limit):
                    break
                seeds.append(seed)
                size += length
            if not seeds:
                return
            # Leave room for dependencies when they cross the proposed boundary.
            # Retry a smaller seed window; never split an indivisible group.
            while True:
                try:
                    group, _ = self.group(seeds)
                    break
                except ValueError as error:
                    if str(error) != 'dependency_group_exceeds_batch_limit' or len(seeds) == 1:
                        raise
                    seeds = seeds[:max(1, len(seeds) // 2)]
            for ordinal, record in group.items():
                if record['kind'] != 'agent_definition':
                    self.done.seek(ordinal)
                    self.done.write(b'1')
            yield [group[ordinal] for ordinal in sorted(group)]
            group.clear()
            cursor = seeds[-1] + 1
