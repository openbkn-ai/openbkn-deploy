import copy
from datetime import datetime, timezone
import io
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from loop_source import LoopSource, limits
from snapshot import save_snapshot, read_snapshot, SQLSource, canonical
from test_core_history import fixture, producer_observation
from core_history import plan_core_history, map_outbox
from test_native_evidence import record
from native_evidence import plan_aggregates
from test_logs import row

DEPLOYMENT = {'instance': 'customer', 'environment': 'test', 'cluster_uid': 'one'}


class LoopSourceTests(unittest.TestCase):
    def source(self, root, records):
        records = copy.deepcopy(records)
        for record in records:
            record.setdefault('source_id', 'bkn-agent' if record['kind'].startswith('agent_') else 'backend')
        snapshot = root / 'source'
        # One-shot iterator: the implementation must not require replayable input.
        save_snapshot(snapshot, iter(records), 'customer', {})
        return LoopSource(root / 'lookup', [snapshot], DEPLOYMENT)

    def test_count_cap_and_no_duplicate_rows(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict('os.environ', {'BKN_HISTORY_BATCH_RECORDS': '5'}):
            records = [{'kind': 'audit', 'source_id': 'vega', 'row': row(event_id=str(n))} for n in range(23)]
            with self.source(Path(tmp), records) as source:
                batches = list(source.batches())
            self.assertEqual([len(b) for b in batches], [5, 5, 5, 5, 3])
            self.assertEqual([r for batch in batches for r in batch], records)

    def test_content_cap_ends_batch_before_record_cap(self):
        records = [{'kind': 'audit', 'source_id': 'vega', 'row': row(event_id=str(n))} for n in range(7)]
        length = len((canonical(records[0]) + '\n').encode())
        with tempfile.TemporaryDirectory() as tmp, patch.dict('os.environ', {'BKN_HISTORY_BATCH_BYTES': str(length * 2 + 20)}):
            with self.source(Path(tmp), records) as source:
                batches = list(source.batches())
            self.assertTrue(all(len(b) <= 2 for b in batches))
            self.assertEqual(sum(map(len, batches)), 7)

    def test_trace_shared_by_conversations_stays_complete(self):
        first, second = record('e1'), record('e2')
        second['row']['envelope']['event']['conversation_id'] = 'conv-two'
        unrelated = record('other', trace_id='b' * 32)
        outer = unrelated['row']['envelope']['event']
        outer['request_id'] = 'independent'
        outer['conversation_id'] = 'independent-conv'
        outer['envelope']['event']['bkn.request.id'] = 'independent'
        with tempfile.TemporaryDirectory() as tmp, patch.dict('os.environ', {'BKN_HISTORY_BATCH_RECORDS': '2'}):
            with self.source(Path(tmp), [first, unrelated, second]) as source:
                batches = list(source.batches())
        self.assertEqual(len(batches), 2)
        together = next(b for b in batches if first in b)
        self.assertIn(second, together)
        items, issues = plan_aggregates(together, datetime.now(timezone.utc))
        self.assertFalse(issues)
        self.assertEqual(items[0]['document']['accepted_event_count'], 2)

    def test_cross_round_candidate_ambiguity_is_preserved(self):
        tables = fixture()
        tables['bkn_trace_interactions'].append(dict(tables['bkn_trace_interactions'][0], interaction_id='other-round'))
        tables['bkn_trace_receipts'].append(dict(tables['bkn_trace_receipts'][0], receipt_id='other',
                                                interaction_id='other-round', request_id='other-request'))
        records = [{'kind': 'core_source', 'source_id': 'trace-core', 'locator': table, 'row': value}
                   for table, rows in tables.items() for value in rows]
        observation = producer_observation()
        records.insert(0, observation)
        with tempfile.TemporaryDirectory() as tmp:
            with self.source(Path(tmp), records) as source:
                batch = next(source.batches())
        selected = {}
        for r in batch:
            if r['kind'] == 'core_source':
                selected.setdefault(r['locator'], []).append(r['row'])
        outcome = map_outbox([observation], plan_core_history(selected)['core'])[0]
        self.assertEqual(outcome['status'], 'ambiguous')

    def test_thread_history_and_definition_stay_together(self):
        from test_agent_history import fixture as thread_fixture
        from agent_thread_history import plan_agent_thread_history
        records = thread_fixture()
        expected = plan_agent_thread_history(records, '2026-10-01T00:00:00Z')
        with tempfile.TemporaryDirectory() as tmp:
            with self.source(Path(tmp), records) as source:
                actual = plan_agent_thread_history(next(source.batches()), '2026-10-01T00:00:00Z')
        self.assertEqual(actual, expected)

    def test_oversized_dependency_is_explicit_and_not_split(self):
        records = [record('e' + str(n)) for n in range(3)]
        with tempfile.TemporaryDirectory() as tmp, patch.dict('os.environ', {'BKN_HISTORY_BATCH_RECORDS': '2'}):
            with self.source(Path(tmp), records) as source:
                with self.assertRaisesRegex(ValueError, 'dependency_group_exceeds'):
                    list(source.batches())

    def test_10000_record_override_preserves_every_record(self):
        records = [{'kind': 'audit', 'source_id': 'vega', 'row': row(event_id=str(n))} for n in range(10003)]
        with tempfile.TemporaryDirectory() as tmp, patch.dict('os.environ', {
                'BKN_HISTORY_BATCH_RECORDS': '10000', 'BKN_HISTORY_BATCH_BYTES': str(32 << 20)}):
            with self.source(Path(tmp), records) as source:
                batches = list(source.batches())
            self.assertEqual([len(batch) for batch in batches], [10000, 3])
            self.assertEqual([item for batch in batches for item in batch], records)

    def test_invalid_limits_fail(self):
        for count in ('0', '10001'):
            with patch.dict('os.environ', {'BKN_HISTORY_BATCH_RECORDS': count}):
                with self.assertRaises(ValueError):
                    limits()


class StreamSnapshotTests(unittest.TestCase):
    def test_failed_capture_is_not_published_and_can_retry(self):
        def broken():
            yield {'source_id': 'one'}
            raise RuntimeError('transport_failed')
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'source'
            with self.assertRaises(RuntimeError):
                save_snapshot(path, broken(), 'customer', {})
            self.assertFalse(path.exists())
            self.assertEqual(list(Path(tmp).iterdir()), [])
            save_snapshot(path, iter([{'source_id': 'one'}]), 'customer', {})
            self.assertEqual(list(read_snapshot(path)), [{'source_id': 'one'}])

    def test_sql_export_streams_and_checks_failure_after_rows(self):
        original = subprocess.Popen
        calls = []
        def launch(command, **kwargs):
            calls.append(command)
            return original([sys.executable, '-c', "print('first', flush=True); print('second'); raise SystemExit(7)"], **kwargs)
        with patch('snapshot.subprocess.Popen', side_effect=launch):
            source = SQLSource({'kind': 'docker', 'container': 'mariadb'})
            rows = source.query('SELECT 1')
            self.assertEqual(next(rows), 'first')
            self.assertEqual(next(rows), 'second')
            with self.assertRaisesRegex(ValueError, 'source_SQL_transport_failed'):
                next(rows)
        self.assertIn('--quick', calls[0][-3])

    def test_oversized_record_fails_before_unbounded_read(self):
        from snapshot import bounded_lines
        with patch.dict('os.environ', {'BKN_HISTORY_RECORD_BYTES': '32'}):
            with self.assertRaisesRegex(ValueError, 'source_record_exceeds'):
                list(bounded_lines(io.BytesIO(b'x' * 100000)))

    def test_streaming_core_dump_matches_existing_parser(self):
        from core_snapshot import read_core_backup
        from test_core_snapshot import CoreBackupTests
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'core.sql'
            path.write_bytes(CoreBackupTests().fixture())
            expected, expected_meta = read_core_backup(path)
            records, metadata = read_core_backup(path, stream=True)
            actual = {}
            for r in records:
                actual.setdefault(r['locator'], []).append(r['row'])
            self.assertEqual(actual, expected)
            self.assertEqual(metadata, expected_meta)

class BlobBudgetTests(unittest.TestCase):
    def test_blob_is_rejected_before_hex_export(self):
        from core_snapshot import export_core
        class Source:
            def __init__(self): self.calls = []
            def query(self, sql):
                self.calls.append(sql)
                if 'information_schema.COLUMNS' in sql:
                    if "TABLE_NAME='bkn_trace_ee_current_explanations'" in sql:
                        return iter(['interaction_id\tvarchar', 'view_json\tlongblob'])
                    return iter(())
                if 'OCTET_LENGTH' in sql:
                    return iter([str(8 << 20)])
                raise AssertionError('HEX export should never run')
        source = Source()
        with self.assertRaisesRegex(ValueError, 'source_blob_exceeds_record_limit'):
            export_core(source, stream=True)
        self.assertTrue(any('OCTET_LENGTH' in sql for sql in source.calls))

    def test_large_checkpoint_messages_preserve_round_content(self):
        import msgpack
        from test_agent_history import fixture as thread_fixture, message
        from agent_thread_history import plan_agent_thread_history
        records = thread_fixture()
        for record in records:
            if record['kind'] == 'agent_blob' and record['row']['version'] == 'v4':
                record['row']['blob_hex'] = msgpack.packb([
                    message('human', 'Q' * (128 << 10), 'h1'),
                    message('ai', 'A' * (128 << 10), 'a1')], use_bin_type=True).hex()
        expected = plan_agent_thread_history(records, '2026-10-01T00:00:00Z')
        with tempfile.TemporaryDirectory() as tmp:
            with LoopSourceTests().source(Path(tmp), records) as source:
                actual = plan_agent_thread_history(next(source.batches()), '2026-10-01T00:00:00Z')
        self.assertEqual(actual, expected)

class IdentityCandidateTests(unittest.TestCase):
    def test_ledger_stream_duplicates_are_not_hidden_by_different_conversations(self):
        records = [{'kind': 'core_source', 'source_id': 'trace-core',
                    'locator': 'bkn_trace.bkn_trace_evidence_event_ledger',
                    'row': {'event_id': 'e' + str(n), 'conversation_id': 'c' + str(n),
                            'interaction_id': 'i' + str(n), 'producer_stream_id': 'stream',
                            'producer_epoch': 1, 'producer_sequence': 7}} for n in range(2)]
        with tempfile.TemporaryDirectory() as tmp:
            with LoopSourceTests().source(Path(tmp), records) as source:
                group, _ = source.group([0])
        self.assertEqual(len(group), 2)

    def test_idempotency_conflicts_have_complete_resource_candidates(self):
        records = [{'kind': 'core_source', 'source_id': 'trace-core',
                    'locator': 'bkn_trace.bkn_trace_idempotency_records',
                    'row': {'scope': 'start', 'external_conversation_key': '', 'idempotency_key': 'same',
                            'application_principal_id': 'agent', 'effective_subject_type': 'user',
                            'effective_subject_id': 'user', 'resource_type': 'conversation',
                            'resource_id': 'c' + str(n)}} for n in range(2)]
        with tempfile.TemporaryDirectory() as tmp:
            with LoopSourceTests().source(Path(tmp), records) as source:
                group, _ = source.group([0])
        self.assertEqual(len(group), 2)
