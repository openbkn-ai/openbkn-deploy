"""Audit readback rounds must remain bounded when the consumer lags."""
import contextlib
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch
import uuid

from loop_upgrade import import_batch, Progress
from run_upgrade import DeploymentRuntime, _report
from reconcile import fetch_audits
from snapshot import canonical, digest, private_jsonl, strict_loads


class AuditReadbackTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.items = []
        for number in range(5000):
            payload = {'event_id': str(uuid.UUID(int=number)), 'source_id': 'vega',
                       'occurred_at': '2026-09-01T00:00:00Z'}
            self.items.append({'kind': 'audit', 'disposition': 'convert', 'payload': payload,
                               'target_id': payload['event_id'], 'source_id': 'vega',
                               'content_hash': 'sha256:' + digest(canonical(payload).encode())})
        self.runtime = DeploymentRuntime()
        self.runtime.source = Mock()
        self.runtime.source.query.return_value = []
        self.published = []
        def native(flags, data):
            requests = [strict_loads(line) for line in data.splitlines()]
            if flags:
                self.published.append(requests)
            return ''.join(canonical({'accepted': True, 'event_id': request['payload']['event_id']}) + '\n'
                           for request in requests).encode()
        self.runtime._native = native
        self.now = 0
        self.sleeps = []

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds

    def row(self, item):
        return {'content_hash': item['content_hash'], 'dedup_hash': item['content_hash'],
                'payload': item['payload'], 'target_table': 'audit_event_202609',
                'source_id': 'vega', 'occurred_at': item['payload']['occurred_at'],
                'topic': 'openbkn.audit.v1', 'partition': 0, 'offset': 1}

    def run_batch(self):
        (self.directory / 'summary.json').write_text(canonical({
            'original_count': 0, 'evidence_count': 0, 'audit_count': len(self.items)}))
        private_jsonl(self.directory / 'audits.jsonl', self.items)
        self.progress = Progress(self.directory)
        with contextlib.redirect_stderr(io.StringIO()), \
                patch('loop_upgrade.time.monotonic', side_effect=lambda: self.now), \
                patch('loop_upgrade.time.sleep', side_effect=self.sleep):
            outcome = import_batch(self.runtime, self.directory, self.progress)
        self.final = [strict_loads(line) for line in (self.directory / 'final-items.jsonl').read_bytes().splitlines()]
        return outcome

    def test_5000_missing_audits_query_once_per_round(self):
        # Exercise the real Runtime and SQL chunker, including absence from cache.
        with patch('run_upgrade.reconcile.fetch_audits', wraps=fetch_audits) as fetch:
            verified, already, retained, reasons = self.run_batch()
        self.assertEqual((verified, already, retained), (0, 0, 5000))
        self.assertEqual(reasons, {'publication_requires_readback': 5000})
        self.assertEqual(len(self.published), 1)
        self.assertEqual(len(self.published[0]), 5000)
        self.assertEqual(fetch.call_count, 6)  # Initial lookup plus five readback rounds.
        self.assertEqual(self.runtime.source.query.call_count, 6 * 25)
        self.assertEqual(self.sleeps, [1, 2, 4, 1])
        self.assertEqual(self.progress.values['audit_readback_round'], 5)
        self.assertEqual(self.progress.values['audit_readback_elapsed_seconds'], 8)
        self.assertEqual(self.progress.values['audit_published'], 5000)
        self.assertEqual(self.progress.values['audit_pending'], 5000)
        self.assertTrue(all(item['reason'] == 'publication_requires_readback' for item in self.final))

    def test_verified_ids_are_removed_from_later_rounds(self):
        rows = {item['target_id']: self.row(item) for item in self.items}
        first_half = set(list(rows)[:2500])
        calls = []
        def fetch(source, ids):
            calls.append(list(ids))
            if len(calls) == 1:
                return {}
            if len(calls) == 2:
                return {identity: rows[identity] for identity in ids if identity in first_half}
            return {identity: rows[identity] for identity in ids}
        with patch('run_upgrade.reconcile.fetch_audits', side_effect=fetch):
            verified, already, retained, reasons = self.run_batch()
        self.assertEqual((verified, already, retained), (5000, 0, 0))
        self.assertEqual(reasons, {})
        self.assertEqual([len(ids) for ids in calls], [5000, 5000, 2500])
        self.assertTrue(first_half.isdisjoint(calls[-1]))
        self.assertEqual(self.sleeps, [1])
        self.assertEqual(self.progress.values['audit_pending'], 0)
        self.assertEqual(self.progress.values['audit_verified'], 5000)

    def test_query_time_exhausts_shared_window(self):
        calls = 0
        def fetch(source, ids):
            nonlocal calls
            calls += 1
            if calls > 1:
                self.now += 9
            return {}
        with patch('run_upgrade.reconcile.fetch_audits', side_effect=fetch):
            verified, already, retained, reasons = self.run_batch()
        self.assertEqual(calls, 2)
        self.assertEqual(self.sleeps, [])
        self.assertEqual(retained, 5000)
        self.assertEqual(self.progress.values['audit_readback_round'], 1)
        self.assertEqual(self.progress.values['audit_readback_elapsed_seconds'], 9)

    def test_already_verified_archive_and_conflict_are_not_published(self):
        self.items = self.items[:3]
        self.items[2].update(disposition='archive', reason='retained_history')
        good, bad = self.items[:2]
        rows = {good['target_id']: self.row(good), bad['target_id']: self.row(bad)}
        rows[bad['target_id']]['content_hash'] = 'different'
        with patch('run_upgrade.reconcile.fetch_audits', return_value=rows) as fetch:
            verified, already, retained, reasons = self.run_batch()
        self.assertEqual((verified, already, retained), (1, 1, 2))
        self.assertEqual(self.published, [])
        self.assertEqual(fetch.call_count, 1)
        self.assertEqual(self.progress.values['audit_total'], 3)
        self.assertEqual(self.progress.values['audit_retained'], 2)
        self.assertEqual(self.progress.values['audit_readback_round'], 0)

    def test_conflict_after_publication_stops_retrying_that_id(self):
        self.items = self.items[:2]
        bad = self.items[0]
        conflict = self.row(bad)
        conflict['payload'] = {'changed': True}
        calls = []
        def fetch(source, ids):
            calls.append(list(ids))
            return {bad['target_id']: conflict} if len(calls) == 2 else {}
        with patch('run_upgrade.reconcile.fetch_audits', side_effect=fetch):
            verified, already, retained, reasons = self.run_batch()
        self.assertEqual((verified, retained), (0, 2))
        self.assertEqual(reasons, {'target_content_conflict': 1, 'publication_requires_readback': 1})
        self.assertEqual([len(ids) for ids in calls], [2, 2, 1, 1, 1, 1])
        self.assertEqual(self.progress.values['audit_pending'], 1)

    def test_lost_ack_still_requires_database_confirmation(self):
        self.items = self.items[:1]
        native = self.runtime._native
        def lose_ack(flags, data):
            if flags:
                raise ValueError('ack lost')
            return native(flags, data)
        self.runtime._native = lose_ack
        item = self.items[0]
        with patch('run_upgrade.reconcile.fetch_audits', side_effect=[{}, {item['target_id']: self.row(item)}]):
            verified, already, retained, reasons = self.run_batch()
        self.assertEqual((verified, already, retained), (1, 0, 0))
        self.assertEqual(self.progress.values['audit_published'], 0)
        self.assertEqual(self.progress.values['audit_verified'], 1)
        self.assertEqual(reasons, {})

    def test_report_contains_audit_summary(self):
        result = {'state': 'partial_requires_reconciliation', 'source_count': 5000,
                  'target_verified_count': 1000, 'already_verified_count': 0, 'retained_count': 4000,
                  'audit_total': 5000, 'audit_published': 5000, 'audit_verified': 1000,
                  'audit_pending': 4000, 'audit_retained': 4000}
        _report(self.directory, result, {'publication_requires_readback': 4000})
        report = (self.directory / 'report.md').read_text()
        for text in ('Audit planned: 5000', 'Audit published (Kafka acknowledged): 5000',
                     'Audit target verified (including already verified): 1000',
                     'Audit published or attempted but not read back: 4000', 'Audit retained: 4000'):
            self.assertIn(text, report)


if __name__ == '__main__':
    unittest.main()
