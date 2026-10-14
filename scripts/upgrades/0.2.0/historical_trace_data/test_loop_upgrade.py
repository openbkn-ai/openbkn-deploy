import contextlib
import copy
import io
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from loop_upgrade import run_loop, Progress
from snapshot import canonical, digest, strict_loads
from test_run_upgrade import FakeRuntime
from test_logs import row


class EmptySource:
    def query(self, sql):
        return iter(())


class LoopRuntime(FakeRuntime):
    def __init__(self, count=13):
        super().__init__()
        self.rows = [{'kind': 'audit', 'source_id': 'vega', 'row': row(event_id=str(n))} for n in range(count)]
        self.source = EmptySource()
        self.stored = {}
        self.native_calls = []
        self.publish_calls = []

    def _tls_options(self, forwarded):
        return {'verify_tls': True}

    def _native(self, flags, data):
        self.native_calls.append(flags)
        return b'{"verified":true,"created":0,"already_verified":0}'

    def rebuild_core_projection(self):
        return {'verified': True}

    def publish(self, requests):
        self.publish_calls.append(len(requests))
        replies = []
        for request in requests:
            event = request['payload']
            self.stored[event['event_id']] = copy.deepcopy(event)
            replies.append({'accepted': True})
        return replies

    def fetch(self, item):
        payload = self.stored.get(item['target_id'])
        if payload is None:
            return None
        occurred = payload['occurred_at']
        content_hash = 'sha256:' + digest(canonical(payload).encode())
        return {'content_hash': content_hash, 'dedup_hash': content_hash, 'payload': payload,
                'target_table': 'audit_event_' + occurred[:7].replace('-', ''),
                'source_id': item['source_id'], 'occurred_at': occurred, 'topic': 'openbkn.audit.v1',
                'partition': 0, 'offset': 1}


class LoopUpgradeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.env = patch.dict('os.environ', {'BKN_HISTORY_BATCH_RECORDS': '5',
                                             'BKN_HISTORY_BEFORE': '2026-10-01T00:00:00Z'})
        self.env.start()
        self.addCleanup(self.env.stop)

    def test_empty_audit_preserves_other_imports_and_resets_batch_progress(self):
        from loop_upgrade import import_batch
        directory = self.root / 'mixed-batch'
        directory.mkdir()
        payloads = {
            'artifact': {'artifacts': [{'artifact_id': 'artifact-one'}]},
            'core': {'conversations': [{'conversation_id': 'conversation-one'}]},
        }
        for category in ('artifact', 'core', 'ledger', 'provenance', 'evidence'):
            folder = directory / category
            folder.mkdir()
            if category in payloads:
                (folder / '00000000.json').write_text(canonical(payloads[category]))
        (directory / 'audits.jsonl').write_bytes(b'')
        (directory / 'summary.json').write_text(canonical({
            'audit_count': 0, 'evidence_count': 0, 'original_count': 1}))
        runtime = LoopRuntime(0)
        progress = Progress(directory)
        audit_keys = ('audit_total', 'audit_already_verified', 'audit_published', 'audit_verified',
                      'audit_pending', 'audit_retained', 'audit_readback_round', 'audit_readback_elapsed_seconds')
        # Simulate counters left by a preceding nonempty Audit batch.
        progress.values.update({key: 7 for key in audit_keys})
        original_open = Path.open

        def guarded_open(path, *args, **kwargs):
            if path == directory / 'audits.jsonl':
                raise AssertionError('empty Audit must not be opened')
            return original_open(path, *args, **kwargs)

        with patch.object(Path, 'open', autospec=True, side_effect=guarded_open), \
                patch.object(runtime, 'validate') as validate, \
                patch.object(runtime, 'fetch_batch') as fetch, \
                patch.object(runtime, 'publish') as publish, contextlib.redirect_stderr(io.StringIO()):
            verified, already, retained, reasons = import_batch(runtime, directory, progress)
        validate.assert_not_called()
        fetch.assert_not_called()
        publish.assert_not_called()
        self.assertEqual(runtime.native_calls, [['--import-artifact-records'], ['--import-core-records']])
        self.assertEqual((verified, already, retained, dict(reasons)), (1, 0, 0, {}))
        self.assertEqual(progress.values['phase'], 'import_audit_skipped')
        self.assertEqual({key: progress.values[key] for key in audit_keys}, {key: 0 for key in audit_keys})
        self.assertEqual((directory / 'audits.jsonl').read_bytes(), b'')
        self.assertEqual((directory / 'final-items.jsonl').read_bytes(), b'')

    def run_quiet(self, runtime):
        with contextlib.redirect_stderr(io.StringIO()):
            return run_loop(runtime, self.root)

    def test_loop_imports_only_current_batch_and_rerun_is_idempotent(self):
        runtime = LoopRuntime()
        first = self.run_quiet(runtime)
        self.assertTrue(first['complete'], first)
        self.assertEqual(runtime.publish_calls, [5, 5, 3])
        self.assertEqual(first['source_count'], 13)
        self.assertEqual(first['target_verified_count'], 13)
        self.assertEqual({key: first[key] for key in ('audit_total', 'audit_published', 'audit_verified', 'audit_pending', 'audit_retained')},
                         {'audit_total': 13, 'audit_published': 13, 'audit_verified': 13, 'audit_pending': 0, 'audit_retained': 0})
        self.assertIn('Audit published (Kafka acknowledged): 13', (Path(first['run_directory']) / 'report.md').read_text())
        second = self.run_quiet(runtime)
        self.assertTrue(second['complete'], second)
        self.assertEqual(runtime.publish_calls, [5, 5, 3])
        self.assertEqual(second['already_verified_count'], 13)
        self.assertEqual(second['audit_published'], 0)
        self.assertEqual(second['audit_verified'], 13)
        progress = strict_loads((Path(second['run_directory']) / 'progress.json').read_text())
        self.assertEqual(progress['completed_batches'], 3)
        self.assertEqual(progress['phase'], 'completed')
        self.assertEqual(progress['audit_total'], 3)
        self.assertEqual(progress['audit_already_verified'], 3)
        self.assertEqual(progress['audit_readback_round'], 0)

    def test_failed_readback_preserves_current_batch_audit_report(self):
        runtime = LoopRuntime(6)
        fetch = runtime.fetch_batch
        calls = 0
        def readback(items):
            nonlocal calls
            calls += 1
            # First batch succeeds. The second publishes, then loses its DB connection.
            if calls == 4:
                raise ValueError('database_readback_failed')
            return fetch(items)
        runtime.fetch_batch = readback
        result = self.run_quiet(runtime)
        self.assertFalse(result['complete'])
        self.assertEqual(result['state'], 'import_audit_failed')
        self.assertEqual(result['audit_total'], 6)
        self.assertEqual(result['audit_published'], 6)
        self.assertEqual(result['audit_verified'], 5)
        self.assertEqual(result['audit_pending'], 1)
        self.assertEqual(result['audit_retained'], 1)
        report = (Path(result['run_directory']) / 'report.md').read_text()
        self.assertIn('Audit published (Kafka acknowledged): 6', report)
        self.assertIn('Audit retained: 1', report)

    def test_failure_before_audit_does_not_recount_previous_batch(self):
        runtime = LoopRuntime(6)
        # Audit-only batches have no artifact file; inject failure at batch entry.
        from loop_upgrade import import_batch
        def failing_import(runtime, directory, progress):
            if runtime.publish_calls:
                progress.update(phase='import_artifact')
                raise ValueError('native_import_failed')
            return import_batch(runtime, directory, progress)
        with patch('loop_upgrade.import_batch', side_effect=failing_import):
            result = self.run_quiet(runtime)
        self.assertFalse(result['complete'])
        self.assertEqual(result['audit_total'], 5)
        self.assertEqual(result['audit_published'], 5)
        self.assertEqual(result['audit_verified'], 5)
        self.assertEqual(result['audit_retained'], 0)

    def test_late_preflight_failure_prevents_all_target_writes(self):
        runtime = LoopRuntime(6)
        runtime.rows[-1] = {'kind': 'evidence', 'source_id': 'backend', 'row': {'envelope': 'invalid'}}
        result = self.run_quiet(runtime)
        self.assertFalse(result['complete'])
        self.assertEqual(runtime.publish_calls, [])
        self.assertFalse(any(flags and flags[0].startswith('--import') for flags in runtime.native_calls))
        error = strict_loads((Path(result['run_directory']) / 'error.json').read_text())
        self.assertEqual(error['message'], 'native_evidence_conversion_incomplete')

    def test_missing_projection_converter_stops_before_prepare_or_target_write(self):
        runtime = LoopRuntime()
        agent = {'kind': 'agent_thread', 'source_id': 'bkn-agent', 'row': {'f_thread_id': 'thread-one'}}
        with patch('agent_history.export_agent_source', return_value=([agent], [])), \
                patch.object(runtime, '_native_ee_projection', side_effect=FileNotFoundError('missing helper')):
            result = self.run_quiet(runtime)
        self.assertFalse(result['complete'])
        self.assertEqual(result['state'], 'precheck_ee_projection_failed')
        self.assertEqual(runtime.snapshot_calls, 1)
        self.assertEqual(runtime.native_calls, [])
        self.assertEqual(runtime.publish_calls, [])
        error = strict_loads((Path(result['run_directory']) / 'error.json').read_text())
        self.assertEqual(error['message'], 'native_ee_projection_precheck_failed')

    def test_audit_only_migration_does_not_require_ee_converter(self):
        runtime = LoopRuntime(1)
        with patch.object(runtime, '_native_ee_projection', side_effect=FileNotFoundError('missing helper')) as helper:
            result = self.run_quiet(runtime)
        self.assertTrue(result['complete'], result)
        helper.assert_not_called()

    def test_interrupted_import_can_be_repeated_without_duplicate_publication(self):
        runtime = LoopRuntime()
        publish = runtime.publish
        def interrupted(requests):
            if runtime.publish_calls:
                raise InterruptedError('migration_interrupted')
            return publish(requests)
        runtime.publish = interrupted
        result = self.run_quiet(runtime)
        self.assertFalse(result['complete'])
        runtime.publish = publish
        again = self.run_quiet(runtime)
        self.assertTrue(again['complete'], again)
        self.assertEqual(runtime.publish_calls, [5, 5, 3])
        self.assertEqual(len(runtime.stored), 13)

    def test_unset_cutover_is_frozen_across_process_reruns(self):
        os.environ.pop('BKN_HISTORY_BEFORE', None)
        runtime = LoopRuntime(1)
        first = self.run_quiet(runtime)
        second = self.run_quiet(runtime)
        self.assertTrue(second['complete'], second)
        self.assertEqual(first['agent_cutover_before'], second['agent_cutover_before'])
        self.assertEqual(runtime.snapshot_calls, 1)

    def test_cutover_change_is_rejected_against_frozen_sources(self):
        runtime = LoopRuntime(1)
        self.run_quiet(runtime)
        os.environ['BKN_HISTORY_BEFORE'] = '2026-10-02T00:00:00Z'
        result = self.run_quiet(runtime)
        self.assertFalse(result['complete'])
        self.assertEqual(runtime.publish_calls, [1])

    def test_private_exception_has_stack_without_secret_contents(self):
        runtime = LoopRuntime(1)
        runtime.discover = lambda: (_ for _ in ()).throw(ValueError('password=private token=secret customer question'))
        result = self.run_quiet(runtime)
        directory = Path(result['run_directory'])
        error = strict_loads((directory / 'error.json').read_text())
        self.assertEqual(error['exception_class'], 'ValueError')
        self.assertTrue(error['stack'])
        for path in ('error.json', 'progress.json', 'report.md'):
            content = (directory / path).read_text()
            self.assertNotIn('private token', content)
            self.assertNotIn('customer question', content)

    def test_progress_heartbeat_during_long_stage(self):
        with contextlib.redirect_stderr(io.StringIO()):
            with Progress(self.root) as progress:
                progress.update(phase='waiting_native')
                with patch.object(progress.stop, 'wait', side_effect=[False, True]):
                    progress.heartbeat()
                current = strict_loads((self.root / 'progress.json').read_text())
                self.assertEqual(current['phase'], 'waiting_native')
                self.assertIn('peak_rss_bytes', current)
