import contextlib
import copy
import io
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import call, patch

from loop_upgrade import run_loop, Progress
from snapshot import canonical, digest, strict_loads
from core_history_batches import HISTORY_CORE_FIELDS
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

    def run_quiet(self, runtime):
        with contextlib.redirect_stderr(io.StringIO()):
            return run_loop(runtime, self.root)

    def test_core_rejection_skips_only_the_rejected_interaction(self):
        from loop_upgrade import _stage_core_batches

        class Runtime:
            def _native(self, flags, data):
                ids = [row['interaction_id'] for row in strict_loads(data)['interactions']]
                if 'rejected' in ids:
                    return b'{"verified":false,"reason":"interaction_invalid"}'
                return b'{"verified":true}'

        class Progress:
            def update(self, **values):
                pass

        plan = {'core': {key: [] for key in HISTORY_CORE_FIELDS}}
        plan['core']['conversations'] = [{'conversation_id': 'conversation'}]
        plan['core']['interactions'] = [
            {'interaction_id': 'accepted', 'conversation_id': 'conversation'},
            {'interaction_id': 'rejected', 'conversation_id': 'conversation'},
        ]
        directory = self.root / 'core-stage'
        directory.mkdir()
        skipped = _stage_core_batches(Runtime(), plan, directory, Progress())
        self.assertEqual([item['interaction_id'] for item in skipped], ['rejected'])
        staged = [strict_loads(path.read_bytes()) for path in (directory / 'core').glob('*.json')]
        self.assertEqual([[row['interaction_id'] for row in body['interactions']] for body in staged], [['accepted']])

    def test_ledger_rejection_skips_only_the_rejected_event(self):
        from loop_upgrade import _stage_ledger_batches

        class Runtime:
            def _native(self, flags, data):
                ids = [row['ledger_id'] for row in strict_loads(data)['ledger']]
                if 'rejected' in ids:
                    return b'{"verified":false,"reason":"ledger_invalid"}'
                return b'{"verified":true}'

        class Progress:
            def update(self, **values):
                pass

        directory = self.root / 'ledger-stage'
        directory.mkdir()
        rows = {'ledger': [
            {'ledger_id': 'accepted', 'envelope': {'envelope': {'event': {'event_type': 'accepted'}}}},
            {'ledger_id': 'rejected', 'envelope': {'envelope': {'event': {'event_type': 'rejected'}}}},
        ]}
        skipped = _stage_ledger_batches(Runtime(), [canonical(rows).encode()], directory, Progress())
        self.assertEqual([item['ledger_id'] for item in skipped], ['rejected'])
        staged = [strict_loads(path.read_bytes()) for path in (directory / 'ledger').glob('*.json')]
        self.assertEqual([[row['ledger_id'] for row in body['ledger']] for body in staged], [['accepted']])

    def test_validator_command_failure_retries_then_stops_instead_of_skipping_ledger(self):
        from loop_upgrade import _stage_ledger_batches

        class CommandFailure(ValueError):
            returncode, stdout, stderr = 1, b'', b'pod unavailable'

        class Runtime:
            def __init__(self):
                self.calls = 0

            def _native(self, flags, data):
                self.calls += 1
                raise CommandFailure('deployment_command_failed')

        class Progress:
            def update(self, **values):
                pass

        directory = self.root / 'failed-ledger-stage'
        directory.mkdir()
        runtime = Runtime()
        with patch('loop_upgrade.time.sleep') as sleep:
            with self.assertRaisesRegex(RuntimeError, 'native_validator_command_failed'):
                _stage_ledger_batches(runtime, [canonical({'ledger': [{'ledger_id': 'one'}]}).encode()],
                                      directory, Progress())
        self.assertEqual(runtime.calls, 3)
        self.assertEqual(sleep.call_args_list, [call(3), call(3)])
        self.assertEqual(list((directory / 'ledger').glob('*.json')), [])

    def test_validator_command_failure_retries_then_stops_instead_of_skipping_core(self):
        from loop_upgrade import _stage_core_batches

        class CommandFailure(ValueError):
            returncode, stdout, stderr = 1, b'', b'validator unavailable'

        class Runtime:
            def __init__(self):
                self.calls = 0

            def _native(self, flags, data):
                self.calls += 1
                raise CommandFailure('deployment_command_failed')

        class Progress:
            def update(self, **values):
                pass

        plan = {'core': {key: [] for key in HISTORY_CORE_FIELDS}}
        plan['core']['conversations'] = [{'conversation_id': 'conversation'}]
        plan['core']['interactions'] = [{'interaction_id': 'interaction', 'conversation_id': 'conversation'}]
        directory = self.root / 'failed-core-stage'
        directory.mkdir()
        runtime = Runtime()
        with patch('loop_upgrade.time.sleep') as sleep:
            with self.assertRaisesRegex(RuntimeError, 'native_validator_command_failed'):
                _stage_core_batches(runtime, plan, directory, Progress())
        self.assertEqual(runtime.calls, 3)
        self.assertEqual(sleep.call_args_list, [call(3), call(3)])
        self.assertEqual(list((directory / 'core').glob('*.json')), [])

    def test_validator_command_retry_can_recover_without_skipping(self):
        from loop_upgrade import _native_preflight

        class CommandFailure(ValueError):
            returncode, stdout, stderr = 1, b'', b'pod starting'

        class Runtime:
            def __init__(self):
                self.calls = 0

            def _native(self, flags, data):
                self.calls += 1
                if self.calls < 3:
                    raise CommandFailure('deployment_command_failed')
                return b'{"verified":true}'

        runtime = Runtime()
        with patch('loop_upgrade.time.sleep') as sleep:
            passed, detail = _native_preflight(runtime, '--validate-ledger-records', b'{}')
        self.assertTrue(passed)
        self.assertEqual(detail, {'verified': True})
        self.assertEqual(runtime.calls, 3)
        self.assertEqual(sleep.call_args_list, [call(3), call(3)])

    def test_loop_imports_only_current_batch_and_rerun_is_idempotent(self):
        runtime = LoopRuntime()
        first = self.run_quiet(runtime)
        self.assertTrue(first['complete'], first)
        self.assertEqual(runtime.publish_calls, [5, 5, 3])
        self.assertEqual(first['source_count'], 13)
        self.assertEqual(first['target_verified_count'], 13)
        second = self.run_quiet(runtime)
        self.assertTrue(second['complete'], second)
        self.assertEqual(runtime.publish_calls, [5, 5, 3])
        self.assertEqual(second['already_verified_count'], 13)
        progress = strict_loads((Path(second['run_directory']) / 'progress.json').read_text())
        self.assertEqual(progress['completed_batches'], 3)
        self.assertEqual(progress['phase'], 'completed')

    def test_late_preflight_failure_prevents_all_target_writes(self):
        runtime = LoopRuntime(6)
        runtime.rows[-1] = {'kind': 'evidence', 'source_id': 'backend', 'row': {'envelope': 'invalid'}}
        result = self.run_quiet(runtime)
        self.assertFalse(result['complete'])
        self.assertEqual(runtime.publish_calls, [])
        self.assertFalse(any(flags and flags[0].startswith('--import') for flags in runtime.native_calls))
        error = strict_loads((Path(result['run_directory']) / 'error.json').read_text())
        self.assertEqual(error['message'], 'native_evidence_conversion_incomplete')

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
