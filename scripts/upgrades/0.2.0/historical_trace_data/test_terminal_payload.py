"""Synthetic regression for the persisted terminal-payload storage contract."""
import copy
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
import unittest

from core_history import plan_core_history
from snapshot import canonical, strict_loads
from test_core_history import fixture


def completion():
    return {'expectation': {'version': 1, 'closed': True, 'events': [
        {'event_id': 'synthetic-event', 'event_type': 'data.query.observed',
         'payload_hash': 'a' * 64, 'producer_id': 'synthetic-producer',
         'publish_disposition': 'accepted'}]},
        'original_durability': 'pending', 'original_observed_refs': [],
        'original_partial_reasons': ['evidence_pending']}


def wrapped_source(field='output_payload', metadata=None):
    source = fixture()
    call = source['bkn_trace_operation_call_facts'][0]
    payload = strict_loads(call['output_payload'])
    payload['_trace_evidence_completion'] = completion() if metadata is None else metadata
    call[field] = canonical(payload)
    if field == 'error_payload':
        call.update(output_payload=None, status='failed')
        source['bkn_trace_receipts'][0]['receipt_status'] = 'failed'
        source['bkn_trace_operations'][-1]['attempt_status'] = 'failed'
    return source


class TerminalPayloadTest(unittest.TestCase):
    def test_restore_output_and_error_without_changing_source_or_business_envelope(self):
        for field, target in [('output_payload', 'output'), ('error_payload', 'error')]:
            for encoding in ['text', 'hex', 'bytes', 'object']:
                with self.subTest(field=field, encoding=encoding):
                    source = wrapped_source(field)
                    row = source['bkn_trace_operation_call_facts'][0]
                    raw = row[field]
                    row[field] = {'text': raw, 'hex': '0x' + raw.encode().hex(),
                                  'bytes': raw.encode(), 'object': strict_loads(raw)}[encoding]
                    original = copy.deepcopy(source)
                    plan = plan_core_history(source)
                    call = plan['core']['call_facts'][0]
                    expected = strict_loads(raw)
                    metadata = expected.pop('_trace_evidence_completion')
                    self.assertEqual(call[target], expected)
                    self.assertEqual(call['evidence_completion'], metadata)
                    self.assertEqual(source, original)
                    self.assertEqual(plan['ledger'], original['bkn_trace_evidence_event_ledger'])
                    for key in ['operation_id', 'conversation_id', 'interaction_id', 'receipt_id', 'request_id', 'trace_id']:
                        self.assertEqual(call[key], row[key])

    def test_legacy_and_inline_same_named_business_key_are_untouched(self):
        source = fixture()
        row = source['bkn_trace_operation_call_facts'][0]
        payload = strict_loads(row['output_payload'])
        payload['inline'] = {'_trace_evidence_completion': {'customer_field': 59}}
        payload['byte_length'] = len(canonical(payload['inline']).encode())
        row['output_payload'] = canonical(payload)
        call = plan_core_history(source)['core']['call_facts'][0]
        self.assertEqual(call['output'], payload)
        self.assertNotIn('evidence_completion', call)

    def test_rejected_expectation_and_all_recorded_durabilities_are_preserved(self):
        for durability in ['pending', 'durable', 'failed']:
            metadata = completion()
            metadata.update(original_durability=durability, rejection_reason='invalid_expectation',
                            rejected_expectation_hash='B' * 64)
            metadata.pop('expectation')
            call = plan_core_history(wrapped_source(metadata=metadata))['core']['call_facts'][0]
            self.assertEqual(call['evidence_completion'], metadata)

    def test_conflicting_terminal_locations_rejected_even_if_identical(self):
        source = wrapped_source()
        row = source['bkn_trace_operation_call_facts'][0]
        row['error_payload'] = row['output_payload']
        with self.assertRaisesRegex(ValueError, 'terminal_completion_conflict'):
            plan_core_history(source)

    def test_malformed_completion_is_rejected_without_default_or_source_mutation(self):
        cases = [None, [], {}, {'original_durability': 'other'},
                 dict(completion(), expectation=None),
                 dict(completion(), expectation={'version': 1, 'closed': False, 'events': []}),
                 dict(completion(), expectation={'version': True, 'closed': True, 'events': []}),
                 dict(completion(), rejected_expectation_hash='broken', rejection_reason='rejected', expectation=None),
                 dict(completion(), rejection_reason='rejected', rejected_expectation_hash='a' * 64)]
        for key, value in [('payload_hash', 'A' * 64), ('producer_id', ''),
                           ('publish_disposition', 'unknown'), ('drop_reason', 'unexpected')]:
            metadata = completion(); metadata['expectation']['events'][0][key] = value
            cases.append(metadata)
        metadata = completion(); metadata['expectation']['events'] *= 2; cases.append(metadata)
        for metadata in cases:
            with self.subTest(metadata=metadata):
                source = wrapped_source()
                row = source['bkn_trace_operation_call_facts'][0]
                payload = strict_loads(row['output_payload'])
                payload['_trace_evidence_completion'] = metadata
                row['output_payload'] = canonical(payload)
                original = copy.deepcopy(source)
                with self.assertRaisesRegex(ValueError, 'terminal_completion_invalid'):
                    plan_core_history(source)
                self.assertEqual(source, original)

    def test_dropped_events_bounds_and_duplicate_control_keys(self):
        metadata = completion()
        metadata['expectation']['events'] = [{'event_id': 'drop', 'event_type': 'query',
            'publish_disposition': 'dropped', 'drop_reason': 'disabled'}]
        call = plan_core_history(wrapped_source(metadata=metadata))['core']['call_facts'][0]
        self.assertEqual(call['evidence_completion'], metadata)
        for events in ([dict(metadata['expectation']['events'][0], event_id=str(i)) for i in range(129)],
                       [dict(metadata['expectation']['events'][0], event_id='中' * 65)]):
            invalid = copy.deepcopy(metadata); invalid['expectation']['events'] = events
            with self.assertRaisesRegex(ValueError, 'terminal_completion_invalid'):
                plan_core_history(wrapped_source(metadata=invalid))
        source = wrapped_source()
        row = source['bkn_trace_operation_call_facts'][0]
        row['output_payload'] = row['output_payload'][:-1] + ', "_trace_evidence_completion":null}'
        with self.assertRaisesRegex(ValueError, 'duplicate JSON key'):
            plan_core_history(source)

    @unittest.skipUnless(os.environ.get('BKN_TEST_CORE_VALIDATOR'), 'set pinned native Core binary path')
    def test_pinned_native_validator_accepts_full_parents_and_rejects_original_storage_shape(self):
        for field, target in [('output_payload', 'output'), ('error_payload', 'error')]:
            with self.subTest(field=field):
                source = wrapped_source(field)
                plan = plan_core_history(source)['core']
                def validate(value):
                    result = subprocess.run([os.environ['BKN_TEST_CORE_VALIDATOR'], '--validate-core-records'],
                                            input=canonical(value).encode(), capture_output=True)
                    return result.returncode, strict_loads(result.stdout)
                code, verdict = validate(plan)
                self.assertEqual((code, verdict.get('verified')), (0, True), verdict)
                original_shape = copy.deepcopy(plan)
                original_shape['call_facts'][0].pop('evidence_completion')
                original_shape['call_facts'][0][target] = strict_loads(source['bkn_trace_operation_call_facts'][0][field])
                code, verdict = validate(original_shape)
                self.assertEqual(code, 1)
                self.assertEqual(verdict['reason'], 'native_core_import_failed_input')
                unknown = copy.deepcopy(plan)
                unknown['call_facts'][0][target]['unknown_storage_field'] = True
                self.assertEqual(validate(unknown)[0], 1)


class TerminalPayloadReportTest(unittest.TestCase):
    def test_report_counts_rows_without_exposing_business_payload(self):
        from report_terminal_payloads import inspect
        sources = [wrapped_source(), wrapped_source('error_payload'), fixture(), wrapped_source()]
        sources[-1]['bkn_trace_operation_call_facts'][0]['output_payload'] = 'not JSON with private source text'
        records = [{'locator': 'bkn_trace.bkn_trace_operation_call_facts', 'row': source['bkn_trace_operation_call_facts'][0]}
                   for source in sources]
        original = copy.deepcopy(records)
        report = inspect(records)
        self.assertEqual(report['counts'], {'call_facts': 4, 'wrapped': 2, 'invalid': 1})
        self.assertEqual([entry['fields'] for entry in report['entries']], [['output'], ['error'], []])
        self.assertNotIn('private source text', canonical(report))
        self.assertNotIn('synthetic-producer', canonical(report))
        self.assertEqual(records, original)
        self.assertFalse(report['native_plan_verified'])

    def test_cli_verifies_snapshot_and_writes_private_report_without_overwrite(self):
        from snapshot import save_snapshot
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = wrapped_source()
            records = [{'locator': 'bkn_trace.bkn_trace_operation_call_facts',
                        'row': source['bkn_trace_operation_call_facts'][0]}]
            save_snapshot(root / 'snapshot', records, 'synthetic-only', {})
            command = [sys.executable, str(Path(__file__).with_name('report_terminal_payloads.py')),
                       '--source', str(root / 'snapshot'), '--output', str(root / 'report.json')]
            result = subprocess.run(command, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(strict_loads(result.stdout), {'call_facts': 1, 'wrapped': 1, 'invalid': 0})
            raw = (root / 'report.json').read_bytes()
            report = strict_loads(raw)
            self.assertEqual((root / 'report.json').stat().st_mode & 0o777, 0o600)
            self.assertEqual(set(report['converter_sha256']), {'core_history.py', 'terminal_payload.py'})
            self.assertNotIn('synthetic-producer', raw.decode())
            self.assertNotEqual(subprocess.run(command, capture_output=True).returncode, 0)
            self.assertEqual((root / 'report.json').read_bytes(), raw)
            (root / 'snapshot' / 'records.jsonl').write_text('{}\n')
            command[-1] = str(root / 'corrupt-report.json')
            self.assertNotEqual(subprocess.run(command, capture_output=True).returncode, 0)
            self.assertFalse((root / 'corrupt-report.json').exists())
