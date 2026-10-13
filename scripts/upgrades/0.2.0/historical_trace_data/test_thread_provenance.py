# Copyright (c) 2026 OpenBKN
# SPDX-License-Identifier: LicenseRef-OpenBKN
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt.
import copy
import os
import subprocess
import unittest
from agent_thread_history import plan_agent_thread_history
from snapshot import strict_loads, canonical
from test_agent_history import fixture
from thread_provenance import prepare_thread_projections, precheck_thread_projections


class ThreadProjectionTests(unittest.TestCase):
    def test_precheck_requires_empty_projection_response(self):
        captured = []
        precheck_thread_projections(lambda data: captured.append(data) or b'{"historical_projections":[]}')
        self.assertEqual(captured, [b'{"interactions":[]}'])
        for response in (b'{}', b'[]', b'{"historical_projections":null}', b'{"historical_projections":[{}]}'):
            with self.assertRaisesRegex(ValueError, 'native_ee_projection_precheck_invalid_response'):
                precheck_thread_projections(lambda _: response)
        with self.assertRaisesRegex(ValueError, 'native_ee_projection_precheck_failed'):
            precheck_thread_projections(lambda _: b'not JSON')

    @unittest.skipUnless(os.environ.get('BKN_TEST_EE_CONVERTER') and os.environ.get('BKN_TEST_CORE_VALIDATOR'),
                         'set native binary paths for the EE/Core contract integration test')
    def test_real_agent_rounds_pass_native_ee_and_core_contracts(self):
        threads = plan_agent_thread_history(fixture(), '2026-10-01T00:00:00Z')
        plan = {'core': threads['core'], 'historical_projections': [], 'explanations': []}
        def native(data):
            return subprocess.run([os.environ['BKN_TEST_EE_CONVERTER']], input=data,
                                  capture_output=True, check=True).stdout
        precheck_thread_projections(native)
        prepare_thread_projections(plan, threads, native)
        repeated = {'historical_projections': []}
        prepare_thread_projections(repeated, threads, native)
        self.assertEqual(plan['historical_projections'], repeated['historical_projections'])
        self.assertEqual(len(plan['historical_projections']), 2)
        from retained_runtime import provenance_import_batches
        for batch in provenance_import_batches(plan):
            result = subprocess.run([os.environ['BKN_TEST_CORE_VALIDATOR'], '--validate-provenance-records'],
                                    input=batch, capture_output=True, check=True)
            self.assertTrue(strict_loads(result.stdout)['verified'])
        for projection in plan['historical_projections']:
            self.assertEqual(projection['status'], 'ready')
            graph = strict_loads(projection['graph_payload'])
            self.assertEqual(graph['interaction_id'], projection['interaction_id'])
            self.assertTrue(graph['operations'])

    def test_native_builder_receives_exact_message_round_and_call_facts(self):
        threads = plan_agent_thread_history(fixture(), '2026-10-01T00:00:00Z')
        original = copy.deepcopy(threads)
        plan = {'historical_projections': []}
        captured = []
        def native(data):
            values = strict_loads(data)['interactions']
            captured.extend(values)
            return canonical({'historical_projections': [
                {'interaction_id': v['summary']['interaction_id']} for v in values]}).encode()
        prepare_thread_projections(plan, threads, native)
        self.assertEqual([v['source_question'] for v in captured], ['Inventory?', 'BOM?'])
        self.assertEqual([v['source_result'] for v in captured], ['500 units', '3 materials'])
        self.assertTrue(all(not f['trace_id'] and not f['request_id'] for v in captured for f in v['call_facts']))
        self.assertEqual(len(plan['historical_projections']), 2)
        self.assertEqual(threads, original)

    def test_existing_source_graph_is_never_replaced(self):
        threads = plan_agent_thread_history(fixture(), '2026-10-01T00:00:00Z')
        iid = threads['core']['interactions'][0]['interaction_id']
        plan = {'historical_projections': [{'interaction_id': iid}]}
        with self.assertRaisesRegex(ValueError, 'thread_projection_source_overlap'):
            prepare_thread_projections(plan, threads, lambda _: b'{}')

    def test_missing_or_duplicate_native_output_is_incomplete(self):
        threads = plan_agent_thread_history(fixture(), '2026-10-01T00:00:00Z')
        with self.assertRaisesRegex(ValueError, 'thread_projection_count_mismatch'):
            prepare_thread_projections({'historical_projections': []}, threads,
                                       lambda _: b'{"historical_projections":[]}')
