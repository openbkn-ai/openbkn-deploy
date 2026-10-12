"""Subprocess RSS regression for the real loop with fake native/target IO.

The source iterator is one-shot; neither fake source nor target retains all rows.
This tests Python orchestration memory, not a production database/Go converter.
"""
import contextlib
import io
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import tempfile
import unittest


def measure(count):
    from loop_upgrade import run_loop
    from test_run_upgrade import FakeRuntime
    from test_logs import row
    class Source:
        def query(self, sql):
            return iter(())
    class Runtime(FakeRuntime):
        def __init__(self):
            super().__init__()
            self.source = Source()
            self.existing = True
            self.rows = ({'kind': 'audit', 'source_id': 'vega',
                          'row': row(event_id=str(n), failure_message='retained-detail-' + str(n) + 'x' * 2048)}
                         for n in range(count))
        def _tls_options(self, forwarded):
            return {'verify_tls': True}
        def _native(self, flags, data):
            return b'{"verified":true,"created":0,"already_verified":0}'
        def rebuild_core_projection(self):
            return {'verified': True}
    os.environ['BKN_HISTORY_BATCH_RECORDS'] = '5000'
    os.environ['BKN_HISTORY_BEFORE'] = '2026-10-01T00:00:00Z'
    with tempfile.TemporaryDirectory() as tmp:
        # Redirect to a file, not StringIO: heartbeat output must not accumulate.
        with open(os.devnull, 'w') as silent, contextlib.redirect_stderr(silent):
            result = run_loop(Runtime(), tmp)
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        stats = {'rows': count, 'complete': result['complete'], 'verified': result['target_verified_count'],
                 'peak_rss_bytes': peak if sys.platform == 'darwin' else peak * 1024}
        if not result['complete']:
            stats['error'] = (Path(result['run_directory']) / 'error.json').read_text()
        return stats


class ScaleMemoryTests(unittest.TestCase):
    def test_130000_rows_do_not_scale_memory_with_source_size(self):
        results = []
        for count in (26000, 130000):
            process = subprocess.run([sys.executable, __file__, '--measure', str(count)],
                                     capture_output=True, text=True, check=True, timeout=300)
            value = json.loads(process.stdout)
            self.assertTrue(value['complete'], value)
            self.assertEqual(value['verified'], count)
            results.append(value)
        small, large = results
        # Five times the data may vary allocator high-water marks, but cannot
        # retain a proportional source list. Allow cross-platform allocator slack.
        self.assertLess(large['peak_rss_bytes'], small['peak_rss_bytes'] + (64 << 20), results)
        self.assertLess(large['peak_rss_bytes'], 300 << 20, results)
        print('loop RSS regression: ' + json.dumps(results), flush=True)


if __name__ == '__main__' and len(sys.argv) > 1 and sys.argv[1] == '--measure':
    print(json.dumps(measure(int(sys.argv[2]))))
elif __name__ == '__main__':
    unittest.main()
