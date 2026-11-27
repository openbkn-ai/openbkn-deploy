# Copyright openbkn.ai
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import yaml
import commercial_latest as latest
import test_commercial_manifest as fixtures


class CommercialLatestTest(unittest.TestCase):
    def test_branch_selection_uses_timestamp_and_matching_release_line(self):
        tags = ['0.2.2-main.20261101000000.shaaaaaaaa',
                '0.2.2-main.20261102000000.shabbbbbbb',
                '0.2.2-release.20261103000000.shaccccccc',
                '0.2.2-release-0.2.2.20261104000000.shaddddddd',
                '0.2.3-main.20261105000000.shaeeeeeee']
        self.assertEqual(latest.select_image_tag(tags, 'main', '0.2.2'), tags[1])
        self.assertEqual(latest.select_image_tag(tags, 'release/0.2.2', '0.2.2'), tags[3])
        with self.assertRaisesRegex(ValueError, 'does not match'):
            latest.select_image_tag(tags, 'release/0.2.1', '0.2.2')
        with self.assertRaisesRegex(ValueError, 'No timestamped'):
            latest.select_image_tag(tags, 'main', '0.2.1')

    def test_generates_matching_override_and_distinct_addon(self):
        fixture = fixtures.CommercialPlanTest()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        manifest = fixture.root / 'commercial.json'
        fixture.manifest['suspendedReleases'] = ['kb-api']
        manifest.write_text(json.dumps(fixture.manifest))
        output = fixture.root / 'generated'
        output.mkdir()
        repo = fixture.root / 'repo'
        (repo / 'scripts').mkdir(parents=True)
        template = yaml.safe_load(fixture.community.read_text())
        template['releases']['kb-api'] = {'chart': 'kb-api', 'version': '0.2.2'}
        (repo / 'scripts/bkn-foundry.template.yaml').write_text(yaml.safe_dump(template))
        calls = []
        def run(args, **kwargs):
            calls.append(args)
            if args[0] == 'git':
                return subprocess.CompletedProcess(args, 0, 'main\n', '')
            if args[0] == 'skopeo':
                return subprocess.CompletedProcess(args, 0, json.dumps({'Tags': ['0.2.2-main.20261124000000.shaabcdef0']}), '')
            src = next(arg.split('=', 1)[1] for arg in args if arg.startswith('--template='))
            dst = next(arg.split('=', 1)[1] for arg in args if arg.startswith('--out='))
            data = yaml.safe_load(Path(src).read_text())
            for entry in data['releases'].values():
                entry['version'] = '0.2.2-main.20261123000000.sha1234567'
            Path(dst).write_text(yaml.safe_dump(data))
            return subprocess.CompletedProcess(args, 0, '', '')
        with patch.object(latest.subprocess, 'run', side_effect=run):
            latest.generate(repo, manifest, output, 'registry.local')
        community = yaml.safe_load((output / 'community.yaml').read_text())
        commercial = json.loads((output / 'commercial/commercial-manifest.json').read_text())
        self.assertEqual(set(community['releases']), {'backend'})
        self.assertEqual(community['releases']['backend']['version'], commercial['releases']['backend']['chartVersion'])
        self.assertEqual(commercial['releases']['addon']['chartVersion'], '0.2.2-main.20261123000000.sha1234567')
        self.assertEqual(commercial['releases']['backend']['image']['tag'], '0.2.2-main.20261124000000.shaabcdef0')
        self.assertEqual(len([c for c in calls if c[0] == 'skopeo']), 2)

    def test_other_branch_rejected_before_registry_access(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(latest.subprocess, 'run') as run:
            run.return_value = subprocess.CompletedProcess([], 0, 'feature/foo\n', '')
            with self.assertRaisesRegex(ValueError, 'main or release'):
                latest.generate(tmp, 'unused', tmp, 'registry.local')
            self.assertEqual(run.call_count, 1)


if __name__ == '__main__':
    unittest.main()
