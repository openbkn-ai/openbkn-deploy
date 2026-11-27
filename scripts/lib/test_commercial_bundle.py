# Copyright openbkn.ai
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import unittest

from commercial_bundle import pack, stage_delivery
from commercial_manifest import compile_plan
import test_commercial_manifest as fixture

ROOT = Path(__file__).resolve().parents[2]


class CommercialBundleTest(unittest.TestCase):
    def setUp(self):
        fixture.CommercialPlanTest.setUp(self)
        self.manifest_path = self.root / 'commercial.json'
        self.manifest_path.write_text(json.dumps(self.manifest))

    def test_generated_delivery_keeps_assets_and_exact_preview_coordinates(self):
        generated = stage_delivery(self.manifest_path, self.root / 'delivery',
                                   version='0.2.2-preview.1', image_tags=['backend=preview-build'])
        manifest = json.loads(generated.read_text())
        self.assertEqual(manifest['version'], '0.2.2-preview.1')
        self.assertEqual(manifest['releases']['backend']['image']['tag'], 'preview-build')
        self.assertEqual(manifest['releases']['addon']['image']['tag'], '0.2.2-build')
        (self.root / 'consumer.yaml').unlink()
        _, profile = compile_plan(generated, self.community, self.root, 'internal:5000')
        self.assertEqual(profile['releases']['backend']['values']['extraEnv'][0]['name'], 'ADDON_URL')
        self.assertTrue((generated.parent / 'runtime.json').is_file())

    def test_packaged_installer_compiles_without_original_delivery_or_ee_checkout(self):
        repo = self.root / 'repo'
        repo.mkdir()
        for name in ('deploy.sh', 'preflight.sh', 'onboard.sh', 'README.md', 'README.zh.md'):
            (repo / name).write_text('fixture\n')
        (repo / 'conf').mkdir()
        (repo / 'scripts/lib').mkdir(parents=True)
        (repo / 'scripts/lib/commercial_manifest.py').write_text((ROOT / 'scripts/lib/commercial_manifest.py').read_text())
        charts = self.root / 'charts'
        charts.mkdir()
        for path in self.root.glob('*.tgz'):
            path.rename(charts / path.name)
        archive_path = pack(repo, self.manifest_path, self.community, charts, self.root / 'package.tar.gz')
        extracted = self.root / 'extracted'
        with tarfile.open(archive_path) as archive:
            archive.extractall(extracted)  # Archive produced locally by this test.
        for path in (self.manifest_path, self.root / 'runtime.json', self.root / 'consumer.yaml', self.root / 'backend.yaml'):
            path.unlink()
        delivery = extracted / 'openbkn-commercial'
        result = subprocess.run([sys.executable, str(delivery / 'scripts/lib/commercial_manifest.py'),
            '--manifest', str(delivery / 'commercial/commercial-manifest.json'),
            '--community', str(delivery / 'community.yaml'), '--charts', str(delivery / 'charts'),
            '--registry', 'internal:5000', '--out', str(self.root / 'plan')], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        profile = json.loads((self.root / 'plan/runtime.json').read_text())
        self.assertEqual(profile['releases']['backend']['values']['image']['registry'], 'internal:5000/openbkn-ai-ee')

    def test_invalid_or_duplicate_assignment_and_missing_assets_do_not_create_output(self):
        for images in (['unknown=tag'], ['backend=one', 'backend=two']):
            with self.assertRaises(ValueError):
                stage_delivery(self.manifest_path, self.root / 'invalid', image_tags=images)
        (self.root / 'consumer.yaml').unlink()
        with self.assertRaises(ValueError):
            stage_delivery(self.manifest_path, self.root / 'invalid')
        self.assertFalse((self.root / 'invalid').exists())

    def test_checked_in_manifest_owns_all_assets_in_deploy_repository(self):
        path = ROOT / 'commercial/0.2.2/commercial-manifest.json'
        manifest = json.loads(path.read_text())
        self.assertEqual(len(manifest['releases']), 9)
        self.assertNotIn('kb-api', manifest['releases'])
        self.assertEqual(manifest['suspendedReleases'], ['kb-api'])
        referenced = {manifest['runtimeProfile'], *manifest.get('examples', [])}
        for entry in manifest['releases'].values():
            referenced.update(entry['valuesFiles'])
            for paths in entry.get('consumers', {}).values():
                referenced.update(paths)
            self.assertNotIn('minEdition', entry)
        for name in referenced:
            self.assertTrue((path.parent / name).is_file(), name)
        profile = json.loads((path.parent / manifest['runtimeProfile']).read_text())
        self.assertFalse(profile.get('addons'))
        self.assertEqual(profile['platformVersion'], manifest['platformVersion'])


if __name__ == '__main__':
    unittest.main()
