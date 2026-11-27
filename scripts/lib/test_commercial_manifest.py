# Copyright openbkn.ai
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.
import copy
import io
import json
from pathlib import Path
import subprocess
import re
import tarfile
import tempfile
import unittest

import yaml

from commercial_manifest import compile_plan
import test_runtime_profile as fixture
import test_runtime_profile_install as installer
runtime = fixture.runtime


class CommercialPlanTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.community = self.root / 'community.yaml'
        self.community.write_text(yaml.safe_dump({'apiVersion': 'deploy.openbkn.ai/v1alpha1', 'kind': 'VersionSet', 'product': 'bkn-foundry', 'version': '0.2.2', 'releases': {
            'backend': {'chart': 'backend', 'version': '0.2.2'}}}))
        (self.root / 'runtime.json').write_text(json.dumps({
            'name': 'commercial', 'apiVersion': runtime.API, 'releases': {'backend': {'values': {}}}}))
        (self.root / 'backend.yaml').write_text('image:\n  repository: backend-ee\n')
        (self.root / 'addon.yaml').write_text('migration:\n  enabled: true\n')
        (self.root / 'consumer.yaml').write_text('extraEnv:\n  - name: ADDON_URL\n    value: http://addon\n')
        self.manifest = {'apiVersion': 'deploy.openbkn.ai/commercial/v1', 'kind': 'CommercialManifest',
                         'version': '0.2.2', 'platformVersion': '0.2.2', 'runtimeProfile': 'runtime.json',
                         'releases': {}}
        for name, kind in [('backend', 'override'), ('addon', 'addon')]:
            self.manifest['releases'][name] = {'type': kind, 'chart': name, 'chartVersion': '0.2.2',
                'valuesFiles': [name + '.yaml'],
                'image': {'repository': 'openbkn-ai-ee/' + name + '-ee', 'tag': '0.2.2-build'}}
            content = yaml.safe_dump({'name': name, 'version': '0.2.2', 'apiVersion': 'v2'}).encode()
            with tarfile.open(self.root / (name + '-0.2.2.tgz'), 'w:gz') as archive:
                info = tarfile.TarInfo(name + '/Chart.yaml')
                info.size = len(content)
                archive.addfile(info, io.BytesIO(content))
        self.manifest['releases']['addon']['consumers'] = {'backend': ['consumer.yaml']}

    def compile(self):
        path = self.root / 'commercial.json'
        path.write_text(json.dumps(self.manifest))
        return compile_plan(path, self.community, self.root, 'registry.local:5000')

    def test_combines_override_addon_and_consumers_without_license_filter(self):
        community, profile = self.compile()
        self.assertEqual(list(community['releases']), ['backend', 'addon'])
        self.assertEqual(profile['addons'], {})
        self.assertTrue(profile['commercialDelivery'])
        self.assertEqual(profile['releases']['backend']['values']['image'], {
            'registry': 'registry.local:5000/openbkn-ai-ee', 'repository': 'backend-ee', 'tag': '0.2.2-build'})
        self.assertEqual(profile['releases']['backend']['values']['extraEnv'][0]['name'], 'ADDON_URL')
        self.assertTrue(profile['releases']['addon']['values']['migration']['enabled'])

    def test_missing_or_wrong_chart_fails_before_cluster_access(self):
        (self.root / 'addon-0.2.2.tgz').unlink()
        with self.assertRaisesRegex(ValueError, 'local Chart'):
            self.compile()

    def test_suspended_kb_is_excluded_even_if_community_manifest_lists_it(self):
        community = yaml.safe_load(self.community.read_text())
        community['releases']['kb-api'] = {'chart': 'kb-api', 'version': '0.2.2'}
        self.community.write_text(yaml.safe_dump(community))
        self.manifest['suspendedReleases'] = ['kb-api']
        plan, profile = self.compile()
        self.assertNotIn('kb-api', plan['releases'])
        self.assertNotIn('kb-api', profile['releases'])
        self.assertEqual(profile['suspendedReleases'], ['kb-api'])
        manifest = self.root / 'commercial.json'
        values = self.root / 'environment.yaml'
        values.write_text('kb-api:\n  objectStorage:\n    endpoint: legacy\n')
        _, profile = compile_plan(manifest, self.community, self.root, 'registry.local', values)
        self.assertNotIn('kb-api', profile['releases'])

    def test_incompatible_platform_or_override_is_rejected(self):
        self.manifest['platformVersion'] = '0.2.3'
        with self.assertRaisesRegex(ValueError, 'versions must match'):
            self.compile()
        self.manifest['platformVersion'] = '0.2.2'
        self.manifest['releases']['backend']['chartVersion'] = '0.2.3'
        with self.assertRaisesRegex(ValueError, 'override does not match'):
            self.compile()

    def test_values_cannot_escape_bundle(self):
        self.manifest['releases']['backend']['valuesFiles'] = ['../outside.yaml']
        with self.assertRaisesRegex(ValueError, 'outside'):
            self.compile()

    def test_environment_values_are_applied_but_image_is_locked(self):
        path = self.root / 'commercial.json'
        path.write_text(json.dumps(self.manifest))
        overrides = self.root / 'env.yaml'
        overrides.write_text('addon:\n  migration:\n    existingSecret: ddl-account\n')
        _, profile = compile_plan(path, self.community, self.root, 'registry.local', overrides)
        self.assertEqual(profile['releases']['addon']['values']['migration']['existingSecret'], 'ddl-account')
        overrides.write_text('addon:\n  image: {}\n')
        with self.assertRaisesRegex(ValueError, 'locked image'):
            compile_plan(path, self.community, self.root, 'registry.local', overrides)

    def test_missing_environment_values_reports_path(self):
        path = self.root / 'commercial.json'
        path.write_text(json.dumps(self.manifest))
        missing = self.root / 'missing-environment.yaml'
        result = subprocess.run(
            ['python3', str(Path(__file__).with_name('commercial_manifest.py')),
             '--manifest', str(path), '--community', str(self.community),
             '--charts', str(self.root), '--registry', 'registry.local',
             '--values', str(missing), '--out', str(self.root / 'out')],
            capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Commercial values file does not exist', result.stderr)
        self.assertIn(str(missing), result.stderr)


class CommercialRuntimeTest(unittest.TestCase):
    prepare = fixture.RuntimeProfileTest.prepare
    def setUp(self):
        fixture.RuntimeProfileTest.setUp(self)
        self.desired['commercialDelivery'] = True
        runtime.write(self.input, self.desired)
        self.args.commercial = True

    # Inherited tests cover the legacy contract separately in test_runtime_profile.
    def test_suspended_kb_is_not_reconciled_or_deleted(self):
        self.desired['releases']['kb-api'] = {'values': {}}
        self.args.releases.append('kb-api')
        runtime.write(self.input, self.desired)
        self.prepare()
        runtime.apply(self.args)
        del self.desired['releases']['kb-api']
        self.desired['suspendedReleases'] = ['kb-api']
        self.args.releases.remove('kb-api')
        runtime.write(self.input, self.desired)
        self.cluster.calls.clear()
        self.prepare()
        self.assertFalse(any('kb-api' in args for args, _ in self.cluster.calls))
        runtime.apply(self.args)
        self.assertFalse(any(args[:2] == ['kubectl', 'delete'] for args, _ in self.cluster.calls))

    def test_commercial_preserves_tag_values_and_keys_on_reinstall(self):
        self.cluster.values['provider'] = {'custom': 'retain', 'settings': {'limit': 9},
                                           'image': {'tag': 'stale'}}
        self.desired['releases']['provider']['values']['settings'] = {'limit': 3, 'enabled': True}
        runtime.write(self.input, self.desired)
        values = self.prepare()
        self.assertEqual(values['image']['tag'], 'pinned')
        self.assertEqual(values['custom'], 'retain')
        self.assertEqual(values['settings'], {'limit': 9, 'enabled': True})
        runtime.apply(self.args)
        keys = copy.deepcopy(self.cluster.objects[('secret', 'provider-keys')])
        self.cluster.values['provider'] = values
        self.prepare()
        self.assertEqual(self.cluster.objects[('secret', 'provider-keys')], keys)

    def test_explicit_environment_values_override_existing_configuration(self):
        self.cluster.values['provider'] = {'settings': {'limit': 9}, 'extraEnv': [{'name': 'CUSTOM', 'value': 'old'}]}
        self.desired['releases']['provider']['environmentValues'] = {
            'settings': {'limit': 4}, 'extraEnv': [{'name': 'CUSTOM', 'value': 'new'}]}
        runtime.write(self.input, self.desired)
        values = self.prepare()
        self.assertEqual(values['settings']['limit'], 4)
        self.assertEqual(runtime.envs(values['extraEnv'])['CUSTOM']['value'], 'new')

    def test_plain_install_cannot_reuse_saved_commercial_profile(self):
        self.prepare()
        runtime.apply(self.args)
        self.args.profile = ''
        self.args.commercial = False
        with self.assertRaisesRegex(ValueError, 'commercial-manifest'):
            runtime.prepare(self.args)

    def test_new_consumer_environment_is_added_without_discarding_custom_entries(self):
        self.cluster.values['provider'] = {'extraEnv': [{'name': 'CUSTOM', 'value': 'retain'}]}
        values = self.prepare()
        self.assertEqual(runtime.envs(values['extraEnv'])['CUSTOM']['value'], 'retain')
        self.assertEqual(runtime.envs(values['extraEnv'])['PROVIDER_ENABLED']['value'], 'true')

    def test_required_storage_configuration_fails_before_secret_writes(self):
        self.desired['releases']['provider']['requiredValues'] = ['objectStorage.endpoint']
        runtime.write(self.input, self.desired)
        with self.assertRaisesRegex(ValueError, 'objectStorage.endpoint'):
            self.prepare()
        self.assertFalse(any(args[1] in ('create', 'replace', 'patch') for args, _ in self.cluster.calls))


class CommercialInstallerTest(unittest.TestCase):
    fixture_chart = installer.RuntimeInstallerTest.fixture_chart
    install = installer.RuntimeInstallerTest.install
    load = installer.RuntimeInstallerTest.load

    def setUp(self):
        installer.RuntimeInstallerTest.setUp(self)
        self.profile['releases']['bkn-backend']['requiredManifestStrings'] = []
        self.profile['releases']['agent-retrieval']['requiredManifestStrings'] = []
        self.input.write_text(json.dumps(self.profile))
        for name in ('bkn-backend', 'agent-retrieval', 'paid-addon'):
            chart = self.root / name
            if not chart.exists():
                self.fixture_chart(name)
            (chart / 'values.yaml').write_text('image:\n  repository: ' + name + '\n  tag: 0.2.2\n')
            (chart / 'templates/runtime.yaml').write_text('''apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ .Release.Name }}
spec:
  template:
    spec:
      containers:
        - name: app
          image: "{{ .Values.image.registry }}/{{ .Values.image.repository }}:{{ .Values.image.tag }}"
          env: {{ .Values.extraEnv | default list | toJson }}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: contract
data:
  upstream: {{ (.Values.upstream | default dict).url | default "" | quote }}
''')
            subprocess.run([self.helm, 'package', str(chart), '-d', str(self.charts)], check=True, capture_output=True)
        manifest = {'apiVersion': 'deploy.openbkn.ai/commercial/v1', 'kind': 'CommercialManifest',
                    'platformVersion': '0.2.2', 'version': '0.2.2', 'runtimeProfile': 'profile.json', 'releases': {}}
        for name, kind in [('bkn-backend', 'override'), ('paid-addon', 'addon')]:
            manifest['releases'][name] = {'type': kind, 'chart': name, 'chartVersion': '0.2.2',
                'image': {'repository': 'openbkn-ai-ee/' + name + '-ee', 'tag': '0.2.2-locked'}}
        self.commercial = self.root / 'commercial.json'
        self.commercial.write_text(json.dumps(manifest))
        self.community = self.root / 'community.yaml'
        self.community.write_text(yaml.safe_dump({'apiVersion': 'deploy.openbkn.ai/v1alpha1', 'kind': 'VersionSet', 'product': 'bkn-foundry', 'version': '0.2.2', 'releases': {
            name: {'chart': name, 'version': '0.2.2'} for name in ('bkn-backend', 'agent-retrieval')}}))
        script = self.script.read_text()
        # Exercise real manifest readers and compilation, leaving infrastructure mocked.
        for function in ('_openbkn_release_names', '_openbkn_resolve_release_version',
                         '_openbkn_resolve_chart_name', '_manifest_read_top_level_field'):
            script = re.sub(function + r'\(\)\{[^}]*\};?', '', script)
        script = script.replace('CORE_RUNTIME_PROFILE="$RUNTIME_TEST_INPUT"', 'CORE_RUNTIME_PROFILE=""')
        script = script.replace('OFFLINE_MODE=false', 'OFFLINE_MODE=true\nOFFLINE_REGISTRY=registry.local:5000')
        script = script.replace('CORE_SET_VALUES=(image.registry=community.example)',
                                'CORE_SET_VALUES=(image.registry=registry.local:5000/openbkn-ai)')
        script = script.replace('CORE_VERSION_MANIFEST_FILE=fixture',
                                'CORE_VERSION_MANIFEST_FILE="' + str(self.community) + '"\n'
                                'CORE_LOCAL_CHARTS_DIR="' + str(self.charts) + '"\n'
                                'CORE_COMMERCIAL_MANIFEST="' + str(self.commercial) + '"')
        self.script.write_text(script)

    def test_full_install_has_locked_images_addons_and_no_license_gate(self):
        result = self.install()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        state = self.load()
        self.assertIn('helm upgrade paid-addon', state['writes'])
        self.assertIn('registry.local:5000/openbkn-ai-ee/bkn-backend-ee:0.2.2-locked', state['renders']['bkn-backend'])
        first = copy.deepcopy(state['objects']['secret|provider-keys']['data'])
        result = self.install()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.load()['objects']['secret|provider-keys']['data'], first)

    def mock_downloads(self, fail=False):
        # Serve fixture archives through the real shell fallback without network access.
        remote = self.root / 'remote'
        remote.mkdir()
        for chart in self.charts.glob('*.tgz'):
            (remote / chart.name).write_bytes(chart.read_bytes())
        definitions = '''
ensure_helm_available(){ :; }
ensure_chart_source(){ echo source >> "PLACEHOLDER/downloads"; }
download_chart_to_cache(){
    echo "$3 $4" >> "PLACEHOLDER/downloads"
    cp "PLACEHOLDER/remote/$3-$4.tgz" "$1/"
}
'''
        definitions = definitions.replace('PLACEHOLDER', str(self.root))
        if fail:
            definitions += 'download_chart_to_cache(){ return 1; }\n'
        self.script.write_text(self.script.read_text().replace(
            '\ninstall_openbkn\n', definitions + '\ninstall_openbkn\n', 1))

    def test_missing_chart_downloads_only_locked_missing_archive(self):
        self.mock_downloads()
        (self.charts / 'paid-addon-0.2.2.tgz').unlink()
        result = self.install()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual((self.root / 'downloads').read_text().splitlines(),
                         ['source', 'paid-addon 0.2.2'])
        self.assertIn('helm upgrade paid-addon', self.load()['writes'])

    def test_no_charts_directory_uses_cache_and_downloads(self):
        self.mock_downloads()
        cache = self.root / 'cache'
        script = self.script.read_text().replace(
            'CORE_LOCAL_CHARTS_DIR="' + str(self.charts) + '"', 'CORE_LOCAL_CHARTS_DIR=""')
        script = script.replace('\ninstall_openbkn\n',
                                'resolve_shared_charts_dir(){ echo "' + str(cache) + '"; }\ninstall_openbkn\n', 1)
        script = re.sub(r'_openbkn_resolve_charts_dir\(\)\{[^}]*\}', '', script)
        self.script.write_text(script)
        result = self.install()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(len(list(cache.glob('*.tgz'))), 3)
        self.assertIn('helm upgrade paid-addon', self.load()['writes'])

    def test_missing_chart_and_online_mode_fail_before_writes(self):
        self.mock_downloads(fail=True)
        (self.charts / 'paid-addon-0.2.2.tgz').unlink()
        result = self.install()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.load()['writes'], [])
        self.script.write_text(self.script.read_text().replace('OFFLINE_MODE=true', 'OFFLINE_MODE=false'))
        result = self.install()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.load()['writes'], [])


if __name__ == '__main__':
    unittest.main()
