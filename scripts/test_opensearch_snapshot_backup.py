"""Snapshot install values: configuration preservation and chart rendering."""
import copy
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('snapshot_values', ROOT / 'scripts/lib/opensearch_snapshot.py')
snapshot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(snapshot)


class SnapshotInstallTests(unittest.TestCase):
    def test_ensure_fills_missing_values_then_retains_existing_storage(self):
        values = snapshot.ensure({}, '/backup/custom', 'busybox:1.36.1')
        self.assertEqual(values, snapshot.merge({}, '/backup/custom', 'busybox:1.36.1'))
        self.assertEqual(snapshot.ensure(values, '/mnt/openbkn-snapshots', 'other-image'), {})
        partial = {'extraVolumes': values['extraVolumes']}
        repaired = snapshot.ensure(partial, '/mnt/openbkn-snapshots', 'busybox:1.36.1')
        self.assertEqual(repaired['extraVolumes'][0]['hostPath']['path'], '/backup/custom')

    def test_ensure_reuses_manual_persistent_mount_and_only_fills_path_repo(self):
        for source in ({'hostPath': {'path': '/existing/repo', 'type': 'Directory'}},
                       {'persistentVolumeClaim': {'claimName': 'snapshots'}},
                       {'nfs': {'server': 'storage', 'path': '/repo'}}):
            with self.subTest(source=source):
                values = {'config': {'opensearch.yml': 'plugins.security.disabled: false\n'},
                          'extraVolumes': [{'name': 'manual-repo', **source}],
                          'extraVolumeMounts': [{'name': 'manual-repo', 'mountPath': snapshot.REPOSITORY}]}
                result = snapshot.ensure(values, '/mnt/openbkn-snapshots', 'busybox')
                self.assertEqual(set(result), {'config'})
                self.assertIn('plugins.security.disabled: false', result['config']['opensearch.yml'])
                self.assertEqual(snapshot.ensure({**values, **result}, '/mnt/openbkn-snapshots', 'busybox'), {})

    def test_ensure_refuses_readonly_ephemeral_or_ambiguous_mounts(self):
        mount = {'name': 'repo', 'mountPath': snapshot.REPOSITORY}
        for mounts, volumes in (
            ([{**mount, 'readOnly': True}], [{'name': 'repo', 'hostPath': {'path': '/repo'}}]),
            ([mount], [{'name': 'repo', 'emptyDir': {}}]),
            ([mount], [{'name': 'repo', 'nfs': {'server': 'storage', 'path': '/repo', 'readOnly': True}}]),
            ([mount], [{'name': 'repo', 'persistentVolumeClaim': {'claimName': 'repo', 'readOnly': True}}]),
            ([mount], []),
            ([mount, mount], [{'name': 'repo', 'persistentVolumeClaim': {'claimName': 'repo'}}]),
        ):
            with self.subTest(mounts=mounts, volumes=volumes), self.assertRaises(ValueError):
                snapshot.ensure({'extraVolumeMounts': mounts, 'extraVolumes': volumes},
                                '/mnt/openbkn-snapshots', 'busybox')

    def test_preserves_custom_configuration_mounts_and_uid(self):
        original = {'config': {'opensearch.yml': 'cluster.name: custom\nplugins.security.disabled: false\n',
                               'log4j2.properties': 'custom'},
                    'extraVolumes': [{'name': 'certs', 'secret': {'secretName': 'certs'}}],
                    'extraVolumeMounts': [{'name': 'certs', 'mountPath': '/certs'}],
                    'extraInitContainers': [{'name': 'custom-init', 'image': 'custom'}],
                    'securityContext': {'runAsUser': 1234}, 'podSecurityContext': {'fsGroup': 1235}}
        before = copy.deepcopy(original)
        result = snapshot.merge(original, '/backup/opensearch', 'busybox:1.36.1')
        self.assertEqual(original, before)
        self.assertTrue(result['config']['opensearch.yml'].startswith(original['config']['opensearch.yml']))
        self.assertEqual(result['config']['log4j2.properties'], 'custom')
        for key in ('extraVolumes', 'extraVolumeMounts', 'extraInitContainers'):
            self.assertEqual(result[key][0], original[key][0])
        init = result['extraInitContainers'][-1]
        self.assertEqual(init['command'][-2:], ['1234', '1235'])
        self.assertEqual(init['image'], 'busybox:1.36.1')
        self.assertEqual(result, snapshot.merge({**original, **result}, '/backup/opensearch', 'busybox:1.36.1'))

    def test_refuses_unsafe_paths_and_conflicting_volumes(self):
        for path in ('/', '/mnt', 'relative/path', '/mnt/../data', '/usr/share/opensearch/data'):
            with self.subTest(path=path), self.assertRaises(ValueError):
                snapshot.merge({}, path, 'busybox')
        cases = [
            {'extraVolumes': [{'name': 'snapshot-repo', 'hostPath': {'path': '/other/repo'}}]},
            {'extraVolumeMounts': [{'name': 'data', 'mountPath': snapshot.REPOSITORY}]},
            {'extraVolumes': 'custom templated YAML'},
            {'config': {'opensearch.yml': 'path.repo: ["/custom/repo"]\n'}},
        ]
        for values in cases:
            with self.subTest(values=values), self.assertRaises(ValueError):
                snapshot.merge(values, '/mnt/openbkn-snapshots', 'busybox')

    @unittest.skipUnless(shutil.which('helm'), 'Helm is needed for offline chart rendering')
    def test_chart_renders_snapshot_volume_init_and_path_repo(self):
        values = snapshot.merge({'singleNode': True}, '/backup/opensearch', 'busybox:1.36.1')
        output = subprocess.run(['helm', 'template', 'opensearch', str(ROOT / 'charts/opensearch-2.36.0.tgz'),
                                 '-f', '-'], input=json.dumps(values), text=True, capture_output=True, check=True).stdout
        for text in ('path.repo: ["/usr/share/opensearch/snapshots"]', 'name: snapshot-repo',
                     'path: /backup/opensearch', 'type: DirectoryOrCreate',
                     'name: prepare-snapshot-repo', 'mountPath: /usr/share/opensearch/snapshots',
                     'chmod 700 /snapshots'):
            self.assertIn(text, output)

    def test_install_and_existing_enable_pass_merged_values_to_helm(self):
        # Every cluster/Helm mutation is intercepted; the real shell installer
        # must pass the correct generated files before cleaning them up.
        with tempfile.TemporaryDirectory() as tmp:
            for mode in ('fresh', 'disabled', 'existing'):
                with self.subTest(mode=mode):
                    output = Path(tmp) / (mode + '.json')
                    script = r'''set -euo pipefail
SCRIPT_DIR="$1"
source "$SCRIPT_DIR/scripts/lib/common.sh"
source "$SCRIPT_DIR/scripts/services/opensearch.sh"
MODE="$2"; OUTPUT="$3"
log_info() { :; }; log_warn() { :; }; log_error() { echo "$*" >&2; }
is_helm_installed() { [[ "$MODE" == existing ]]; }
kubectl() { :; }
config_yaml_dep_field() { echo test-password; }
bkn_helm_uninstall_if_not_deployed() { :; }
OPENSEARCH_PERSISTENCE_ENABLED=false
OPENSEARCH_MEMORY_REQUEST=512Mi
OPENSEARCH_JAVA_OPTS='-Xms512m -Xmx512m'
OPENSEARCH_SNAPSHOT_HOST_PATH=/backup/test-snapshots
AUTO_GENERATE_CONFIG=false
[[ "$MODE" != disabled ]] || OPENSEARCH_SNAPSHOT_ENABLED=false
helm() {
    if [[ "$1" == get ]]; then
        echo '{"config":{"opensearch.yml":"cluster.name: original\nplugins.security.disabled: false\n"},"extraVolumes":[{"name":"keep","emptyDir":{}}]}'
        return
    fi
    python3 - "$OUTPUT" "$MODE" "$@" <<'CHECK'
import json,sys
args=sys.argv[3:]; mode=sys.argv[2]
if mode=='disabled':
 assert '-f' not in args
else:
 values=json.load(open(args[args.index('-f')+1]))
 assert values['extraVolumes'][-1]['hostPath']['path']=='/backup/test-snapshots'
 assert values['extraInitContainers'][-1]['name']=='prepare-snapshot-repo'
 if mode=='existing':
  assert '--reuse-values' in args
  assert 'plugins.security.disabled: false' in values['config']['opensearch.yml']
  assert values['extraVolumes'][0]['name']=='keep'
 else:
  index=args.index('--set-file')
  config=open(args[index+1].split('=',1)[1]).read()
  assert 'path.repo: ["/usr/share/opensearch/snapshots"]' in config
json.dump({'passed':True},open(sys.argv[1],'w'))
CHECK
}
if [[ "$MODE" == existing ]]; then enable_opensearch_snapshots; else install_opensearch; fi
'''
                    subprocess.run(['bash', '-c', script, 'snapshot-test', str(ROOT), mode, str(output)],
                                   check=True, capture_output=True, text=True)
                    self.assertTrue(json.loads(output.read_text())['passed'])

    def test_install_reconciles_legacy_releases_and_preserves_values(self):
        original = {'config': {'opensearch.yml': 'cluster.name: original\nplugins.security.disabled: false\n'},
                    'image': {'repository': 'custom/opensearch', 'tag': '2.19.4'},
                    'opensearchJavaOpts': '-Xms1g -Xmx1g',
                    'resources': {'requests': {'memory': '2Gi'}},
                    'extraVolumes': [{'name': 'certs', 'secret': {'secretName': 'certs'}}],
                    'extraVolumeMounts': [{'name': 'certs', 'mountPath': '/certs'}],
                    'extraInitContainers': [{'name': 'custom', 'image': 'custom'}]}
        configured = {**original, **snapshot.merge(original, '/backup/retained', 'busybox:1.36.1')}
        cases = [
            ('missing', original, 1, True, 0, 1),
            ('complete', configured, 1, True, 0, 0),
            ('disabled', original, 1, False, 0, 0),
            ('limits', {**original, 'resources': {'requests': {'memory': '2Gi'}, 'limits': {'memory': '3Gi'}}},
             0, True, 0, 1),
            ('limits-complete', {**configured, 'resources': {'requests': {'memory': '2Gi'}, 'limits': {'memory': '3Gi'}}},
             0, True, 0, 1),
            ('conflict', {**original, 'config': {'opensearch.yml': 'path.repo: ["/another/path"]\n'}},
             1, True, 1, 0),
            ('read-failure', original, 1, True, 1, 0),
            ('upgrade-failure', original, 1, True, 1, 1),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            for mode, values, limit_status, enabled, failure, updates in cases:
                with self.subTest(mode=mode):
                    root = Path(tmp)
                    fixture, output = root / 'values.json', root / 'calls.jsonl'
                    values = copy.deepcopy(values)
                    if not limit_status:
                        values['extraInitContainers'][0]['resources'] = {
                            'requests': {'cpu': '10m'}, 'limits': {'memory': '1Gi'}}
                    fixture.write_text(json.dumps(values))
                    output.write_text('')
                    script = r'''set -euo pipefail
SCRIPT_DIR="$1"
source "$SCRIPT_DIR/scripts/lib/common.sh"
source "$SCRIPT_DIR/scripts/services/opensearch.sh"
MODE="$2"; FIXTURE="$3"; OUTPUT="$4"; LIMIT_STATUS="$5"; OPENSEARCH_SNAPSHOT_ENABLED="$6"
log_info() { :; }; log_warn() { :; }; log_error() { echo "$*" >&2; }
is_helm_installed() { return 0; }
bkn_helm_release_has_resource_limits() { return "$LIMIT_STATUS"; }
kubectl() { :; }
config_yaml_dep_field() { echo test-password; }
bkn_helm_uninstall_if_not_deployed() { :; }
OPENSEARCH_PERSISTENCE_ENABLED=false
OPENSEARCH_SNAPSHOT_HOST_PATH=/backup/test-snapshots
AUTO_GENERATE_CONFIG=false
helm() {
    if [[ "$1" == get ]]; then
        [[ "$MODE" != read-failure ]] || return 1
        cat "$FIXTURE"
        return
    fi
    python3 - "$OUTPUT" "$FIXTURE" "$@" <<'CHECK'
import json,sys
args=sys.argv[3:]
base=json.load(open(sys.argv[2]))
# Read every overlay in actual Helm order; arrays are replaced, not appended.
values={**base} if '--reuse-values' in args else {}
for index,arg in enumerate(args[:-1]):
 if arg=='-f': values.update(json.load(open(args[index+1])))
 if arg=='--set-file':
  key,path=args[index+1].split('=',1)
  if key=='config.opensearch\\.yml':
   values.setdefault('config',{})['opensearch.yml']=open(path).read()
with open(sys.argv[1],'a') as f: f.write(json.dumps({'args':args,'values':values})+'\n')
CHECK
    [[ "$MODE" != upgrade-failure ]]
}
install_opensearch
'''
                    result = subprocess.run(['bash', '-c', script, 'snapshot-upgrade-test', str(ROOT), mode,
                                             str(fixture), str(output), str(limit_status), str(enabled).lower()],
                                            capture_output=True, text=True)
                    self.assertEqual(bool(result.returncode), bool(failure), result.stderr)
                    calls = [json.loads(line) for line in output.read_text().splitlines()]
                    self.assertEqual(len(calls), updates, result.stderr)
                    if not updates:
                        continue
                    actual = calls[0]['values']
                    self.assertIn('path.repo: ["/usr/share/opensearch/snapshots"]', actual['config']['opensearch.yml'])
                    self.assertIn('plugins.security.disabled: false', actual['config']['opensearch.yml'])
                    for key in ('extraVolumes', 'extraVolumeMounts', 'extraInitContainers'):
                        expected = copy.deepcopy(values[key][0])
                        if key == 'extraInitContainers' and not limit_status:
                            expected['resources'].pop('limits')
                        self.assertEqual(actual[key][0], expected)
                    expected_path = '/backup/retained' if mode == 'limits-complete' else '/backup/test-snapshots'
                    self.assertEqual(actual['extraVolumes'][-1]['hostPath']['path'], expected_path)
                    self.assertEqual(actual['resources']['requests']['memory'], '2Gi')
                    self.assertEqual(actual['opensearchJavaOpts'], original['opensearchJavaOpts'])
                    if limit_status:
                        self.assertIn('--reuse-values', calls[0]['args'])
                        self.assertEqual(actual['image'], original['image'])
                    else:
                        self.assertNotIn('limits', actual['resources'])
                        self.assertNotIn('limits', actual['extraInitContainers'][0]['resources'])
                    if mode == 'limits' and shutil.which('helm'):
                        rendered = subprocess.run(
                            ['helm', 'template', 'opensearch', str(ROOT / 'charts/opensearch-2.36.0.tgz'), '-f', '-'],
                            input=json.dumps(actual), text=True, capture_output=True, check=True).stdout
                        self.assertIn('path.repo: ["/usr/share/opensearch/snapshots"]', rendered)
                        self.assertIn('path: /backup/test-snapshots', rendered)
                        self.assertIn('name: prepare-snapshot-repo', rendered)
