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
