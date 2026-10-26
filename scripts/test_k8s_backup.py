"""K8s inventory is read-only and must never enter automatic restore."""
import contextlib
import io
import json
import tarfile
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import backup
from k8s_backup import K8sBackup, MODE, clean, manifest, write_package, validate_archive


def make_package(path):
    write_package(path, {'mode': MODE, 'restore_supported': False,
                         'namespaces': [], 'cluster_resources': []}, backup.BackupError)
    return {'mode': MODE, 'restore_supported': False, 'namespaces': ['openbkn'], 'secrets': []}


class K8sTests(unittest.TestCase):
    def setUp(self):
        self.args = backup.parser().parse_args(['backup', 'check', '--component', 'k8s',
                                              '--k8s-namespaces', 'openbkn'])
        self.calls = []
        self.secret_type = 'Opaque'
        self.service = K8sBackup(self.args, self.run_command, backup.BackupError, lambda *a: None)

    def run_command(self, command, **kwargs):
        self.calls.append(command)
        if command[0] == 'helm':
            result = [{'name': 'app', 'chart': 'app-1.2.3', 'revision': '1'}] if command[1] == 'list' else {'custom': 'kept'}
        elif command[1] == 'version':
            result = {'serverVersion': {'gitVersion': 'v1.30.0'}}
        elif 'namespace' in command:
            result = {'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': 'openbkn'}}
        elif 'secret' in command:
            result = {'apiVersion': 'v1', 'kind': 'Secret', 'type': self.secret_type,
                      'metadata': {'name': command[command.index('secret') + 1], 'namespace': 'openbkn', 'resourceVersion': '123'},
                      'data': {'status': 'c2VjcmV0'}}
        elif 'persistentvolumes' in command:
            result = {'items': [ {'apiVersion': 'v1', 'kind': 'PersistentVolume', 'metadata': {'name': ns}, 'spec': {'claimRef': {'namespace': ns}}}
                                for ns in ('openbkn', 'other')]}
        elif 'storageclasses' in command:
            result = {'items': []}
        else:
            result = {'items': [{'apiVersion': 'v1', 'kind': 'ConfigMap',
                                 'metadata': {'name': 'custom', 'uid': 'old', 'managedFields': []},
                                 'data': {'status': 'important'}, 'status': {'ignored': True}}]}
        return json.dumps(result).encode()

    def test_read_only_inventory_filters_pvs_and_has_no_default_secret_export(self):
        document = self.service.collect()
        self.assertEqual([r['metadata']['name'] for r in document['cluster_resources']], ['openbkn'])
        self.assertEqual(document['secrets'], [])
        self.assertFalse(any('secret' in call for call in self.calls))
        resource = document['namespaces'][0]['resources'][0]
        self.assertEqual(resource['data']['status'], 'important')
        self.assertNotIn('status', resource)
        self.assertNotIn('uid', resource['metadata'])
        self.assertEqual(document['namespaces'][0]['helm'][0]['values'], {'custom': 'kept'})
        self.assertTrue(all(call[1] in ('get', 'version', 'list', '-n') for call in self.calls))
        self.assertFalse(any('apply' in c or 'create' in c or 'delete' in c for c in self.calls))

    def test_explicit_secret_allowlist_and_internal_token_rejection(self):
        self.args.k8s_secrets = ['openbkn/app-key']
        service = K8sBackup(self.args, self.run_command, backup.BackupError, lambda *a: None)
        self.assertEqual(service.collect()['secrets'][0]['data']['status'], 'c2VjcmV0')
        for secret_type in ('kubernetes.io/service-account-token', 'helm.sh/release.v1'):
            self.secret_type = secret_type
            with self.assertRaisesRegex(backup.BackupError, '不允许导出'):
                service.collect()
        self.args.k8s_secrets = ['other/app-key']
        with self.assertRaisesRegex(backup.BackupError, 'namespace/name'):
            K8sBackup(self.args, self.run_command, backup.BackupError, lambda *a: None)

    def test_permission_failure_does_not_publish_partial_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'k8s.tar'
            self.service.run = mock.Mock(side_effect=backup.BackupError('Forbidden'))
            with self.assertRaisesRegex(backup.BackupError, 'Forbidden'):
                self.service.backup(path)
            self.assertFalse(path.exists())

    def test_explicit_k8s_restore_refused_before_cluster_or_archive_access(self):
        for args in (['--component', 'k8s'], ['--components', 'mariadb', 'k8s']):
            with mock.patch.object(backup, 'Cluster') as cluster, mock.patch.object(backup, 'unpack') as unpack:
                with self.assertRaisesRegex(backup.BackupError, '不支持自动恢复'):
                    backup.main(['restore', 'check', '/missing/package.tar.gz', *args])
                cluster.assert_not_called()
                unpack.assert_not_called()

    def test_default_restore_skips_inventory_and_still_restores_data(self):
        from test_component_backup import ComponentTests
        fixture = ComponentTests(); fixture.setUp()
        try:
            package = fixture.package(fixture.cluster(['mariadb', 'k8s']))
            with tempfile.TemporaryDirectory() as directory:
                backup.unpack(package, Path(directory), for_restore=True)
                self.assertFalse((Path(directory) / 'k8s.tar').exists())
                self.assertTrue((Path(directory) / 'mariadb.tar').exists())
            target = fixture.cluster(['mariadb'])
            def create(args):
                target.args, target.components = args, args.components
                return target
            with mock.patch.object(backup, 'Cluster', side_effect=create), mock.patch.object(target, 'check'), contextlib.redirect_stdout(io.StringIO()) as out:
                backup.main(['restore', 'run', str(package), '--writers-stopped', '--overwrite'])
            self.assertEqual(json.loads(out.getvalue())['components'], ['mariadb'])
            self.assertIn('import', target.events)
        finally:
            fixture.doCleanups()

    def test_inventory_only_default_restore_has_no_cluster_calls(self):
        from test_component_backup import ComponentTests
        fixture = ComponentTests(); fixture.setUp()
        try:
            package = fixture.package(fixture.cluster(['k8s']))
            with mock.patch.object(backup, 'Cluster') as cluster:
                with self.assertRaisesRegex(backup.BackupError, '没有可自动恢复'):
                    backup.main(['restore', 'run', str(package), '--writers-stopped'])
                cluster.assert_not_called()
        finally:
            fixture.doCleanups()

    def test_package_contains_individual_manifests_values_and_recovery_instructions(self):
        self.args.k8s_secrets = ['openbkn/app-key']
        service = K8sBackup(self.args, self.run_command, backup.BackupError, lambda *a: None)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'k8s.tar'
            metadata = service.backup(path)
            self.assertEqual(metadata['mode'], MODE)
            validate_archive(path, backup.BackupError)
            with tarfile.open(path) as archive:
                names = archive.getnames()
                self.assertIn('manifests/cluster/namespace/openbkn.json', names)
                self.assertIn('manifests/namespaced/openbkn/configmap/custom.json', names)
                self.assertIn('manifests/namespaced/openbkn/secret/app-key.json', names)
                self.assertIn('reference/persistentvolumes/openbkn.json', names)
                self.assertNotIn('manifests/cluster/persistentvolume/openbkn.json', names)
                self.assertIn('RESTORE.zh.md', names)
                values = json.load(archive.extractfile('helm/openbkn/app/values.json'))
                self.assertEqual(values, {'custom': 'kept'})
                cm = json.load(archive.extractfile('manifests/namespaced/openbkn/configmap/custom.json'))
                self.assertEqual(cm['metadata']['namespace'], 'openbkn')
                self.assertEqual(cm['data']['status'], 'important')
                self.assertNotIn('status', cm)
                self.assertNotIn('uid', cm['metadata'])

    def test_restore_manifests_remove_old_allocations_keep_headless_and_pvc_intent(self):
        service = {'apiVersion': 'v1', 'kind': 'Service',
                   'metadata': {'name': 'app', 'ownerReferences': [{'uid': 'old'}], 'finalizers': ['old']},
                   'spec': {'type': 'NodePort', 'clusterIP': '10.0.0.2', 'clusterIPs': ['10.0.0.2'],
                            'ports': [{'port': 80, 'nodePort': 30080}]}}
        result = manifest(service)
        self.assertNotIn('clusterIP', result['spec'])
        self.assertNotIn('nodePort', result['spec']['ports'][0])
        self.assertNotIn('ownerReferences', result['metadata'])
        self.assertEqual(service['spec']['clusterIP'], '10.0.0.2')
        service['spec']['clusterIP'] = 'None'
        self.assertEqual(manifest(service)['spec']['clusterIP'], 'None')
        pvc = {'kind': 'PersistentVolumeClaim', 'metadata': {'name': 'data', 'annotations': {
            'volume.kubernetes.io/selected-node': 'old-node', 'custom': 'keep'}},
            'spec': {'volumeName': 'old-pv', 'storageClassName': 'local', 'resources': {'requests': {'storage': '8Gi'}}}}
        cleaned = manifest(pvc)
        self.assertNotIn('volumeName', cleaned['spec'])
        self.assertEqual(cleaned['metadata']['annotations'], {'custom': 'keep'})
        self.assertEqual(cleaned['spec']['resources']['requests']['storage'], '8Gi')

    def test_nested_archive_integrity_rejects_modified_resource(self):
        with tempfile.TemporaryDirectory() as directory:
            good, bad = Path(directory) / 'good.tar', Path(directory) / 'bad.tar'
            self.service.backup(good)
            with tarfile.open(good) as source, tarfile.open(bad, 'w') as target:
                for member in source:
                    content = source.extractfile(member).read()
                    if member.name.endswith('custom.json'):
                        content += b'corrupted'
                        member.size = len(content)
                    target.addfile(member, io.BytesIO(content))
            with self.assertRaisesRegex(backup.BackupError, '校验失败'):
                validate_archive(bad, backup.BackupError)

    def test_legacy_single_json_package_is_readable(self):
        import hashlib
        from test_component_backup import ComponentTests
        fixture = ComponentTests(); fixture.setUp()
        try:
            original = fixture.package(fixture.cluster(['k8s']))
            legacy = fixture.root / 'legacy.tar.gz'
            content = json.dumps({'mode': 'kubernetes-reference-v1', 'restore_supported': False,
                                  'namespaces': [], 'cluster_resources': []}).encode()
            with tarfile.open(original) as source, tarfile.open(legacy, 'w:gz') as target:
                metadata = json.load(source.extractfile('manifest.json'))
                metadata['k8s']['mode'] = 'kubernetes-reference-v1'
                metadata['sha256'] = {'k8s.json': hashlib.sha256(content).hexdigest()}
                for name, data in [('manifest.json', json.dumps(metadata).encode()), ('k8s.json', content)]:
                    member = tarfile.TarInfo(name); member.size = len(data)
                    target.addfile(member, io.BytesIO(data))
            destination = fixture.root / 'legacy-extracted'
            backup.unpack(legacy, destination)
            self.assertEqual(json.loads((destination / 'k8s.json').read_text())['mode'], 'kubernetes-reference-v1')
        finally:
            fixture.doCleanups()
