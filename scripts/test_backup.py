"""Offline regression tests; no Kubernetes or production data access."""
import contextlib
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest import mock

import backup


class FakeCluster(backup.Cluster):
    def __init__(self, args):
        self.args = args
        self.components = getattr(args, 'components', ['mariadb', 'opensearch'] if args.component == 'all' else [args.component])
        self.databases = ['openbkn', 'safe', 'bkn_trace']
        self.mariadb_version = '11.4.7-MariaDB'
        self.opensearch_version = '2.19.4'
        self.events = []
        self.old = [{'snapshot': 'old', 'state': 'SUCCESS', 'indices': ['business']}]
        self.new = None
        self.failed = False
        self.tables = b''
        self.conflicts = []
        self.directory = b''
        self.repositories = {}
        self.fail_import = False
        self.engine = b''
        self.commands = []

    @contextlib.contextmanager
    def lock(self):
        yield

    def sql(self, query=None, *, source=None, target=None, databases=None):
        if target is not None:
            self.events.append('dump')
            target.write(b'-- database dump\nCREATE DATABASE `openbkn`;\n')
        elif source is not None:
            self.events.append('import')
            if self.fail_import:
                raise backup.BackupError('import failed')
            assert source.read()
        elif 'ENGINE' in query:
            return self.engine
        elif 'information_schema.TABLES' in query:
            return self.tables
        return b''

    def exec(self, service, command, **kwargs):
        self.commands.append((service, command))
        if command[0] == 'tar' and '-cf' in command:
            self.events.append('export-repository')
            with tarfile.open(fileobj=kwargs['target'], mode='w:') as archive:
                content = b'repository metadata'
                member = tarfile.TarInfo('./index-0')
                member.size = len(content)
                archive.addfile(member, io.BytesIO(content))
        elif command[0] == 'tar' and '-xf' in command:
            self.events.append('upload-repository')
            assert kwargs['source'].read()
        elif command[0] == 'sh' and 'for item in' in command[2]:
            return self.directory
        return b''

    def api(self, method, path, body=None, **kwargs):
        if path == '/_snapshot/_status':
            return {'snapshots': []}
        if path == '/_snapshot':
            return self.repositories
        if path == self.repository and method == 'PUT':
            self.events.append('register-readonly' if body['settings']['readonly'] else 'register')
            return {'acknowledged': True}
        if path.endswith('/_verify'):
            return {'nodes': {'node-0': {}}}
        if path.endswith('/_all'):
            return {'snapshots': self.old + ([self.new] if self.new else [])}
        if path.endswith('/_restore?wait_for_completion=true'):
            self.events.append('restore-snapshot')
            return {'snapshot': {'shards': {'failed': 0, 'successful': 1, 'total': 1}}}
        if path.startswith('/_cat/indices'):
            return [{'index': name} for name in self.conflicts]
        if path.startswith('/_cluster/health'):
            return {'status': 'yellow', 'timed_out': False}
        if method == 'PUT':
            self.events.append('create-snapshot')
            self.new = {'snapshot': path.rsplit('/', 1)[1],
                        'state': 'FAILED' if self.failed else 'SUCCESS', 'indices': ['business']}
            return {'accepted': True}
        if method == 'DELETE':
            self.events.append('delete:' + path.rsplit('/', 1)[1])
            if path.endswith('/old'):
                self.old = []
            return {'acknowledged': True}
        if method == 'GET':
            return {'snapshots': [self.new]}
        raise AssertionError((method, path))


class BackupTests(unittest.TestCase):
    def setUp(self):
        self.quiet = mock.patch.object(backup, 'log')
        self.quiet.start()
        self.addCleanup(self.quiet.stop)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.args = backup.parser().parse_args(['backup', 'run', '--skip-redis', '--skip-minio'])
        self.cluster = FakeCluster(self.args)

    def package(self):
        with contextlib.redirect_stdout(io.StringIO()):
            self.cluster.backup(self.root)
        package = next(self.root.glob('*.tar.gz'))
        stage = self.root / 'restore'
        stage.mkdir(exist_ok=True)
        manifest = backup.unpack(package, stage)
        return package, stage, manifest

    def test_backup_portable_archive_and_snapshot_prune_order(self):
        package, stage, manifest = self.package()
        self.assertTrue(package.stat().st_size)
        self.assertEqual(manifest['databases'], self.cluster.databases)
        self.assertEqual(manifest['indices'], ['business'])
        self.assertEqual(self.cluster.events[:5], ['dump', 'register', 'create-snapshot', 'delete:old', 'export-repository'])
        self.assertEqual(len(list(self.root.glob('*.partial'))), 0)
        self.assertEqual(backup.digest(stage / 'mariadb.sql'), manifest['sha256']['mariadb.sql'])

    def test_failed_snapshot_never_prunes_or_publishes(self):
        self.cluster.failed = True
        with self.assertRaisesRegex(backup.BackupError, '快照失败'):
            self.cluster.backup(self.root)
        self.assertNotIn('delete:old', self.cluster.events)
        self.assertFalse(list(self.root.glob('*.tar.gz')))

    def test_dump_failure_never_creates_snapshot(self):
        with mock.patch.object(self.cluster, 'sql', side_effect=backup.BackupError('dump failed')):
            with self.assertRaises(backup.BackupError):
                self.cluster.backup(self.root)
        self.assertNotIn('create-snapshot', self.cluster.events)

    def test_non_innodb_table_rejected(self):
        self.cluster.engine = b'openbkn\ttable\tMyISAM'
        with self.assertRaisesRegex(backup.BackupError, '非 InnoDB'):
            self.cluster.backup(self.root)
        self.assertFalse(self.cluster.events)

    def test_restore_roundtrip(self):
        _, stage, manifest = self.package()
        self.cluster.args.writers_stopped = True
        self.cluster.events.clear()
        with contextlib.redirect_stdout(io.StringIO()):
            self.cluster.restore(stage, manifest)
        self.assertEqual(self.cluster.events, ['upload-repository', 'register-readonly', 'restore-snapshot', 'import'])

    def test_restore_refuses_existing_data_before_writes(self):
        _, stage, manifest = self.package()
        self.cluster.args.writers_stopped = True
        self.cluster.tables = b'openbkn\tbusiness'
        self.cluster.events.clear()
        with self.assertRaisesRegex(backup.BackupError, '--overwrite'):
            self.cluster.restore(stage, manifest)
        self.assertEqual(self.cluster.events, [])

    def test_restore_requires_stopped_writers(self):
        _, stage, manifest = self.package()
        self.cluster.events.clear()
        with self.assertRaisesRegex(backup.BackupError, '--writers-stopped'):
            self.cluster.restore(stage, manifest)
        self.assertEqual(self.cluster.events, [])

    def test_overwrite_deletes_only_conflicting_indices(self):
        _, stage, manifest = self.package()
        self.args.writers_stopped = self.args.overwrite = True
        self.cluster.conflicts = ['business', 'other-index']
        self.cluster.events.clear()
        with contextlib.redirect_stdout(io.StringIO()):
            self.cluster.restore(stage, manifest)
        self.assertIn('delete:business', self.cluster.events)
        self.assertNotIn('delete:other-index', self.cluster.events)
        self.assertLess(self.cluster.events.index('delete:business'), self.cluster.events.index('restore-snapshot'))

    def test_restore_never_merges_repository_files(self):
        _, stage, manifest = self.package()
        self.args.writers_stopped = self.args.overwrite = True
        self.cluster.directory = b'/snapshots/index-0'
        self.cluster.events.clear()
        with self.assertRaisesRegex(backup.BackupError, '非空'):
            self.cluster.restore(stage, manifest)
        self.assertEqual(self.cluster.events, [])

    def test_restore_directory_check_works_without_find_command(self):
        _, stage, manifest = self.package()
        self.args.writers_stopped = True
        self.cluster.events.clear()
        with contextlib.redirect_stdout(io.StringIO()):
            self.cluster.restore(stage, manifest)
        shell_checks = [command for service, command in self.cluster.commands
                        if service == 'opensearch' and command[0] == 'sh' and 'for item in' in command[2]]
        self.assertTrue(shell_checks)
        self.assertFalse(any(command[0] == 'find' for service, command in self.cluster.commands))

    def test_incompatible_versions_before_writes(self):
        _, stage, manifest = self.package()
        self.args.writers_stopped = True
        self.cluster.opensearch_version = '3.0.0'
        self.cluster.events.clear()
        with self.assertRaisesRegex(backup.BackupError, '主次版本'):
            self.cluster.restore(stage, manifest)
        self.assertEqual(self.cluster.events, [])

    def test_changed_repository_during_export_not_published(self):
        snapshots = self.cluster.snapshots
        calls = []
        def changed():
            calls.append(1)
            result = snapshots()
            return result + ([{'snapshot': 'unexpected'}] if len(calls) >= 3 else [])
        with mock.patch.object(self.cluster, 'snapshots', side_effect=changed):
            with self.assertRaisesRegex(backup.BackupError, '其他任务修改'):
                self.cluster.backup(self.root)
        self.assertFalse(list(self.root.glob('*.tar.gz')))

    def rewrite_package(self, package, destination, *, bad_checksum=False, bad_index=False):
        with tarfile.open(package, 'r:gz') as src, tarfile.open(destination, 'w:gz') as dst:
            for member in src:
                data = src.extractfile(member).read()
                if member.name == 'manifest.json':
                    manifest = json.loads(data)
                    if bad_checksum:
                        manifest['sha256']['mariadb.sql'] = 'bad'
                    if bad_index:
                        manifest['indices'] = ['*']
                    data = json.dumps(manifest).encode()
                    member.size = len(data)
                dst.addfile(member, io.BytesIO(data))

    def test_corrupt_package_rejected_offline(self):
        package, _, _ = self.package()
        bad = self.root / 'bad.tar.gz'
        self.rewrite_package(package, bad, bad_checksum=True)
        with self.assertRaisesRegex(backup.BackupError, '校验失败'):
            backup.unpack(bad, self.root / 'bad')

    def test_wildcard_index_rejected_offline(self):
        package, _, _ = self.package()
        bad = self.root / 'bad.tar.gz'
        self.rewrite_package(package, bad, bad_index=True)
        with self.assertRaisesRegex(backup.BackupError, '索引清单'):
            backup.unpack(bad, self.root / 'bad')

    def test_unsafe_paths_links_and_duplicate_paths_rejected(self):
        for name, kind in [('../escape', tarfile.REGTYPE), ('/absolute', tarfile.REGTYPE),
                           ('link', tarfile.SYMTYPE), ('device', tarfile.CHRTYPE)]:
            with self.subTest(name=name):
                entry = tarfile.TarInfo(name)
                entry.type = kind
                with self.assertRaises(backup.BackupError):
                    list(backup.safe_members([entry]))
        with self.assertRaisesRegex(backup.BackupError, '重复路径'):
            list(backup.safe_members([tarfile.TarInfo('file'), tarfile.TarInfo('./file')]))

    def test_existing_repository_not_repointed(self):
        self.cluster.repositories = {'offsite-backup': {'type': 's3', 'settings': {}}}
        with self.assertRaisesRegex(backup.BackupError, '其他存储'):
            self.cluster.register()
        self.assertEqual(self.cluster.events, [])

    def test_dump_command_uses_single_transaction_and_all_selected_databases(self):
        instance = object.__new__(backup.Cluster)
        with mock.patch.object(instance, 'exec', return_value=b'') as execute:
            instance.sql(databases=['openbkn', 'safe', 'bkn_trace'])
            command = execute.call_args.args[1]
            self.assertIn('--single-transaction', command[2])
            self.assertIn('--routines --events --triggers', command[2])
            self.assertEqual(command[-3:], ['openbkn', 'safe', 'bkn_trace'])
            self.assertIn('MYSQL_PWD', command[2])

    def test_partial_opensearch_restore_does_not_import_sql(self):
        _, stage, manifest = self.package()
        self.args.writers_stopped = True
        api = self.cluster.api
        def partial(method, path, body=None, **kwargs):
            if '/_restore?' in path:
                return {'snapshot': {'shards': {'total': 2, 'successful': 1, 'failed': 1}}}
            return api(method, path, body, **kwargs)
        self.cluster.events.clear()
        with mock.patch.object(self.cluster, 'api', side_effect=partial):
            with self.assertRaisesRegex(backup.BackupError, '恢复不完整'):
                self.cluster.restore(stage, manifest)
        self.assertNotIn('import', self.cluster.events)

    def test_sql_import_failure_never_reports_success(self):
        _, stage, manifest = self.package()
        self.args.writers_stopped = True
        self.cluster.fail_import = True
        with contextlib.redirect_stdout(io.StringIO()) as output:
            with self.assertRaisesRegex(backup.BackupError, 'import failed'):
                self.cluster.restore(stage, manifest)
        self.assertEqual(output.getvalue(), '')

    def test_invalid_archive_never_connects_to_cluster(self):
        package, _, _ = self.package()
        bad = self.root / 'bad.tar.gz'
        self.rewrite_package(package, bad, bad_checksum=True)
        with mock.patch.object(backup, 'Cluster') as cluster:
            with self.assertRaises(backup.BackupError):
                backup.main(['restore', 'run', str(bad), '--writers-stopped'])
        cluster.assert_not_called()

    def test_api_reports_opensearch_error_body(self):
        instance = object.__new__(backup.Cluster)
        with mock.patch.object(instance, 'exec', return_value=b'{"error":"repository not writable"}\n500'):
            with self.assertRaisesRegex(backup.BackupError, 'repository not writable'):
                instance.api('PUT', '/_snapshot/offsite-backup', {})

    def test_discovery_and_check_against_kubectl_contract(self):
        def pod(service):
            return {'metadata': {'name': service + '-0'},
                    'status': {'phase': 'Running', 'conditions': [{'type': 'Ready', 'status': 'True'}]},
                    'spec': {'containers': [{'name': service, 'volumeMounts': [
                        {'name': 'repo', 'mountPath': self.args.repository_path}]}],
                             'volumes': [{'name': 'repo', 'hostPath': {'path': '/mnt/openbkn-snapshots'}}]}}
        def kubectl(argv, **kwargs):
            service = 'mariadb' if 'app=mariadb' in argv else 'opensearch'
            if 'get' in argv:
                obj = {'items': [pod(service)]} if 'pods' in argv else pod(service)
                return json.dumps(obj).encode()
            if argv[-1].startswith('http://127.0.0.1:9200'):
                obj = {'nodes': {'one': {'settings': {'path.repo': [self.args.repository_path]}}}} if '/_nodes/' in argv[-1] else {'version': {'number': '2.19.4'}}
                return json.dumps(obj).encode() + b'\n200'
            query = kwargs.get('data', b'')
            if query == b'SELECT VERSION();':
                return b'11.4.7-MariaDB\n'
            if query == b'SHOW DATABASES;':
                return b'mysql\ninformation_schema\nperformance_schema\nsys\nopenbkn\nsafe\nbkn_trace\n'
            return b''
        with mock.patch.object(backup, 'run', side_effect=kubectl):
            cluster = backup.Cluster(self.args)
            cluster.check()
        self.assertEqual(cluster.databases, ['openbkn', 'safe', 'bkn_trace'])
        self.assertEqual(cluster.targets['mariadb'], ('resource', 'mariadb-0', 'mariadb'))

    def test_discovery_refuses_multiple_pods(self):
        pod = {'metadata': {'name': 'pod'}, 'status': {'phase': 'Running',
               'conditions': [{'type': 'Ready', 'status': 'True'}]}, 'spec': {'containers': []}}
        with mock.patch.object(backup, 'run', return_value=json.dumps({'items': [pod, pod]}).encode()):
            with self.assertRaisesRegex(backup.BackupError, '唯一就绪'):
                backup.Cluster(self.args)

    def test_run_handles_large_output_and_failure(self):
        with tempfile.TemporaryFile() as output:
            backup.run(['sh', '-c', 'head -c 1048576 /dev/zero'], target=output)
            self.assertEqual(output.tell(), 1048576)
        with self.assertRaisesRegex(backup.BackupError, 'failed'):
            backup.run(['sh', '-c', 'echo failed >&2; exit 1'])


if __name__ == '__main__':
    unittest.main()
