"""Offline regression tests; no Kubernetes or production data access."""
import contextlib
import errno
import io
import json
from pathlib import Path
import tarfile
import tempfile
import sys
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

    def test_real_component_locks_are_released_when_backup_raises(self):
        cluster = backup.Cluster.__new__(backup.Cluster)
        cluster.components = ['mariadb', 'opensearch']
        cluster.exec = mock.Mock()
        with self.assertRaisesRegex(backup.BackupError, 'object read failed'):
            with cluster.lock():
                raise backup.BackupError('object read failed')
        self.assertEqual(cluster.exec.call_args_list, [
            mock.call('mariadb', ['mkdir', '/tmp/openbkn-data-backup.lock'], timeout=60),
            mock.call('opensearch', ['mkdir', '/tmp/openbkn-data-backup.lock'], timeout=60),
            mock.call('opensearch', ['rm', '-rf', '--', '/tmp/openbkn-data-backup.lock'], timeout=60),
            mock.call('mariadb', ['rm', '-rf', '--', '/tmp/openbkn-data-backup.lock'], timeout=60)])

    def test_failed_lock_acquisition_never_removes_existing_lock(self):
        cluster = backup.Cluster.__new__(backup.Cluster)
        cluster.components = ['mariadb', 'opensearch']
        def execute(service, command, **kwargs):
            if service == 'opensearch':
                raise backup.BackupError('File exists')
        cluster.exec = mock.Mock(side_effect=execute)
        with self.assertRaisesRegex(backup.BackupError, 'File exists'):
            with cluster.lock():
                self.fail('must not enter backup')
        self.assertEqual(cluster.exec.call_args_list[-1],
                         mock.call('mariadb', ['rm', '-rf', '--', '/tmp/openbkn-data-backup.lock'], timeout=60))
        self.assertFalse(any(call.args[0] == 'opensearch' and call.args[1][0] == 'rm'
                             for call in cluster.exec.call_args_list))

    def test_lock_cleanup_failure_logs_component_and_actual_error(self):
        execute = mock.Mock(side_effect=backup.BackupError('API connection refused'))
        with mock.patch.object(backup, 'log') as log:
            backup.Cluster.release_lock(execute, '/tmp/openbkn-data-backup.lock', 'mariadb')
        self.assertIn('mariadb', log.call_args.args[0])
        self.assertIn('API connection refused', log.call_args.args[0])

    def test_backup_portable_archive_and_snapshot_prune_order(self):
        package, stage, manifest = self.package()
        self.assertTrue(package.stat().st_size)
        self.assertEqual(manifest['databases'], self.cluster.databases)
        self.assertEqual(manifest['indices'], ['business'])
        self.assertEqual(self.cluster.events[:7], ['dump'] * 3 + ['register', 'create-snapshot', 'delete:old', 'export-repository'])
        self.assertIn('delete:' + manifest['snapshot'], self.cluster.events)
        self.assertLess(self.cluster.events.index('export-repository'),
                        self.cluster.events.index('delete:' + manifest['snapshot']))
        self.assertEqual(len(list(self.root.glob('*.partial'))), 0)
        self.assertEqual(backup.digest(stage / 'mariadb.tar'), manifest['sha256']['mariadb.tar'])

    def test_failed_snapshot_never_prunes_or_publishes(self):
        self.cluster.failed = True
        with self.assertRaisesRegex(backup.BackupError, '快照失败'):
            self.cluster.backup(self.root)
        self.assertNotIn('delete:old', self.cluster.events)
        self.assertFalse(list(self.root.glob('*.tar.gz')))

    def test_cleanup_failure_keeps_verified_archive_and_reports_warning(self):
        original = self.cluster.api
        def fail_cleanup(method, path, body=None, **kwargs):
            if method == 'DELETE' and self.cluster.new and path.endswith('/' + self.cluster.new['snapshot']):
                raise backup.BackupError('snapshot cleanup unavailable')
            return original(method, path, body, **kwargs)
        with mock.patch.object(self.cluster, 'api', side_effect=fail_cleanup):
            with contextlib.redirect_stdout(io.StringIO()) as output:
                self.cluster.backup(self.root)
        result = json.loads(output.getvalue())
        self.assertEqual(result['result'], 'success')
        self.assertIn('cleanup_warning', result)
        self.assertTrue(Path(result['archive']).is_file())

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
        self.assertEqual(self.cluster.events, ['upload-repository', 'register-readonly', 'restore-snapshot'] + ['import'] * 3)

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

    def test_force_replace_repository_saves_original_then_unregisters(self):
        _, stage, manifest = self.package()
        self.args.writers_stopped = self.args.overwrite = self.args.replace_repository = True
        self.args.output_dir = str(self.root / 'rollbacks')
        self.cluster.directory = b'non-empty\n'
        self.cluster.repositories = {'offsite-backup': {'type': 'fs', 'settings': {
            'location': self.args.repository_path, 'readonly': 'true'}}}
        self.cluster.events.clear()
        with contextlib.redirect_stdout(io.StringIO()):
            self.cluster.restore(stage, manifest)
        self.assertEqual(len(list((self.root / 'rollbacks').glob('*.tar'))), 1)
        self.assertLess(self.cluster.events.index('delete:offsite-backup'),
                        self.cluster.events.index('upload-repository'))
        self.assertIn('restore-snapshot', self.cluster.events)

    def test_force_replace_rejects_foreign_repository(self):
        _, stage, manifest = self.package()
        self.args.writers_stopped = self.args.overwrite = self.args.replace_repository = True
        self.cluster.repositories = {'other-repo': {'type': 'fs', 'settings': {
            'location': self.args.repository_path}}}
        self.cluster.events.clear()
        with self.assertRaisesRegex(backup.BackupError, '其他仓库'):
            self.cluster.restore(stage, manifest)
        self.assertEqual(self.cluster.events, [])

    def test_force_replace_preserves_original_when_rollback_copy_fails(self):
        _, stage, manifest = self.package()
        self.args.writers_stopped = self.args.overwrite = self.args.replace_repository = True
        self.args.output_dir = str(self.root / 'rollbacks')
        self.cluster.directory = b'non-empty\n'
        self.cluster.repositories = {'offsite-backup': {'type': 'fs', 'settings': {
            'location': self.args.repository_path}}}
        self.cluster.events.clear()
        original = self.cluster.exec
        def fail_tar(service, command, **kwargs):
            if command[:2] == ['tar', '-C']:
                raise backup.BackupError('repository copy failed')
            return original(service, command, **kwargs)
        with mock.patch.object(self.cluster, 'exec', side_effect=fail_tar):
            with self.assertRaisesRegex(backup.BackupError, 'repository copy failed'):
                self.cluster.restore(stage, manifest)
        self.assertNotIn('delete:offsite-backup', self.cluster.events)
        self.assertNotIn('upload-repository', self.cluster.events)

    def test_replace_repository_requires_explicit_overwrite(self):
        args = backup.parser().parse_args(['restore', 'check', 'archive.tar.gz', '--replace-repository'])
        args.components = ['opensearch']
        with self.assertRaisesRegex(backup.BackupError, '--overwrite'):
            backup.validate_options(args)

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
                        manifest['sha256']['mariadb.tar'] = 'bad'
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

    def test_hyphenated_database_discovery_backup_and_restore(self):
        self.cluster.databases = ['ecommerce-ops', 'ecommerce_ops']
        with mock.patch.object(self.cluster, 'sql', wraps=self.cluster.sql) as sql:
            _, stage, manifest = self.package()
            self.assertEqual(manifest['databases'], ['ecommerce-ops', 'ecommerce_ops'])
            self.args.writers_stopped = True
            with contextlib.redirect_stdout(io.StringIO()):
                self.cluster.restore(stage, manifest)
            queries = [call.args[0] for call in sql.call_args_list if call.args]
            self.assertTrue(any("TABLE_SCHEMA IN ('ecommerce-ops','ecommerce_ops')" in query for query in queries))
            self.assertIn('USE `ecommerce-ops`; SHOW TABLES;', queries)

        instance = object.__new__(backup.Cluster)
        instance.args = self.args
        instance.args.module = 'backup'
        instance.args.databases = ['ecommerce-ops', 'ecommerce_ops']
        with mock.patch.object(instance, 'exec', return_value=b''), mock.patch.object(instance, 'sql') as sql:
            sql.side_effect = [b'11.4.7-MariaDB\n', b'mysql\necommerce-ops\necommerce_ops\n']
            instance.check_mariadb()
        self.assertEqual(instance.databases, ['ecommerce-ops', 'ecommerce_ops'])

    def test_database_names_cannot_inject_dump_options(self):
        instance = object.__new__(backup.Cluster)
        with mock.patch.object(instance, 'exec', return_value=b'') as execute:
            instance.sql(databases=['ecommerce-ops', '--all-databases'])
        command = execute.call_args.args[1]
        self.assertIn('--databases -- "$@"', command[2])
        self.assertEqual(command[-2:], ['ecommerce-ops', '--all-databases'])

    def test_database_validation_still_rejects_sql_metacharacters(self):
        for name in ("db' OR 1=1 --", 'db`', 'db\\name', 'db;DROP DATABASE mysql'):
            with self.subTest(name=name):
                self.cluster.databases = [name]
                with self.assertRaisesRegex(backup.BackupError, '数据库清单'):
                    self.package()

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
        self.args.components = ["mariadb", "opensearch"]
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
                return b'mysql\ninformation_schema\nperformance_schema\nsys\nopenbkn\nsafe\nbkn_trace\nbkn_audit\ndeploy\n'
            return b''
        with mock.patch.object(backup, 'run', side_effect=kubectl):
            cluster = backup.Cluster(self.args)
            cluster.check()
        self.assertEqual(cluster.databases, list(backup.DEFAULT_DATABASES))
        self.assertEqual(cluster.targets['mariadb'], ('resource', 'mariadb-0', 'mariadb'))

    def test_discovery_refuses_multiple_pods(self):
        pod = {'metadata': {'name': 'pod'}, 'status': {'phase': 'Running',
               'conditions': [{'type': 'Ready', 'status': 'True'}]}, 'spec': {'containers': []}}
        with mock.patch.object(backup, 'run', return_value=json.dumps({'items': [pod, pod]}).encode()):
            with self.assertRaisesRegex(backup.BackupError, '唯一就绪'):
                backup.Cluster(self.args)

    def test_deleted_repository_mount_is_rejected_before_upload_or_index_changes(self):
        package, stage, manifest = self.package()
        self.args.writers_stopped = True
        mountinfo = (b'5354 5264 253:0 /mnt/openbkn-snapshots//deleted '
                     b'/usr/share/opensearch/snapshots rw,relatime - ext4 /dev/root rw\n')
        original_exec = self.cluster.exec

        def execute(service, command, **kwargs):
            if command[0] == 'sh' and command[3] == 'check-repository-directory':
                return mountinfo
            return original_exec(service, command, **kwargs)

        self.cluster.events.clear()
        with mock.patch.object(self.cluster, 'exec', side_effect=execute):
            with self.assertRaisesRegex(backup.BackupError, '失效挂载'):
                self.cluster.restore_opensearch(stage, manifest, ['business'])
        self.assertNotIn('upload-repository', self.cluster.events)
        self.assertNotIn('delete:business', self.cluster.events)
        self.assertNotIn('restore-snapshot', self.cluster.events)

    def test_repository_mount_check_decodes_paths_and_ignores_other_deleted_mounts(self):
        instance = object.__new__(backup.Cluster)
        instance.args = self.args
        self.args.repository_path = '/repo with space'
        prefix = b'1 2 253:0 '
        for root, mountpoint, rejected in (
            (b'/mnt/snapshots', b'/repo\\040with\\040space', False),
            (b'/mnt/snapshots//deleted', b'/unrelated', False),
            (b'/mnt/snapshots//deleted', b'/repo\\040with\\040space', True),
        ):
            with self.subTest(root=root, mountpoint=mountpoint):
                raw = prefix + root + b' ' + mountpoint + b' rw - ext4 /dev/root rw\n'
                with mock.patch.object(instance, 'exec', return_value=raw):
                    if rejected:
                        with self.assertRaisesRegex(backup.BackupError, '失效挂载'):
                            instance.check_opensearch_repository_directory()
                    else:
                        instance.check_opensearch_repository_directory()

    def test_repository_directory_check_fails_if_path_is_missing(self):
        instance = object.__new__(backup.Cluster)
        instance.args = self.args
        self.args.repository_path = str(self.root / 'missing')

        def execute(service, command, **kwargs):
            return backup.run(command, **kwargs)

        with mock.patch.object(instance, 'exec', side_effect=execute):
            with self.assertRaises(backup.BackupError):
                instance.check_opensearch_repository_directory()

    def test_run_handles_large_output_and_failure(self):
        with tempfile.TemporaryFile() as output:
            backup.run(['sh', '-c', 'head -c 1048576 /dev/zero'], target=output)
            self.assertEqual(output.tell(), 1048576)
        with self.assertRaisesRegex(backup.BackupError, 'failed'):
            backup.run(['sh', '-c', 'echo failed >&2; exit 1'])

    def test_run_empty_stderr_retains_exit_code_and_directory_without_guessing_cause(self):
        with self.assertRaises(backup.BackupError) as raised:
            backup.run(['sh', '-c', 'exit 7'])
        message = str(raised.exception)
        self.assertIn('sh，退出码 7', message)
        self.assertIn(tempfile.gettempdir(), message)
        self.assertIn('未提供错误输出', message)
        self.assertNotIn('空间不足', message)

    def test_run_reports_full_temporary_filesystem_even_without_stderr(self):
        for blocks, inodes in [(0, 10), (10, 0)]:
            with self.subTest(blocks=blocks, inodes=inodes):
                usage = mock.Mock(f_bavail=blocks, f_files=100, f_favail=inodes)
                with mock.patch.object(backup.os, 'statvfs', return_value=usage):
                    with self.assertRaises(backup.BackupError) as raised:
                        backup.run(['sh', '-c', 'exit 1'])
                message = str(raised.exception)
                self.assertIn('临时目录空间或 inode 不足', message)
                self.assertIn(tempfile.gettempdir(), message)
                self.assertIn('TMPDIR', message)
                self.assertIn('退出码 1', message)

    def test_run_temporary_file_enospc_retains_reason_and_recovery_advice(self):
        with mock.patch.object(backup.tempfile, 'TemporaryFile', side_effect=OSError(errno.ENOSPC, 'No space left on device')):
            with self.assertRaises(backup.BackupError) as raised:
                backup.run(['sh', '-c', 'exit 0'])
        message = str(raised.exception)
        for expected in ['临时目录空间不足', tempfile.gettempdir(), 'TMPDIR', '[Errno 28]']:
            self.assertIn(expected, message)

    def test_run_permission_failure_is_not_reported_as_capacity_failure(self):
        with mock.patch.object(backup.tempfile, 'TemporaryFile', side_effect=PermissionError(errno.EACCES, 'Permission denied')):
            with self.assertRaises(PermissionError):
                backup.run(['sh', '-c', 'exit 0'])

    def test_run_preserves_stderr_when_child_output_has_enospc(self):
        with open('/dev/full', 'wb', buffering=0) as output:
            with self.assertRaises(backup.BackupError) as raised:
                backup.run([sys.executable, '-c', 'import os; os.write(1, b"data")'], target=output)
        self.assertIn('[Errno 28]', str(raised.exception))
        self.assertIn('No space left on device', str(raised.exception))
        self.assertIn('子进程报告空间不足', str(raised.exception))
        self.assertNotIn('临时目录空间不足', str(raised.exception))

    def test_run_drains_large_stderr_with_bounded_diagnostics(self):
        with self.assertRaises(backup.BackupError) as raised:
            backup.run([sys.executable, '-c', 'import sys; sys.stderr.write("x" * 2097152); sys.exit(3)'], timeout=10)
        self.assertIn('退出码 3', str(raised.exception))
        self.assertEqual(str(raised.exception).rsplit('）: ', 1)[1], 'x' * 8192)

    def test_run_preserves_enospc_when_temporary_stdout_is_full(self):
        # /dev/full produces real ENOSPC without filling a disk or mounting tmpfs.
        def full_temporary_file(**kwargs):
            return open('/dev/full', 'w+b', buffering=0)
        with mock.patch.object(backup.tempfile, 'TemporaryFile', side_effect=full_temporary_file):
            with self.assertRaises(backup.BackupError) as raised:
                backup.run([sys.executable, '-c', 'import os; os.write(1, b"data")'])
        self.assertIn('子进程报告空间不足', str(raised.exception))
        self.assertIn('[Errno 28]', str(raised.exception))

    def test_run_reports_enospc_when_writing_temporary_stdin(self):
        real_file = tempfile.TemporaryFile
        calls = 0
        def temporary_file(**kwargs):
            nonlocal calls
            calls += 1
            return open('/dev/full', 'w+b', buffering=0) if calls == 2 else real_file(**kwargs)
        with mock.patch.object(backup.tempfile, 'TemporaryFile', side_effect=temporary_file):
            with self.assertRaisesRegex(backup.BackupError, '临时目录空间不足'):
                backup.run(['sh', '-c', 'cat'], data=b'query')

    def test_run_output_directory_capacity_is_distinguished_from_temporary_space(self):
        available = mock.Mock(f_bavail=100, f_files=100, f_favail=100)
        full = mock.Mock(f_bavail=0, f_files=100, f_favail=100)
        with open(self.root / 'dump.sql', 'wb') as output:
            with mock.patch.object(backup.os, 'statvfs', side_effect=[available, full]):
                with self.assertRaises(backup.BackupError) as raised:
                    backup.run(['sh', '-c', 'echo write-failed >&2; exit 1'], target=output)
        message = str(raised.exception)
        self.assertIn('输出目录空间或 inode 不足：' + str(self.root), message)
        self.assertIn('write-failed', message)
        self.assertNotIn('TMPDIR', message)

    def test_run_failure_does_not_log_sensitive_arguments(self):
        with self.assertRaises(backup.BackupError) as raised:
            backup.run(['sh', '-c', 'exit 1', 'secret-password'])
        self.assertNotIn('secret-password', str(raised.exception))

    def test_run_timeout_still_terminates_child(self):
        with self.assertRaisesRegex(backup.BackupError, '命令超时'):
            backup.run([sys.executable, '-c', 'import time; time.sleep(30)'], timeout=0.1)

    def test_run_reports_full_configured_directory_when_tempfile_cannot_find_a_directory(self):
        with mock.patch.dict(backup.os.environ, {'TMPDIR': str(self.root)}):
            with mock.patch.object(backup.tempfile, 'gettempdir', side_effect=FileNotFoundError('No usable temporary directory')):
                with mock.patch.object(backup.os, 'statvfs', return_value=mock.Mock(f_bavail=0)):
                    with self.assertRaises(backup.BackupError) as raised:
                        backup.run(['sh', '-c', 'exit 0'])
        self.assertIn('临时目录空间或 inode 不足：' + str(self.root), str(raised.exception))
        self.assertIn('No usable temporary directory', str(raised.exception))

    def test_run_capacity_probe_failure_does_not_hide_original_command_error(self):
        with mock.patch.object(backup.os, 'statvfs', side_effect=PermissionError('cannot inspect filesystem')):
            with self.assertRaisesRegex(backup.BackupError, 'original-error'):
                backup.run(['sh', '-c', 'echo original-error >&2; exit 1'])


if __name__ == '__main__':
    unittest.main()
