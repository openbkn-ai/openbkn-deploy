"""Per-database payloads, explicit scope, failure isolation and legacy restore."""
import contextlib
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest import mock

import backup
from test_backup import FakeCluster


class MariaDBTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.args = backup.parser().parse_args(['backup', 'run', '--components', 'mariadb'])
        self.args.components = ['mariadb']
        self.cluster = FakeCluster(self.args)
        self.cluster.databases = list(backup.DEFAULT_DATABASES)
        patch = mock.patch.object(backup, 'log')
        patch.start()
        self.addCleanup(patch.stop)

    def package(self):
        with contextlib.redirect_stdout(io.StringIO()):
            self.cluster.backup(self.root)
        package = next(self.root.glob('*.tar.gz'))
        stage = self.root / 'restore'
        manifest = backup.unpack(package, stage)
        return package, stage, manifest

    def check_scope(self, existing):
        instance = object.__new__(backup.Cluster)
        instance.args = self.args
        with mock.patch.object(instance, 'exec'), mock.patch.object(instance, 'sql',
                side_effect=[b'11.4.7-MariaDB', '\n'.join(existing).encode()]):
            instance.check_mariadb()
        return instance.databases

    def test_default_only_five_named_business_databases(self):
        existing = [*backup.DEFAULT_DATABASES, 'ecommerce_ops', 'test5000', 'mysql']
        self.assertEqual(self.check_scope(existing), list(backup.DEFAULT_DATABASES))
        with self.assertRaisesRegex(backup.BackupError, 'safe'):
            self.check_scope([db for db in existing if db != 'safe'])

    def test_explicit_database_selection_and_empty_restore_target(self):
        self.args.databases = ['safe']
        self.assertEqual(self.check_scope(['safe', 'openbkn']), ['safe'])
        self.args.module = 'restore'
        self.assertEqual(self.check_scope(['mysql']), [])

    def test_each_dump_is_scoped_and_each_sql_is_packaged(self):
        with mock.patch.object(self.cluster, 'sql', wraps=self.cluster.sql) as sql:
            _, stage, manifest = self.package()
        calls = [call.kwargs['databases'] for call in sql.call_args_list if 'databases' in call.kwargs]
        self.assertEqual(calls, [[db] for db in backup.DEFAULT_DATABASES])
        self.assertEqual(manifest['format'], 5)
        with tarfile.open(stage / 'mariadb.tar') as archive:
            self.assertEqual(archive.getnames(), [db + '.sql' for db in backup.DEFAULT_DATABASES])
        self.assertEqual(backup.digest(stage / 'mariadb.tar'), manifest['sha256']['mariadb.tar'])

    def test_selected_restore_only_imports_selected_library_and_checks_its_conflicts(self):
        _, stage, manifest = self.package()
        self.args.writers_stopped = True
        self.args.databases = ['safe']
        with mock.patch.object(self.cluster, 'sql', wraps=self.cluster.sql) as sql, contextlib.redirect_stdout(io.StringIO()):
            self.cluster.restore(stage, manifest)
        sources = [call.kwargs['source'].name for call in sql.call_args_list if 'source' in call.kwargs]
        self.assertEqual(sources, [str(stage / 'mariadb' / 'safe.sql')])
        queries = [call.args[0] for call in sql.call_args_list if call.args]
        self.assertIn("SELECT TABLE_SCHEMA,TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA IN ('safe');", queries)
        self.args.databases = ['not-in-backup']
        with self.assertRaisesRegex(backup.BackupError, '不包含指定数据库'):
            backup.restore_databases(self.args, manifest)

    def test_failed_database_stops_later_databases_and_other_components(self):
        _, stage, manifest = self.package()
        self.args.writers_stopped = True
        original = self.cluster.sql
        imports = []
        def execute(query=None, **kwargs):
            if 'source' in kwargs:
                name = Path(kwargs['source'].name).stem
                imports.append(name)
                if name == 'bkn_trace':
                    raise backup.BackupError('SQL error')
            return original(query, **kwargs)
        with mock.patch.object(self.cluster, 'sql', side_effect=execute):
            with self.assertRaisesRegex(backup.BackupError, '已完成: bkn_audit；未执行: deploy, openbkn, safe'):
                self.cluster.restore(stage, manifest)
        self.assertEqual(imports, ['bkn_audit', 'bkn_trace'])

    def test_failed_dump_never_publishes_archive(self):
        original = self.cluster.sql
        def execute(query=None, **kwargs):
            if kwargs.get('databases') == ['bkn_trace']:
                raise backup.BackupError('dump failed')
            return original(query, **kwargs)
        with mock.patch.object(self.cluster, 'sql', side_effect=execute):
            with self.assertRaisesRegex(backup.BackupError, '数据库 bkn_trace'):
                self.cluster.backup(self.root)
        self.assertFalse(list(self.root.glob('*.tar.gz')))

    def test_legacy_package_imports_all_databases_once_and_rejects_subset(self):
        stage = self.root / 'legacy'
        stage.mkdir()
        (stage / 'mariadb.sql').write_bytes(b'-- legacy SQL\n')
        manifest = {'format': 4, 'components': ['mariadb'],
                    'databases': ['openbkn', 'safe'], 'mariadb_version': '11.4.7'}
        self.args.writers_stopped = True
        with mock.patch.object(self.cluster, 'sql', wraps=self.cluster.sql) as sql, contextlib.redirect_stdout(io.StringIO()):
            self.cluster.restore(stage, manifest)
        self.assertEqual(len([call for call in sql.call_args_list if 'source' in call.kwargs]), 1)
        self.args.databases = ['safe']
        with self.assertRaisesRegex(backup.BackupError, '旧版合并 SQL'):
            backup.restore_databases(self.args, manifest)

    def test_inner_archive_rejects_missing_extra_empty_or_unsafe_files(self):
        package, _, _ = self.package()
        for names in (['safe.sql'], ['../safe.sql'], ['safe.sql', 'extra.sql'], list(db + '.sql' for db in backup.DEFAULT_DATABASES)):
            inner = io.BytesIO()
            with tarfile.open(fileobj=inner, mode='w:') as archive:
                for name in names:
                    member = tarfile.TarInfo(name)
                    archive.addfile(member)  # Empty SQL must also be rejected.
            payload = inner.getvalue()
            bad = self.root / 'bad.tar.gz'
            with tarfile.open(package) as source, tarfile.open(bad, 'w:gz') as target:
                for member in source:
                    data = source.extractfile(member).read()
                    if member.name == 'mariadb.tar':
                        data = payload
                    elif member.name == 'manifest.json':
                        manifest = json.loads(data)
                        manifest['sha256']['mariadb.tar'] = backup.hashlib.sha256(payload).hexdigest()
                        data = json.dumps(manifest).encode()
                    member.size = len(data)
                    target.addfile(member, io.BytesIO(data))
            with self.assertRaises(backup.BackupError):
                backup.unpack(bad, self.root / 'bad-stage')


if __name__ == '__main__':
    unittest.main()
