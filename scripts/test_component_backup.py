"""Component isolation, selected extraction, CLI routing and legacy compatibility."""
import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from unittest import mock

import backup
from test_backup import FakeCluster
from test_minio_backup import MinioHarness


class ComponentTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        patch = mock.patch.object(backup, 'log')
        patch.start()
        self.addCleanup(patch.stop)

    def cluster(self, components):
        args = backup.parser().parse_args(['backup', 'run'])
        args.components = list(components)
        cluster = FakeCluster(args)
        if 'minio' in components:
            cluster.minio = MinioHarness()
        if 'redis' in components:
            cluster.redis = mock.Mock()
            def dump(path):
                path.write_bytes(b'REDIS-test')
                return {'version': '7.4.6', 'snapshot_time': 1792372153}
            cluster.redis.backup.side_effect = dump
        return cluster

    def package(self, cluster, directory='source'):
        path = self.root / directory
        path.mkdir()
        with contextlib.redirect_stdout(io.StringIO()):
            cluster.backup(path)
        return next(path.glob('*.tar.gz'))

    def rewrite(self, package, path, update):
        with tarfile.open(package, 'r:gz') as source, tarfile.open(path, 'w:gz') as target:
            for member in source:
                content = source.extractfile(member).read()
                if member.name == 'manifest.json':
                    manifest = json.loads(content)
                    update(manifest)
                    content = json.dumps(manifest).encode()
                    member.size = len(content)
                target.addfile(member, io.BytesIO(content))
        return path

    def test_each_component_backup_and_restore_has_no_other_component_calls(self):
        for component in backup.PAYLOADS:
            with self.subTest(component=component):
                cluster = self.cluster([component])
                if component != 'mariadb':
                    cluster.sql = mock.Mock(side_effect=AssertionError('must not access MariaDB'))
                if component != 'opensearch':
                    cluster.api = mock.Mock(side_effect=AssertionError('must not access OpenSearch'))
                    cluster.exec = mock.Mock(side_effect=AssertionError('must not exec OpenSearch'))
                package = self.package(cluster, component)
                stage = self.root / (component + '-restore')
                manifest = backup.unpack(package, stage)
                self.assertEqual(manifest['components'], [component])
                self.assertEqual(manifest['format'], 4)
                self.assertEqual({p.name for p in stage.iterdir()}, {'manifest.json', backup.PAYLOADS[component]})
                if component == 'minio':
                    cluster.minio.objects = {}
                cluster.args.writers_stopped = True
                with contextlib.redirect_stdout(io.StringIO()):
                    cluster.restore(stage, manifest)
                if component == 'redis':
                    cluster.redis.restore.assert_called_once()

    def test_minio_retry_from_full_package_only_extracts_and_restores_minio(self):
        source = self.cluster(list(backup.PAYLOADS))
        package = self.package(source)
        target = self.cluster(['minio'])
        target.minio.objects = {}
        target.sql = mock.Mock(side_effect=AssertionError('unexpected SQL'))
        target.api = mock.Mock(side_effect=AssertionError('unexpected OpenSearch request'))
        target.exec = mock.Mock(side_effect=AssertionError('unexpected exec'))
        def target_cluster(args):
            target.args, target.components = args, args.components
            return target
        with mock.patch.object(backup, 'Cluster', side_effect=target_cluster) as create:
            with mock.patch.object(target, 'check'), contextlib.redirect_stdout(io.StringIO()) as output:
                backup.main(['restore', 'run', str(package), '--component', 'minio', '--writers-stopped', '--overwrite'])
            self.assertEqual(create.call_args.args[0].components, ['minio'])
        self.assertEqual(json.loads(output.getvalue())['components'], ['minio'])
        self.assertEqual(target.events, [])
        self.assertEqual(target.minio.objects['bucket-a']['dir/file.txt'], b'hello')

    def test_missing_component_fails_before_cluster_discovery(self):
        package = self.package(self.cluster(['mariadb']))
        with mock.patch.object(backup, 'Cluster') as create:
            with self.assertRaisesRegex(backup.BackupError, '不包含'):
                backup.main(['restore', 'run', str(package), '--component', 'minio', '--writers-stopped'])
            create.assert_not_called()

    def test_selected_payload_hash_checked_without_extracting_unrelated_data(self):
        package = self.package(self.cluster(list(backup.PAYLOADS)))
        bad = self.rewrite(package, self.root / 'unrelated-bad.tar.gz',
                           lambda m: m['sha256'].update({'mariadb.sql': 'corrupt'}))
        stage = self.root / 'selected'
        backup.unpack(bad, stage, ['minio'])
        self.assertEqual({p.name for p in stage.iterdir()}, {'manifest.json', 'minio.tar'})
        with self.assertRaisesRegex(backup.BackupError, '校验失败'):
            backup.unpack(bad, self.root / 'all')
        bad_minio = self.rewrite(package, self.root / 'minio-bad.tar.gz',
                                 lambda m: m['sha256'].update({'minio.tar': 'corrupt'}))
        with mock.patch.object(backup, 'Cluster') as create:
            with self.assertRaisesRegex(backup.BackupError, '校验失败'):
                backup.main(['restore', 'run', str(bad_minio), '--component', 'minio', '--writers-stopped'])
            create.assert_not_called()

    def test_v1_v2_v3_packages_support_full_and_component_reads(self):
        for version, components in [(1, ['mariadb', 'opensearch']),
                                    (2, ['mariadb', 'opensearch', 'redis']),
                                    (3, list(backup.PAYLOADS))]:
            with self.subTest(version=version):
                package = self.package(self.cluster(components), 'v' + str(version))
                legacy = self.rewrite(package, self.root / ('legacy%d.tar.gz' % version),
                                      lambda m: (m.update(format=version), m.pop('components')))
                manifest = backup.unpack(legacy, self.root / ('legacy%d-all' % version))
                self.assertEqual(backup.manifest_components(manifest), components)
                for component in components:
                    stage = self.root / ('legacy%d-%s' % (version, component))
                    backup.unpack(legacy, stage, [component])
                    self.assertEqual({p.name for p in stage.iterdir()}, {'manifest.json', backup.PAYLOADS[component]})

    def test_component_declaration_must_match_files(self):
        package = self.package(self.cluster(['mariadb']))
        for components in ([], ['unknown'], ['mariadb', 'mariadb'], ['minio']):
            with self.subTest(components=components):
                bad = self.rewrite(package, self.root / 'bad.tar.gz', lambda m: m.update(components=components))
                with self.assertRaises(backup.BackupError):
                    backup.unpack(bad, self.root / 'bad')

    def test_discovery_and_check_only_use_selected_component(self):
        for component in backup.PAYLOADS:
            with self.subTest(component=component):
                args = backup.parser().parse_args(['backup', 'check', '--component', component])
                pod = {'metadata': {'name': component + '-0'},
                       'spec': {'containers': [{'name': component}]},
                       'status': {'phase': 'Running', 'conditions': [{'type': 'Ready', 'status': 'True'}]}}
                def kube(argv, **kwargs):
                    self.assertIn('get', argv)
                    self.assertIn(args.mariadb_selector if component == 'mariadb' else args.opensearch_selector, argv)
                    return json.dumps({'items': [pod]}).encode()
                with mock.patch.object(backup, 'run', side_effect=kube) as run:
                    with mock.patch.object(backup.Cluster, 'check_mariadb') as mariadb, \
                         mock.patch.object(backup.Cluster, 'check_opensearch') as opensearch, \
                         mock.patch.object(backup, 'RedisBackup') as redis, \
                         mock.patch.object(backup, 'MinioBackup') as minio:
                        cluster = backup.Cluster(args)
                        cluster.check()
                        self.assertEqual(mariadb.call_count, int(component == 'mariadb'))
                        self.assertEqual(opensearch.call_count, int(component == 'opensearch'))
                        self.assertEqual(redis.call_count, int(component == 'redis'))
                        self.assertEqual(minio.call_count, int(component == 'minio'))
                        self.assertEqual(run.call_count, int(component in ('mariadb', 'opensearch')))

    def test_selected_restore_keeps_writer_and_overwrite_guards(self):
        cluster = self.cluster(['minio'])
        package = self.package(cluster)
        stage = self.root / 'restore'
        manifest = backup.unpack(package, stage)
        with self.assertRaisesRegex(backup.BackupError, '--writers-stopped'):
            cluster.restore(stage, manifest)
        cluster.args.writers_stopped = True
        with self.assertRaisesRegex(backup.BackupError, '--overwrite'):
            cluster.restore(stage, manifest)

    def test_component_locks_release_correct_pods_even_when_later_acquire_fails(self):
        cluster = object.__new__(backup.Cluster)
        cluster.components = ['mariadb', 'opensearch', 'minio']
        events = []
        cluster.exec = lambda component, command, **kwargs: events.append((component, command[0]))
        cluster.minio = mock.Mock()
        cluster.minio.execute.side_effect = lambda command, **kwargs: events.append(('minio', command[0]))
        with cluster.lock():
            pass
        self.assertEqual(events, [('mariadb', 'mkdir'), ('opensearch', 'mkdir'), ('minio', 'mkdir'),
                                  ('minio', 'rm'), ('opensearch', 'rm'), ('mariadb', 'rm')])
        events.clear()
        cluster.minio.execute.side_effect = backup.BackupError('busy')
        with self.assertRaisesRegex(backup.BackupError, 'busy'):
            with cluster.lock():
                self.fail('must not enter operation')
        self.assertEqual(events, [('mariadb', 'mkdir'), ('opensearch', 'mkdir'),
                                  ('opensearch', 'rm'), ('mariadb', 'rm')])

    def test_shell_routes_component_and_full_commands_to_python(self):
        shim = self.root / 'python3'
        shim.write_text('#!/bin/sh\nprintf "%s\\n" "$@"\n')
        shim.chmod(0o700)
        env = dict(os.environ, PATH=str(self.root) + os.pathsep + os.environ['PATH'])
        script = Path(backup.__file__).resolve().parents[1] / 'deploy.sh'
        for component in backup.PAYLOADS:
            for operation in ('backup', 'restore'):
                arguments = [component, operation, 'check'] + (['/tmp/example.tar.gz', '--overwrite'] if operation == 'restore' else [])
                result = subprocess.run(['bash', str(script), *arguments], env=env, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                output = result.stdout.splitlines()
                self.assertEqual(output[1:3], [operation, 'check'])
                self.assertEqual(output[-2:], ['--component', component])
        result = subprocess.run(['bash', str(script), 'backup', 'check'], env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines()[1:], ['backup', 'check'])


if __name__ == '__main__':
    unittest.main()
