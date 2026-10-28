"""Component isolation, selected extraction, CLI routing and legacy compatibility."""
import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest import mock

import backup
from test_backup import FakeCluster
from test_minio_backup import MinioHarness
from test_kafka_backup import make_package


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
        if 'k8s' in components:
            from test_k8s_backup import make_package as make_k8s_package
            cluster.k8s = mock.Mock()
            cluster.k8s.backup.side_effect = make_k8s_package
        if 'kafka' in components:
            cluster.kafka = mock.Mock()
            cluster.kafka.backup.side_effect = make_package
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
                self.assertEqual(manifest['format'], 5 if component == 'mariadb' else 4)
                self.assertEqual({p.name for p in stage.iterdir()}, ({'manifest.json', 'mariadb.tar', 'mariadb'} if component == 'mariadb' else {'manifest.json', backup.PAYLOADS[component]}))
                if component == 'minio':
                    cluster.minio.objects = {}
                if component == 'k8s':
                    continue  # Configuration-only payload is never restored.
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
                           lambda m: m['sha256'].update({'mariadb.tar': 'corrupt'}))
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
                                    (3, ['mariadb', 'opensearch', 'redis', 'minio'])]:
            with self.subTest(version=version):
                package = self.package(self.cluster(components), 'v' + str(version))
                legacy = self.root / ('legacy%d.tar.gz' % version)
                with tarfile.open(package, 'r:gz') as source, tarfile.open(legacy, 'w:gz') as target:
                    manifest = json.load(source.extractfile('manifest.json'))
                    manifest.update(format=version)
                    manifest.pop('components')
                    manifest['sha256'].pop('mariadb.tar')
                    sql = b'CREATE DATABASE openbkn;\n'
                    manifest['sha256']['mariadb.sql'] = backup.hashlib.sha256(sql).hexdigest()
                    for member in source:
                        content = source.extractfile(member).read()
                        if member.name == 'mariadb.tar':
                            member.name, content = 'mariadb.sql', sql
                        elif member.name == 'manifest.json':
                            content = json.dumps(manifest).encode()
                        member.size = len(content)
                        target.addfile(member, io.BytesIO(content))
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
                         mock.patch.object(backup, 'MinioBackup') as minio, \
                         mock.patch.object(backup, 'KafkaBackup') as kafka, mock.patch.object(backup, 'K8sBackup') as k8s:
                        cluster = backup.Cluster(args)
                        cluster.check()
                        self.assertEqual(mariadb.call_count, int(component == 'mariadb'))
                        self.assertEqual(opensearch.call_count, int(component == 'opensearch'))
                        self.assertEqual(redis.call_count, int(component == 'redis'))
                        self.assertEqual(minio.call_count, int(component == 'minio'))
                        self.assertEqual(kafka.call_count, int(component == 'kafka'))
                        self.assertEqual(k8s.call_count, int(component == 'k8s'))
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

    def test_both_cli_entries_report_enospc_and_backup_after_space_recovers(self):
        # Only local stand-ins are executed; no Kubernetes or database access.
        (self.root / 'sitecustomize.py').write_text('''
import errno, os, tempfile
if os.environ.get('BACKUP_TEST_ENOSPC') == '1':
    def fail(*args, **kwargs):
        raise OSError(errno.ENOSPC, 'No space left on device')
    tempfile.TemporaryFile = fail
''')
        kubectl = self.root / 'kubectl'
        kubectl.write_text('#!' + sys.executable + '''
import json, sys
if 'get' in sys.argv:
    print(json.dumps({'items': [{'metadata': {'name': 'mariadb-0'},
        'status': {'phase': 'Running', 'conditions': [{'type': 'Ready', 'status': 'True'}]},
        'spec': {'containers': [{'name': 'mariadb'}]}}]}))
elif 'exec' in sys.argv:
    command = sys.argv[sys.argv.index('--') + 1:]
    if command[0] == 'sh':
        query = sys.stdin.read()
        if 'mariadb-dump' in command[2]:
            print('CREATE DATABASE `qa_backup`;')
        elif 'SELECT VERSION()' in query:
            print('11.4.7-MariaDB')
        elif 'SHOW DATABASES' in query:
            print('qa_backup')
''')
        kubectl.chmod(0o700)
        temporary = self.root / 'temporary'
        temporary.mkdir()
        root = Path(backup.__file__).resolve().parents[1]
        env = dict(os.environ, PATH=str(self.root) + os.pathsep + os.environ['PATH'],
                   PYTHONPATH=str(self.root), TMPDIR=str(temporary), PYTHONDONTWRITEBYTECODE='1')
        entries = [[sys.executable, str(root / 'scripts/backup.py'), 'backup', 'run', '--components', 'mariadb'],
                   ['bash', str(root / 'deploy.sh'), 'mariadb', 'backup', 'run']]
        for index, entry in enumerate(entries):
            with self.subTest(entry=entry):
                output = self.root / ('output-' + str(index))
                command = entry + ['--databases', 'qa_backup', '--output-dir', str(output), '--timeout', '10']
                failed = subprocess.run(command, env=dict(env, BACKUP_TEST_ENOSPC='1'), capture_output=True, text=True, timeout=20)
                self.assertEqual(failed.returncode, 1, failed.stderr)
                for expected in ['临时目录空间不足', str(temporary), 'TMPDIR', '[Errno 28]']:
                    self.assertIn(expected, failed.stderr)
                self.assertEqual(failed.stdout, '')
                self.assertFalse(list(output.glob('*.tar.gz')))
                recovered = subprocess.run(command, env=dict(env, BACKUP_TEST_ENOSPC='0'), capture_output=True, text=True, timeout=20)
                self.assertEqual(recovered.returncode, 0, recovered.stderr)
                result = json.loads(recovered.stdout)
                self.assertEqual(result['result'], 'success')
                self.assertTrue(Path(result['archive']).is_file())
                self.assertFalse(list(output.glob('*.partial')))

    def test_default_backup_selects_all_data_and_k8s_components(self):
        args = backup.parser().parse_args(['backup', 'run'])
        self.assertEqual(backup.configured_components(args), list(backup.PAYLOADS))

    def test_multiple_component_backup_and_subset_restore(self):
        target = self.cluster(['mariadb', 'opensearch'])
        def selected_cluster(args):
            target.args, target.components = args, args.components
            return target
        outdir = self.root / 'multi'
        with mock.patch.object(backup, 'Cluster', side_effect=selected_cluster), mock.patch.object(target, 'check'), contextlib.redirect_stdout(io.StringIO()):
            backup.main(['backup', 'run', '--components', 'opensearch', 'mariadb', '--output-dir', str(outdir)])
        package = next(outdir.glob('*.tar.gz'))
        with tarfile.open(package) as archive:
            manifest = json.load(archive.extractfile('manifest.json'))
            self.assertEqual(manifest['components'], ['mariadb', 'opensearch'])
            self.assertEqual(set(archive.getnames()), {'manifest.json', 'mariadb.tar', 'repository.tar'})
        # A full archive has additional components; restore must neither extract
        # nor contact those that were not selected.
        source = self.cluster(list(backup.PAYLOADS))
        full = self.package(source)
        target.new = source.new
        with mock.patch.object(backup, 'Cluster', side_effect=selected_cluster), mock.patch.object(target, 'check'), contextlib.redirect_stdout(io.StringIO()) as output:
            backup.main(['restore', 'run', str(full), '--components', 'opensearch', 'mariadb', '--writers-stopped', '--overwrite'])
        self.assertEqual(json.loads(output.getvalue())['components'], ['mariadb', 'opensearch'])
        self.assertIn('import', target.events)
        self.assertTrue(any('restore' in str(event) for event in target.events))

    def test_multiple_selection_refuses_missing_component_before_cluster_access(self):
        package = self.package(self.cluster(['mariadb']))
        with mock.patch.object(backup, 'Cluster') as cluster:
            with self.assertRaisesRegex(backup.BackupError, '不包含'):
                backup.main(['restore', 'check', str(package), '--components', 'mariadb', 'opensearch'])
        cluster.assert_not_called()

    def test_selection_errors_do_not_access_cluster(self):
        cases = [
            ['--components', 'mariadb', 'mariadb'],
            ['--components', 'mariadb', '--components', 'mariadb'],
            ['--components', 'mariadb', '--skip-redis'],
            ['--components', 'mariadb', '--skip-minio'],
            ['--components', 'mariadb', '--include-kafka'],
            ['--component', 'mariadb', '--components', 'redis'],
            ['--components'], ['--components', 'unknown'],
        ]
        for options in cases:
            with self.subTest(options=options), mock.patch.object(backup, 'Cluster') as cluster:
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises((backup.BackupError, SystemExit)):
                    backup.main(['backup', 'check', *options])
                cluster.assert_not_called()

    def test_multiple_selection_discovery_does_not_contact_unselected_stores(self):
        args = backup.parser().parse_args(['backup', 'check', '--components', 'redis', 'minio'])
        args.components = backup.configured_components(args)
        with mock.patch.object(backup, 'run', side_effect=AssertionError('unexpected kubectl')), mock.patch.object(backup, 'RedisBackup') as redis, mock.patch.object(backup, 'MinioBackup') as minio, mock.patch.object(backup, 'KafkaBackup') as kafka, mock.patch.object(backup, 'K8sBackup') as k8s:
            cluster = backup.Cluster(args)
            cluster.check()
            redis.assert_called_once()
            minio.assert_called_once()
            kafka.assert_not_called()


if __name__ == '__main__':
    unittest.main()
