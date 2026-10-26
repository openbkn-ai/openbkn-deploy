"""Redis archive, persistence conversion and rollback regression tests (offline)."""
import contextlib
import io
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from unittest import mock

import backup
from redis_backup import RedisBackup
from test_backup import FakeCluster


METADATA = {'version': '7.4.6', 'snapshot_time': 1792372153}


class RedisHarness(RedisBackup):
    """Emulate Kubernetes/Redis; run the file move/rollback scripts on real files."""
    def __init__(self, root):
        self.args = backup.parser().parse_args(['backup', 'run', '--poll-interval', '1'])
        self.error = backup.BackupError
        self.log = lambda message: None
        self.ns, self.sts, self.pvc = 'resource', 'redis', 'redis-data'
        self.pod = {'metadata': {'name': 'redis-0'}, 'spec': {'nodeName': 'test-node'}}
        self.directory = str(root)
        self.version = '7.4.6'
        self.config = dict(dir=str(root), dbfilename='dump.rdb', appenddirname='appendonlydir',
                           appendfilename='appendonly.aof', appendonly='yes')
        self.files = ['dump.rdb', 'appendonlydir', 'appendonly.aof']
        self.uid, self.gid, self.image, self.resources = 999, 999, 'redis:7.4.6', {}
        self.statefulset = {'metadata': {'uid': 'redis-uid'}, 'spec': {'replicas': 1, 'template': {}}}
        self.events = []
        self.fail_start = False
        self.fail_aof = False
        self.replicas = 1
        self.cli_calls = []
        self.save_count = 1
        self.bgsave_works = True
        self.run_id = 'same-process'
        (root / 'dump.rdb').write_bytes(b'REDIS-old')
        (root / 'appendonlydir').mkdir()
        (root / 'appendonlydir' / 'appendonly.aof.manifest').write_text('old-aof')

    def kube(self, *args, **kwargs):
        self.events.append(args)
        if args[:2] == ('get', 'statefulset'):
            return json.dumps(self.statefulset).encode()
        if args[:2] == ('auth', 'can-i'):
            return b'yes\n'
        raise AssertionError(args)

    def execute(self, command, *, pod=None, **kwargs):
        self.events.append(tuple(command))
        if command[0] == 'redis-server':
            return b''
        if command[0] == 'redis-check-rdb':
            if not Path(command[1]).read_bytes().startswith(b'REDIS'):
                raise self.error('invalid RDB')
            return b'RDB looks OK'
        if command[0] == 'redis-cli':
            args = command[5:]
            if args == ['CONFIG', 'SET', 'appendonly', 'yes']:
                if self.fail_aof:
                    raise self.error('AOF conversion failed')
                aof = Path(self.directory) / 'appendonlydir'
                aof.mkdir()
                (aof / 'appendonly.aof.manifest').write_text('restored-aof')
            if args == ['INFO', 'keyspace']:
                return b'db1:keys=1,expires=0,avg_ttl=0,subexpiry=0\n'
            if args == ['INFO', 'persistence']:
                return b'aof_enabled:1\naof_rewrite_in_progress:0\naof_rewrite_scheduled:0\naof_last_bgrewrite_status:ok\naof_last_write_status:ok\n'
            return b'PONG\n' if args == ['PING'] else b'OK\n'
        result = subprocess.run(command, stdin=kwargs.get('source'), stdout=kwargs.get('target') or subprocess.PIPE,
                                stderr=subprocess.PIPE, check=False)
        if result.returncode:
            raise self.error(result.stderr.decode())
        return result.stdout or b''

    def cli(self, *args):
        self.cli_calls.append(args)
        if args == ('BGSAVE',):
            if self.bgsave_works:
                self.save_count += 1
            return b'Background saving started'
        raise AssertionError(args)

    def info(self, section):
        if section == 'server':
            return {'run_id': self.run_id, 'redis_version': self.version}
        if section == 'keyspace':
            return {'db1': 'keys=1,expires=0,avg_ttl=0'}
        if section == 'persistence':
            return dict(rdb_bgsave_in_progress='0', aof_rewrite_in_progress='0', rdb_saves=str(self.save_count),
                        rdb_last_bgsave_status='ok', rdb_last_save_time='1792372153', loading='0',
                        aof_enabled='1', aof_last_write_status='ok')
        raise AssertionError(section)

    def scale(self, replicas):
        self.events.append(('scale', replicas))
        self.replicas = replicas
        if replicas == 1 and self.fail_start:
            self.fail_start = False
            raise self.error('formal Redis start failed')
        if replicas == 1 and self.config['appendonly'] == 'yes':
            if not (Path(self.directory) / 'appendonlydir' / 'appendonly.aof.manifest').exists():
                raise self.error('missing AOF')

    def create_helper(self, name):
        self.events.append(('helper-create', name))

    def delete_helper(self, name):
        self.events.append(('helper-delete', name))


class RedisTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.data = self.root / 'data'
        self.data.mkdir()
        self.redis = RedisHarness(self.data)
        self.snapshot = self.root / 'backup.rdb'
        self.snapshot.write_bytes(b'REDIS-restored')

    def test_restore_converts_aof_and_verifies_two_starts(self):
        self.redis.restore(self.snapshot, METADATA, True)
        self.assertEqual((self.data / 'dump.rdb').read_bytes(), b'REDIS-restored')
        self.assertEqual((self.data / 'appendonlydir/appendonly.aof.manifest').read_text(), 'restored-aof')
        saved = next(self.data.glob('.openbkn-redis-rollback-*'))
        self.assertEqual((saved / 'dump.rdb').read_bytes(), b'REDIS-old')
        self.assertEqual([e for e in self.redis.events if e[0] == 'scale'],
                         [('scale', 0), ('scale', 1), ('scale', 0), ('scale', 1)])
        self.assertFalse((self.data / '.openbkn-redis-restore.lock').exists())
        launch = next(e for e in self.redis.events if e[0] == 'redis-server')
        self.assertEqual(launch[launch.index('--port') + 1], '0')
        self.assertEqual(launch[launch.index('--appendonly') + 1], 'no')

    def test_restore_without_aof_does_not_enable_it(self):
        self.redis.config['appendonly'] = 'no'
        self.redis.restore(self.snapshot, METADATA, True)
        self.assertFalse((self.data / 'appendonlydir').exists())
        self.assertFalse(any(e[0] == 'redis-cli' and 'CONFIG' in e for e in self.redis.events))

    def test_aof_failure_rolls_back_original_files(self):
        self.redis.fail_aof = True
        with self.assertRaisesRegex(backup.BackupError, 'AOF conversion'):
            self.redis.restore(self.snapshot, METADATA, True)
        self.assertEqual((self.data / 'dump.rdb').read_bytes(), b'REDIS-old')
        self.assertEqual((self.data / 'appendonlydir/appendonly.aof.manifest').read_text(), 'old-aof')
        self.assertEqual(self.redis.replicas, 1)

    def test_formal_start_failure_rolls_back_generated_aof(self):
        self.redis.fail_start = True
        with self.assertRaisesRegex(backup.BackupError, 'formal Redis'):
            self.redis.restore(self.snapshot, METADATA, True)
        self.assertEqual((self.data / 'appendonlydir/appendonly.aof.manifest').read_text(), 'old-aof')
        self.assertEqual((self.data / 'dump.rdb').read_bytes(), b'REDIS-old')

    def test_partial_preserve_failure_does_not_quarantine_untouched_original(self):
        # RDB moves first, then reject a symlink as AOF directory. Rollback must
        # restore the RDB without moving away the unmodified AOF path.
        aof = self.data / 'appendonlydir'
        aof.rename(self.data / 'actual-aof')
        aof.symlink_to(self.data / 'actual-aof', target_is_directory=True)
        with self.assertRaises(backup.BackupError):
            self.redis.restore(self.snapshot, METADATA, True)
        self.assertEqual((self.data / 'dump.rdb').read_bytes(), b'REDIS-old')
        self.assertTrue(aof.is_symlink())

    def test_preflight_refuses_version_and_existing_data_before_scaling(self):
        for metadata, overwrite, message in [({'version': '7.2.0', 'snapshot_time': 1}, True, '版本完全一致'),
                                              (METADATA, False, '已有数据')]:
            with self.subTest(message=message):
                with self.assertRaisesRegex(backup.BackupError, message):
                    self.redis.restore(self.snapshot, metadata, overwrite)
                self.assertFalse(any(e[0] == 'scale' for e in self.redis.events))

    def test_restart_count_validation_allows_expiry_but_detects_missing_durable_keys(self):
        self.redis.validate_running({'db1': 'keys=2,expires=1'})
        with self.assertRaisesRegex(backup.BackupError, 'key 数量'):
            self.redis.validate_running({'db1': 'keys=2,expires=0'})

    def test_backup_requires_fresh_save_and_copies_all_db_rdb(self):
        path = self.root / 'redis.rdb'
        metadata = self.redis.backup(path)
        self.assertEqual(path.read_bytes(), b'REDIS-old')
        self.assertEqual(metadata['version'], '7.4.6')
        self.assertIn(('BGSAVE',), self.redis.cli_calls)
        self.redis.bgsave_works = False
        with self.assertRaisesRegex(backup.BackupError, '禁止使用旧 RDB'):
            self.redis.backup(path)

    def test_discovery_reads_renamed_config_and_pvc_without_host_paths(self):
        args = backup.parser().parse_args(['backup', 'check'])
        pod = {'metadata': {'name': 'redis-0', 'ownerReferences': [
                    {'controller': True, 'kind': 'StatefulSet', 'name': 'redis'}]},
               'status': {'phase': 'Running', 'conditions': [{'type': 'Ready', 'status': 'True'}]},
               'spec': {'nodeName': 'test', 'containers': [{'name': 'redis', 'image': 'redis:7.4.6',
                   'volumeMounts': [{'name': 'data', 'mountPath': '/data'}]}],
                   'volumes': [{'name': 'data', 'persistentVolumeClaim': {'claimName': 'redis-data'}}]}}
        def run(argv, **kwargs):
            if 'get' in argv:
                resource = argv[argv.index('get') + 1]
                if resource == 'pods':
                    result = {'items': [pod]}
                elif resource == 'statefulset':
                    result = {'metadata': {'uid': 'uid'}, 'spec': {'replicas': 1, 'template': {}}}
                elif resource == 'pvc':
                    result = {'status': {'phase': 'Bound'}}
                else:
                    raise AssertionError(argv)
                return json.dumps(result).encode()
            if 'INFO' in argv:
                return {'server': b'redis_version:7.4.6', 'replication': b'role:master\nconnected_slaves:0',
                        'cluster': b'cluster_enabled:0'}[argv[-1]]
            if 'MODULE' in argv:
                return b'[]'
            if 'CONFIG_NEW' in argv:
                return json.dumps(['dir', '/data', 'dbfilename', 'dump.rdb', 'appendonly', 'yes',
                                   'appenddirname', 'appendonlydir', 'appendfilename', 'appendonly.aof']).encode()
            if argv[-1] == 'id -u; id -g':
                return b'999\n999\n'
            return b''
        redis = RedisBackup(args, run, backup.BackupError, lambda m: None)
        self.assertEqual(redis.pvc, 'redis-data')
        self.assertEqual(redis.config['appendonly'], 'yes')
        self.assertEqual(redis.uid, 999)
        pod['spec']['containers'][0]['volumeMounts'][0]['subPath'] = 'redis'
        with self.assertRaisesRegex(backup.BackupError, 'subPath'):
            RedisBackup(args, run, backup.BackupError, lambda m: None)

    def test_cli_errors_fail_and_credentials_stay_in_pod(self):
        with mock.patch.object(self.redis, 'execute', return_value=b'OK') as execute:
            RedisBackup.cli(self.redis, 'BGSAVE')
        command = execute.call_args.args[0]
        self.assertIn('REDISCLI_AUTH', command[2])
        self.assertIn('redis-cli -e', command[2])
        self.assertEqual(command[-2:], ['root', 'BGSAVE'])

    def test_cli_auth_failure_does_not_stop_redis(self):
        with mock.patch.object(self.redis, 'info', side_effect=backup.BackupError('NOAUTH')):
            with self.assertRaisesRegex(backup.BackupError, 'NOAUTH'):
                self.redis.restore(self.snapshot, METADATA, True)
        self.assertFalse(any(e[0] == 'scale' for e in self.redis.events))

    def test_failed_rollback_leaves_original_stopped_and_recovery_details(self):
        self.redis.fail_aof = True
        original = self.redis.execute
        def fail_rollback(command, **kwargs):
            if command[0] == 'sh' and 'rollback' in command[3:4]:
                raise backup.BackupError('disk failure')
            return original(command, **kwargs)
        with mock.patch.object(self.redis, 'execute', side_effect=fail_rollback):
            with self.assertRaisesRegex(backup.BackupError, '自动回退失败.*PVC=redis-data'):
                self.redis.restore(self.snapshot, METADATA, True)
        self.assertEqual(self.redis.replicas, 0)

    def test_bad_rdb_does_not_replace_original_files(self):
        self.snapshot.write_bytes(b'invalid')
        with self.assertRaisesRegex(backup.BackupError, 'invalid RDB'):
            self.redis.restore(self.snapshot, METADATA, True)
        self.assertEqual((self.data / 'dump.rdb').read_bytes(), b'REDIS-old')
        self.assertEqual(self.redis.replicas, 1)

    def test_helper_uses_target_image_pvc_uid_and_no_service_account(self):
        spec = self.redis.helper_spec('test')['spec']
        self.assertFalse(spec['automountServiceAccountToken'])
        self.assertEqual(spec['volumes'][0]['persistentVolumeClaim']['claimName'], 'redis-data')
        self.assertEqual(spec['securityContext']['runAsUser'], 999)
        self.assertEqual(spec['nodeName'], 'test-node')


class RedisArchiveTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.cluster = FakeCluster(backup.parser().parse_args(['backup', 'run']))
        self.cluster.redis = mock.Mock()
        self.cluster.components.append('redis')
        def dump(path):
            path.write_bytes(b'REDIS-snapshot')
            return METADATA.copy()
        self.cluster.redis.backup.side_effect = dump

    def package(self):
        with mock.patch.object(backup, 'log'), contextlib.redirect_stdout(io.StringIO()):
            self.cluster.backup(self.root)
        package = next(self.root.glob('*.tar.gz'))
        stage = self.root / 'unpacked'
        stage.mkdir(exist_ok=True)
        return package, stage, backup.unpack(package, stage)

    def test_v4_archive_round_trip_includes_redis_checksum_and_restore(self):
        _, stage, manifest = self.package()
        self.assertEqual(manifest['format'], 5)
        self.assertEqual(manifest['redis'], METADATA)
        self.assertEqual(manifest['sha256']['redis.rdb'], backup.digest(stage / 'redis.rdb'))
        self.cluster.args.writers_stopped = True
        with mock.patch.object(backup, 'log'), contextlib.redirect_stdout(io.StringIO()):
            self.cluster.restore(stage, manifest)
        self.cluster.redis.restore.assert_called_once_with(stage / 'redis.rdb', METADATA, False)

    def test_redis_failure_does_not_publish_partial_package(self):
        self.cluster.redis.backup.side_effect = backup.BackupError('Redis backup failed')
        with mock.patch.object(backup, 'log'), self.assertRaisesRegex(backup.BackupError, 'Redis backup failed'):
            self.cluster.backup(self.root)
        self.assertFalse(list(self.root.glob('*.tar.gz')))

    def test_v2_redis_metadata_required_offline(self):
        package, _, _ = self.package()
        bad = self.root / 'bad.tar.gz'
        with tarfile.open(package, 'r:gz') as source, tarfile.open(bad, 'w:gz') as target:
            for member in source:
                content = source.extractfile(member).read()
                if member.name == 'manifest.json':
                    manifest = json.loads(content)
                    del manifest['redis']
                    content = json.dumps(manifest).encode()
                    member.size = len(content)
                target.addfile(member, io.BytesIO(content))
        with self.assertRaisesRegex(backup.BackupError, '元数据'):
            backup.unpack(bad, self.root / 'bad')

    def test_legacy_main_restores_without_discovering_redis(self):
        self.cluster.redis = None
        self.cluster.components.remove('redis')
        package, _, _ = self.package()
        with mock.patch.object(backup, 'RedisBackup') as redis:
            with mock.patch.object(backup, 'Cluster', return_value=self.cluster) as cluster:
                with mock.patch.object(self.cluster, 'check'), contextlib.redirect_stdout(io.StringIO()):
                    backup.main(['restore', 'check', str(package)])
                self.assertNotIn('redis', cluster.call_args.args[0].components)
            redis.assert_not_called()


if __name__ == '__main__':
    unittest.main()
