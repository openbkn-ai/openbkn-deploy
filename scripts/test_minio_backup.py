"""Offline MinIO backup/restore tests using real tar files and a fake S3 server."""
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
from minio_backup import MinioBackup, archive_inventory, object_key, validate_metadata, mc_error_detail
from test_backup import FakeCluster


class MinioHarness(MinioBackup):
    def __init__(self):
        self.args = backup.parser().parse_args(['backup', 'run'])
        self.error, self.log = backup.BackupError, lambda message: None
        self.ns, self.pod, self.container = 'openbkn', 'minio-0', 'minio'
        self.objects = {'bucket-a': {'dir/file.txt': b'hello', 'zero': b'', 'sessions/': b'', 'sessions/child/': b''}, 'empty-bucket': {}}
        self.buckets = sorted(self.objects)
        self.events, self.workspaces = [], []
        self.fail_transfer = False
        self.change_during_backup = False
        self.fail_verification = False

    @contextlib.contextmanager
    def workspace(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.workspaces.append(tmp)
            (Path(tmp) / 'data').mkdir()
            yield tmp

    def execute(self, command, **kwargs):
        self.events.append(command)
        result = subprocess.run(command, stdin=kwargs.get('source'),
                                stdout=kwargs.get('target') or subprocess.PIPE,
                                stderr=subprocess.PIPE, check=False)
        if result.returncode:
            raise self.error(result.stderr.decode())
        return result.stdout or b''

    def mc(self, work, *args, **kwargs):
        self.events.append(args)
        if args == ('ls', 'local'):
            rows = [{'status': 'success', 'key': b + '/', 'type': 'folder'} for b in self.objects]
        elif args[:2] == ('ls', '--recursive'):
            bucket = args[2].split('/', 1)[1]
            rows = [{'status': 'success', 'key': k, 'type': 'folder' if k.endswith('/') else 'file', 'size': len(v)}
                    for k, v in self.objects[bucket].items()]
            if self.fail_verification:
                rows = []
        elif args[0] in ('cat', 'pipe'):
            if self.fail_transfer:
                raise self.error('transfer failed')
            bucket, key = args[-1].split('/', 2)[1:]
            if args[0] == 'cat':
                value = self.objects[bucket][key]
                if self.change_during_backup:
                    value += b'changed'
                kwargs['target'].write(value)
            else:
                self.objects[bucket][key] = kwargs['source'].read()
            return b''
        elif args[0] == 'mb':
            self.objects.setdefault(args[-1].split('/', 1)[1], {})
            return b''
        else:
            raise AssertionError(args)
        return b'\n'.join(json.dumps(row).encode() for row in rows)


class MinioTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.minio = MinioHarness()

    def test_online_roundtrip_including_empty_bucket_and_zero_byte_object(self):
        path = self.root / 'minio.tar'
        metadata = self.minio.backup(path)
        original = {'bucket-a': {'dir/file.txt': b'hello', 'zero': b''}, 'empty-bucket': {}}
        self.assertEqual(metadata['buckets'], ['bucket-a', 'empty-bucket'])
        self.minio.objects = {}
        self.minio.restore(path, metadata, False)
        self.assertEqual(self.minio.objects, original)
        self.assertFalse(any(c[0] == 'pipe' and c[-1].endswith('/') for c in self.minio.events))
        self.assertTrue(all(not Path(w).exists() for w in self.minio.workspaces))
        self.assertFalse(any(c[0] in ('scale', 'delete', 'shutdown') for c in self.minio.events))

    def test_existing_objects_require_overwrite_and_extra_objects_survive(self):
        path = self.root / 'minio.tar'
        metadata = self.minio.backup(path)
        with self.assertRaisesRegex(backup.BackupError, '--overwrite'):
            self.minio.preflight(metadata, False)
        self.minio.objects['bucket-a']['extra'] = b'keep'
        self.minio.objects['bucket-a']['zero'] = b'old'
        self.minio.restore(path, metadata, True)
        self.assertEqual(self.minio.objects['bucket-a']['zero'], b'')
        self.assertEqual(self.minio.objects['bucket-a']['extra'], b'keep')
        self.assertFalse(any('--remove' in c for c in self.minio.events))

    def test_transfer_failure_cleans_workspace(self):
        self.minio.fail_transfer = True
        with self.assertRaisesRegex(backup.BackupError, 'transfer failed'):
            self.minio.backup(self.root / 'minio.tar')
        self.assertTrue(all(not Path(w).exists() for w in self.minio.workspaces))

    def test_concurrent_size_change_invalidates_export(self):
        self.minio.change_during_backup = True
        with self.assertRaisesRegex(backup.BackupError, '并发修改'):
            self.minio.backup(self.root / 'minio.tar')

    def test_sandbox_workspace_is_exported_and_each_bucket_reports_count(self):
        self.minio.objects['sandbox-workspace'] = {'workspace/file': b'sandbox content'}
        self.minio.buckets = sorted(self.minio.objects)
        self.minio.log = mock.Mock()
        path = self.root / 'all-buckets.tar'
        metadata = self.minio.backup(path)
        self.assertEqual(archive_inventory(path, metadata, backup.BackupError)[
            ('sandbox-workspace', 'workspace/file')], len(b'sandbox content'))
        messages = [call.args[0] for call in self.minio.log.call_args_list]
        self.assertIn('MinIO sandbox-workspace: 导出完成 1/1 个对象', messages)
        self.assertIn('MinIO empty-bucket: 导出完成 0/0 个对象', messages)

    def test_download_failure_reports_object_and_mc_stdout_error(self):
        original = self.minio.mc
        def missing_object(work, *args, **kwargs):
            if args[0] == 'cat':
                kwargs['target'].write(json.dumps({'status': 'error', 'error': {
                    'message': 'Unable to read object', 'cause': {'message': 'The specified key does not exist.',
                    'error': {'Code': 'NoSuchKey'}}}}).encode())
                raise backup.BackupError('command terminated with exit code 1')
            return original(work, *args, **kwargs)
        with mock.patch.object(self.minio, 'mc', side_effect=missing_object):
            with self.assertRaisesRegex(backup.BackupError, 'bucket=bucket-a.*key=dir/file.txt.*NoSuchKey'):
                self.minio.backup(self.root / 'failed.tar')

    def test_partial_object_contents_are_not_printed_on_download_failure(self):
        original = self.minio.mc
        def interrupted(work, *args, **kwargs):
            if args[0] == 'cat':
                kwargs['target'].write(b'private object contents')
                raise backup.BackupError('connection reset')
            return original(work, *args, **kwargs)
        with mock.patch.object(self.minio, 'mc', side_effect=interrupted):
            with self.assertRaises(backup.BackupError) as caught:
                self.minio.backup(self.root / 'failed.tar')
        self.assertIn('key=dir/file.txt', str(caught.exception))
        self.assertIn('connection reset', str(caught.exception))
        self.assertNotIn('private object contents', str(caught.exception))

    def test_error_detail_ignores_non_error_object_json(self):
        self.assertEqual(mc_error_detail(io.BytesIO(b'{"private":"object data"}')), '')
        self.assertEqual(mc_error_detail(io.BytesIO(b'x' * (64 * 1024 + 1))), '')

    def test_unrepresentable_keys_rejected_before_export(self):
        for key in ('a//b', 'a/../b', '/absolute', './a', 'a\\b', '.'):
            with self.subTest(key=key), self.assertRaises(backup.BackupError):
                object_key(key, backup.BackupError)
        self.minio.objects = {'bucket-a': {'a': b'1', 'a/b': b'2', 'sessions/': b'', 'sessions/one/': b''}}
        self.minio.buckets = ['bucket-a']
        original = {'bucket-a': {'a': b'1', 'a/b': b'2'}}
        path = self.root / 'minio.tar'
        metadata = self.minio.backup(path)
        self.minio.objects = {}
        self.minio.restore(path, metadata, False)
        self.assertEqual(self.minio.objects, original)

    def test_unsafe_inner_archive_rejected(self):
        for name, kind in (('../escape', tarfile.REGTYPE), ('bucket-a/link', tarfile.SYMTYPE),
                           ('other-bucket/x', tarfile.REGTYPE)):
            path = self.root / 'bad.tar'
            with tarfile.open(path, 'w:') as tar:
                entry = tarfile.TarInfo(name)
                entry.type = kind
                tar.addfile(entry)
            with self.subTest(name=name), self.assertRaises(backup.BackupError):
                archive_inventory(path, {'mode': 'current-objects', 'buckets': ['bucket-a']}, backup.BackupError)

    def test_invalid_object_mapping_is_rejected_before_target_access(self):
        for record in (
                {'file': '../escape', 'bucket': 'bucket-a', 'key': 'good', 'size': 0},
                {'file': 'objects/000000000000', 'bucket': 'undeclared', 'key': 'good', 'size': 0},
                {'file': 'objects/000000000000', 'bucket': 'bucket-a', 'key': 'a/../b', 'size': 0},
                {'file': 'objects/000000000000', 'bucket': 'bucket-a', 'key': 'good', 'size': 99}):
            path = self.root / 'bad-map.tar'
            with tarfile.open(path, 'w:') as tar:
                index = json.dumps([record]).encode()
                member = tarfile.TarInfo('objects.json')
                member.size = len(index)
                tar.addfile(member, io.BytesIO(index))
                tar.addfile(tarfile.TarInfo('objects/000000000000'))
            metadata = {'mode': 'current-objects', 'buckets': ['bucket-a']}
            with mock.patch.object(self.minio, 'preflight') as preflight:
                with self.subTest(record=record), self.assertRaises(backup.BackupError):
                    self.minio.restore(path, metadata, True)
                preflight.assert_not_called()

    def test_restore_verification_failure_is_reported(self):
        path = self.root / 'minio.tar'
        metadata = self.minio.backup(path)
        self.minio.objects = {}
        self.minio.fail_verification = True
        with self.assertRaisesRegex(backup.BackupError, '校验失败'):
            self.minio.restore(path, metadata, False)

    def test_no_container_tar_or_object_staging_required(self):
        path = self.root / 'minio.tar'
        metadata = self.minio.backup(path)
        self.minio.objects = {}
        self.minio.restore(path, metadata, False)
        self.assertFalse(any(c[0] in ('tar', 'df', 'mirror') for c in self.minio.events))
        self.assertTrue(any(c[0] == 'cat' for c in self.minio.events))
        self.assertTrue(any(c[0] == 'pipe' for c in self.minio.events))
        upload = next(c for c in self.minio.events if c[0] == 'pipe')
        self.assertEqual(upload[1:5], ('--part-size', '8MiB', '--concurrent', '1'))

    def test_metadata_validation(self):
        for buckets in (['bucket-a', 'bucket-a'], [None], [{}], ['../bad']):
            with self.subTest(buckets=buckets), self.assertRaises(backup.BackupError):
                validate_metadata({'mode': 'current-objects', 'buckets': buckets}, backup.BackupError)

    def test_v4_package_roundtrip_with_optional_redis(self):
        for with_redis in (False, True):
            root = self.root / str(with_redis)
            root.mkdir()
            args = backup.parser().parse_args(['backup', 'run'])
            cluster = FakeCluster(args)
            cluster.minio = self.minio
            cluster.components.append('minio')
            if with_redis:
                cluster.redis = mock.Mock()
                cluster.components.append('redis')
                def save(path):
                    path.write_bytes(b'REDIS-test')
                    return {'version': '7.4.6', 'snapshot_time': 1792372153}
                cluster.redis.backup.side_effect = save
            with mock.patch.object(backup, 'log'), contextlib.redirect_stdout(io.StringIO()):
                cluster.backup(root)
                stage = root / 'unpack'
                stage.mkdir()
                manifest = backup.unpack(next(root.glob('*.tar.gz')), stage)
                self.assertEqual(manifest['format'], 5)
                self.assertEqual('redis' in manifest, with_redis)
                self.assertIn('minio.tar', manifest['sha256'])
                cluster.args.writers_stopped = True
                self.minio.objects = {}
                cluster.restore(stage, manifest)
            self.assertEqual(self.minio.objects['bucket-a']['dir/file.txt'], b'hello')

    def test_failed_minio_export_does_not_publish_package(self):
        cluster = FakeCluster(backup.parser().parse_args(['backup', 'run']))
        cluster.minio = self.minio
        cluster.components.append('minio')
        self.minio.fail_transfer = True
        with mock.patch.object(backup, 'log'), self.assertRaises(backup.BackupError):
            cluster.backup(self.root)
        self.assertFalse(list(self.root.glob('*.tar.gz')))

    def test_container_credentials_stay_out_of_host_argv(self):
        args = backup.parser().parse_args(['backup', 'check'])
        pod = {'metadata': {'name': 'minio-0'}, 'spec': {'containers': [{'name': 'minio'}]},
               'status': {'phase': 'Running', 'conditions': [{'type': 'Ready', 'status': 'True'}]}}
        calls = []
        def run(argv, **kwargs):
            calls.append(argv)
            if 'get' in argv:
                return json.dumps({'items': [pod]}).encode()
            if 'ls' in argv:
                return b'{"status":"success","key":"bucket-a/","type":"folder"}\n'
            return b''
        minio = MinioBackup(args, run, backup.BackupError, lambda message: None)
        self.assertEqual(minio.buckets, ['bucket-a'])
        self.assertTrue(all(c[0] == 'kubectl' for c in calls))
        auth = next(c for c in calls if 'minio-auth' in c)
        self.assertIn('MINIO_ROOT_PASSWORD', auth[auth.index('-c', auth.index('--')) + 1])
        self.assertTrue(any('rm' in c for c in calls))


if __name__ == '__main__':
    unittest.main()
