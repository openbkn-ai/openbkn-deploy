"""Offline Kafka archive and existing-image orchestration regressions."""
import contextlib
import copy
import hashlib
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest import mock

import backup
import kafka_backup
import kafka_worker as worker


def make_package(path):
    records = [dict(offset=100, timestamp=1234, timestamp_type=1, key=None, value=worker.encode(b'\x00\xff\n'),
                    headers=[['same', None], ['same', ''], ['中文🙂', worker.encode(b'\\\n')]]),
               dict(offset=101, timestamp=1234, timestamp_type=1, key='', value=None, headers=[]),
               dict(offset=105, timestamp=1234, timestamp_type=1, key=worker.encode(b'key'), value='', headers=[])]
    raw = b''.join(worker.json_bytes(row) for row in records)
    canonical = b''.join(worker.content(row) for row in records)
    config = {key: '1' for key in worker.CONFIG_KEYS}
    config.update({'cleanup.policy': 'delete', 'message.timestamp.type': 'CreateTime', 'retention.ms': '-1', 'retention.bytes': '-1'})
    part = dict(id=0, low=100, high=106, file='topic-a-0.ndjson', count=3,
                sha256=hashlib.sha256(raw).hexdigest(), content_sha256=hashlib.sha256(canonical).hexdigest())
    meta = dict(mode=worker.MODE, created_at=1.0, source_cluster_id='source',
                topics=[dict(name='topic-a', topic_id='original', config=config, partitions=[part])],
                groups={'consumer-a': [dict(topic='topic-a', partition=0, offset=102)]})
    with tarfile.open(path, 'w:') as archive:
        for name, data in [('metadata.json', worker.json_bytes(meta)), (part['file'], raw)]:
            entry = tarfile.TarInfo(name)
            entry.size = len(data)
            archive.addfile(entry, io.BytesIO(data))
    return meta


class KubeHarness:
    def __init__(self, path, meta):
        self.path, self.meta, self.calls, self.pods = path, meta, [], []
        self.waiting, self.events = None, []
        self.broker = {'metadata': {'name': 'kafka-broker-0', 'labels': {'app.kubernetes.io/instance': 'kafka'}},
                       'spec': {'nodeName': 'node1', 'containers': [{'name': 'kafka', 'image': 'existing/kafka:3.9'}]},
                       'status': {'phase': 'Running', 'conditions': [{'type': 'Ready', 'status': 'True'}],
                                  'containerStatuses': [{'name': 'kafka', 'imageID': 'docker-pullable://existing/kafka@sha256:abc'}]}}

    def __call__(self, argv, **kwargs):
        self.calls.append((argv, kwargs))
        args = argv[3:]
        if args[:2] == ['get', 'pods']:
            return json.dumps({'items': [self.broker]}).encode()
        if args[:2] == ['get', 'pod']:
            if args[2] == 'kafka-broker-0':
                return json.dumps(self.broker).encode()
            status = dict(phase='Pending', containerStatuses=[dict(state=dict(waiting=self.waiting))]) if self.waiting else self.broker['status']
            return json.dumps({'status': status}).encode()
        if args[:2] == ['get', 'events']:
            return json.dumps({'items': self.events}).encode()
        if args[0] == 'create':
            resource = json.loads(kwargs['data'])
            if resource['kind'] == 'Pod':
                self.pods.append(resource)
            return b''
        if args[0] == 'delete':
            return b''
        if args[0] == 'exec':
            if 'OpenBknKafkaBackup' in args:
                operation = args[args.index('OpenBknKafkaBackup') + 1]
                return json.dumps({'check': self.meta, 'export': self.meta, 'health': {'topics': [], 'mode': worker.MODE},
                                   'restore-check': {'topics': ['topic-a']}, 'restore': {'result': 'success', 'records': 3}}[operation]).encode()
            if 'tar' in args:
                kwargs['target'].write(self.path.read_bytes())
            elif kwargs.get('source'):
                assert kwargs['source'].read()
            else:
                assert kwargs['data'].startswith(b'PK')
            return b''
        raise AssertionError(argv)


class KafkaTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.path = self.root / 'source.tar'
        self.meta = make_package(self.path)
        self.args = backup.parser().parse_args(['backup', 'check', '--component', 'kafka'])
        self.kube = KubeHarness(self.path, self.meta)
        self.service = kafka_backup.KafkaBackup(self.args, self.kube, backup.BackupError, mock.Mock())

    def test_binary_null_empty_unicode_and_duplicate_headers_preserved(self):
        self.assertEqual(worker.inventory(self.path), self.meta)
        with tarfile.open(self.path) as archive:
            records = list(worker.rows(archive.extractfile('topic-a-0.ndjson')))
        self.assertEqual(worker.decode(records[0]['value']), b'\x00\xff\n')
        self.assertIsNone(records[1]['value'])
        self.assertEqual(records[2]['value'], '')
        self.assertEqual(records[0]['headers'][:2], [['same', None], ['same', '']])

    def test_compact_and_out_of_range_group_position_rejected(self):
        for position in (99, 107):
            meta = copy.deepcopy(self.meta)
            meta['groups']['consumer-a'][0]['offset'] = position
            with self.assertRaisesRegex(worker.KafkaBackupError, '消费位置'):
                worker.validate_metadata(meta)
        meta = copy.deepcopy(self.meta)
        meta['topics'][0]['config']['cleanup.policy'] = 'compact,delete'
        with self.assertRaisesRegex(worker.KafkaBackupError, 'cleanup.policy'):
            worker.validate_metadata(meta)

    def test_malformed_archive_rejected_before_kubernetes_access(self):
        bad = self.root / 'bad.tar'
        with tarfile.open(self.path) as source, tarfile.open(bad, 'w:') as target:
            for entry in source:
                data = source.extractfile(entry).read()
                if entry.name.endswith('.ndjson'):
                    entry.name = '../outside'
                target.addfile(entry, io.BytesIO(data))
        with self.assertRaises(backup.BackupError):
            self.service.restore(bad, self.meta)
        self.assertEqual(self.kube.calls, [])

    def test_reuses_existing_image_same_node_without_pulls_or_host_kafka_dependency(self):
        self.service.check()
        spec = self.kube.pods[0]['spec']
        container = spec['containers'][0]
        self.assertEqual(container['image'], 'existing/kafka@sha256:abc')
        self.assertEqual(container['imagePullPolicy'], 'Never')
        self.assertEqual(container['command'][0], 'sh')
        self.assertEqual(spec['affinity']['nodeAffinity']['requiredDuringSchedulingIgnoredDuringExecution']
                         ['nodeSelectorTerms'][0]['matchFields'][0]['values'], ['node1'])
        self.assertFalse(spec['automountServiceAccountToken'])
        self.assertEqual(spec['volumes'][1]['projected']['sources'][0]['secret']['name'], 'kafka-sasl')
        self.assertEqual(self.args.kafka_bootstrap, 'kafka.resource.svc.cluster.local:9092')
        self.assertNotIn('sasl.password', json.dumps(spec))
        self.assertEqual(self.kube.calls[-1][0][3:5], ['delete', 'pod'])
        java = next(a for a, k in self.kube.calls if 'OpenBknKafkaBackup' in a)
        self.assertIn('-Djava.io.tmpdir=/work', java)
        self.assertIn('-Dfile.encoding=UTF-8', java)
        self.assertIn('-Dlog4j.configuration=file:/work/log4j.properties', java)

    def test_image_error_reports_immediately_and_cleans_up(self):
        self.kube.waiting = {'reason': 'ErrImageNeverPull', 'message': 'image missing on node1'}
        with mock.patch.object(kafka_backup.time, 'sleep', side_effect=AssertionError('must fail immediately')):
            with self.assertRaisesRegex(backup.BackupError, 'ErrImageNeverPull.*image missing'):
                self.service.check()
        self.assertEqual(self.kube.calls[-1][0][3:5], ['delete', 'pod'])

    def test_missing_secret_reports_without_three_minute_wait(self):
        self.kube.waiting = {'reason': 'ContainerCreating'}
        self.kube.events = [{'reason': 'FailedMount', 'message': 'secret kafka-sasl not found'}]
        with mock.patch.object(kafka_backup.time, 'sleep', side_effect=AssertionError('must fail immediately')):
            with self.assertRaisesRegex(backup.BackupError, '挂载失败.*not found'):
                self.service.check()

    def test_restore_health_does_not_require_target_business_topics(self):
        self.args.module = 'restore'
        self.service.check()
        java_calls = [a for a, k in self.kube.calls if 'OpenBknKafkaBackup' in a]
        self.assertEqual(java_calls[0][-1], 'health')

    def test_backup_archive_is_streamed_using_existing_tar(self):
        path = self.root / 'copy.tar'
        meta = self.service.backup(path)
        self.assertEqual(worker.inventory(path), meta)
        self.assertTrue(any('tar' in a and '-cf' in a for a, k in self.kube.calls))
        self.assertTrue(any(a[3:5] == ['delete', 'configmap'] for a, k in self.kube.calls))

    def test_restore_validates_uploads_then_runs_java(self):
        self.service.restore(self.path, self.meta)
        java_calls = [a for a, k in self.kube.calls if 'OpenBknKafkaBackup' in a]
        self.assertEqual(java_calls[-1][-2:], ['restore', '/work/import'])

    def test_historical_replay_error_is_local_clear_and_cannot_be_overwritten(self):
        for name in worker.LEDGER_GROUPS:
            metadata = copy.deepcopy(self.meta)
            metadata['topics'][0]['name'] = name
            with self.assertRaisesRegex(backup.BackupError, name + '.*offset 空洞.*未写入目标数据'):
                self.service.preflight(metadata, overwrite=True)
        self.assertEqual(self.kube.calls, [])

    def test_backup_check_warns_about_unsupported_restore_topics(self):
        self.meta['topics'][0]['name'] = 'openbkn.audit.v1'
        self.service.check()
        self.assertTrue(any('恢复限制' in call.args[0] and 'openbkn.audit.v1' in call.args[0]
                            for call in self.service.log.call_args_list))

    def test_blocked_package_fails_before_cluster_discovery_for_check_and_run(self):
        meta = copy.deepcopy(self.meta)
        name = 'openbkn.audit.v1'
        meta['topics'][0]['name'] = name
        meta['topics'][0]['partitions'][0]['file'] = name + '-0.ndjson'
        meta['groups']['consumer-a'][0]['topic'] = name
        inner = self.root / 'audit.tar'
        with tarfile.open(self.path) as original, tarfile.open(inner, 'w:') as target:
            for entry in original:
                data = original.extractfile(entry).read()
                if entry.name == 'metadata.json':
                    data = worker.json_bytes(meta)
                else:
                    entry.name = name + '-0.ndjson'
                entry.size = len(data)
                target.addfile(entry, io.BytesIO(data))
        # Backup validation accepts the archive; only restore capability is gated.
        self.assertEqual(worker.inventory(inner), meta)
        manifest = dict(format=4, components=['kafka'], kafka=meta,
                        sha256={'kafka.tar': hashlib.sha256(inner.read_bytes()).hexdigest()})
        package = self.root / 'audit.tar.gz'
        with tarfile.open(package, 'w:gz') as target:
            for filename, data in [('manifest.json', worker.json_bytes(manifest)), ('kafka.tar', inner.read_bytes())]:
                entry = tarfile.TarInfo(filename)
                entry.size = len(data)
                target.addfile(entry, io.BytesIO(data))
        with mock.patch.object(backup, 'Cluster') as cluster:
            for action in ('check', 'run'):
                with self.assertRaisesRegex(backup.BackupError, name + '.*offset 空洞'):
                    backup.main(['restore', action, str(package), '--component', 'kafka', '--writers-stopped', '--overwrite'])
            cluster.assert_not_called()

    def ledger_meta(self, name='openbkn.audit.v1', high=117):
        meta = copy.deepcopy(self.meta)
        topic = meta['topics'][0]
        topic['name'] = name
        topic['config']['message.timestamp.type'] = 'LogAppendTime'
        topic['partitions'][0].update(low=0, high=high, count=high, file=name + '-0.ndjson')
        meta['groups'] = {worker.LEDGER_GROUPS[name]: [dict(topic=name, partition=0, offset=high)]} if high else {}
        return meta

    def test_fully_consumed_complete_audit_history_is_restorable_without_replay(self):
        meta = self.ledger_meta()
        self.assertEqual(kafka_backup.blocked_restore_topics(meta), [])
        kafka_backup.validate_restore_metadata(meta, backup.BackupError)
        self.service.preflight(meta)
        self.assertTrue(any('OpenBknKafkaBackup' in a for a, k in self.kube.calls))
        self.assertEqual(worker.ledger_restore_topics(meta), ['openbkn.audit.v1'])

    def test_empty_evidence_never_requires_historical_replay_or_database_confirmation(self):
        meta = self.ledger_meta('openbkn.evidence.v1', 0)
        kafka_backup.validate_restore_metadata(meta, backup.BackupError)
        kafka_backup.require_restored_ledger(meta, False, backup.BackupError)

    def test_consumed_history_requires_same_batch_ledger_confirmation(self):
        meta = self.ledger_meta()
        with self.assertRaisesRegex(backup.BackupError, 'MariaDB 账本.*同批恢复.*--kafka-ledger-restored'):
            kafka_backup.require_restored_ledger(meta, False, backup.BackupError)
        kafka_backup.require_restored_ledger(meta, True, backup.BackupError)

    def test_ledger_backlog_missing_group_or_secondary_group_backlog_stays_blocked(self):
        for case in ('backlog', 'missing', 'other'):
            with self.subTest(case=case):
                meta = self.ledger_meta()
                if case == 'backlog':
                    meta['groups']['bkn-trace-audit-ledger-v1'][0]['offset'] = 116
                elif case == 'missing':
                    meta['groups'] = {}
                else:
                    meta['groups']['other'] = [dict(topic='openbkn.audit.v1', partition=0, offset=116)]
                with self.assertRaisesRegex(backup.BackupError, '可信历史重放'):
                    kafka_backup.validate_restore_metadata(meta, backup.BackupError)

    def test_coordinate_reuse_is_blocked_even_with_all_groups_at_end(self):
        for low, count in [(1, 116), (0, 116)]:
            meta = self.ledger_meta()
            meta['topics'][0]['partitions'][0].update(low=low, count=count)
            with self.assertRaisesRegex(backup.BackupError, 'Kafka 坐标'):
                kafka_backup.validate_restore_metadata(meta, backup.BackupError)

    def test_no_ready_broker_fails_before_helper_creation(self):
        self.kube.broker['status']['conditions'] = []
        with self.assertRaisesRegex(backup.BackupError, '未找到就绪'):
            self.service.check()
        self.assertEqual(self.kube.pods, [])

    def test_discovery_uses_namespace_release_and_explicit_endpoint(self):
        self.kube.broker['metadata']['labels']['app.kubernetes.io/instance'] = 'tenant-kafka'
        self.args.kafka_namespace = 'custom'
        self.service.discover()
        self.assertEqual(self.args.kafka_bootstrap, 'tenant-kafka.custom.svc.cluster.local:9092')
        self.assertEqual(self.args.kafka_secret, 'tenant-kafka-sasl')

    def test_bundled_jar_and_sources_match_release_metadata(self):
        jar = self.service.artifact()
        self.assertTrue(jar.startswith(b'PK'))
        self.assertLess(len(jar), 100 * 1024)
        root = Path(kafka_backup.__file__).resolve().parents[1] / 'tools/kafka-backup'
        info = json.loads((root / 'artifact.json').read_text())
        self.assertEqual(hashlib.sha256((root / 'tests/KafkaBackupSmoke.java').read_bytes()).hexdigest(), info['tests_source_sha256'])
        self.assertEqual(hashlib.sha256((root / 'kafka-backup-tests.jar').read_bytes()).hexdigest(), info['tests_jar_sha256'])

    def test_default_scope_and_include_flag(self):
        args = backup.parser().parse_args(['backup', 'run'])
        self.assertEqual(list(backup.PAYLOADS), backup.configured_components(args))
        args.include_kafka = True
        self.assertIn('kafka', backup.configured_components(args))


if __name__ == '__main__':
    unittest.main()
