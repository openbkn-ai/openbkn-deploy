"""Host side Kafka orchestration; only stdlib and kubectl are required here."""
import contextlib
import hashlib
import json
from pathlib import Path
import time
import uuid

from kafka_worker import inventory, KafkaBackupError, restore_blockers, ledger_restore_topics, LEDGER_GROUPS, IGNORED_TOPICS


def archive_inventory(path, expected, error):
    try:
        return inventory(path, expected)
    except (KafkaBackupError, TypeError, KeyError, ValueError) as exc:
        raise error('Kafka 备份包无效: ' + str(exc)) from exc


def blocked_restore_topics(metadata, *, counts_known=True):
    return sorted(restore_blockers(metadata, counts_known=counts_known))


def validate_restore_metadata(metadata, error):
    blocked = restore_blockers(metadata)
    if blocked:
        raise error('Kafka 备份包包含暂不支持恢复的 Topic: '
                    + '；'.join(name + '（' + reason + '）' for name, reason in sorted(blocked.items()))
                    + '。本次未写入目标数据；--overwrite 不能绕过此限制。')


def require_restored_ledger(metadata, restored, error):
    topics = ledger_restore_topics(metadata)
    if topics and not restored:
        raise error('恢复审计/Evidence 保留记录前，请确认原 MariaDB 账本已保留或同批恢复，'
                    '并保持备份中的消费组 ID；完成后添加 --kafka-ledger-restored 确认。'
                    '消费组将从备份提交位置继续，已消费记录不会从头重放: ' + ', '.join(topics))


def add_arguments(parser):
    import os
    parser.add_argument('--include-kafka', action='store_true', help='兼容旧命令；默认备份已包含 Kafka，无需指定')
    parser.add_argument('--kafka-namespace', default=os.environ.get('KAFKA_NAMESPACE', 'resource'))
    parser.add_argument('--kafka-bootstrap', help='默认按 Broker 所属 Helm release/namespace 推导 Service 地址')
    parser.add_argument('--kafka-selector', default='app.kubernetes.io/name=kafka,app.kubernetes.io/component=broker')
    parser.add_argument('--kafka-pod', help='指定用于发现镜像和节点的现有 Broker Pod')
    parser.add_argument('--kafka-container', default='kafka')
    parser.add_argument('--kafka-libs', default='/opt/bitnami/kafka/libs', help='现有 Kafka 镜像中的客户端库目录')
    parser.add_argument('--kafka-protocol', choices=['PLAINTEXT', 'SASL_PLAINTEXT', 'SSL', 'SASL_SSL'], default='SASL_PLAINTEXT')
    parser.add_argument('--kafka-user', default='kafkauser')
    parser.add_argument('--kafka-secret', help='默认使用 <Kafka Helm release>-sasl，密码不离开集群')
    parser.add_argument('--kafka-password-key', default='client-passwords')
    parser.add_argument('--kafka-ca-secret', help='含 ca.crt 的 Secret，供 TLS 连接使用')
    parser.add_argument('--kafka-mechanism', choices=['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512'], default='PLAIN')
    parser.add_argument('--kafka-topics', nargs='+', help='仅备份：默认枚举非内部 Topic，始终排除 connect-configs/connect-offsets/connect-status；其他 compact Topic 不支持')
    parser.add_argument('--kafka-replicas', type=int, default=1, help='恢复时新建 Topic 的副本数')
    parser.add_argument('--kafka-ledger-restored', action='store_true',
                        help='确认停写后同批备份的审计/Evidence 账本已保留或恢复，并沿用原消费组位置')
    parser.add_argument('--kafka-work-size', default='10Gi', help='临时工具 Pod 磁盘上限，需容纳导出或导入数据')


class KafkaBackup:
    def __init__(self, args, run, error, log):
        self.args, self.run, self.error, self.log = args, run, error, log
        self.broker = None

    def kube(self, *command, **kwargs):
        return self.run(['kubectl', '-n', self.args.kafka_namespace, *command],
                        timeout=kwargs.pop('timeout', self.args.timeout), **kwargs)

    @contextlib.contextmanager
    def helper(self, *, overwrite=False):
        artifact = self.artifact()
        broker, container = self.discover()
        name = 'openbkn-kafka-backup-' + uuid.uuid4().hex[:12]
        env = [{'name': name, 'value': str(value)} for name, value in {
            'TASK_KAFKA_BROKERS': self.args.kafka_bootstrap,
            'TASK_KAFKA_PROTOCOL': self.args.kafka_protocol,
            'TASK_KAFKA_USER': self.args.kafka_user,
            'TASK_KAFKA_MECHANISM': self.args.kafka_mechanism,
            'TASK_KAFKA_REPLICAS': self.args.kafka_replicas,
            'TASK_KAFKA_LEDGER_RESTORED': str(self.args.kafka_ledger_restored).lower(),
            'TASK_KAFKA_OVERWRITE': str(overwrite).lower(),
        }.items()]
        sources = []
        if self.args.kafka_protocol.startswith('SASL'):
            sources.append({'secret': {'name': self.args.kafka_secret, 'items': [
                {'key': self.args.kafka_password_key, 'path': 'password'}]}})
        if self.args.kafka_ca_secret:
            sources.append({'secret': {'name': self.args.kafka_ca_secret, 'items': [
                {'key': 'ca.crt', 'path': 'ca.crt'}]}})
        volumes = [{'name': 'work', 'emptyDir': {'sizeLimit': self.args.kafka_work_size}}]
        mounts = [{'name': 'work', 'mountPath': '/work'}]
        if sources:
            volumes.append({'name': 'credentials', 'projected': {'defaultMode': 0o444, 'sources': sources}})
            mounts.append({'name': 'credentials', 'mountPath': '/credentials', 'readOnly': True})
        spec = {'restartPolicy': 'Never', 'automountServiceAccountToken': False,
                'activeDeadlineSeconds': self.args.timeout + 300,
                'securityContext': {'runAsNonRoot': True, 'runAsUser': 1000, 'runAsGroup': 1000, 'fsGroup': 1000},
                'affinity': {'nodeAffinity': {'requiredDuringSchedulingIgnoredDuringExecution': {
                    'nodeSelectorTerms': [{'matchFields': [{'key': 'metadata.name', 'operator': 'In',
                                                           'values': [broker['spec']['nodeName']]}]}]}}},
                'tolerations': broker['spec'].get('tolerations', []),
                'volumes': volumes, 'containers': [{
                    'name': 'backup', 'image': container['image'], 'imagePullPolicy': 'Never',
                    'command': ['sh', '-c', 'umask 077; exec sleep 86400'], 'env': env,
                    'volumeMounts': mounts,
                    'resources': {'requests': {'cpu': '100m', 'memory': '128Mi', 'ephemeral-storage': '1Gi'},
                                  'limits': {'cpu': '1', 'memory': '512Mi', 'ephemeral-storage': self.args.kafka_work_size}},
                    'securityContext': {'allowPrivilegeEscalation': False, 'readOnlyRootFilesystem': True,
                                        'capabilities': {'drop': ['ALL']}}}]}
        pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': name,
               'labels': {'app.kubernetes.io/name': 'openbkn-kafka-backup'}}, 'spec': spec}
        created = False
        try:
            self.kube('create', '-f', '-', data=json.dumps(pod).encode(), timeout=60)
            created = True
            self.active_pod = name
            self.log('复用 Kafka 镜像 %s，节点 %s，创建临时 Pod %s' %
                     (container['image'], broker['spec']['nodeName'], name))
            self.wait_ready(name)
            command = ['exec', '-i', name, '-c', 'backup', '--']
            self.kube(*command, 'sh', '-c',
                "umask 077; cat > /work/kafka-backup.jar; printf '%s\\n' "
                "'log4j.rootLogger=ERROR,stderr' "
                "'log4j.appender.stderr=org.apache.log4j.ConsoleAppender' "
                "'log4j.appender.stderr.Target=System.err' "
                "'log4j.appender.stderr.layout=org.apache.log4j.PatternLayout' "
                "'log4j.appender.stderr.layout.ConversionPattern=%p %c: %m%n' > /work/log4j.properties",
                data=artifact, timeout=60)
            yield lambda operation, *rest, **kw: self.kube(*command, 'java', '-Xms32m', '-Xmx256m',
                '-XX:MaxDirectMemorySize=64m', '-XX:-UsePerfData', '-Djava.io.tmpdir=/work',
                '-Dfile.encoding=UTF-8', '-Dlog4j.configuration=file:/work/log4j.properties',
                '-cp', '/work/kafka-backup.jar:' + self.args.kafka_libs + '/*',
                'OpenBknKafkaBackup', operation, *rest, **kw)
        finally:
            if created:
                try:
                    self.kube('delete', 'pod', name, '--wait=false', timeout=60)
                except Exception:
                    self.log('Kafka 工具 Pod 清理失败，请手工清理: ' + name)
                self.active_pod = None

    def artifact(self):
        root = Path(__file__).resolve().parents[1] / 'tools/kafka-backup'
        try:
            info = json.loads((root / 'artifact.json').read_text())
            jar = (root / 'kafka-backup.jar').read_bytes()
            source = (root / 'src/OpenBknKafkaBackup.java').read_bytes()
        except (OSError, ValueError) as exc:
            raise self.error('缺少随部署仓库交付的 Kafka Java 工具，请同步完整的 tools/kafka-backup 目录') from exc
        if (hashlib.sha256(jar).hexdigest() != info.get('jar_sha256')
                or hashlib.sha256(source).hexdigest() != info.get('source_sha256')):
            raise self.error('Kafka Java 工具与源码/校验清单不匹配，请同步同一版本的部署文件')
        return jar

    def discover(self):
        if self.broker is None:
            args = ['get', 'pod', self.args.kafka_pod] if self.args.kafka_pod else ['get', 'pods', '-l', self.args.kafka_selector]
            result = json.loads(self.kube(*args, '-o', 'json', timeout=30))
            pods = [result] if self.args.kafka_pod else result.get('items', [])
            ready = [pod for pod in pods if not pod['metadata'].get('deletionTimestamp')
                     and pod.get('status', {}).get('phase') == 'Running'
                     and any(c.get('type') == 'Ready' and c.get('status') == 'True' for c in pod.get('status', {}).get('conditions', []))]
            if not ready:
                raise self.error('未找到就绪 Kafka Broker，请指定 --kafka-pod 或 --kafka-selector')
            self.broker = sorted(ready, key=lambda p: p['metadata']['name'])[0]
            release = self.broker['metadata'].get('labels', {}).get('app.kubernetes.io/instance', 'kafka')
            if not self.args.kafka_bootstrap:
                self.args.kafka_bootstrap = '%s.%s.svc.cluster.local:9092' % (release, self.args.kafka_namespace)
            if not self.args.kafka_secret:
                self.args.kafka_secret = release + '-sasl'
        candidates = [c for c in self.broker['spec']['containers'] if c['name'] == self.args.kafka_container]
        if len(candidates) != 1 or not self.broker['spec'].get('nodeName'):
            raise self.error('Kafka Broker 容器/节点发现失败，请检查 --kafka-container')
        container = dict(candidates[0])
        # A digest avoids tags changing after the Broker was started. Never pulls a new image.
        status = next((c for c in self.broker.get('status', {}).get('containerStatuses', [])
                       if c['name'] == container['name']), {})
        image_id = status.get('imageID', '').removeprefix('docker-pullable://')
        if '@sha256:' in image_id and '://' not in image_id:
            container['image'] = image_id
        return self.broker, container

    def wait_ready(self, name):
        deadline, next_log = time.monotonic() + min(self.args.timeout, 180), time.monotonic() + 10
        while time.monotonic() < deadline:
            pod = json.loads(self.kube('get', 'pod', name, '-o', 'json', timeout=30))
            status = pod.get('status', {})
            if status.get('phase') in ('Failed', 'Succeeded'):
                raise self.error('Kafka 临时 Pod 已退出: %s %s' % (name, status.get('message', status.get('reason', ''))))
            if any(c.get('type') == 'Ready' and c.get('status') == 'True' for c in status.get('conditions', [])):
                return
            for container in status.get('containerStatuses', []):
                state = container.get('state', {})
                waiting = state.get('waiting', {})
                if state.get('terminated') or waiting.get('reason') in (
                        'ErrImagePull', 'ImagePullBackOff', 'ErrImageNeverPull', 'InvalidImageName',
                        'CreateContainerConfigError', 'CreateContainerError', 'CrashLoopBackOff'):
                    raise self.error('Kafka 临时 Pod 启动失败 %s: %s %s' %
                                     (name, waiting.get('reason', 'Terminated'), waiting.get('message', '')))
            conditions = status.get('conditions', [])
            events = json.loads(self.kube('get', 'events', '--field-selector',
                'involvedObject.name=' + name + ',type=Warning', '-o', 'json', timeout=30)).get('items', [])
            fatal = next((e for e in events if e.get('reason') == 'FailedMount' and
                          ('not found' in e.get('message', '') or 'references non-existent' in e.get('message', ''))), None)
            if fatal:
                raise self.error('Kafka 临时 Pod 挂载失败: ' + fatal.get('message', ''))
            if time.monotonic() >= next_log:
                reason = next((c.get('message', '') for c in conditions if c.get('type') == 'PodScheduled' and c.get('status') == 'False'), '')
                self.log('等待 Kafka 临时 Pod %s: %s' % (name, reason or status.get('phase', 'Pending')))
                next_log = time.monotonic() + 10
            time.sleep(1)
        raise self.error('Kafka 临时 Pod 启动超时: ' + name + '；请检查调度、Secret 和节点缓存镜像')

    @contextlib.contextmanager
    def lock(self):
        self.discover()
        # Atomic create serializes different execution hosts using this bootstrap endpoint.
        name = 'openbkn-kafka-lock-' + hashlib.sha256(self.args.kafka_bootstrap.encode()).hexdigest()[:12]
        config = {'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'name': name},
                  'data': {'purpose': 'openbkn Kafka backup/restore', 'token': uuid.uuid4().hex}}
        self.kube('create', '-f', '-', data=json.dumps(config).encode(), timeout=60)
        try:
            yield
        finally:
            try:
                self.kube('delete', 'configmap', name, '--wait=false', timeout=60)
            except Exception:
                self.log('Kafka 锁清理失败，确认任务结束后清理 ConfigMap: ' + name)

    def check(self):
        with self.helper() as execute:
            if self.args.module == 'restore':
                self.metadata = json.loads(execute('health'))
            else:
                self.metadata = json.loads(execute('check', data=json.dumps({'topics': self.args.kafka_topics}).encode()))
                self.log_ignored(self.metadata)
                blocked = blocked_restore_topics(self.metadata, counts_known=False)
                if blocked:
                    self.log('注意：以下 Topic 的当前范围存在恢复限制，导出后还将核对 offset 连续性: ' + ', '.join(blocked))
        return {'topics': [t['name'] for t in self.metadata['topics']], 'mode': self.metadata['mode']}

    def backup(self, path):
        self.log('在线导出 Kafka 已保留的已提交消息；不修改业务消费组')
        with self.lock(), self.helper() as execute:
            metadata = json.loads(execute('export', '/work/export', data=json.dumps({'topics': self.args.kafka_topics}).encode()))
            self.log_ignored(metadata)
            files = ['metadata.json'] + [p['file'] for t in metadata['topics'] for p in t['partitions']]
            # Validate names before passing them to tar, without shell interpolation.
            from kafka_worker import validate_metadata
            validate_metadata(metadata)
            with open(path, 'wb') as target:
                # tar streams to the host; no second full copy in the Pod.
                self.kube('exec', '-i', self.active_pod, '-c', 'backup', '--', 'tar', '-C', '/work/export',
                          '-cf', '-', '--', *files, target=target)
        return archive_inventory(path, metadata, self.error)

    def preflight(self, metadata, overwrite=False):
        validate_restore_metadata(metadata, self.error)
        self.restore_note(metadata)
        with self.helper(overwrite=overwrite) as execute:
            result = json.loads(execute('restore-check', data=json.dumps(metadata).encode()))
            if result.get('replace_topics'):
                self.log('覆盖恢复将删除并重建以下 Topic（本次检查不修改）: ' + ', '.join(result['replace_topics']))
            changes = result.get('config_changes', {})
            if changes:
                self.log('恢复时将按备份对齐 Topic 配置（本次检查不修改）: ' + json.dumps(changes, ensure_ascii=False))
            return result

    def restore(self, path, metadata, overwrite=False):
        archive_inventory(path, metadata, self.error)
        validate_restore_metadata(metadata, self.error)
        require_restored_ledger(metadata, self.args.kafka_ledger_restored, self.error)
        self.restore_note(metadata)
        self.log('恢复 Kafka 消息及映射后的消费位置；Kafka 保持运行，目标业务生产者/消费者必须停止')
        with self.lock(), self.helper(overwrite=overwrite) as execute:
            with open(path, 'rb') as source:
                self.kube('exec', '-i', self.active_pod, '-c', 'backup', '--', 'sh', '-c',
                          'umask 077; mkdir /work/import; exec tar -xf - -C /work/import --no-same-owner --no-same-permissions', source=source)
            result = json.loads(execute('restore', '/work/import'))
        self.log('Kafka 恢复完成并回读校验: ' + json.dumps(result, ensure_ascii=False))

    def restore_note(self, metadata):
        self.log_ignored(metadata)
        topics = ledger_restore_topics(metadata)
        if topics:
            groups = [LEDGER_GROUPS[name] for name in topics]
            self.log('恢复审计/Evidence 记录，保持原 offset；仅恢复备份中已有的消费组提交位置，缺失的不创建。'
                     '请确认停写后同批备份的 MariaDB 账本已保留或恢复；相关消费组: '
                     + ', '.join(groups))

    def log_ignored(self, metadata):
        ignored = (set(metadata.get('ignored_topics', [])) & IGNORED_TOPICS
                   | {topic['name'] for topic in metadata['topics'] if topic['name'] in IGNORED_TOPICS})
        if ignored:
            self.log('跳过 Kafka Connect Topic（不备份、不恢复、不覆盖）: ' + ', '.join(sorted(ignored)))
