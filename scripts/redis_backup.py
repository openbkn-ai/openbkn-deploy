"""Single-instance Redis RDB backup and PVC restore; credentials stay in the Pod."""
import contextlib
import datetime
import hashlib
import json
from pathlib import PurePosixPath
import re
import time
import uuid


def info_fields(raw):
    return dict(line.split(':', 1) for line in raw.decode().splitlines()
                if ':' in line and not line.startswith('#'))


def validate_manifest(metadata, error):
    if (not isinstance(metadata, dict)
            or not isinstance(metadata.get('version'), str)
            or not re.fullmatch(r'7\.\d+\.\d+', metadata['version'])
            or type(metadata.get('snapshot_time')) is not int
            or metadata['snapshot_time'] <= 0):
        raise error('Redis 备份元数据无效（当前支持 Redis 7）')


class RedisBackup:
    def __init__(self, args, run, error, log):
        self.args, self.run, self.error, self.log = args, run, error, log
        self.ns = args.redis_namespace
        self.pod = None
        self.container = args.redis_container or 'redis'
        self.inspect()

    def kube(self, *args, **kwargs):
        return self.run(['kubectl', '-n', self.ns, *args],
                        timeout=kwargs.pop('timeout', 60), **kwargs)

    def execute(self, command, *, pod=None, **kwargs):
        return self.kube('exec', '-i', pod or self.pod['metadata']['name'], '-c',
                         'restore' if pod else self.container, '--', *command, **kwargs)

    def cli(self, *args):
        return self.execute(['sh', '-c',
            'set -eu; export REDISCLI_AUTH="${REDIS_PASSWORD:?Pod 缺少 REDIS_PASSWORD}"; '
            'exec redis-cli -e --raw --user "$@"', 'redis-backup', self.args.redis_user, *args])

    def info(self, section):
        return info_fields(self.cli('INFO', section))

    def inspect(self):
        if self.args.redis_pod:
            pods = [json.loads(self.kube('get', 'pod', self.args.redis_pod, '-o', 'json'))]
        else:
            pods = json.loads(self.kube('get', 'pods', '-l', self.args.redis_selector,
                                        '-o', 'json'))['items']
        if len(pods) != 1 or not self.ready(pods[0]):
            raise self.error('Redis 要求唯一就绪 Pod；不支持多副本或 Redis Cluster')
        self.pod = pods[0]
        owners = [o for o in self.pod['metadata'].get('ownerReferences', [])
                  if o.get('controller') and o['kind'] == 'StatefulSet']
        if len(owners) != 1:
            raise self.error('Redis 必须由单副本 StatefulSet 管理')
        self.sts = owners[0]['name']
        self.statefulset = json.loads(self.kube('get', 'statefulset', self.sts, '-o', 'json'))
        if self.statefulset['spec'].get('replicas', 1) != 1:
            raise self.error('Redis 只支持单副本 StatefulSet，不能自动恢复 Sentinel 高可用集群')
        containers = [c for c in self.pod['spec']['containers'] if c['name'] == self.container]
        if len(containers) != 1:
            raise self.error('Redis 容器不存在，请指定 --redis-container')
        self.image = containers[0]['image']
        self.resources = containers[0].get('resources', {})
        server = self.info('server')
        self.version = server['redis_version']
        if not re.fullmatch(r'7\.\d+\.\d+', self.version):
            raise self.error('当前 Redis 备份恢复仅支持 Redis 7')
        if self.info('replication').get('role') != 'master' or self.info('cluster').get('cluster_enabled') != '0':
            raise self.error('Redis 必须是非 Cluster 的独立主节点')
        if self.info('replication').get('connected_slaves') != '0':
            raise self.error('Redis 不能有已连接副本；恢复前需另行处理复制拓扑')
        if json.loads(self.cli('--json', 'MODULE', 'LIST')):
            raise self.error('暂不支持使用 Redis Module 的数据恢复')
        config = json.loads(self.cli('--json', self.args.redis_config_command, 'GET',
                                    'dir', 'dbfilename', 'appendonly', 'appenddirname', 'appendfilename'))
        self.config = config if isinstance(config, dict) else dict(zip(config[::2], config[1::2]))
        self.directory = self.config['dir']
        path = PurePosixPath(self.directory)
        if not path.is_absolute() or '..' in path.parts or str(path) == '/':
            raise self.error('Redis dir 必须是专用绝对目录')
        self.files = [self.config[k] for k in ('dbfilename', 'appenddirname', 'appendfilename')]
        if (len(set(self.files)) != 3 or any(not re.fullmatch(r'[A-Za-z0-9_.-]+', f)
                or f.startswith('.') for f in self.files)):
            raise self.error('Redis 持久化文件名不受支持')
        mounts = [m for m in containers[0].get('volumeMounts', [])
                  if m['mountPath'] == self.directory and not m.get('readOnly') and not m.get('subPath')
                  and not m.get('subPathExpr')]
        volumes = {v['name']: v for v in self.pod['spec']['volumes']}
        if len(mounts) != 1 or 'persistentVolumeClaim' not in volumes.get(mounts[0]['name'], {}):
            raise self.error('Redis dir 必须直接挂载可写 PVC（不支持 subPath）')
        self.pvc = volumes[mounts[0]['name']]['persistentVolumeClaim']['claimName']
        pvc = json.loads(self.kube('get', 'pvc', self.pvc, '-o', 'json'))
        users = json.loads(self.kube('get', 'pods', '-o', 'json'))['items']
        for pod in users:
            if pod['metadata']['name'] == self.pod['metadata']['name'] or pod.get('status', {}).get('phase') in ('Succeeded', 'Failed'):
                continue
            if any(v.get('persistentVolumeClaim', {}).get('claimName') == self.pvc
                   for v in pod['spec'].get('volumes', [])):
                raise self.error('Redis PVC 被其他 Pod 引用，不能保证独占恢复: ' + pod['metadata']['name'])
        if pvc.get('status', {}).get('phase') != 'Bound':
            raise self.error('Redis PVC 未绑定')
        uid_gid = self.execute(['sh', '-c', 'id -u; id -g']).decode().split()
        self.uid, self.gid = map(int, uid_gid)
        self.execute(['sh', '-c', 'set -eu; for tool in redis-cli redis-server redis-check-rdb '
                      'sha256sum cat mv mkdir rm rmdir sleep; do command -v "$tool" >/dev/null; done; '
                      'test -w "$1"', 'redis-tools', self.directory])

    @staticmethod
    def ready(pod):
        return (not pod['metadata'].get('deletionTimestamp')
                and pod.get('status', {}).get('phase') == 'Running'
                and any(c['type'] == 'Ready' and c['status'] == 'True'
                        for c in pod.get('status', {}).get('conditions', [])))

    @contextlib.contextmanager
    def lock(self):
        lock = self.directory + '/.openbkn-redis-restore.lock'
        self.execute(['mkdir', lock])
        try:
            yield
        finally:
            try:
                self.execute(['rmdir', lock])
            except Exception:
                self.log('Redis 锁保留: ' + lock + '；确认无任务后手动清理')

    def backup(self, path):
        with self.lock():
            before = self.info('persistence')
            run_id = self.info('server')['run_id']
            if before.get('rdb_bgsave_in_progress') != '0' or before.get('aof_rewrite_in_progress') != '0':
                raise self.error('Redis 正在保存 RDB 或重写 AOF，请等待后重试')
            self.cli('BGSAVE')
            deadline = time.monotonic() + self.args.timeout
            while True:
                state = self.info('persistence')
                if state.get('rdb_bgsave_in_progress') == '0':
                    if (state.get('rdb_last_bgsave_status') != 'ok'
                            or int(state.get('rdb_saves', 0)) <= int(before.get('rdb_saves', 0))):
                        raise self.error('Redis 本次 BGSAVE 未成功，禁止使用旧 RDB')
                    break
                if time.monotonic() >= deadline:
                    raise self.error('等待 Redis BGSAVE 超时')
                self.log('等待 Redis BGSAVE 完成')
                time.sleep(self.args.poll_interval)
            remote = self.directory + '/' + self.config['dbfilename']
            self.execute(['redis-check-rdb', remote])
            expected = self.execute(['sha256sum', remote]).decode().split()[0]
            with open(path, 'wb') as target:
                self.execute(['cat', remote], target=target, timeout=self.args.timeout)
            with open(path, 'rb') as source:
                h = hashlib.sha256()
                if source.read(5) != b'REDIS':
                    raise self.error('Redis RDB 文件头无效')
                source.seek(0)
                for block in iter(lambda: source.read(1024 * 1024), b''):
                    h.update(block)
            if (h.hexdigest() != expected
                    or self.execute(['sha256sum', remote]).decode().split()[0] != expected
                    or self.info('server')['run_id'] != run_id
                    or self.info('persistence').get('rdb_saves') != state.get('rdb_saves')):
                raise self.error('Redis 备份期间进程或 RDB 发生变化，请重试')
            return {'version': self.version, 'snapshot_time': int(state['rdb_last_save_time']),
                    'created_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    'keyspace_reference': self.info('keyspace')}

    def preflight(self, metadata, overwrite):
        current = json.loads(self.kube('get', 'statefulset', self.sts, '-o', 'json'))
        if (current['metadata']['uid'] != self.statefulset['metadata']['uid']
                or current['spec'].get('replicas', 1) != 1
                or current['spec']['template'] != self.statefulset['spec']['template']):
            raise self.error('Redis StatefulSet 配置或副本数已改变，请重新检查')
        validate_manifest(metadata, self.error)
        if metadata['version'] != self.version:
            raise self.error('Redis 恢复要求源端与目标端版本完全一致')
        if self.info('keyspace') and not overwrite:
            raise self.error('Redis 已有数据；替换所有 DB 需要 --overwrite，并先停止业务写入')
        for verb, resource in [('create', 'pods'), ('delete', 'pods'), ('create', 'pods/exec'),
                               ('patch', 'statefulsets.apps')]:
            if self.kube('auth', 'can-i', verb, resource).strip() != b'yes':
                raise self.error('Redis 恢复权限不足: ' + verb + ' ' + resource)

    def wait(self, predicate, message):
        deadline = time.monotonic() + self.args.timeout
        next_log = time.monotonic() + 20
        while not predicate():
            now = time.monotonic()
            if now >= deadline:
                raise self.error(message)
            if now >= next_log:
                self.log(message.replace('超时', '') + '，仍在等待')
                next_log = now + 20
            time.sleep(self.args.poll_interval)

    def scale(self, replicas):
        self.kube('scale', 'statefulset', self.sts, '--replicas=' + str(replicas))
        name = self.pod['metadata']['name']
        def reached():
            pods = json.loads(self.kube('get', 'pods', '--field-selector=metadata.name=' + name,
                                        '-o', 'json'))['items']
            return not pods if replicas == 0 else len(pods) == 1 and self.ready(pods[0])
        self.wait(reached, '等待 Redis 停止/启动超时')

    def helper_spec(self, name):
        return {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': name, 'namespace': self.ns,
                    'labels': {'openbkn.ai/restore-statefulset': self.sts}},
                'spec': {'restartPolicy': 'Never', 'nodeName': self.pod['spec']['nodeName'],
                         'automountServiceAccountToken': False,
                         'tolerations': self.pod['spec'].get('tolerations', []),
                         'imagePullSecrets': self.pod['spec'].get('imagePullSecrets', []),
                         'securityContext': {'runAsUser': self.uid, 'runAsGroup': self.gid, 'fsGroup': self.gid},
                         'containers': [{'name': 'restore', 'image': self.image,
                                         'command': ['sh', '-c', 'sleep 2147483647'],
                                         'resources': self.resources,
                                         'volumeMounts': [{'name': 'data', 'mountPath': self.directory}]}],
                         'volumes': [{'name': 'data', 'persistentVolumeClaim': {'claimName': self.pvc}}]}}

    def create_helper(self, name):
        self.kube('create', '-f', '-', data=json.dumps(self.helper_spec(name)).encode())
        def ready():
            pod = json.loads(self.kube('get', 'pod', name, '-o', 'json'))
            return self.ready(pod)
        self.wait(ready, '等待 Redis 恢复 Pod 超时')

    def delete_helper(self, name):
        self.kube('delete', 'pod', name, '--ignore-not-found', '--wait=false')
        self.wait(lambda: not json.loads(self.kube('get', 'pods',
                  '--field-selector=metadata.name=' + name, '-o', 'json'))['items'],
                  '等待 Redis 恢复 Pod 退出超时')

    def validate_running(self, expected=None):
        if self.info('persistence').get('loading') != '0':
            raise self.error('Redis 尚未完成加载')
        if self.info('server')['redis_version'] != self.version:
            raise self.error('Redis 重启后版本变化')
        persistence = self.info('persistence')
        if self.config['appendonly'] == 'yes' and (persistence.get('aof_enabled') != '1'
                or persistence.get('aof_last_write_status') != 'ok'):
            raise self.error('Redis 恢复后 AOF 状态异常')
        actual = self.info('keyspace')
        if expected is not None:
            for database in set(expected) | set(actual):
                old = dict(part.split('=', 1) for part in expected.get(database, 'keys=0,expires=0').split(','))
                new = dict(part.split('=', 1) for part in actual.get(database, 'keys=0,expires=0').split(','))
                minimum = 0 if int(old.get('subexpiry', 0)) else int(old['keys']) - int(old.get('expires', 0))
                if not minimum <= int(new['keys']) <= int(old['keys']):
                    raise self.error('Redis 重启后 DB key 数量不符合恢复预期: ' + database)
        self.log('Redis 恢复后 keyspace: ' + json.dumps(actual))

    def restore(self, path, metadata, overwrite):
        self.preflight(metadata, overwrite)
        token = uuid.uuid4().hex[:12]
        helper = 'openbkn-redis-restore-' + token
        rollback = self.directory + '/.openbkn-redis-rollback-' + token
        staged = self.directory + '/.openbkn-redis-input-' + token
        sock = '/tmp/openbkn-redis-restore.sock'
        stopped = touched = helper_created = uploaded = False
        with self.lock():
            try:
                self.log('停止 Redis StatefulSet ' + self.ns + '/' + self.sts)
                stopped = True  # A timed-out scale request may already have taken effect.
                self.scale(0)
                helper_created = True
                self.create_helper(helper)
                uploaded = True
                with open(path, 'rb') as source:
                    self.execute(['sh', '-c', 'set -eu; set -C; umask 077; cat > "$1"', 'stage', staged],
                                 pod=helper, source=source, timeout=self.args.timeout)
                self.execute(['redis-check-rdb', staged], pod=helper)
                touched = True
                self.execute(['sh', '-c', '''set -eu
mkdir "$1"
shift
root=$1; input=$2; rollback=$3; shift 3
for file do
  test ! -L "$root/$file"
  if [ -e "$root/$file" ]; then mv "$root/$file" "$rollback/$file"; else : > "$rollback/.absent-$file"; fi
done
mv "$input" "$root/$1"
''', 'preserve', rollback, self.directory, staged, rollback, *self.files], pod=helper)
                self.log('Redis 原数据保留于 PVC: ' + rollback)
                command = ['redis-server', '--dir', self.directory, '--dbfilename', self.config['dbfilename'],
                           '--appendonly', 'no', '--save', '', '--port', '0', '--unixsocket', sock,
                           '--unixsocketperm', '700', '--daemonize', 'yes', '--logfile', '/tmp/redis-restore.log',
                           '--pidfile', '/tmp/redis-restore.pid', '--appenddirname', self.config['appenddirname'],
                           '--appendfilename', self.config['appendfilename']]
                self.execute(command, pod=helper)
                def local(*args):
                    return self.execute(['redis-cli', '-e', '--raw', '-s', sock, *args], pod=helper)
                def listening():
                    try:
                        return local('PING').strip() == b'PONG'
                    except self.error:
                        return False
                self.wait(listening, '等待临时 Redis 加载 RDB 超时；请检查恢复 Pod /tmp/redis-restore.log')
                expected = info_fields(local('INFO', 'keyspace'))
                if self.config['appendonly'] == 'yes':
                    local('CONFIG', 'SET', 'appendonly', 'yes')
                    def aof_ready():
                        state = info_fields(local('INFO', 'persistence'))
                        if state.get('aof_last_bgrewrite_status') == 'err':
                            raise self.error('Redis 恢复 AOF 生成失败')
                        return (state.get('aof_enabled') == '1' and state.get('aof_rewrite_in_progress') == '0'
                                and state.get('aof_rewrite_scheduled') == '0'
                                and state.get('aof_last_bgrewrite_status') == 'ok'
                                and state.get('aof_last_write_status') == 'ok')
                    self.wait(aof_ready, '等待 Redis 恢复 AOF 生成超时')
                    manifest = self.directory + '/' + self.config['appenddirname'] + '/' + self.config['appendfilename'] + '.manifest'
                    self.execute(['test', '-s', manifest], pod=helper)
                local('SHUTDOWN', 'NOSAVE')
                self.delete_helper(helper)
                helper_created = False
                self.scale(1)
                self.validate_running(expected)
                self.log('再次重启 Redis，验证持久化恢复')
                self.scale(0)
                self.scale(1)
                self.validate_running(expected)
            except (Exception, KeyboardInterrupt) as original:
                try:
                    if stopped:
                        self.scale(0)
                        if touched:
                            # If a helper request timed out, delete it before mounting again.
                            self.delete_helper(helper)
                            helper_created = True
                            self.create_helper(helper)
                            self.execute(['rm', '-f', staged], pod=helper)
                            self.execute(['sh', '-c', '''set -eu
root=$1; rollback=$2; shift 2
if [ -d "$rollback" ]; then
  mkdir -p "$rollback/failed"
  for file do
    if [ -e "$rollback/$file" ] || [ -f "$rollback/.absent-$file" ]; then
      if [ -e "$root/$file" ] || [ -L "$root/$file" ]; then mv "$root/$file" "$rollback/failed/$file"; fi
      if [ -e "$rollback/$file" ]; then mv "$rollback/$file" "$root/$file"; fi
    fi
  done
fi
''', 'rollback', self.directory, rollback, *self.files], pod=helper)
                        if helper_created:
                            if uploaded and not touched:
                                self.execute(['rm', '-f', staged], pod=helper)
                            self.delete_helper(helper)
                            helper_created = False
                        self.scale(1)
                        self.log('Redis 恢复失败，已启动原数据；其他存储不会自动回滚，业务请保持停止')
                except (Exception, KeyboardInterrupt) as rollback_error:
                    raise self.error('Redis 自动回退失败；保持业务停止。StatefulSet=' + self.sts
                                     + ' PVC=' + self.pvc + ' 回退目录=' + rollback + ' helper=' + helper
                                     + '；原错误=' + str(original) + '；回退错误=' + str(rollback_error)) from original
                raise
            finally:
                if helper_created and not touched:
                    self.delete_helper(helper)
