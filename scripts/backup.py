#!/usr/bin/env python3
"""Portable MariaDB/OpenSearch backup. Python standard library + kubectl only."""
import argparse
import contextlib
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import signal
import subprocess
import sys
import tarfile
import tempfile
import time
import uuid
from urllib.parse import quote


class BackupError(Exception):
    pass


SYSTEM_DATABASES = {'information_schema', 'performance_schema', 'mysql', 'sys'}


def log(message):
    print(time.strftime('%Y-%m-%d %H:%M:%S ') + message, file=sys.stderr, flush=True)


def run(argv, *, data=None, source=None, target=None, timeout=3600):
    """Stream large files; show a heartbeat and retain stderr separately from data."""
    with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
        with contextlib.ExitStack() as stack:
            incoming = source
            if data is not None:
                incoming = stack.enter_context(tempfile.TemporaryFile())
                incoming.write(data)
                incoming.seek(0)
            process = subprocess.Popen(argv, stdin=incoming or subprocess.DEVNULL,
                                       stdout=target or out, stderr=err, start_new_session=True)
            started = time.monotonic()
            next_log = started + 20
            try:
                while process.poll() is None:
                    now = time.monotonic()
                    if now - started > timeout:
                        raise BackupError('命令超时；请检查集群中的任务状态后再重试')
                    if now >= next_log:
                        log('仍在执行当前步骤，已用时 %ds' % (now - started))
                        next_log = now + 20
                    time.sleep(0.1)
            finally:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait()
            if process.returncode:
                err.seek(0)
                raise BackupError('命令执行失败: ' + err.read(8192).decode(errors='replace'))
            out.seek(0)
            return out.read() if target is None else b''


def digest(path):
    h = hashlib.sha256()
    with open(path, 'rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def safe_members(archive):
    names = set()
    for member in archive:
        path = PurePosixPath(member.name)
        if path.is_absolute() or '..' in path.parts or not (member.isfile() or member.isdir()):
            raise BackupError('备份包含不安全的路径或文件类型: ' + member.name)
        if str(path) in names:
            raise BackupError('备份包含重复路径: ' + member.name)
        names.add(str(path))
        yield member


def unpack(package, destination):
    with tarfile.open(package, 'r:gz') as archive:
        members = list(safe_members(archive))
        if {m.name for m in members} != {'manifest.json', 'mariadb.sql', 'repository.tar'}:
            raise BackupError('备份包文件列表不符合一期格式')
        archive.extractall(destination, members=members)
    manifest = json.loads((destination / 'manifest.json').read_text())
    if manifest.get('format') != 1:
        raise BackupError('不支持的备份包版本')
    if set(manifest.get('sha256', {})) != {'mariadb.sql', 'repository.tar'}:
        raise BackupError('备份包缺少校验清单')
    for name, expected in manifest['sha256'].items():
        if digest(destination / name) != expected:
            raise BackupError('备份包校验失败: ' + name)
    with tarfile.open(destination / 'repository.tar', 'r:') as archive:
        members = list(safe_members(archive))
        if not any(PurePosixPath(m.name).name.startswith('index-') for m in members):
            raise BackupError('快照仓库缺少 index 元数据')
    databases = manifest.get('databases', [])
    if not databases or any(not re.fullmatch(r'[A-Za-z0-9_]+', db) for db in databases):
        raise BackupError('数据库清单无效')
    if not re.fullmatch(r'[a-z0-9-]+', manifest.get('snapshot', '')):
        raise BackupError('快照名称无效')
    if set(databases) & SYSTEM_DATABASES:
        raise BackupError('备份包不能包含系统库')
    indices = manifest.get('indices', [])
    if not indices or any(not isinstance(i, str) or re.search(r'[*,/\\?#\s]', i) or i in ('.', '..') or not i
                          for i in indices):
        raise BackupError('索引清单无效或为空')
    for name in ('mariadb_version', 'opensearch_version'):
        if not re.match(r'^\d+\.\d+\.', manifest.get(name, '')):
            raise BackupError('备份包缺少有效版本: ' + name)
    return manifest


class Cluster:
    def __init__(self, args):
        self.args = args
        self.targets = {}
        for service in ('mariadb', 'opensearch'):
            namespace = getattr(args, service + '_namespace')
            pod = getattr(args, service + '_pod')
            argv = ['kubectl', '-n', namespace, 'get', 'pods']
            if pod:
                argv = ['kubectl', '-n', namespace, 'get', 'pod', pod]
            else:
                argv += ['-l', getattr(args, service + '_selector')]
            result = json.loads(run(argv + ['-o', 'json'], timeout=60))
            pods = [result] if pod else result.get('items', [])
            ready = [p for p in pods if not p['metadata'].get('deletionTimestamp')
                     and p.get('status', {}).get('phase') == 'Running'
                     and any(c.get('type') == 'Ready' and c.get('status') == 'True'
                             for c in p.get('status', {}).get('conditions', []))]
            if len(ready) != 1 or len(pods) != 1:
                raise BackupError(service + ' 一期要求唯一就绪 Pod；请检查 selector，副本集群需另行设计')
            containers = ready[0]['spec']['containers']
            container = getattr(args, service + '_container')
            if not container:
                candidates = [c['name'] for c in containers if c['name'] == service]
                if not candidates and len(containers) == 1:
                    candidates = [containers[0]['name']]
                if len(candidates) != 1:
                    raise BackupError('请指定 --' + service + '-container')
                container = candidates[0]
            self.targets[service] = (namespace, ready[0]['metadata']['name'], container)

    def exec(self, service, command, **kwargs):
        ns, pod, container = self.targets[service]
        return run(['kubectl', '-n', ns, 'exec', '-i', pod, '-c', container, '--'] + command,
                   timeout=kwargs.pop('timeout', self.args.timeout), **kwargs)

    def sql(self, query=None, *, source=None, target=None, databases=None):
        # Credentials remain in the Pod environment, never in the local argv/log/package.
        script = ('set -eu; export MYSQL_PWD="${MARIADB_ROOT_PASSWORD:-${MYSQL_ROOT_PASSWORD:-}}"; '
                  'test -n "$MYSQL_PWD" || { echo "Pod 缺少 root 密码环境变量" >&2; exit 1; }; ')
        if databases is not None:
            script += ('exec mariadb-dump --user=root --single-transaction --quick '
                       '--routines --events --triggers --hex-blob --add-drop-database --databases "$@"')
            command = ['sh', '-c', script, 'backup'] + databases
        else:
            script += 'exec mariadb --user=root --batch --skip-column-names'
            command = ['sh', '-c', script]
        return self.exec('mariadb', command, data=query.encode() if query else None,
                         source=source, target=target)

    def api(self, method, path, body=None, *, timeout=60):
        # Loopback inside OpenSearch Pod: no manually supplied URL/port-forward required.
        command = ['curl', '-sS', '--connect-timeout', '10', '--max-time', str(timeout),
                   '-w', '\n%{http_code}', '-X', method,
                   '-H', 'Content-Type: application/json']
        if body is not None:
            command += ['--data-binary', '@-']
        command += ['http://127.0.0.1:9200' + path]
        raw = self.exec('opensearch', command,
                        data=json.dumps(body).encode() if body is not None else None,
                        timeout=timeout + 15)
        content, status = raw.rsplit(b'\n', 1)
        if not 200 <= int(status) < 300:
            raise BackupError('OpenSearch HTTP ' + status.decode() + ': ' + content[:4096].decode(errors='replace'))
        result = json.loads(content)
        if 'error' in result:
            raise BackupError('OpenSearch: ' + str(result['error']))
        return result

    @property
    def repository(self):
        return '/_snapshot/' + self.args.repository

    def check(self):
        log('检查 MariaDB 连接、业务库及 OpenSearch 快照路径')
        self.mariadb_version = self.sql('SELECT VERSION();').decode().strip()
        self.exec('mariadb', ['sh', '-c', 'command -v mariadb-dump >/dev/null'], timeout=60)
        all_databases = self.sql('SHOW DATABASES;').decode().splitlines()
        self.databases = self.args.databases or [db for db in all_databases if db not in
                         SYSTEM_DATABASES]
        if (not self.databases and self.args.module == 'backup') or any(not re.fullmatch(r'[A-Za-z0-9_]+', db) for db in self.databases):
            raise BackupError('无业务库或库名不受支持；请用 --databases 明确指定')
        if set(self.databases) - set(all_databases):
            raise BackupError('指定的业务库不存在')
        if set(self.databases) & SYSTEM_DATABASES:
            raise BackupError('一期只备份业务库，不能指定系统库')
        info = self.api('GET', '/')
        self.opensearch_version = info['version']['number']
        nodes = self.api('GET', '/_nodes/settings?flat_settings=true')['nodes']
        if len(nodes) != 1:
            raise BackupError('一期仅支持单节点 OpenSearch；多节点必须使用共享快照存储')
        allowed = next(iter(nodes.values())).get('settings', {}).get('path.repo', [])
        if isinstance(allowed, str):
            allowed = [allowed]
        if self.args.repository_path not in allowed:
            raise BackupError('path.repo 未包含 ' + self.args.repository_path + '；请按 BACKUP.zh.md 完成一次性配置并重启')
        self.exec('opensearch', ['sh', '-c', 'command -v tar >/dev/null; test -d "$1" && test -r "$1" && test -w "$1"',
                                  'check', self.args.repository_path], timeout=60)
        # Require a persistent mount: a writable container layer is not a portable repository.
        ns, pod, _ = self.targets['opensearch']
        spec = json.loads(run(['kubectl', '-n', ns, 'get', 'pod', pod, '-o', 'json'], timeout=60))['spec']
        mounts = next(c for c in spec['containers'] if c['name'] == self.targets['opensearch'][2]).get('volumeMounts', [])
        volumes = {v['name']: v for v in spec.get('volumes', [])}
        if not any(m.get('mountPath') == self.args.repository_path and not m.get('readOnly')
                   and any(key in volumes.get(m['name'], {}) for key in ('hostPath', 'persistentVolumeClaim', 'nfs'))
                   for m in mounts):
            raise BackupError('快照目录必须有可写持久卷挂载；请按 BACKUP.zh.md 配置')

    @contextlib.contextmanager
    def lock(self):
        # Shared by backup/restore launched on different admin machines against this Pod.
        lock = '/tmp/openbkn-data-backup.lock'
        self.exec('opensearch', ['mkdir', lock], timeout=60)
        try:
            yield
        finally:
            self.exec('opensearch', ['rmdir', lock], timeout=60)

    def register(self, readonly=False):
        existing = self.api('GET', '/_snapshot')
        for name, repository in existing.items():
            location = repository.get('settings', {}).get('location')
            if name == self.args.repository and (repository.get('type') != 'fs' or location != self.args.repository_path):
                raise BackupError('同名快照仓库已指向其他存储，请使用专用仓库名')
            if name != self.args.repository and location == self.args.repository_path:
                raise BackupError('此目录已被其他仓库注册，请使用专用目录')
        result = self.api('PUT', self.repository, {'type': 'fs', 'settings': {
            'location': self.args.repository_path, 'readonly': readonly}})
        if not result.get('acknowledged'):
            raise BackupError('快照仓库注册未确认')
        if not self.api('POST', self.repository + '/_verify').get('nodes'):
            raise BackupError('快照仓库校验未返回节点')

    def snapshots(self):
        return self.api('GET', self.repository + '/_all')['snapshots']

    def idle(self):
        if self.api('GET', '/_snapshot/_status').get('snapshots'):
            raise BackupError('集群有运行中的快照；请等待完成，打包期间禁止其他工具修改仓库')

    def backup(self, destination):
        with self.lock(), tempfile.TemporaryDirectory(prefix='.openbkn-backup-', dir=destination) as tmp:
            stage = Path(tmp)
            log('1/5 导出 MariaDB 业务库: ' + ', '.join(self.databases))
            db_list = ','.join("'%s'" % db for db in self.databases)
            engines = self.sql('SELECT TABLE_SCHEMA,TABLE_NAME,ENGINE FROM information_schema.TABLES '
                               'WHERE TABLE_SCHEMA IN (' + db_list + ") AND TABLE_TYPE='BASE TABLE' AND ENGINE <> 'InnoDB';")
            if engines.strip():
                raise BackupError('业务库存在非 InnoDB 表，无法保证在线事务一致性；一期暂不支持')
            with open(stage / 'mariadb.sql', 'wb') as target:
                self.sql(databases=self.databases, target=target)
            if not (stage / 'mariadb.sql').stat().st_size:
                raise BackupError('MariaDB 导出文件为空')
            log('2/5 注册并校验快照仓库，创建 OpenSearch 快照')
            self.idle()
            self.register()
            # Capture prior snapshots before creation; only prune this dedicated repository.
            previous = self.snapshots()
            snapshot = 'openbkn-' + datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S') + '-' + uuid.uuid4().hex[:8]
            self.api('PUT', self.repository + '/' + snapshot,
                     {'indices': '*', 'include_global_state': False, 'ignore_unavailable': False,
                      'partial': False})
            deadline = time.monotonic() + self.args.timeout
            while True:
                result = self.api('GET', self.repository + '/' + snapshot)['snapshots'][0]
                if result['state'] == 'SUCCESS':
                    break
                if result['state'] != 'IN_PROGRESS':
                    raise BackupError('快照失败或不完整: ' + json.dumps(result))
                if time.monotonic() >= deadline:
                    raise BackupError('等待快照超时；服务端可能仍在备份，请检查后重试')
                log('OpenSearch 快照进行中: ' + snapshot)
                time.sleep(self.args.poll_interval)
            log('3/5 新快照成功，通过 API 删除本仓库中的旧快照')
            for old in previous:
                result = self.api('DELETE', self.repository + '/' + quote(old['snapshot'], safe=''), timeout=self.args.timeout)
                if not result.get('acknowledged'):
                    raise BackupError('旧快照删除未确认')
            self.idle()
            log('4/5 导出完整 OpenSearch 快照仓库')
            inventory = self.snapshots()
            with open(stage / 'repository.tar', 'wb') as target:
                self.exec('opensearch', ['tar', '-C', self.args.repository_path, '-cf', '-', '.'], target=target)
            self.idle()
            if self.snapshots() != inventory:
                raise BackupError('打包时仓库被其他任务修改，本次备份终止')
            with tarfile.open(stage / 'repository.tar', 'r:') as archive:
                list(safe_members(archive))
            manifest = {'format': 1, 'created_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                        'databases': self.databases, 'snapshot': snapshot,
                        'mariadb_version': self.mariadb_version, 'opensearch_version': self.opensearch_version,
                        'sha256': {name: digest(stage / name) for name in ('mariadb.sql', 'repository.tar')}}
            # Read back exact index names for safe conflict checks during restore.
            manifest['indices'] = self.api('GET', self.repository + '/' + snapshot)['snapshots'][0]['indices']
            (stage / 'manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + '\n')
            log('5/5 压缩备份包并校验')
            package = destination / (snapshot + '.tar.gz')
            partial = destination / (package.name + '.partial')
            try:
                with tarfile.open(partial, 'w:gz') as archive:
                    for name in ('manifest.json', 'mariadb.sql', 'repository.tar'):
                        archive.add(stage / name, arcname=name)
                with tempfile.TemporaryDirectory(dir=destination) as verify:
                    unpack(partial, Path(verify))
                with open(partial, 'rb') as complete:
                    os.fsync(complete.fileno())
                os.replace(partial, package)
            finally:
                partial.unlink(missing_ok=True)
            log('备份完成: ' + str(package))
            print(json.dumps({'result': 'success', 'archive': str(package), 'snapshot': snapshot}))

    def restore_preflight(self, manifest):
        if self.mariadb_version.split('.')[0] != manifest['mariadb_version'].split('.')[0]:
            raise BackupError('一期要求 MariaDB 主版本一致')
        if self.opensearch_version.split('.')[:2] != manifest['opensearch_version'].split('.')[:2]:
            raise BackupError('一期要求 OpenSearch 主次版本一致')
        databases = manifest['databases']
        db_list = ','.join("'%s'" % db for db in databases)
        tables = self.sql('SELECT TABLE_SCHEMA,TABLE_NAME FROM information_schema.TABLES '
                          'WHERE TABLE_SCHEMA IN (' + db_list + ');').decode().strip()
        current = self.api('GET', '/_cat/indices?format=json&expand_wildcards=all')
        conflicts = sorted({item['index'] for item in current} & set(manifest['indices']))
        if (tables or conflicts) and not self.args.overwrite:
            raise BackupError('目标业务库已有表或存在同名索引；覆盖恢复需要 --overwrite，并先停止业务写入')
        # Do not merge repositories or overwrite a repository currently registered elsewhere.
        # The bundled OpenSearch image does not necessarily include `find`.
        # Use only POSIX shell globbing so this check works in the slim image.
        content = self.exec('opensearch', ['sh', '-c',
            'for item in "$1"/* "$1"/.[!.]* "$1"/..?*; do '
            '[ ! -e "$item" ] || { printf "non-empty\\n"; exit 0; }; done',
            'check-repository', self.args.repository_path], timeout=60)
        if content.strip():
            raise BackupError('恢复目标快照目录非空；请使用空的持久化快照目录，禁止混入已有仓库')
        repositories = self.api('GET', '/_snapshot')
        if any(r.get('settings', {}).get('location') == self.args.repository_path for r in repositories.values()):
            raise BackupError('恢复目录仍被仓库注册引用；先用 DELETE /_snapshot/<仓库名> 注销（不会删除文件）')
        self.idle()
        return conflicts

    def restore(self, stage, manifest):
        if not self.args.writers_stopped:
            raise BackupError('恢复前请停止业务写入，并显式传入 --writers-stopped')
        with self.lock():
            conflicts = self.restore_preflight(manifest)
            log('1/4 将完整快照仓库复制到目标 Pod 持久卷')
            with open(stage / 'repository.tar', 'rb') as source:
                self.exec('opensearch', ['tar', '--no-same-owner', '-C', self.args.repository_path, '-xf', '-'], source=source)
            self.register(readonly=True)
            snapshots = self.snapshots()
            snapshot = next((s for s in snapshots if s['snapshot'] == manifest['snapshot']), None)
            if not snapshot or snapshot['state'] != 'SUCCESS' or sorted(snapshot['indices']) != sorted(manifest['indices']):
                raise BackupError('仓库快照与备份清单不一致')
            log('2/4 恢复 OpenSearch 索引' + ('（删除同名索引）' if conflicts else ''))
            for index in conflicts:
                if not self.api('DELETE', '/' + quote(index, safe='')).get('acknowledged'):
                    raise BackupError('索引删除未确认: ' + index)
            result = self.api('POST', self.repository + '/' + manifest['snapshot'] + '/_restore?wait_for_completion=true',
                              {'indices': '*', 'include_global_state': False, 'include_aliases': True}, timeout=self.args.timeout)
            shards = result.get('snapshot', {}).get('shards', {})
            if shards.get('failed') != 0 or not shards.get('total') or shards.get('successful') != shards.get('total'):
                raise BackupError('OpenSearch 恢复不完整: ' + json.dumps(result))
            log('3/4 恢复 MariaDB 业务库（替换备份清单内的库）')
            with open(stage / 'mariadb.sql', 'rb') as source:
                self.sql(source=source)
            log('4/4 验证目标业务库及索引可用')
            for database in manifest['databases']:
                self.sql('USE `' + database + '`; SHOW TABLES;')
            health = self.api('GET', '/_cluster/health?wait_for_status=yellow&timeout=60s', timeout=75)
            if health.get('timed_out') or health.get('status') == 'red':
                raise BackupError('恢复后 OpenSearch 未达到 yellow 状态')
            log('恢复完成；请验证业务后再启动业务服务。恢复仓库保持只读。')
            print(json.dumps({'result': 'success', 'snapshot': manifest['snapshot'], 'databases': manifest['databases']}))


def parser():
    result = argparse.ArgumentParser(description='MariaDB + OpenSearch 单文件备份恢复（一期：单节点、HTTP 无认证 OpenSearch）')
    result.add_argument('module', choices=['backup', 'restore'])
    result.add_argument('action', choices=['check', 'run'])
    result.add_argument('archive', nargs='?', help='恢复用的 .tar.gz 备份包')
    result.add_argument('--output-dir', default='/var/backups/openbkn')
    result.add_argument('--repository', default='offsite-backup', help='专用仓库；备份成功后删除其中所有旧快照')
    result.add_argument('--repository-path', default='/usr/share/opensearch/snapshots')
    result.add_argument('--databases', nargs='+', help='默认自动发现所有非系统库')
    result.add_argument('--timeout', type=int, default=3600)
    result.add_argument('--poll-interval', type=int, default=10)
    result.add_argument('--overwrite', action='store_true', help='允许替换目标业务库及同名索引')
    result.add_argument('--writers-stopped', action='store_true', help='确认业务服务已停止写入（恢复必需）')
    for service, selector in [('mariadb', 'app=mariadb'), ('opensearch', 'app.kubernetes.io/instance=opensearch')]:
        result.add_argument('--' + service + '-namespace', default=os.environ.get(service.upper() + '_NAMESPACE', 'resource'))
        result.add_argument('--' + service + '-selector', default=selector)
        result.add_argument('--' + service + '-pod')
        result.add_argument('--' + service + '-container')
    return result


def main(argv=None):
    args = parser().parse_args(argv)
    if not re.fullmatch(r'[a-z0-9-]+', args.repository):
        raise BackupError('仓库名称只支持小写字母、数字和横线')
    if not args.repository_path.startswith('/') or '..' in PurePosixPath(args.repository_path).parts or args.repository_path == '/':
        raise BackupError('仓库路径必须是专用绝对路径')
    if args.timeout < 1 or not 1 <= args.poll_interval <= 30:
        raise BackupError('timeout 必须为正数，poll-interval 必须在 1 到 30 秒之间')
    if args.module == 'backup':
        if args.archive:
            raise BackupError('backup 不接受备份包参数')
        cluster = Cluster(args)
        cluster.check()
        if args.action == 'check':
            print(json.dumps({'result': 'ready', 'databases': cluster.databases,
                              'repository_path': args.repository_path, 'note': '仅完成前置检查，尚未备份；请确保输出目录有足够空间'}))
        else:
            destination = Path(args.output_dir).resolve()
            destination.mkdir(parents=True, exist_ok=True, mode=0o700)
            with open(destination / '.backup.lock', 'a') as lock:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError as error:
                    raise BackupError('输出目录有另一个备份任务') from error
                cluster.backup(destination)
    else:
        if not args.archive:
            raise BackupError('restore 必须指定备份包')
        with tempfile.TemporaryDirectory(prefix='openbkn-restore-') as tmp:
            stage = Path(tmp)
            manifest = unpack(args.archive, stage)
            cluster = Cluster(args)
            cluster.check()
            if args.action == 'check':
                cluster.restore_preflight(manifest)
                print(json.dumps({'result': 'ready', 'snapshot': manifest['snapshot'], 'note': '校验完成，尚未恢复'}))
            else:
                cluster.restore(stage, manifest)


if __name__ == '__main__':
    os.umask(0o077)
    try:
        main()
    except (BackupError, OSError, ValueError, KeyError, tarfile.TarError) as error:
        print('backup: ' + str(error), file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print('backup: 已中断；请检查服务端快照/恢复状态。恢复中断后业务应保持停止。', file=sys.stderr)
        sys.exit(130)
