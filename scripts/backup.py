#!/usr/bin/env python3
"""Portable component backups. Host requires Python standard library + kubectl only."""
import argparse
import contextlib
import datetime
import errno
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import select
import signal
import subprocess
import sys
import tarfile
import tempfile
import time
import uuid
from urllib.parse import quote

from k8s_backup import K8sBackup, add_arguments as add_k8s_arguments, validate_archive as validate_k8s_archive, validate_document as validate_k8s_document, validate_metadata as validate_k8s_metadata

from redis_backup import RedisBackup, validate_manifest
from minio_backup import MinioBackup, validate_metadata, archive_inventory
from kafka_backup import KafkaBackup, add_arguments as add_kafka_arguments, archive_inventory as kafka_inventory
from kafka_backup import validate_restore_metadata as validate_kafka_restore
from kafka_backup import blocked_restore_topics as kafka_restore_blocked_topics
from kafka_backup import require_restored_ledger as require_kafka_ledger
from kafka_worker import ledger_restore_topics


class BackupError(Exception):
    pass


SYSTEM_DATABASES = {'information_schema', 'performance_schema', 'mysql', 'sys'}
DEFAULT_DATABASES = ('bkn_audit', 'bkn_trace', 'deploy', 'openbkn', 'safe')
DATABASE_NAME_PATTERN = r'[A-Za-z0-9_-]+'
PAYLOADS = {'mariadb': 'mariadb.sql', 'opensearch': 'repository.tar',
            'redis': 'redis.rdb', 'minio': 'minio.tar', 'kafka': 'kafka.tar', 'k8s': 'k8s.tar'}


def requested_components(args):
    selected = getattr(args, 'selected_components', None)
    if selected is not None:
        if len(set(selected)) != len(selected):
            raise BackupError('--components 不能重复指定同一组件')
        return [name for name in PAYLOADS if name in selected]
    return None if args.component == 'all' else [args.component]


def configured_components(args):
    selected = requested_components(args)
    if selected is not None:
        return selected
    return [name for name in PAYLOADS
            if not (name == 'redis' and args.skip_redis or name == 'minio' and args.skip_minio)]


def manifest_components(manifest):
    if not isinstance(manifest, dict):
        raise BackupError('备份包清单必须是 JSON 对象')
    version = manifest.get('format')
    if type(version) is not int or version not in (1, 2, 3, 4, 5):
        raise BackupError('不支持的备份包版本')
    if version >= 4:
        names = manifest.get('components')
        if (not isinstance(names, list) or not names
                or any(not isinstance(n, str) or n not in PAYLOADS for n in names)
                or len(set(names)) != len(names)):
            raise BackupError('备份包组件清单无效')
        if any((name in manifest) != (name in names) for name in ('redis', 'minio', 'kafka', 'k8s')):
            raise BackupError('备份包组件清单与元数据不匹配')
        return names
    if (version == 1 and 'redis' in manifest or version == 2 and 'redis' not in manifest):
        raise BackupError('备份包版本与 Redis 元数据不匹配')
    if version < 3 and 'minio' in manifest:
        raise BackupError('旧格式备份包不能声明 MinIO')
    if 'kafka' in manifest or 'k8s' in manifest:
        raise BackupError('旧格式备份包不能声明 Kafka/K8s')
    return ['mariadb', 'opensearch'] + (['redis'] if 'redis' in manifest else []) + (['minio'] if version == 3 else [])


def log(message):
    print(time.strftime('%Y-%m-%d %H:%M:%S ') + message, file=sys.stderr, flush=True)


def interactive_restore(args):
    return (args.module == 'restore' and args.action == 'run' and not args.non_interactive
            and sys.stdin.isatty() and sys.stderr.isatty())


def confirm_restore(question):
    while True:
        print(question + ' [y/n] ', end='', file=sys.stderr, flush=True)
        try:
            answer = input().strip().lower()
        except EOFError:
            raise BackupError('未收到确认，恢复已取消')
        if answer in ('y', 'yes'):
            return True
        if answer in ('n', 'no'):
            return False
        print('请明确输入 y/yes 或 n/no，空行和其他输入不作选择。', file=sys.stderr, flush=True)


def prepare_restore_options(args, manifest):
    if not interactive_restore(args):
        return
    log('恢复组件: ' + ', '.join(args.components))
    if 'kafka' in args.components and ledger_restore_topics(manifest['kafka']) and not args.kafka_ledger_restored:
        if not confirm_restore('是否确认 MariaDB 审计/Evidence 账本已保留或同批恢复，且备份时生产者、消费者均已停止？'):
            raise BackupError('未确认 Kafka 与原账本匹配，恢复已取消')
        args.kafka_ledger_restored = True
    if not args.overwrite:
        impacts = {'mariadb': '替换所选业务库', 'opensearch': '删除同名索引后恢复',
                   'redis': '替换全部 Redis DB', 'minio': '覆盖同名对象，保留目标多出的对象',
                   'kafka': '删除重建备份内需要替换的 Topic'}
        args.overwrite = confirm_restore('目标已有数据时，是否允许覆盖？' + '；'.join(impacts[c] for c in args.components if c in impacts))
        if not args.overwrite:
            raise BackupError('未允许覆盖，恢复已取消；未停止业务或修改目标数据')
    if 'opensearch' in args.components and not args.replace_repository:
        args.replace_repository = confirm_restore('是否允许替换目标快照仓库？先将原仓库留存到 '
            + args.output_dir + '，再注销同名仓库并清空快照目录内容（保留挂载目录）')


def prepare_restore_stop(args):
    if args.writers_stopped or not interactive_restore(args):
        return
    if not confirm_restore('恢复前必须停止相关业务写入和 Kafka 消费。是否停止 '
            + args.business_namespace + ' 命名空间的业务 Deployment 并等待 Pod 删除？'
            ' MinIO 等数据服务保持运行；独立任务和外部写入方需另行停止。'):
        raise BackupError('恢复已取消；可手动执行 openbkn stop，确认停写后添加 --writers-stopped')
    run([sys.executable, str(Path(__file__).with_name('openbkn_workloads.py')), 'stop',
         '--namespace', args.business_namespace], timeout=args.timeout)
    args.writers_stopped = True
    log('业务 Deployment 的 Pod 已退出；恢复失败时保持停止，核验成功后执行 openbkn start')


def exhausted_directory(path, label):
    """Only diagnose local capacity failure when the filesystem confirms it."""
    try:
        usage = os.statvfs(path)
    except OSError:
        return None
    if usage.f_bavail == 0 or (usage.f_files > 0 and usage.f_favail == 0):
        return (label + '空间或 inode 不足：' + str(path)
                + '。请释放空间或 inode' + ('，或将 TMPDIR 指向容量充足的目录' if label == '临时目录' else '') + '后重试。')
    return None


def run(argv, *, data=None, source=None, target=None, timeout=3600):
    """Stream data to disk, retaining bounded stderr in memory even if disk fills."""
    try:
        temporary_directory = tempfile.gettempdir()
    except OSError as error:
        configured = os.environ.get('TMPDIR')
        capacity = exhausted_directory(configured, '临时目录') if configured else None
        if capacity:
            raise BackupError(capacity + '\n底层原因: ' + str(error)) from error
        raise
    command = Path(argv[0]).name  # Do not expose arguments containing credentials.
    try:
        with contextlib.ExitStack() as stack:
            out = stack.enter_context(tempfile.TemporaryFile(dir=temporary_directory)) if target is None else None
            incoming = source
            if data is not None:
                incoming = stack.enter_context(tempfile.TemporaryFile(dir=temporary_directory))
                incoming.write(data)
                incoming.seek(0)
            process = subprocess.Popen(argv, stdin=incoming or subprocess.DEVNULL,
                                       stdout=target if target is not None else out,
                                       stderr=subprocess.PIPE, start_new_session=True)
            err = stack.enter_context(process.stderr)
            os.set_blocking(err.fileno(), False)
            diagnostic = bytearray()
            stderr_eof = False

            def drain_stderr():
                nonlocal stderr_eof
                # Keep draining after the limit to avoid blocking a noisy child.
                # Bound each iteration so timeout/heartbeat checks still run.
                for _ in range(16):
                    try:
                        block = os.read(err.fileno(), 65536)
                    except BlockingIOError:
                        break
                    if not block:
                        stderr_eof = True
                        break
                    diagnostic.extend(block[:max(0, 8192 - len(diagnostic))])

            started = time.monotonic()
            next_log = started + 20
            try:
                while process.poll() is None:
                    drain_stderr()
                    now = time.monotonic()
                    if now - started > timeout:
                        raise BackupError('命令超时；请检查集群中的任务状态后再重试')
                    if now >= next_log:
                        log('仍在执行当前步骤，已用时 %ds' % (now - started))
                        next_log = now + 20
                    select.select([] if stderr_eof else [err], [], [], 0.1)
            finally:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait()
            drain_stderr()
            if process.returncode:
                details = diagnostic.decode(errors='replace').strip() or '子进程未提供错误输出'
                capacity = exhausted_directory(temporary_directory, '临时目录')
                target_name = getattr(target, 'name', None)
                if not capacity and isinstance(target_name, (str, os.PathLike)):
                    capacity = exhausted_directory(Path(target_name).resolve().parent, '输出目录')
                if not capacity and any(marker in details for marker in ('ENOSPC', '[Errno 28]', 'No space left on device')):
                    capacity = ('子进程报告空间不足；请检查命令目标的存储空间和 inode，'
                                '并释放空间后重试。本地临时目录：' + temporary_directory
                                + '（可通过 TMPDIR 调整）。')
                message = ('命令执行失败（' + command + '，退出码 ' + str(process.returncode)
                           + '，临时目录 ' + temporary_directory + '）: ' + details)
                raise BackupError((capacity + '\n' if capacity else '') + message)
            if out is not None:
                out.seek(0)
                return out.read()
            return b''
    except OSError as error:
        if error.errno == errno.ENOSPC:
            raise BackupError('临时目录空间不足：' + temporary_directory
                              + '。请释放空间或将 TMPDIR 指向容量充足的目录后重试。'
                              + '\n底层原因: ' + str(error)) from error
        raise


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


def unpack(package, destination, components=None, *, for_restore=False):
    """Check the envelope, then extract/hash only the requested component payloads."""
    with tarfile.open(package, 'r:gz') as archive:
        members = list(safe_members(archive))
        names = {m.name for m in members}
        if 'manifest.json' not in names or names - {'manifest.json', 'k8s.json'} - (set(PAYLOADS.values()) | {'mariadb.tar'}) or any(not m.isfile() for m in members):
            raise BackupError('备份包文件列表不符合支持的格式')
        with archive.extractfile('manifest.json') as source:
            manifest = json.load(source)
        available = manifest_components(manifest)
        selected = ([c for c in available if c != 'k8s'] if for_restore else available) if components is None else components
        if not selected and for_restore:
            raise BackupError('备份包仅含 K8s 配置留档，没有可自动恢复的数据')
        if not selected or set(selected) - set(available):
            raise BackupError('备份包不包含请求的组件: ' + ', '.join(set(selected) - set(available)))
        payloads = dict(PAYLOADS)
        if manifest['format'] >= 5:
            payloads['mariadb'] = 'mariadb.tar'
        if 'k8s.json' in names and 'k8s.tar' not in names:
            payloads['k8s'] = 'k8s.json'  # Early configuration-only packages.
        expected_files = {payloads[c] for c in available}
        if names != expected_files | {'manifest.json'}:
            raise BackupError('备份包格式与文件列表不匹配')
        if not isinstance(manifest.get('sha256'), dict) or set(manifest['sha256']) != expected_files:
            raise BackupError('备份包缺少校验清单')
        selected_files = {payloads[c] for c in selected}
        destination.mkdir(parents=True, exist_ok=True)
        archive.extractall(destination, members=[m for m in members if m.name in selected_files | {'manifest.json'}])
    for name in selected_files:
        if digest(destination / name) != manifest['sha256'][name]:
            raise BackupError('备份包校验失败: ' + name)
    if 'k8s' in selected:
        if payloads['k8s'] == 'k8s.json':
            validate_k8s_document(json.loads((destination / 'k8s.json').read_text()), BackupError)
        else:
            validate_k8s_archive(destination / 'k8s.tar', BackupError)
        validate_k8s_metadata(manifest.get('k8s'), BackupError)
    if 'redis' in selected:
        validate_manifest(manifest.get('redis'), BackupError)
        with open(destination / 'redis.rdb', 'rb') as rdb:
            if rdb.read(5) != b'REDIS':
                raise BackupError('Redis RDB 文件头无效')
    if 'opensearch' in selected:
        with tarfile.open(destination / 'repository.tar', 'r:') as archive:
            members = list(safe_members(archive))
            if not any(PurePosixPath(m.name).name.startswith('index-') for m in members):
                raise BackupError('快照仓库缺少 index 元数据')
        if not re.fullmatch(r'[a-z0-9-]+', manifest.get('snapshot', '')):
            raise BackupError('快照名称无效')
        indices = manifest.get('indices', [])
        if not isinstance(indices, list) or not indices or any(not isinstance(i, str) or re.search(r'[*,/\\?#\s]', i) or i in ('.', '..') or not i for i in indices):
            raise BackupError('索引清单无效或为空')
    if 'minio' in selected:
        validate_metadata(manifest.get('minio'), BackupError)
        archive_inventory(destination / 'minio.tar', manifest['minio'], BackupError)
    if 'kafka' in selected:
        if not isinstance(manifest.get('kafka'), dict):
            raise BackupError('Kafka 备份包缺少组件元数据')
        kafka_inventory(destination / 'kafka.tar', manifest.get('kafka'), BackupError)
    if 'mariadb' in selected:
        databases = validate_databases(manifest.get('databases'))
        if manifest['format'] >= 5:
            with tarfile.open(destination / 'mariadb.tar', 'r:') as archive:
                members = list(safe_members(archive))
                if (any(not m.isfile() or not m.size for m in members)
                        or {m.name for m in members} != {db + '.sql' for db in databases}):
                    raise BackupError('MariaDB 分库文件与数据库清单不匹配')
                folder = destination / 'mariadb'
                folder.mkdir(exist_ok=True)
                archive.extractall(folder, members=members)

    for component in ('mariadb', 'opensearch'):
        if component in selected:
            version = manifest.get(component + '_version')
            if not isinstance(version, str) or not re.match(r'^\d+\.\d+\.', version):
                raise BackupError('备份包缺少有效版本: ' + component + '_version')
    return manifest


def validate_databases(databases):
    if (not isinstance(databases, list) or not databases
            or any(not isinstance(db, str) or not re.fullmatch(DATABASE_NAME_PATTERN, db) for db in databases)
            or len(set(databases)) != len(databases)):
        raise BackupError('数据库清单无效，库名仅支持字母、数字、下划线和横线，且不能重复')
    if set(databases) & SYSTEM_DATABASES:
        raise BackupError('不能备份或恢复系统库')
    return databases


def restore_databases(args, manifest):
    databases = validate_databases(manifest.get('databases'))
    selected = getattr(args, 'databases', None)
    if selected is None:
        return databases
    validate_databases(selected)
    if set(selected) - set(databases):
        raise BackupError('备份包不包含指定数据库: ' + ', '.join(sorted(set(selected) - set(databases))))
    if manifest['format'] < 5 and selected != databases:
        raise BackupError('旧版合并 SQL 备份包不支持按库选择恢复，请恢复包内全部库')
    return selected


class Cluster:
    def __init__(self, args):
        self.args = args
        self.components = getattr(args, 'components', configured_components(args))
        self.redis = None
        self.minio = None
        self.kafka = None
        self.k8s = None
        self.targets = {}
        for service in ('mariadb', 'opensearch'):
            if service not in self.components:
                continue
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
                       '--routines --events --triggers --hex-blob --add-drop-database --databases -- "$@"')
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
        log('检查组件: ' + ', '.join(self.components))
        if 'mariadb' in self.components:
            self.check_mariadb()
        if 'opensearch' in self.components:
            self.check_opensearch()
        if 'redis' in self.components:
            self.redis = RedisBackup(self.args, run, BackupError, log)
        if 'minio' in self.components:
            self.minio = MinioBackup(self.args, run, BackupError, log)
        if 'kafka' in self.components:
            self.kafka = KafkaBackup(self.args, run, BackupError, log)
            self.kafka.check()

        if 'k8s' in self.components:
            self.k8s = K8sBackup(self.args, run, BackupError, log)
            self.k8s.check()

    def check_mariadb(self):
        self.mariadb_version = self.sql('SELECT VERSION();').decode().strip()
        self.exec('mariadb', ['sh', '-c', 'command -v mariadb-dump >/dev/null'], timeout=60)
        all_databases = self.sql('SHOW DATABASES;').decode().splitlines()
        if self.args.module == 'restore':
            # Restore scope comes from the archive, not the target's existing databases.
            self.databases = []
            return
        self.databases = list(self.args.databases or DEFAULT_DATABASES)
        validate_databases(self.databases)
        missing = set(self.databases) - set(all_databases)
        if missing:
            raise BackupError('指定的业务库不存在: ' + ', '.join(sorted(missing)))

    def check_opensearch(self):
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
        self.check_opensearch_repository_directory()
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

    def check_opensearch_repository_directory(self):
        raw = self.exec('opensearch', ['sh', '-c',
            'test -d "$1" && test -r "$1" && test -w "$1" && cat /proc/self/mountinfo',
            'check-repository-directory', self.args.repository_path], timeout=60)
        for line in raw.decode().splitlines():
            fields = line.split()
            if len(fields) < 5:
                continue
            decode = lambda value: re.sub(r'\\([0-7]{3})', lambda m: chr(int(m[1], 8)), value)
            root, mountpoint = decode(fields[3]), decode(fields[4])
            if mountpoint == self.args.repository_path and root.endswith('//deleted'):
                raise BackupError('快照目录为失效挂载；重建宿主机目录并重启 OpenSearch Pod 后重试')

    @contextlib.contextmanager
    def lock(self):
        # Full and component commands use the same per-Pod lock. Redis already
        # holds its PVC lock inside backup/restore; it never needs OpenSearch.
        with contextlib.ExitStack() as stack:
            for component in ('mariadb', 'opensearch', 'minio'):
                if component not in self.components:
                    continue
                execute = self.minio.execute if component == 'minio' else lambda command, component=component, **kw: self.exec(component, command, **kw)
                path = '/tmp/openbkn-data-backup.lock'
                execute(['mkdir', path], timeout=60)
                stack.callback(self.release_lock, execute, path, component)
            yield

    @staticmethod
    def release_lock(execute, path, component="unknown"):
        try:
            execute(['rm', '-rf', '--', path], timeout=60)
        except Exception as exc:
            log(component + ' 组件锁清理失败: ' + str(exc) + '；确认没有任务后再清理 Pod 内目录: ' + path)

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
            stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S') + '-' + uuid.uuid4().hex[:8]
            name = 'openbkn-' + (self.components[0] + '-' if len(self.components) == 1 else '') + stamp
            manifest = {'format': 5 if 'mariadb' in self.components else 4, 'components': self.components,
                        'created_at': datetime.datetime.now(datetime.timezone.utc).isoformat()}
            if 'mariadb' in self.components:
                self.backup_mariadb(stage / 'mariadb.tar')
                manifest.update(databases=self.databases, mariadb_version=self.mariadb_version)
            if 'opensearch' in self.components:
                manifest.update(self.backup_opensearch(stage / 'repository.tar', name))
            if 'redis' in self.components:
                log('导出 Redis 全部 DB 的 RDB 快照')
                manifest['redis'] = self.redis.backup(stage / 'redis.rdb')
            if 'minio' in self.components:
                log('在线导出 MinIO 业务 bucket（不停止业务或 MinIO）')
                manifest['minio'] = self.minio.backup(stage / 'minio.tar')
            if 'kafka' in self.components:
                manifest['kafka'] = self.kafka.backup(stage / 'kafka.tar')
            if 'k8s' in self.components:
                manifest['k8s'] = self.k8s.backup(stage / 'k8s.tar')
            payloads = ['mariadb.tar' if c == 'mariadb' else PAYLOADS[c] for c in self.components]
            manifest['sha256'] = {name: digest(stage / name) for name in payloads}
            (stage / 'manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + '\n')
            log('压缩备份包并校验')
            package = destination / (name + '.tar.gz')
            partial = destination / (package.name + '.partial')
            try:
                with tarfile.open(partial, 'w:gz') as archive:
                    for name in ['manifest.json'] + payloads:
                        archive.add(stage / name, arcname=name)
                with tempfile.TemporaryDirectory(dir=destination) as verify:
                    unpack(partial, Path(verify))
                with open(partial, 'rb') as complete:
                    os.fsync(complete.fileno())
                os.replace(partial, package)
            finally:
                partial.unlink(missing_ok=True)
            cleanup_warning = None
            if 'snapshot' in manifest:
                try:
                    self.idle()
                    if not self.api('DELETE', self.repository + '/' + manifest['snapshot'], timeout=self.args.timeout).get('acknowledged'):
                        raise BackupError('快照删除未确认')
                except Exception as exc:
                    cleanup_warning = str(exc)
                    log('备份包已完成，但快照清理失败: ' + cleanup_warning)
            log('备份完成: ' + str(package))
            result = {'result': 'success', 'archive': str(package), 'components': self.components, 'format': manifest['format']}
            if cleanup_warning:
                result['cleanup_warning'] = cleanup_warning
            if 'kafka' in manifest:
                blocked = kafka_restore_blocked_topics(manifest['kafka'])
                if blocked:
                    result['restore_blocked_topics'] = blocked
                    log('注意：备份文件已生成，但以下 Kafka Topic 当前尚不能恢复: ' + ', '.join(blocked))
                ledger_topics = ledger_restore_topics(manifest['kafka'])
                if ledger_topics:
                    result['kafka_ledger_restore_required'] = ledger_topics
            if 'snapshot' in manifest:
                result['snapshot'] = manifest['snapshot']
            print(json.dumps(result))

    def backup_mariadb(self, path):
        validate_databases(self.databases)
        log('导出 MariaDB 业务库: ' + ', '.join(self.databases))
        db_list = ','.join("'%s'" % db for db in self.databases)
        engines = self.sql('SELECT TABLE_SCHEMA,TABLE_NAME,ENGINE FROM information_schema.TABLES '
                           'WHERE TABLE_SCHEMA IN (' + db_list + ") AND TABLE_TYPE='BASE TABLE' AND ENGINE <> 'InnoDB';")
        if engines.strip():
            raise BackupError('业务库存在非 InnoDB 表，无法保证在线事务一致性；一期暂不支持')
        with tempfile.TemporaryDirectory(prefix='mariadb-', dir=path.parent) as tmp, tarfile.open(path, 'w:') as archive:
            for number, database in enumerate(self.databases, 1):
                started = time.monotonic()
                log('MariaDB 导出 %d/%d: %s' % (number, len(self.databases), database))
                sql_file = Path(tmp) / (database + '.sql')
                try:
                    with open(sql_file, 'wb') as target:
                        self.sql(databases=[database], target=target)
                    if not sql_file.stat().st_size:
                        raise BackupError('导出文件为空')
                    archive.add(sql_file, arcname=database + '.sql')
                except (BackupError, OSError) as exc:
                    raise BackupError('MariaDB 导出失败，数据库 ' + database + ': ' + str(exc)) from exc
                sql_file.unlink()
                log('MariaDB 导出完成: %s，耗时 %ds' % (database, time.monotonic() - started))

    def backup_opensearch(self, path, snapshot):
        log('注册并校验快照仓库，创建 OpenSearch 快照')
        self.idle()
        self.register()
        previous = self.snapshots()
        self.api('PUT', self.repository + '/' + snapshot,
                 {'indices': '*', 'include_global_state': False, 'ignore_unavailable': False, 'partial': False})
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
        log('新快照成功，通过 API 删除本仓库中的旧快照')
        for old in previous:
            result = self.api('DELETE', self.repository + '/' + quote(old['snapshot'], safe=''), timeout=self.args.timeout)
            if not result.get('acknowledged'):
                raise BackupError('旧快照删除未确认')
        self.idle()
        log('导出完整 OpenSearch 快照仓库')
        inventory = self.snapshots()
        with open(path, 'wb') as target:
            self.exec('opensearch', ['tar', '-C', self.args.repository_path, '-cf', '-', '.'], target=target)
        self.idle()
        if self.snapshots() != inventory:
            raise BackupError('打包时仓库被其他任务修改，本次备份终止')
        with tarfile.open(path, 'r:') as archive:
            list(safe_members(archive))
        return {'snapshot': snapshot, 'opensearch_version': self.opensearch_version,
                'indices': self.api('GET', self.repository + '/' + snapshot)['snapshots'][0]['indices']}

    def restore_preflight(self, manifest):
        if "k8s" in self.components:
            raise BackupError("K8s 仅配置留档，不支持自动恢复")
        if set(self.components) - set(manifest_components(manifest)):
            raise BackupError('备份包不包含请求的恢复组件')
        # Kafka capability/conflict checks must pass before any component is overwritten.
        if 'kafka' in self.components:
            self.kafka.preflight(manifest['kafka'], self.args.overwrite)
        if 'redis' in self.components:
            if getattr(self, 'redis', None) is None:
                raise BackupError('备份范围包含 Redis，不能跳过恢复')
            self.redis.preflight(manifest['redis'], self.args.overwrite)
        if 'minio' in self.components:
            if getattr(self, 'minio', None) is None:
                raise BackupError('备份包包含 MinIO，不能跳过恢复')
            self.minio.preflight(manifest['minio'], self.args.overwrite)
        if 'mariadb' in self.components:
            self.preflight_mariadb(manifest)
        if 'opensearch' in self.components:
            return self.preflight_opensearch(manifest)
        return []

    def preflight_mariadb(self, manifest):
        if self.mariadb_version.split('.')[0] != manifest['mariadb_version'].split('.')[0]:
            raise BackupError('一期要求 MariaDB 主版本一致')
        databases = restore_databases(self.args, manifest)
        db_list = ','.join("'%s'" % db for db in databases)
        tables = self.sql('SELECT TABLE_SCHEMA,TABLE_NAME FROM information_schema.TABLES '
                          'WHERE TABLE_SCHEMA IN (' + db_list + ');').decode().strip()
        if tables and not self.args.overwrite:
            raise BackupError('目标业务库已有表；覆盖恢复需要 --overwrite，并先停止业务写入')

    def preflight_opensearch(self, manifest):
        if self.opensearch_version.split('.')[:2] != manifest['opensearch_version'].split('.')[:2]:
            raise BackupError('一期要求 OpenSearch 主次版本一致')
        current = self.api('GET', '/_cat/indices?format=json&expand_wildcards=all')
        conflicts = sorted({item['index'] for item in current} & set(manifest['indices']))
        if conflicts and not self.args.overwrite:
            raise BackupError('目标存在同名索引；覆盖恢复需要 --overwrite，并先停止业务写入')
        # Do not merge repositories or overwrite a repository currently registered elsewhere.
        # The bundled OpenSearch image does not necessarily include `find`.
        # Use only POSIX shell globbing so this check works in the slim image.
        content = self.exec('opensearch', ['sh', '-c',
            'for item in "$1"/* "$1"/.[!.]* "$1"/..?*; do '
            '[ ! -e "$item" ] || { printf "non-empty\\n"; exit 0; }; done',
            'check-repository', self.args.repository_path], timeout=60)
        if content.strip() and not self.args.replace_repository:
            raise BackupError('恢复目标快照目录非空；请使用空的持久化快照目录，禁止混入已有仓库')
        repositories = self.api('GET', '/_snapshot')
        for name, repository in repositories.items():
            location = repository.get('settings', {}).get('location')
            if location == self.args.repository_path and name != self.args.repository:
                raise BackupError('恢复目录被其他仓库引用: ' + name)
            if name == self.args.repository and (location != self.args.repository_path or repository.get('type') != 'fs'):
                raise BackupError('同名仓库指向其他存储，不能替换')
        if not self.args.replace_repository and any(r.get('settings', {}).get('location') == self.args.repository_path for r in repositories.values()):
            raise BackupError('恢复目录仍被仓库注册引用；先用 DELETE /_snapshot/<仓库名> 注销（不会删除文件）')
        self.idle()
        return conflicts

    def restore(self, stage, manifest):
        if not self.args.writers_stopped:
            raise BackupError('恢复前请停止业务写入，并显式传入 --writers-stopped')
        if 'kafka' in self.components:
            validate_kafka_restore(manifest['kafka'], BackupError)
            require_kafka_ledger(manifest['kafka'], self.args.kafka_ledger_restored, BackupError)
        with self.lock():
            conflicts = self.restore_preflight(manifest)
            if 'opensearch' in self.components:
                self.restore_opensearch(stage, manifest, conflicts)
            if 'mariadb' in self.components:
                databases = restore_databases(self.args, manifest)
                if manifest['format'] < 5:
                    log('恢复 MariaDB 旧版合并 SQL（包内全部库）')
                    with open(stage / 'mariadb.sql', 'rb') as source:
                        self.sql(source=source)
                    for database in databases:
                        self.sql('USE `' + database + '`; SHOW TABLES;')
                else:
                    completed = []
                    for number, database in enumerate(databases, 1):
                        started = time.monotonic()
                        log('MariaDB 恢复 %d/%d: %s' % (number, len(databases), database))
                        try:
                            with open(stage / 'mariadb' / (database + '.sql'), 'rb') as source:
                                self.sql(source=source)
                            self.sql('USE `' + database + '`; SHOW TABLES;')
                        except (BackupError, OSError) as exc:
                            raise BackupError('MariaDB 恢复失败，数据库 %s（可能已部分写入）；已完成: %s；未执行: %s。原因: %s'
                                              % (database, ', '.join(completed) or '无',
                                                 ', '.join(databases[number:]) or '无', exc)) from exc
                        completed.append(database)
                        log('MariaDB 恢复完成: %s，耗时 %ds' % (database, time.monotonic() - started))
            if 'redis' in self.components:
                log('恢复 Redis 全部 DB（停止 Redis、转换 AOF 并验证重启）')
                self.redis.restore(stage / 'redis.rdb', manifest['redis'], self.args.overwrite)
            if 'minio' in self.components:
                log('恢复 MinIO 对象（覆盖同名对象，保留目标多出的对象）')
                self.minio.restore(stage / 'minio.tar', manifest['minio'], self.args.overwrite)
            if 'kafka' in self.components:
                self.kafka.restore(stage / 'kafka.tar', manifest['kafka'], self.args.overwrite)
            log('恢复完成: ' + ', '.join(self.components) + '；请验证相关业务。')
            result = {'result': 'success', 'components': self.components,
                      'redis_restored': 'redis' in self.components, 'minio_restored': 'minio' in self.components}
            if 'mariadb' in self.components:
                result['databases'] = restore_databases(self.args, manifest)
            if 'opensearch' in self.components:
                result['snapshot'] = manifest['snapshot']
            print(json.dumps(result))

    def restore_opensearch(self, stage, manifest, conflicts):
        self.check_opensearch_repository_directory()
        self.idle()
        if self.args.replace_repository:
            self.preflight_opensearch(manifest)
            destination = Path(self.args.output_dir)
            destination.mkdir(parents=True, exist_ok=True, mode=0o700)
            package = destination / ('openbkn-repository-before-restore-' + uuid.uuid4().hex + '.tar')
            partial = package.with_suffix('.partial')
            try:
                with open(partial, 'wb') as target:
                    self.exec('opensearch', ['tar', '-C', self.args.repository_path, '-cf', '-', '.'], target=target)
                    target.flush()
                    os.fsync(target.fileno())
                with tarfile.open(partial, 'r:') as archive:
                    list(safe_members(archive))
                os.replace(partial, package)
            finally:
                partial.unlink(missing_ok=True)
            log('原快照仓库已留存: ' + str(package))
            repositories = self.api('GET', '/_snapshot')
            if self.args.repository in repositories:
                if not self.api('DELETE', self.repository).get('acknowledged'):
                    raise BackupError('原仓库注销未确认')
            self.exec('opensearch', ['sh', '-c',
                'set -eu; for item in "$1"/* "$1"/.[!.]* "$1"/..?*; do '
                '[ ! -e "$item" ] && [ ! -L "$item" ] || rm -rf -- "$item"; done',
                'clear-repository', self.args.repository_path])
        log('将完整快照仓库复制到目标 Pod 持久卷')
        with open(stage / 'repository.tar', 'rb') as source:
            self.exec('opensearch', ['tar', '--no-same-owner', '-C', self.args.repository_path, '-xf', '-'], source=source)
        self.register(readonly=True)
        snapshot = next((s for s in self.snapshots() if s['snapshot'] == manifest['snapshot']), None)
        if not snapshot or snapshot['state'] != 'SUCCESS' or sorted(snapshot['indices']) != sorted(manifest['indices']):
            raise BackupError('仓库快照与备份清单不一致')
        log('恢复 OpenSearch 索引' + ('（删除同名索引）' if conflicts else ''))
        for index in conflicts:
            if not self.api('DELETE', '/' + quote(index, safe='')).get('acknowledged'):
                raise BackupError('索引删除未确认: ' + index)
        result = self.api('POST', self.repository + '/' + manifest['snapshot'] + '/_restore?wait_for_completion=true',
                          {'indices': '*', 'include_global_state': False, 'include_aliases': True}, timeout=self.args.timeout)
        shards = result.get('snapshot', {}).get('shards', {})
        if shards.get('failed') != 0 or not shards.get('total') or shards.get('successful') != shards.get('total'):
            raise BackupError('OpenSearch 恢复不完整: ' + json.dumps(result))
        health = self.api('GET', '/_cluster/health?wait_for_status=yellow&timeout=60s', timeout=75)
        if health.get('timed_out') or health.get('status') == 'red':
            raise BackupError('恢复后 OpenSearch 未达到 yellow 状态')


def parser():
    result = argparse.ArgumentParser(description='MariaDB / OpenSearch / Redis / MinIO / Kafka 单文件备份恢复')
    result.add_argument('module', choices=['backup', 'restore'])
    result.add_argument('action', choices=['check', 'run'])
    result.add_argument('archive', nargs='?', help='恢复用的 .tar.gz 备份包')
    selection = result.add_mutually_exclusive_group()
    selection.add_argument('--component', choices=['all', *PAYLOADS], default='all', help='兼容单组件入口')
    selection.add_argument('--components', dest='selected_components', nargs='+', action='extend',
                           choices=list(PAYLOADS), help='选择一个或多个组件；备份默认全部，恢复默认包内全部')
    result.add_argument('--output-dir', default='/var/backups/openbkn')
    result.add_argument('--repository', default='offsite-backup', help='专用仓库；备份成功后删除其中所有旧快照')
    result.add_argument('--repository-path', default='/usr/share/opensearch/snapshots')
    result.add_argument('--databases', nargs='+', help='备份默认 bkn_audit bkn_trace deploy openbkn safe；恢复可选择新包内的数据库')
    result.add_argument('--timeout', type=int, default=3600)
    result.add_argument('--poll-interval', type=int, default=10)
    result.add_argument('--non-interactive', action='store_true', help='禁用恢复交互确认')
    result.add_argument('--business-namespace', default=os.environ.get('CORE_NAMESPACE', 'openbkn'))
    result.add_argument('--replace-repository', action='store_true', help='覆盖恢复时留存并替换目标快照仓库')
    result.add_argument('--overwrite', action='store_true', help='允许替换目标业务库、同名索引、Redis 全部 DB，覆盖 MinIO 同名对象（保留多余对象）')
    result.add_argument('--writers-stopped', action='store_true', help='确认业务服务已停止写入；恢复 Kafka 时消费者也必须停止（恢复必需）')
    result.add_argument('--skip-minio', action='store_true', help='仅备份时使用：不包含 MinIO')
    result.add_argument('--minio-namespace', default=os.environ.get('MINIO_NAMESPACE', 'openbkn'))
    result.add_argument('--minio-selector', default='component=minio')
    result.add_argument('--minio-pod')
    result.add_argument('--minio-container')
    result.add_argument('--minio-endpoint', default='http://127.0.0.1:9000', help='从 MinIO 容器内访问的 S3 API 地址')
    result.add_argument('--minio-part-size', default='8MiB', help='mc pipe 上传分片大小；小内存 Pod 可降低，范围由 mc 校验')
    result.add_argument('--minio-buckets', nargs='+', help='仅备份时使用：默认备份所有业务 bucket，不含 MinIO 内部配置')
    result.add_argument('--skip-redis', action='store_true', help='仅备份时使用：不包含 Redis')
    result.add_argument('--redis-namespace', default=os.environ.get('REDIS_NAMESPACE', 'resource'))
    result.add_argument('--redis-selector', default='app=redis-redis')
    result.add_argument('--redis-pod')
    result.add_argument('--redis-container', default='redis')
    result.add_argument('--redis-user', default='root')
    result.add_argument('--redis-config-command', default='CONFIG_NEW', help='Redis 重命名后的 CONFIG 命令，原生命令可填 CONFIG')
    add_kafka_arguments(result)
    add_k8s_arguments(result)
    for service, selector in [('mariadb', 'app=mariadb'), ('opensearch', 'app.kubernetes.io/instance=opensearch')]:
        result.add_argument('--' + service + '-namespace', default=os.environ.get(service.upper() + '_NAMESPACE', 'resource'))
        result.add_argument('--' + service + '-selector', default=selector)
        result.add_argument('--' + service + '-pod')
        result.add_argument('--' + service + '-container')
    return result


def validate_options(args):
    if args.replace_repository and (args.module != 'restore' or 'opensearch' not in args.components or not args.overwrite):
        raise BackupError('--replace-repository 仅用于 OpenSearch 恢复，且需要 --overwrite')
    if args.k8s_secrets and (args.module != 'backup' or 'k8s' not in args.components):
        raise BackupError('--k8s-secrets 仅用于包含 K8s 配置留档的备份')
    if args.include_kafka and (args.module != 'backup' or requested_components(args) is not None):
        raise BackupError('--include-kafka 仅用于完整备份；单独操作请用 kafka backup/restore')
    if args.kafka_topics and ('kafka' not in args.components or args.module != 'backup'):
        raise BackupError('--kafka-topics 仅用于 Kafka 备份，恢复范围由包决定')
    if args.kafka_replicas < 1:
        raise BackupError('--kafka-replicas 必须为正数')
    if args.kafka_ledger_restored and (args.module != 'restore' or 'kafka' not in args.components):
        raise BackupError('--kafka-ledger-restored 仅用于包含 Kafka 的恢复')
    if 'opensearch' in args.components:
        if not re.fullmatch(r'[a-z0-9-]+', args.repository):
            raise BackupError('仓库名称只支持小写字母、数字和横线')
        if not args.repository_path.startswith('/') or '..' in PurePosixPath(args.repository_path).parts or args.repository_path == '/':
            raise BackupError('仓库路径必须是专用绝对路径')
    if args.timeout < 1 or not 1 <= args.poll_interval <= 30:
        raise BackupError('timeout 必须为正数，poll-interval 必须在 1 到 30 秒之间')
    if args.databases:
        if 'mariadb' not in args.components:
            raise BackupError('--databases 仅用于 MariaDB 备份或恢复')
        validate_databases(args.databases)
    if args.minio_buckets and ('minio' not in args.components or args.module == 'restore'):
        raise BackupError('--minio-buckets 仅用于 MinIO 备份，恢复范围由备份包决定')


def main(argv=None):
    args = parser().parse_args(argv)
    selected = requested_components(args)
    if args.module == 'restore' and selected and 'k8s' in selected:
        raise BackupError('K8s 仅配置留档，不支持自动恢复；请解包 k8s.tar 后按 RESTORE.zh.md 人工恢复')
    if args.include_kafka and (args.module != 'backup' or selected is not None):
        raise BackupError('--include-kafka 已由默认全组件备份包含，不能用于恢复或显式组件选择')
    if (args.skip_redis or args.skip_minio) and (args.module == 'restore' or requested_components(args) is not None):
        raise BackupError('--skip-redis/--skip-minio 仅用于默认范围备份，不能与显式组件选择同时使用')
    if args.module == 'backup':
        if args.archive:
            raise BackupError('backup 不接受备份包参数')
        args.components = configured_components(args)
        validate_options(args)
        cluster = Cluster(args)
        cluster.check()
        if args.action == 'check':
            result = {'result': 'ready', 'components': args.components,
                      'note': '仅完成前置检查，尚未备份；请确保输出目录有足够空间'}
            if 'mariadb' in args.components:
                result['databases'] = cluster.databases
            if 'opensearch' in args.components:
                result['repository_path'] = args.repository_path
            if 'minio' in args.components:
                result['minio'] = {'buckets': cluster.minio.buckets, 'pod': cluster.minio.pod}
            if 'redis' in args.components:
                result['redis'] = {'version': cluster.redis.version, 'pvc': cluster.redis.pvc}
            if 'kafka' in args.components:
                result['kafka'] = {'topics': [t['name'] for t in cluster.kafka.metadata['topics']],
                                   'restore_blocked_topics': kafka_restore_blocked_topics(cluster.kafka.metadata, counts_known=False)}
            print(json.dumps(result, ensure_ascii=False))
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
            manifest = unpack(args.archive, stage, selected, for_restore=True)
            if 'k8s' in manifest_components(manifest):
                log('K8s 配置留档不自动恢复；需要时解包 k8s.tar 按 RESTORE.zh.md 人工恢复')
            args.components = [c for c in manifest_components(manifest) if c != 'k8s'] if selected is None else selected
            validate_options(args)
            if 'mariadb' in args.components:
                restore_databases(args, manifest)
            prepare_restore_options(args, manifest)
            if 'kafka' in args.components:
                validate_kafka_restore(manifest['kafka'], BackupError)
                if args.action == 'run':
                    require_kafka_ledger(manifest['kafka'], args.kafka_ledger_restored, BackupError)
            cluster = Cluster(args)
            cluster.check()
            if args.action == 'check':
                cluster.restore_preflight(manifest)
                result = {'result': 'ready', 'components': args.components, 'note': '校验完成，尚未恢复'}
                if 'mariadb' in args.components:
                    result['databases'] = restore_databases(args, manifest)
                if 'kafka' in args.components and ledger_restore_topics(manifest['kafka']):
                    result['kafka_ledger_restore_required'] = ledger_restore_topics(manifest['kafka'])
                    result['required_run_options'] = ['--writers-stopped', '--kafka-ledger-restored']
                if 'opensearch' in args.components:
                    result['snapshot'] = manifest['snapshot']
                print(json.dumps(result, ensure_ascii=False))
            else:
                prepare_restore_stop(args)
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
