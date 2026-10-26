"""Online MinIO object export/import using tools in the existing server Pod."""
import contextlib
import io
import json
from pathlib import PurePosixPath
import re
import shutil
import tarfile
import tempfile
import time
import uuid


def mc_error_detail(source):
    """Read only structured mc errors, never log downloaded object contents."""
    source.seek(0, 2)
    if source.tell() > 64 * 1024:
        return ''
    source.seek(0)
    try:
        row = json.loads(source.read())
    except (ValueError, UnicodeError):
        return ''
    if not isinstance(row, dict) or row.get('status') != 'error':
        return ''
    error = row.get('error')
    if not isinstance(error, dict):
        return ''
    cause = error.get('cause', {})
    details = cause.get('error', {}) if isinstance(cause, dict) else {}
    code = details.get('Code', '') if isinstance(details, dict) else ''
    messages = [code, error.get('message'), cause.get('message') if isinstance(cause, dict) else None]
    return '; '.join(' '.join(item.split())[:500] for item in messages if isinstance(item, str) and item)


def validate_metadata(metadata, error):
    if (not isinstance(metadata, dict) or metadata.get('mode') != 'current-objects'
            or not isinstance(metadata.get('buckets'), list)):
        raise error('MinIO 备份清单无效')
    names = metadata['buckets']
    if any(not isinstance(n, str) for n in names):
        raise error('MinIO bucket 名称无效')
    if len(names) != len(set(names)) or any(not re.fullmatch(
            r'[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]', n) or '..' in n for n in names):
        raise error('MinIO bucket 名称无效')


def object_key(key, error):
    # Reject keys that mc could normalize before issuing S3 requests.
    path = PurePosixPath(key)
    canonical = str(path) + ('/' if key.endswith('/') else '')
    if (not key or not path.parts or path.is_absolute() or canonical != key or '..' in path.parts
            or '\\' in key or any(ord(c) < 32 for c in key)):
        raise error('MinIO 对象名可能被 mc 规范化，暂不支持: ' + repr(key))
    return key


def archive_records(path, metadata, error):
    """Store object names in JSON, never interpret them as archive paths."""
    validate_metadata(metadata, error)
    with tarfile.open(path, 'r:') as archive:
        members = {}
        for member in archive:
            if (not member.isfile() or member.name in members
                    or not (member.name == 'objects.json' or re.fullmatch(r'objects/[0-9]{12}', member.name))):
                raise error('MinIO 对象包包含不安全路径、重复项或文件类型')
            members[member.name] = member
        if 'objects.json' not in members:
            raise error('MinIO 对象包缺少对象清单')
        with archive.extractfile(members['objects.json']) as source:
            records = json.load(source)
        if not isinstance(records, list):
            raise error('MinIO 对象清单无效')
        expected, keys = {'objects.json'}, set()
        for record in records:
            if not isinstance(record, dict):
                raise error('MinIO 对象记录无效')
            name, bucket, key, size = (record.get(k) for k in ('file', 'bucket', 'key', 'size'))
            if (not isinstance(name, str) or not re.fullmatch(r'objects/[0-9]{12}', name)
                    or bucket not in metadata['buckets'] or not isinstance(key, str)
                    or type(size) is not int or size < 0):
                raise error('MinIO 对象记录无效')
            object_key(key, error)
            if key.endswith('/') and size != 0:
                raise error('MinIO 目录标记必须为零字节')
            if name in expected or (bucket, key) in keys or name not in members or members[name].size != size:
                raise error('MinIO 对象文件与清单不一致')
            expected.add(name)
            keys.add((bucket, key))
        if set(members) != expected:
            raise error('MinIO 对象包包含未声明文件')
    return records


def archive_inventory(path, metadata, error):
    return {(r['bucket'], r['key']): r['size'] for r in archive_records(path, metadata, error)}


class MinioBackup:
    def __init__(self, args, run, error, log):
        self.args, self.run, self.error, self.log = args, run, error, log
        self.ns = args.minio_namespace
        query = ['get', 'pod', args.minio_pod] if args.minio_pod else [
            'get', 'pods', '-l', args.minio_selector]
        result = json.loads(self.kube(*query, '-o', 'json'))
        pods = [result] if args.minio_pod else result.get('items', [])
        ready = [p for p in pods if not p['metadata'].get('deletionTimestamp')
                 and p.get('status', {}).get('phase') == 'Running'
                 and any(c.get('type') == 'Ready' and c.get('status') == 'True'
                         for c in p.get('status', {}).get('conditions', []))]
        if len(pods) != 1 or len(ready) != 1:
            raise error('MinIO 要求唯一就绪 Pod；请用 --minio-pod 或 --minio-selector 指定')
        self.pod = ready[0]['metadata']['name']
        containers = ready[0]['spec']['containers']
        self.container = args.minio_container or ('minio' if any(c['name'] == 'minio' for c in containers)
                                                 else containers[0]['name'] if len(containers) == 1 else None)
        if not any(c['name'] == self.container for c in containers):
            raise error('请指定有效的 --minio-container')
        if not re.fullmatch(r'https?://[^\s/@]+(?::[0-9]+)?', args.minio_endpoint):
            raise error('--minio-endpoint 必须是不含凭证和路径的 HTTP(S) 地址')
        self.execute(['sh', '-c', 'set -eu; for t in mc mkdir rm cat; do command -v "$t" >/dev/null; done; '
                      'test -d /tmp && test -w /tmp'])
        # Credentials/config are created and removed only inside the selected Pod.
        with self.workspace() as work:
            self.buckets = self.bucket_names(work)
            requested = args.minio_buckets
            if requested is not None:
                validate_metadata({'mode': 'current-objects', 'buckets': requested}, error)
                if set(requested) - set(self.buckets) and args.module == 'backup':
                    raise error('指定的 MinIO bucket 不存在')
                self.buckets = requested

    def kube(self, *args, **kwargs):
        return self.run(['kubectl', '-n', self.ns, *args],
                        timeout=kwargs.pop('timeout', self.args.timeout), **kwargs)

    def execute(self, command, **kwargs):
        return self.kube('exec', '-i', self.pod, '-c', self.container, '--', *command, **kwargs)

    @contextlib.contextmanager
    def workspace(self):
        work = '/tmp/openbkn-minio-' + uuid.uuid4().hex
        script = '''set -eu
umask 077
mkdir "$1" "$1/config"
user="${MINIO_ROOT_USER:-${MINIO_ACCESS_KEY:-}}"
password="${MINIO_ROOT_PASSWORD:-${MINIO_SECRET_KEY:-}}"
if [ -z "$user" ] && [ -n "${MINIO_ROOT_USER_FILE:-}" ]; then user=$(cat "$MINIO_ROOT_USER_FILE"); fi
if [ -z "$password" ] && [ -n "${MINIO_ROOT_PASSWORD_FILE:-}" ]; then password=$(cat "$MINIO_ROOT_PASSWORD_FILE"); fi
[ -n "$user" ] && [ -n "$password" ] || { echo 'MinIO Pod 缺少访问凭证环境变量' >&2; exit 1; }
mc --config-dir "$1/config" alias set local "$2" "$user" "$password" >/dev/null
'''
        try:
            self.execute(['sh', '-c', script, 'minio-auth', work, self.args.minio_endpoint])
            yield work
        finally:
            try:
                self.execute(['rm', '-rf', '--', work], timeout=60)
            except Exception:
                self.log('MinIO 临时目录清理失败，请确认任务结束后清理: ' + work)

    def mc(self, work, *args, **kwargs):
        return self.execute(['mc', '--config-dir', work + '/config', '--json', *args], **kwargs)

    def rows(self, work, *args):
        rows = []
        for line in self.mc(work, *args).splitlines():
            row = json.loads(line)
            if row.get('status') != 'success':
                raise self.error('MinIO 命令返回失败，请检查容器凭证、权限和网络')
            rows.append(row)
        return rows

    def bucket_names(self, work):
        names = sorted(row['key'].rstrip('/') for row in self.rows(work, 'ls', 'local'))
        validate_metadata({'mode': 'current-objects', 'buckets': names}, self.error)
        return names

    def listing(self, work, bucket):
        objects = {}
        for row in self.rows(work, 'ls', '--recursive', 'local/' + bucket):
            key = object_key(row['key'], self.error)
            if row.get('type') == 'folder':
                if not key.endswith('/') or row.get('size') != 0:
                    raise self.error('MinIO 目录标记无效')
                # S3 prefixes are virtual directories. A zero-byte marker is not
                # file data and mc pipe treats a trailing-slash target as a folder.
                continue
            if row.get('type') != 'file' or type(row.get('size')) is not int or row['size'] < 0:
                raise self.error('MinIO 对象类型无效')
            if key in objects:
                raise self.error('MinIO 对象清单重复')
            objects[key] = row['size']
        return objects

    def backup(self, path):
        metadata = {'mode': 'current-objects', 'buckets': self.buckets}
        records = []
        with self.workspace() as work, tarfile.open(path, 'w:') as archive:
            for bucket in self.buckets:
                self.log('MinIO 在线导出 bucket: ' + bucket)
                try:
                    listing = self.listing(work, bucket)
                except self.error as exc:
                    raise self.error('MinIO 列举对象失败，bucket=' + bucket + ': ' + str(exc)) from exc
                self.log('MinIO ' + bucket + ': 列举到 %d 个对象，总大小 %d bytes'
                         % (len(listing), sum(listing.values())))
                if not listing:
                    self.log('MinIO ' + bucket + ': 当前无可导出对象，备份中保留空 bucket')
                last_log = time.monotonic()
                for number, (key, size) in enumerate(listing.items(), 1):
                    # One local disk spool avoids buffering objects in memory; the Pod
                    # needs neither tar nor enough space for a second object copy.
                    with tempfile.TemporaryFile(dir=path.parent) as downloaded:
                        try:
                            self.mc(work, 'cat', 'local/' + bucket + '/' + key, target=downloaded)
                        except self.error as exc:
                            received = downloaded.tell()
                            detail = mc_error_detail(downloaded)
                            message = ('MinIO 对象导出失败 %d/%d，bucket=%s，key=%s，预期大小=%d bytes，'
                                       '已接收=%d bytes: %s' % (number, len(listing), bucket, key, size, received, exc))
                            if detail:
                                message += '; mc: ' + detail
                            if 'NoSuchKey' in detail:
                                message += '；对象在列举后已不存在，可能被业务或生命周期规则删除。请停写后重试'
                            raise self.error(message) from exc
                        if downloaded.tell() != size:
                            raise self.error('MinIO 对象大小发生变化（可能有并发修改），请重试: ' + bucket + '/' + key)
                        downloaded.seek(0)
                        name = 'objects/%012d' % len(records)
                        records.append({'file': name, 'bucket': bucket, 'key': key, 'size': size})
                        member = tarfile.TarInfo(name)
                        member.size, member.mode = size, 0o600
                        archive.addfile(member, downloaded)
                    if time.monotonic() - last_log >= 20:
                        self.log('MinIO ' + bucket + ': 已导出 %d/%d 个对象' % (number, len(listing)))
                        last_log = time.monotonic()
                self.log('MinIO ' + bucket + ': 导出完成 %d/%d 个对象' % (len(listing), len(listing)))
            index = json.dumps(records, ensure_ascii=False).encode()
            member = tarfile.TarInfo('objects.json')
            member.size, member.mode = len(index), 0o600
            archive.addfile(member, io.BytesIO(index))
        archive_inventory(path, metadata, self.error)
        return metadata

    def preflight(self, metadata, overwrite):
        validate_metadata(metadata, self.error)
        with self.workspace() as work:
            existing = self.bucket_names(work)
            for bucket in metadata['buckets']:
                if bucket in existing and self.listing(work, bucket) and not overwrite:
                    raise self.error('MinIO 目标 bucket 已有对象；覆盖同名对象需要 --overwrite')

    def restore(self, path, metadata, overwrite):
        records = archive_records(path, metadata, self.error)
        expected = {(r['bucket'], r['key']): r['size'] for r in records
                    if not (r['key'].endswith('/') and r['size'] == 0)}
        by_file = {r['file']: r for r in records}
        self.preflight(metadata, overwrite)
        with self.workspace() as work:
            for bucket in metadata['buckets']:
                self.log('MinIO 恢复 bucket: ' + bucket)
                self.mc(work, 'mb', '--ignore-existing', 'local/' + bucket)
            last_log, completed = time.monotonic(), 0
            with tarfile.open(path, 'r:') as archive:
                for number, member in enumerate(archive, 1):
                    if member.name == 'objects.json':
                        continue
                    record = by_file[member.name]
                    if record['key'].endswith('/') and record['size'] == 0:
                        continue
                    self.log('MinIO 上传对象 %d/%d，大小 %d bytes' % (number, len(records), record['size']))
                    key = record['bucket'] + '/' + record['key']
                    # Extract only to a temporary file on the admin host, never to
                    # a user-controlled filesystem path or the container data dir.
                    with archive.extractfile(member) as incoming, tempfile.TemporaryFile(dir=path.parent) as upload:
                        shutil.copyfileobj(incoming, upload, length=1024 * 1024)
                        upload.seek(0)
                        with tempfile.TemporaryFile() as progress:
                            try:
                                self.mc(work, 'pipe', '--part-size', self.args.minio_part_size,
                                        '--concurrent', '1', 'local/' + key, source=upload, target=progress)
                            except self.error as exc:
                                progress.seek(0)
                                detail = progress.read(2048).decode(errors='replace').strip()
                                suffix = ': ' + detail if detail else ''
                                raise self.error('MinIO mc pipe 上传失败: ' + str(exc) + suffix) from exc
                    completed += 1
                    if time.monotonic() - last_log >= 20:
                        self.log('MinIO 已恢复 %d/%d 个对象' % (completed, len(records)))
                        last_log = time.monotonic()
            for bucket in metadata['buckets']:
                current = self.listing(work, bucket)
                if any(current.get(key) != size for (b, key), size in expected.items() if b == bucket):
                    raise self.error('MinIO 恢复后对象名称或大小校验失败: ' + bucket)
