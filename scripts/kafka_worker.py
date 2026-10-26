#!/usr/bin/env python3
"""Host-side portable Kafka archive codec and validation; no Kafka client dependency."""
import base64
import hashlib
import json
import re
import tarfile


class KafkaBackupError(Exception):
    pass


MODE = 'retained-committed-records-v1'
IGNORED_TOPICS = {'connect-configs', 'connect-offsets', 'connect-status'}
CONFIG_KEYS = ('cleanup.policy', 'retention.ms', 'retention.bytes', 'segment.ms',
               'segment.bytes', 'max.message.bytes', 'message.timestamp.type',
               'min.insync.replicas')
LEDGER_GROUPS = {'openbkn.audit.v1': 'bkn-trace-audit-ledger-v1',
                 'openbkn.evidence.v1': 'bkn-trace-evidence-ledger-v1'}
MAX_LINE = 24 * 1024 * 1024


def ledger_restore_topics(metadata):
    return sorted(topic['name'] for topic in metadata['topics']
                  if topic['name'] in LEDGER_GROUPS and any(part['high'] > 0 for part in topic['partitions']))


def restore_blockers(metadata, *, counts_known=True):
    """Preserve ledger coordinates and resume groups at their saved positions."""
    blockers = {}
    for topic in metadata['topics']:
        name = topic['name']
        if name not in LEDGER_GROUPS:
            continue
        expected_group = LEDGER_GROUPS[name]
        positions = {(item['topic'], item['partition']): item['offset']
                     for item in metadata['groups'].get(expected_group, [])}
        for part in topic['partitions']:
            if part['low'] != 0 or counts_known and part['count'] != part['high']:
                blockers[name] = '保留范围不完整或存在 offset 空洞，逻辑导入会改变账本引用的 Kafka 坐标'
                break
            if part['high'] == 0:
                continue
            position = positions.get((name, part['id']))
            if position is None or not 0 <= position <= part['high']:
                blockers[name] = '消费组 %s 缺少有效的分区提交位置，不能确定恢复起点' % expected_group
                break
    return blockers


def topic_name(name):
    if (not isinstance(name, str) or not re.fullmatch(r'[A-Za-z0-9._-]{1,249}', name)
            or name in ('.', '..') or name.startswith('__')):
        raise KafkaBackupError('无效或内部 Kafka Topic: ' + str(name))
    return name


def encode(value):
    return None if value is None else base64.b64encode(value).decode('ascii')


def decode(value):
    if value is None:
        return None
    if not isinstance(value, str):
        raise KafkaBackupError('消息字节编码无效')
    return base64.b64decode(value, validate=True)


def json_bytes(value):
    return (json.dumps(value, ensure_ascii=True, separators=(',', ':')) + '\n').encode()


def record(message):
    kind, stamp = message.timestamp()
    return {'offset': message.offset(), 'timestamp': stamp, 'timestamp_type': kind,
            'key': encode(message.key()), 'value': encode(message.value()),
            'headers': [[key, encode(value)] for key, value in message.headers() or []]}


def content(row):
    """Digest data independently of broker-assigned offset/timestamp."""
    return json_bytes([row['key'], row['value'], row['headers']])


def rows(source):
    while True:
        line = source.readline(MAX_LINE + 1)
        if not line:
            return
        if len(line) > MAX_LINE or not line.endswith(b'\n'):
            raise KafkaBackupError('消息过大或记录被截断（单条上限 24MiB Base64）')
        row = json.loads(line)
        if (not isinstance(row, dict) or type(row.get('offset')) is not int
                or type(row.get('timestamp')) is not int
                or row.get('timestamp_type') not in (0, 1, 2)
                or not isinstance(row.get('headers'), list)):
            raise KafkaBackupError('消息记录无效')
        decode(row.get('key'))
        decode(row.get('value'))
        if 'key' not in row or 'value' not in row:
            raise KafkaBackupError('消息缺少 key/value')
        for header in row['headers']:
            if not isinstance(header, list) or len(header) != 2 or not isinstance(header[0], str):
                raise KafkaBackupError('消息 header 无效')
            decode(header[1])
        yield row


def validate_metadata(meta):
    if (not isinstance(meta, dict) or meta.get('mode') != MODE
            or not isinstance(meta.get('topics'), list)
            or not isinstance(meta.get('groups'), dict)):
        raise KafkaBackupError('Kafka 清单格式无效')
    names, files, bounds = set(), set(), {}
    for topic in meta['topics']:
        name = topic_name(topic['name'])
        if (name in names or not isinstance(topic.get('config'), dict)
                or not isinstance(topic.get('topic_id'), str) or not topic['topic_id']):
            raise KafkaBackupError('Kafka Topic 重复或配置无效')
        names.add(name)
        cfg = topic['config']
        if (set(cfg) != set(CONFIG_KEYS) or any(not isinstance(v, str) for v in cfg.values())
                or (name not in IGNORED_TOPICS and cfg['cleanup.policy'] != 'delete')
                or cfg['message.timestamp.type'] not in ('CreateTime', 'LogAppendTime')):
            raise KafkaBackupError('当前仅支持 cleanup.policy=delete Topic')
        parts = topic['partitions']
        if not isinstance(parts, list) or not parts:
            raise KafkaBackupError('Kafka 分区清单为空')
        for index, part in enumerate(parts):
            low, high = part['low'], part['high']
            filename = '%s-%d.ndjson' % (name, index)
            if (part['id'] != index or type(low) is not int or type(high) is not int
                    or low < 0 or high < low or part['file'] != filename
                    or type(part['count']) is not int or part['count'] < 0
                    or not re.fullmatch(r'[a-f0-9]{64}', part['sha256'])
                    or not re.fullmatch(r'[a-f0-9]{64}', part['content_sha256'])):
                raise KafkaBackupError('Kafka 分区边界或校验清单无效')
            if filename in files:
                raise KafkaBackupError('Kafka 分区文件重复')
            files.add(filename)
            bounds[(name, index)] = (low, high)
    for group, offsets in meta['groups'].items():
        if not isinstance(group, str) or not group or not isinstance(offsets, list) or not offsets:
            raise KafkaBackupError('消费组清单无效')
        seen = set()
        for item in offsets:
            key = (item['topic'], item['partition'])
            offset = item['offset']
            if (key not in bounds or key in seen or type(offset) is not int
                    or not bounds[key][0] <= offset <= bounds[key][1]):
                raise KafkaBackupError('消费位置不在备份范围内；不能安全恢复消费进度')
            seen.add(key)
    return files


def inventory(path, expected=None):
    """Validate all members/records before contacting a target or extracting anything."""
    with tarfile.open(path, 'r:') as archive:
        members = archive.getmembers()
        names = [m.name for m in members]
        if (len(names) != len(set(names)) or any(not m.isfile() for m in members)
                or 'metadata.json' not in names
                or archive.getmember('metadata.json').size > 8 * 1024 * 1024):
            raise KafkaBackupError('Kafka 包文件列表无效')
        meta = json.load(archive.extractfile('metadata.json'))
        files = validate_metadata(meta)
        if set(names) != files | {'metadata.json'} or expected is not None and meta != expected:
            raise KafkaBackupError('Kafka 包清单或文件列表不匹配')
        for topic in meta['topics']:
            for part in topic['partitions']:
                sha, data_sha, count, previous = hashlib.sha256(), hashlib.sha256(), 0, -1
                with archive.extractfile(part['file']) as source:
                    # Separate raw-byte validation from semantic validation; both stream.
                    for block in iter(lambda: source.read(1024 * 1024), b''):
                        sha.update(block)
                with archive.extractfile(part['file']) as source:
                    for row in rows(source):
                        if not part['low'] <= row['offset'] < part['high'] or row['offset'] <= previous:
                            raise KafkaBackupError('Kafka 消息偏移量乱序或越界')
                        previous = row['offset']
                        data_sha.update(content(row))
                        count += 1
                if (count != part['count'] or sha.hexdigest() != part['sha256']
                        or data_sha.hexdigest() != part['content_sha256']):
                    raise KafkaBackupError('Kafka 消息数量或校验和不符')
    return meta
