#!/usr/bin/env python3
"""Opt-in cluster integration test. Creates/deletes an isolated StatefulSet/PVC.

Requires an approved test cluster and Redis 7 image; never targets the app Redis.
"""
import argparse
import json
from pathlib import Path
import tempfile
import uuid
from unittest import mock

import backup
from redis_backup import RedisBackup


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true', help='Create, write and delete test cluster resources')
    parser.add_argument('--namespace', default='resource')
    parser.add_argument('--storage-class', default='local-path')
    parser.add_argument('--image', required=True, help='The target Redis 7 image (redis-server/redis-cli required)')
    options = parser.parse_args()
    if not options.execute:
        parser.error('This integration test requires --execute and approval for cluster mutations')
    name = 'openbkn-redis-e2e-' + uuid.uuid4().hex[:10]
    ns = options.namespace
    image = options.image
    def kube(*args, **kw):
        return backup.run(['kubectl', '-n', ns, *args], **kw)
    fixture = {'apiVersion': 'apps/v1', 'kind': 'StatefulSet', 'metadata': {'name': name, 'namespace': ns},
     'spec': {'serviceName': name, 'replicas': 1, 'selector': {'matchLabels': {'app': name}},
              'template': {'metadata': {'labels': {'app': name}}, 'spec': {
                 'securityContext': {'runAsUser': 999, 'runAsGroup': 999, 'fsGroup': 999},
                 'containers': [{'name': 'redis', 'image': image,
                    'command': ['redis-server'], 'args': ['--dir', '/data', '--appendonly', 'yes',
                      '--rename-command', 'CONFIG', 'CONFIG_NEW', '--user',
                      'root', 'on', '>redis-integration-test', '~*', '&*', '+@all'],
                    'env': [{'name': 'REDIS_PASSWORD', 'value': 'redis-integration-test'}],
                    'volumeMounts': [{'name': 'data', 'mountPath': '/data'}],
                    'readinessProbe': {'exec': {'command': ['sh', '-c',
                        'REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli --user root PING']},
                        'periodSeconds': 2}}]}},
              'volumeClaimTemplates': [{'metadata': {'name': 'data'}, 'spec': {
                 'accessModes': ['ReadWriteOnce'], 'storageClassName': options.storage_class,
                 'resources': {'requests': {'storage': '256Mi'}}}}]}}
    print('isolated fixture: ' + ns + '/' + name, flush=True)
    created = True  # Attempt cleanup even if the API created it but the response was lost.
    try:
        kube('create', '-f', '-', data=json.dumps(fixture).encode())
        kube('rollout', 'status', 'statefulset/' + name, '--timeout=90s', timeout=100)
        args = backup.parser().parse_args(['backup', 'run', '--redis-namespace', ns, '--redis-selector', 'app=' + name,
                                          '--timeout', '90', '--poll-interval', '1'])
        redis = RedisBackup(args, backup.run, backup.BackupError, backup.log)
        redis.cli('-n', '1', 'HSET', 'quota', 'value', 'snapshot-value')
        redis.cli('-n', '3', 'RPUSH', 'tasks', 'one', 'two')
        redis.cli('-n', '3', 'EXPIRE', 'tasks', '3600')
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'redis.rdb'
            metadata = redis.backup(path)
            redis.cli('-n', '1', 'HSET', 'quota', 'value', 'changed-after-backup')
            redis.cli('-n', '0', 'SET', 'extra', 'not-in-backup')
            redis.restore(path, metadata, True)
            assert redis.cli('-n', '1', 'HGET', 'quota', 'value').strip() == b'snapshot-value'
            assert redis.cli('-n', '3', 'LRANGE', 'tasks', '0', '-1').strip() == b'one\ntwo'
            assert redis.cli('-n', '0', 'EXISTS', 'extra').strip() == b'0'
            assert int(redis.cli('-n', '3', 'TTL', 'tasks')) > 0
            print('PASS: backup + RDB->AOF restore + two formal starts + multi DB/types/TTL', flush=True)
            redis.cli('-n', '1', 'HSET', 'quota', 'value', 'before-failed-restore')
            original = redis.execute
            def fail_conversion(command, **kw):
                if command[0] == 'redis-cli' and command[-4:] == ['CONFIG', 'SET', 'appendonly', 'yes']:
                    raise backup.BackupError('injected conversion failure')
                return original(command, **kw)
            try:
                with mock.patch.object(redis, 'execute', side_effect=fail_conversion):
                    redis.restore(path, metadata, True)
            except backup.BackupError as error:
                assert 'injected conversion failure' in str(error), error
            else:
                raise AssertionError('expected restore failure')
            assert redis.cli('-n', '1', 'HGET', 'quota', 'value').strip() == b'before-failed-restore'
            print('PASS: injected AOF conversion failure rolled back original data and restarted formal Redis', flush=True)
    finally:
        if created:
            kube('delete', 'statefulset', name, '--ignore-not-found', '--wait=true', timeout=90)
            # The StatefulSet deletion waits for its Pod; handle a lost create response too.
            kube('delete', 'pod', name + '-0', '--ignore-not-found', '--wait=true', timeout=70)
            kube('delete', 'pods', '-l', 'openbkn.ai/restore-statefulset=' + name,
                 '--ignore-not-found', '--wait=true', timeout=90)
            kube('delete', 'pvc', 'data-' + name + '-0', '--ignore-not-found', '--wait=true', timeout=90)
            print('isolated fixture removed: ' + name, flush=True)


if __name__ == '__main__':
    main()
