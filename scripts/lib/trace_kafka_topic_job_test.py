#!/usr/bin/env python3
"""Cluster-free regressions for Evidence Kafka topic prerequisites."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from trace_kafka_topic_job import ADMIN_SCRIPT, render


class TopicJobTest(unittest.TestCase):
    def test_manifest_limits_and_secret_refs(self):
        job = render('test', 'kafka:test', 'broker:9092', 'credentials', 'PLAIN', '1', '1')
        pod = job['spec']['template']['spec']
        self.assertFalse(pod['automountServiceAccountToken'])
        container = pod['containers'][0]
        self.assertEqual(container['resources']['limits']['memory'], '256Mi')
        env = {e['name']: e for e in container['env']}
        self.assertEqual(env['KAFKA_HEAP_OPTS']['value'], '-Xms32m -Xmx96m')
        self.assertEqual(env['TASK_KAFKA_PASSWORD']['valueFrom']['secretKeyRef'],
                         {'name': 'credentials', 'key': 'password'})
        self.assertNotIn('value', env['TASK_KAFKA_PASSWORD'])

    def test_invalid_settings(self):
        for mechanism, partitions in [('invalid', '1'), ('PLAIN', '0')]:
            with self.assertRaises(ValueError):
                render('test', 'kafka:test', 'broker', 'credentials', mechanism, partitions, '1')

    def run_admin(self, timestamp, mechanism='PLAIN', creation_fails=False, exists=False):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            topics = directory / 'kafka-topics.sh'
            topics.write_text('#!/bin/bash\nprintf "%s\\n" "$@" >> "$TASK_CALLS"\n'
                              'if [[ " $* " == *" --describe "* ]]; then [[ "$TASK_TOPIC_EXISTS" == true ]]; else [[ "$TASK_CREATION_FAILS" != true ]]; fi\n')
            configs = directory / 'kafka-configs.sh'
            configs.write_text('#!/bin/bash\nprintf "  message.timestamp.type=%s sensitive=false\\n" "$TASK_TIMESTAMP"\n')
            topics.chmod(0o700)
            configs.chmod(0o700)
            client = directory / 'client.properties'
            script = ADMIN_SCRIPT.replace('/opt/bitnami/kafka/bin/', temporary + '/')
            script = script.replace('/tmp/client.properties', str(client))
            script = script.replace('/tmp/topic.properties', str(directory / 'topic.properties'))
            env = dict(os.environ, TASK_KAFKA_USER='test-user', TASK_KAFKA_PASSWORD='a\\b"c',
                       TASK_KAFKA_MECHANISM=mechanism, TASK_KAFKA_BROKERS='broker:9092',
                       TASK_TOPIC_PARTITIONS='1', TASK_TOPIC_REPLICAS='1',
                       TASK_TIMESTAMP=timestamp, TASK_CREATION_FAILS=str(creation_fails).lower(),
                       TASK_TOPIC_EXISTS=str(exists).lower(),
                       TASK_CALLS=str(directory / 'calls'))
            result = subprocess.run(['bash', '-ec', script], env=env, capture_output=True, text=True)
            return result, client.read_text(), (directory / 'calls').read_text()

    def test_existing_append_time_passes(self):
        result, config, calls = self.run_admin('LogAppendTime')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('--if-not-exists', calls)
        self.assertIn('message.timestamp.type=LogAppendTime', calls)
        self.assertIn('password="a\\\\b\\"c"', config)
        self.assertNotIn('a\\b"c', result.stdout + result.stderr)

    def test_existing_create_time_fails_without_altering(self):
        result, _, calls = self.run_admin('CreateTime')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('existing records/configuration were not changed', result.stderr)
        self.assertNotIn('--alter', calls)

    def test_scram_uses_matching_login_module(self):
        result, config, _ = self.run_admin('LogAppendTime', 'SCRAM-SHA-512')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('ScramLoginModule', config)

    def test_creation_failure_stops(self):
        result, _, _ = self.run_admin('LogAppendTime', creation_fails=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn('verified', result.stdout)

    def test_preprovisioned_topic_does_not_require_creation(self):
        result, _, calls = self.run_admin('LogAppendTime', creation_fails=True, exists=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn('--create', calls)


if __name__ == '__main__':
    unittest.main()
