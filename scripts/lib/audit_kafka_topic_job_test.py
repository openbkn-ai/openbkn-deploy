#!/usr/bin/env python3
"""Cluster-free Audit Job regressions, including negative command traces."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

from audit_consumer import defaults
from audit_kafka_topic_job import ADMIN_SCRIPT, render


class AuditTopicJobTests(unittest.TestCase):
    def test_manifest_is_bounded_and_has_only_secret_references(self):
        values = defaults("broker:9092", "PLAIN", "existing")
        values["kafkaConsumers"]["audit"]["brokers"].append("other:9092")
        values["kafkaConsumers"]["audit"]["existingSecret"].update(usernameKey="custom-user", passwordKey="custom-pass")
        job = render(values, "test", "kafka:test")
        pod = job["spec"]["template"]["spec"]
        self.assertEqual(job["metadata"]["generateName"], "bkn-trace-audit-topic-")
        self.assertEqual(job["spec"]["backoffLimit"], 0)
        self.assertEqual(job["spec"]["activeDeadlineSeconds"], 90)
        self.assertEqual(job["spec"]["ttlSecondsAfterFinished"], 120)
        self.assertFalse(pod["automountServiceAccountToken"])
        container = pod["containers"][0]
        self.assertEqual(container["resources"]["limits"]["memory"], "256Mi")
        env = {item["name"]: item for item in container["env"]}
        self.assertEqual(env["TASK_KAFKA_BROKERS"]["value"], "broker:9092,other:9092")
        self.assertEqual(env["TASK_KAFKA_PASSWORD"]["valueFrom"]["secretKeyRef"],
                         {"name": "existing", "key": "custom-pass"})
        self.assertNotIn("value", env["TASK_KAFKA_PASSWORD"])
        self.assertNotIn("openbkn.evidence.v1", json.dumps(job))

    def test_invalid_inputs_rejected(self):
        for field, value in (("topic", "other"), ("saslMechanism", "invalid"), ("brokers", [])):
            values = defaults("broker", "PLAIN", "existing")
            values["kafkaConsumers"]["audit"][field] = value
            with self.assertRaises(ValueError):
                render(values, "test", "kafka:test")
        with self.assertRaises(ValueError):
            render(defaults("broker", "PLAIN", "existing"), "test", "kafka:test", "0", "1")

    def run_admin(self, state="exists", timestamp="LogAppendTime", mechanism="PLAIN", create_fails=False):
        bash = shutil.which("bash")
        if not bash:
            self.fail("Bash is required for the Audit Job script regressions")
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            topics = directory / "kafka-topics.sh"
            topics.write_text('''#!/bin/bash
printf '%s\\n' "$*" >>"$TASK_CALLS"
while [[ $# -gt 0 ]]; do
  if [[ "$1" == --command-config ]]; then cp "$2" "$TASK_OBSERVED"; break; fi
  shift
done
if [[ " $* " == *" --describe "* ]]; then
  case "$TASK_STATE" in
    missing) echo "Error: java.lang.IllegalArgumentException: Topic 'openbkn.audit.v1' does not exist as expected"; exit 1 ;;
    auth) echo 'SaslAuthenticationException: private auth failure'; exit 1 ;;
    denied) echo 'TopicAuthorizationException'; exit 1 ;;
    timeout) echo 'TimeoutException'; exit 1 ;;
    unknown) echo 'unrecognized tool failure'; exit 1 ;;
  esac
else
  [[ "$TASK_CREATE_FAILS" != true ]] || exit 1
fi
''', encoding="utf-8")
            configs = directory / "kafka-configs.sh"
            configs.write_text('''#!/bin/bash
printf '%s\\n' "$*" >>"$TASK_CALLS"
echo "message.timestamp.type=$TASK_TIMESTAMP sensitive=false synonyms={DEFAULT_CONFIG:log.message.timestamp.type=$TASK_TIMESTAMP}"
''', encoding="utf-8")
            topics.chmod(0o700)
            configs.chmod(0o700)
            script = ADMIN_SCRIPT.replace("tools=/opt/bitnami/kafka/bin", "tools='" + directory.as_posix() + "'")
            env = dict(os.environ, TASK_KAFKA_USER="test-user", TASK_KAFKA_PASSWORD='a\\b"c',
                       TASK_KAFKA_MECHANISM=mechanism, TASK_KAFKA_BROKERS="broker:9092",
                       TASK_TOPIC_PARTITIONS="1", TASK_TOPIC_REPLICAS="1", TASK_STATE=state,
                       TASK_TIMESTAMP=timestamp, TASK_CREATE_FAILS=str(create_fails).lower(),
                       TASK_CALLS=(directory / "calls").as_posix(), TASK_OBSERVED=(directory / "observed").as_posix())
            # stdin avoids native Windows command-line quote/escape rewriting.
            result = subprocess.run([bash, "-s"], input=script, env=env, capture_output=True, text=True)
            calls = (directory / "calls").read_text() if (directory / "calls").exists() else ""
            properties = (directory / "observed").read_text() if (directory / "observed").exists() else ""
            return result, calls, properties

    def test_existing_topic_never_created_or_altered(self):
        result, calls, _ = self.run_admin()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("--create", calls)
        self.assertNotIn("--alter", calls)
        self.assertNotIn("--delete", calls)

    def test_missing_topic_created_with_append_time(self):
        result, calls, _ = self.run_admin(state="missing")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--create --if-not-exists --topic openbkn.audit.v1", calls)
        self.assertIn("message.timestamp.type=LogAppendTime", calls)

    def test_auth_permission_timeout_and_unknown_errors_do_not_create(self):
        for state in ("auth", "denied", "timeout", "unknown"):
            result, calls, _ = self.run_admin(state=state)
            with self.subTest(state=state):
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn("--create", calls)
                self.assertNotIn("private auth failure", result.stdout + result.stderr)

    def test_create_time_fails_without_mutating_existing_topic(self):
        result, calls, _ = self.run_admin(timestamp="CreateTime")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("historical records were not changed", result.stderr)
        self.assertNotIn("--create", calls)
        self.assertNotIn("--alter", calls)

    def test_creation_failure_and_escaping(self):
        result, calls, _ = self.run_admin(state="missing", create_fails=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("--alter", calls)
        result, _, properties = self.run_admin(mechanism="SCRAM-SHA-512")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("ScramLoginModule", properties)
        self.assertIn('password="a\\\\\\\\b\\\\"c"', properties)
        self.assertNotIn('a\\b"c', result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
