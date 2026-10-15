#!/usr/bin/env python3
# Copyright openbkn.ai
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.
"""Render an isolated, bounded Audit prerequisite Job (Secret references only)."""


ADMIN_SCRIPT = r'''
set -euo pipefail
umask 077
client=$(mktemp)
details=$(mktemp)
trap 'rm -f "$client" "$details"' EXIT
case "$TASK_KAFKA_USER$TASK_KAFKA_PASSWORD" in
  *$'\r'*|*$'\n'*) echo 'Audit credentials must be single-line.' >&2; exit 1 ;;
esac
user=${TASK_KAFKA_USER//\\/\\\\}
user=${user//\"/\\\"}
password=${TASK_KAFKA_PASSWORD//\\/\\\\}
password=${password//\"/\\\"}
case "$TASK_KAFKA_MECHANISM" in
  PLAIN) module=org.apache.kafka.common.security.plain.PlainLoginModule ;;
  SCRAM-SHA-256|SCRAM-SHA-512) module=org.apache.kafka.common.security.scram.ScramLoginModule ;;
  *) echo 'Unsupported Audit SASL mechanism.' >&2; exit 1 ;;
esac
jaas="$module required username=\"$user\" password=\"$password\";"
# java.util.Properties consumes one escape layer before JAAS reads its strings.
jaas=${jaas//\\/\\\\}
printf 'security.protocol=SASL_PLAINTEXT\nsasl.mechanism=%s\nsasl.jaas.config=%s\nrequest.timeout.ms=10000\ndefault.api.timeout.ms=15000\n' "$TASK_KAFKA_MECHANISM" "$jaas" >"$client"
unset user password jaas TASK_KAFKA_USER TASK_KAFKA_PASSWORD
admin=(--bootstrap-server "$TASK_KAFKA_BROKERS" --command-config "$client")
tools=/opt/bitnami/kafka/bin
if ! "$tools/kafka-topics.sh" "${admin[@]}" --describe --topic openbkn.audit.v1 >"$details" 2>&1; then
  # Fail closed for unrecognized CLI errors, auth failures, and timeouts. Do not
  # translate an arbitrary failed describe into permission to create a topic.
  if ! grep -Eq "Topic '(openbkn[.]audit[.]v1|Optional\\[openbkn[.]audit[.]v1\\])' does not exist as expected" "$details" ||
     grep -Eiq 'AuthenticationException|AuthorizationException|TimeoutException' "$details"; then
    echo 'Audit topic describe failed; check connectivity/authentication/permissions.' >&2
    exit 1
  fi
  if ! "$tools/kafka-topics.sh" "${admin[@]}" --create --if-not-exists --topic openbkn.audit.v1 \
      --partitions "$TASK_TOPIC_PARTITIONS" --replication-factor "$TASK_TOPIC_REPLICAS" \
      --config message.timestamp.type=LogAppendTime >"$details" 2>&1; then
    echo 'Audit topic creation failed; no existing topic was modified.' >&2
    exit 1
  fi
fi
if ! "$tools/kafka-configs.sh" "${admin[@]}" --entity-type topics --entity-name openbkn.audit.v1 \
    --describe --all >"$details" 2>&1; then
  echo 'Audit topic configuration is not readable.' >&2; exit 1
fi
if ! grep -Eq '(^|[,[:space:]])message.timestamp.type=LogAppendTime([,[:space:]]|$)' "$details"; then
  echo 'Audit topic requires LogAppendTime. Existing CreateTime configuration and historical records were not changed; operator-reviewed migration is required.' >&2
  exit 1
fi
echo 'Audit topic LogAppendTime verified.'
'''


def render(values, namespace, image, partitions="1", replicas="1"):
    consumer = values["kafkaConsumers"]["audit"]
    if consumer.get("topic") != "openbkn.audit.v1":
        raise ValueError("Audit topic is fixed to openbkn.audit.v1")
    mechanism = consumer["saslMechanism"]
    if mechanism not in ("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"):
        raise ValueError("unsupported Audit SASL mechanism")
    if not image or int(partitions) < 1 or int(replicas) < 1:
        raise ValueError("Audit Job requires an image and positive partitions/replicas")
    brokers = consumer["brokers"]
    if not isinstance(brokers, list) or not brokers or not all(isinstance(b, str) and b.strip() for b in brokers):
        raise ValueError("Audit Job requires brokers")
    secret = consumer["existingSecret"]
    env = [{"name": k, "value": v} for k, v in {
        "KAFKA_HEAP_OPTS": "-Xms32m -Xmx96m",
        "TASK_KAFKA_BROKERS": ",".join(brokers), "TASK_KAFKA_MECHANISM": mechanism,
        "TASK_TOPIC_PARTITIONS": str(partitions), "TASK_TOPIC_REPLICAS": str(replicas),
    }.items()]
    for name, key in (("TASK_KAFKA_USER", "usernameKey"), ("TASK_KAFKA_PASSWORD", "passwordKey")):
        if not secret.get("name") or not secret.get(key):
            raise ValueError("Audit Job requires named Secret keys")
        env.append({"name": name, "valueFrom": {"secretKeyRef": {"name": secret["name"], "key": secret[key]}}})
    return {"apiVersion": "batch/v1", "kind": "Job",
            "metadata": {"generateName": "bkn-trace-audit-topic-", "namespace": namespace},
            "spec": {"backoffLimit": 0, "activeDeadlineSeconds": 90, "ttlSecondsAfterFinished": 120,
                     "template": {"spec": {"restartPolicy": "Never", "automountServiceAccountToken": False,
                                           "containers": [{"name": "admin", "image": image, "imagePullPolicy": "IfNotPresent",
                                                           "resources": {"requests": {"cpu": "100m", "memory": "128Mi"},
                                                                         "limits": {"cpu": "500m", "memory": "256Mi"}},
                                                           "env": env, "command": ["/bin/bash", "-ec"], "args": [ADMIN_SCRIPT]}]}}}}
