#!/usr/bin/env python3
"""Render a bounded Kafka Admin Job; no credential values enter the manifest."""
import json
import sys


ADMIN_SCRIPT = r'''
set -euo pipefail
umask 077
user=${TASK_KAFKA_USER//\\/\\\\}
user=${user//\"/\\\"}
password=${TASK_KAFKA_PASSWORD//\\/\\\\}
password=${password//\"/\\\"}
case "$TASK_KAFKA_MECHANISM" in
  PLAIN) module=org.apache.kafka.common.security.plain.PlainLoginModule ;;
  SCRAM-SHA-256|SCRAM-SHA-512) module=org.apache.kafka.common.security.scram.ScramLoginModule ;;
  *) exit 1 ;;
esac
printf 'security.protocol=SASL_PLAINTEXT\nsasl.mechanism=%s\nsasl.jaas.config=%s required username="%s" password="%s";\n' "$TASK_KAFKA_MECHANISM" "$module" "$user" "$password" > /tmp/client.properties
admin=(--bootstrap-server "$TASK_KAFKA_BROKERS" --command-config /tmp/client.properties)
# Preprovisioned external topics need describe permission, not create permission.
# --if-not-exists also protects a concurrent creation without changing configs.
if ! /opt/bitnami/kafka/bin/kafka-topics.sh "${admin[@]}" --describe --topic openbkn.evidence.v1 >/dev/null 2>&1; then
  /opt/bitnami/kafka/bin/kafka-topics.sh "${admin[@]}" --create --if-not-exists --topic openbkn.evidence.v1 --partitions "$TASK_TOPIC_PARTITIONS" --replication-factor "$TASK_TOPIC_REPLICAS" --config message.timestamp.type=LogAppendTime
fi
/opt/bitnami/kafka/bin/kafka-configs.sh "${admin[@]}" --entity-type topics --entity-name openbkn.evidence.v1 --describe --all > /tmp/topic.properties
if ! grep -Eq '(^|[[:space:]])message.timestamp.type=LogAppendTime([[:space:]]|$)' /tmp/topic.properties; then
  echo 'Evidence topic requires LogAppendTime; existing records/configuration were not changed.' >&2
  exit 1
fi
echo 'Evidence topic LogAppendTime verified.'
'''


def render(namespace, image, brokers, secret, mechanism, partitions, replicas):
    if mechanism not in ('PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512'):
        raise ValueError('unsupported Kafka SASL mechanism')
    if int(partitions) < 1 or int(replicas) < 1:
        raise ValueError('topic partitions and replicas must be positive')
    env = [dict(name=k, value=v) for k, v in {
        'KAFKA_HEAP_OPTS': '-Xms32m -Xmx96m',
        'TASK_KAFKA_BROKERS': brokers,
        'TASK_KAFKA_MECHANISM': mechanism,
        'TASK_TOPIC_PARTITIONS': str(partitions),
        'TASK_TOPIC_REPLICAS': str(replicas),
    }.items()]
    env += [dict(name=name, valueFrom=dict(secretKeyRef=dict(name=secret, key=key)))
            for name, key in [('TASK_KAFKA_USER', 'username'), ('TASK_KAFKA_PASSWORD', 'password')]]
    return dict(apiVersion='batch/v1', kind='Job',
                metadata=dict(generateName='bkn-trace-evidence-topic-', namespace=namespace),
                spec=dict(backoffLimit=0, activeDeadlineSeconds=90,
                          template=dict(spec=dict(restartPolicy='Never',
                              automountServiceAccountToken=False,
                              containers=[dict(name='admin', image=image, imagePullPolicy='IfNotPresent',
                                  resources=dict(requests=dict(cpu='100m', memory='128Mi'),
                                                 limits=dict(cpu='500m', memory='256Mi')),
                                  env=env, command=['/bin/bash', '-ec'], args=[ADMIN_SCRIPT])]))))


if __name__ == '__main__':
    json.dump(render(*sys.argv[1:]), sys.stdout)
