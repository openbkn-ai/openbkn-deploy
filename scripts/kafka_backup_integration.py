#!/usr/bin/env python3
"""Start two isolated localhost Kafka processes using an existing distribution/JRE."""
import argparse
import base64
import contextlib
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import uuid


def free_port():
    with socket.socket() as server:
        server.bind(('127.0.0.1', 0))
        return server.getsockname()[1]


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def memory_credentials(work, password, stack):
    """Exercise Java's Secret-file reader without persisting a password to disk.

    Integration runs on Linux (including the Ubuntu CI runner). Java inherits
    only this descriptor; opening the symlink gives each client its own reader.
    """
    if not hasattr(os, 'memfd_create'):
        raise RuntimeError('SASL integration requires Linux memfd_create and /proc/self/fd')
    descriptor = os.memfd_create('openbkn-kafka-test', os.MFD_CLOEXEC)
    stack.callback(os.close, descriptor)
    os.fchmod(descriptor, 0o600)
    payload = password.encode()
    if os.write(descriptor, payload) != len(payload):
        raise OSError('Incomplete in-memory test credential write')
    directory = work / 'credentials'
    directory.mkdir(mode=0o700)
    (directory / 'password').symlink_to('/proc/self/fd/%d' % descriptor)
    return directory, descriptor


def sasl_jaas(password):
    escaped = password.replace('\\', '\\\\').replace('"', '\\"')
    return ('org.apache.kafka.common.security.plain.PlainLoginModule required '
            'username="kafkauser" password="%s" user_kafkauser="%s";' % (escaped, escaped))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--java', default='java')
    parser.add_argument('--kafka-home', required=True, help='Kafka 3.9 distribution containing libs/')
    parser.add_argument('--sasl', action='store_true', help='Test SASL/PLAIN and Secret file reading instead of PLAINTEXT')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    libs = str(Path(args.kafka_home).resolve() / 'libs/*')
    with tempfile.TemporaryDirectory(prefix='openbkn-kafka-integration-') as tmp, contextlib.ExitStack() as stack:
        work = Path(tmp)
        ports = set()
        while len(ports) < 4:
            ports.add(free_port())
        source, source_controller, target, target_controller = sorted(ports)
        password = 'test"password\\with-escape'
        environment = dict(os.environ, TASK_KAFKA_PROTOCOL='SASL_PLAINTEXT' if args.sasl else 'PLAINTEXT',
                           TASK_KAFKA_USER='kafkauser', TASK_KAFKA_MECHANISM='PLAIN', TASK_KAFKA_REPLICAS='1',
                           TASK_KAFKA_LEDGER_RESTORED='true', KAFKA_BACKUP_DISPOSABLE_TEST='yes')
        inherited = ()
        credential_option = []
        if args.sasl:
            directory, descriptor = memory_credentials(work, password, stack)
            inherited = (descriptor,)
            credential_option = ['-Dopenbkn.kafka.credentials=' + str(directory)]
            environment['KAFKA_TEST_JAAS'] = sasl_jaas(password)
        for node, port, controller in [(1, source, source_controller), (2, target, target_controller)]:
            props = work / ('broker-%d.properties' % node)
            values = {
                'process.roles': 'broker,controller', 'node.id': node,
                'controller.quorum.voters': '%d@127.0.0.1:%d' % (node, controller),
                'listeners': 'CLIENT://127.0.0.1:%d,CONTROLLER://127.0.0.1:%d' % (port, controller),
                'advertised.listeners': 'CLIENT://127.0.0.1:%d' % port,
                'listener.security.protocol.map': 'CONTROLLER:PLAINTEXT,CLIENT:' + ('SASL_PLAINTEXT' if args.sasl else 'PLAINTEXT'),
                'controller.listener.names': 'CONTROLLER', 'inter.broker.listener.name': 'CLIENT',
                'log.dirs': str(work / ('data-%d' % node)), 'num.partitions': 1,
                'offsets.topic.replication.factor': 1, 'transaction.state.log.replication.factor': 1,
                'transaction.state.log.min.isr': 1, 'group.initial.rebalance.delay.ms': 0,
                'num.network.threads': 2, 'num.io.threads': 2,
            }
            if args.sasl:
                values.update({'sasl.enabled.mechanisms': 'PLAIN', 'sasl.mechanism.inter.broker.protocol': 'PLAIN',
                               'config.providers': 'env',
                               'config.providers.env.class': 'org.apache.kafka.common.config.provider.EnvVarConfigProvider',
                               'config.providers.env.param.allowlist.pattern': 'KAFKA_TEST_JAAS',
                               'listener.name.client.plain.sasl.jaas.config': '${env:KAFKA_TEST_JAAS}'})
            props.write_text('\n'.join('%s=%s' % item for item in values.items()) + '\n')
            cluster = base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip('=')
            subprocess.run([args.java, '-Xmx128m', '-cp', libs, 'kafka.tools.StorageTool', 'format',
                            '-t', cluster, '-c', str(props)], check=True, capture_output=True, env=environment)
            log = stack.enter_context(open(work / ('broker-%d.log' % node), 'w+'))
            process = subprocess.Popen([args.java, '-Xms64m', '-Xmx256m', '-cp', libs, 'kafka.Kafka', str(props)],
                                       stdout=log, stderr=log, env=environment)
            stack.callback(stop, process)
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline and process.poll() is None:
                try:
                    with socket.create_connection(('127.0.0.1', port), timeout=0.5):
                        break
                except OSError:
                    time.sleep(0.2)
            else:
                log.flush()
                raise RuntimeError('Disposable broker failed: ' + Path(log.name).read_text()[-8000:])
        jars = root / 'tools/kafka-backup'
        command = [args.java, '-Xmx256m', '-Djava.io.tmpdir=' + str(work), *credential_option,
                   '-cp', '%s:%s:%s' % (jars / 'kafka-backup.jar', jars / 'kafka-backup-tests.jar', libs),
                   'KafkaBackupSmoke', '127.0.0.1:%d' % source, '127.0.0.1:%d' % target, str(work / 'smoke')]
        subprocess.run(command, env=environment, pass_fds=inherited, check=True, timeout=240)
        # Independently validate Java exports using the portable host archive reader.
        import tarfile
        from kafka_worker import inventory
        for name in ('payload', 'ledger-payload'):
            payload = work / 'smoke' / name
            package = work / (name + '.tar')
            with tarfile.open(package, 'w:') as archive:
                for file in payload.iterdir():
                    archive.add(file, arcname=file.name)
            inventory(package)
        print('PASS: Java export -> Python archive validation; test brokers stopped and temporary data removed')


if __name__ == '__main__':
    main()
