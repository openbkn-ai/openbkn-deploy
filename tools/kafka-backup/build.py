#!/usr/bin/env python3
"""Maintainer-only build. Operators use the bundled JAR without a JDK or downloads."""
import argparse
import glob
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import zipfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--classpath', required=True, help='Kafka 3.9 libs directory glob, e.g. /opt/kafka/libs/*')
    parser.add_argument('--javac', default='javac')
    parser.add_argument('--ecj', help='Optional Eclipse compiler JAR for maintainer environments with only a JRE')
    parser.add_argument('--java', default='java')
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    source = root / 'src/OpenBknKafkaBackup.java'
    test_source = root / 'tests/KafkaBackupSmoke.java'
    classpath = ':'.join(sorted(glob.glob(args.classpath)))
    if not classpath:
        parser.error('classpath 未找到 Kafka/Jackson JAR')
    with tempfile.TemporaryDirectory(prefix='openbkn-kafka-build-') as tmp:
        classes = Path(tmp)
        command = [args.java, '-jar', args.ecj, '-17'] if args.ecj else [args.javac, '--release', '17']
        subprocess.run([*command, '-encoding', 'UTF-8', '-cp', classpath, '-d', str(classes), str(source), str(test_source)], check=True)
        jar = root / 'kafka-backup.jar'
        for name, prefix in [('kafka-backup.jar', 'OpenBknKafkaBackup'), ('kafka-backup-tests.jar', 'KafkaBackupSmoke')]:
            with zipfile.ZipFile(root / name, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
                for item in sorted(classes.rglob(prefix + '*.class')):
                    entry = zipfile.ZipInfo(str(item.relative_to(classes)), date_time=(2020, 1, 1, 0, 0, 0))
                    entry.compress_type = zipfile.ZIP_DEFLATED
                    archive.writestr(entry, item.read_bytes())
    metadata = {'java_release': 17, 'kafka_client': '3.9',
                'source_sha256': hashlib.sha256(source.read_bytes()).hexdigest(),
                'jar_sha256': hashlib.sha256(jar.read_bytes()).hexdigest(),
                'tests_source_sha256': hashlib.sha256(test_source.read_bytes()).hexdigest(),
                'tests_jar_sha256': hashlib.sha256((root / 'kafka-backup-tests.jar').read_bytes()).hexdigest()}
    (root / 'artifact.json').write_text(json.dumps(metadata, indent=2) + '\n')
    print('Built %s (%d bytes)' % (jar, jar.stat().st_size))


if __name__ == '__main__':
    main()
