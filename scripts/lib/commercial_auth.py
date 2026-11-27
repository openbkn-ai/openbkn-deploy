# Copyright openbkn.ai
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.
"""Prepare temporary registry authentication and provision its Kubernetes Secret."""
import argparse
import base64
import json
import os
from pathlib import Path
import subprocess
import sys

SECRET = 'openbkn-commercial-pull'


def credentials():
    user, password = os.environ.get('SWR_USER', ''), os.environ.get('SWR_PASSWORD', '')
    if bool(user) != bool(password):
        raise ValueError('Set both SWR_USER and SWR_PASSWORD, or neither')
    return user, password


def create_authfile(directory, registry):
    user, password = credentials()
    if not user:
        return None
    host = registry.split('/', 1)[0]
    if not host or '://' in registry:
        raise ValueError('Invalid image registry')
    encoded = base64.b64encode((user + ':' + password).encode()).decode()
    path = Path(directory) / 'registry-auth.json'
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as stream:
        json.dump({'auths': {host: {'auth': encoded}}}, stream)
    return path


def provision(namespace, auth_file):
    config = json.loads(Path(auth_file).read_text())
    payload = {'apiVersion': 'v1', 'kind': 'Secret', 'type': 'kubernetes.io/dockerconfigjson',
               'metadata': {'name': SECRET, 'namespace': namespace},
               'data': {'.dockerconfigjson': base64.b64encode(json.dumps(config).encode()).decode()}}
    result = subprocess.run(['kubectl', 'get', 'secret', SECRET, '-n', namespace,
                             '--ignore-not-found', '-o', 'json'], capture_output=True, text=True)
    if result.returncode:
        raise ValueError('Cannot check commercial image pull Secret')
    if result.stdout.strip():
        try:
            existing = json.loads(result.stdout)
            data = json.loads(base64.b64decode(existing['data']['.dockerconfigjson'], validate=True))
            if existing.get('type') != payload['type'] or data != config:
                raise ValueError('Existing openbkn-commercial-pull Secret differs; update or remove it explicitly')
        except (KeyError, TypeError, json.JSONDecodeError):
            raise ValueError('Existing openbkn-commercial-pull Secret is invalid; repair it explicitly') from None
        return
    result = subprocess.run(['kubectl', 'create', '-f', '-'], input=json.dumps(payload),
                            capture_output=True, text=True)
    if result.returncode:
        raise ValueError('Cannot create commercial image pull Secret')


def ensure_default_service_account(namespace):
    args = ['kubectl', 'get', 'serviceaccount', 'default', '-n', namespace, '-o', 'json']
    result = subprocess.run(args, capture_output=True, text=True)
    if result.returncode:
        raise ValueError('Cannot read the default ServiceAccount for commercial Helm hooks')
    try:
        account = json.loads(result.stdout)
        entries = account.get('imagePullSecrets') or []
        if not isinstance(entries, list):
            raise ValueError('Default ServiceAccount imagePullSecrets is invalid')
        if any(isinstance(item, dict) and item.get('name') == SECRET for item in entries):
            return
        entries.append({'name': SECRET})
        version = account.get('metadata', {}).get('resourceVersion')
        if not version:
            raise ValueError('Default ServiceAccount resource version is missing')
        patch = json.dumps([{'op': 'test', 'path': '/metadata/resourceVersion', 'value': version},
                            {'op': 'add', 'path': '/imagePullSecrets', 'value': entries}])
    except (TypeError, json.JSONDecodeError):
        raise ValueError('Cannot parse default ServiceAccount') from None
    result = subprocess.run(['kubectl', 'patch', 'serviceaccount', 'default', '-n', namespace,
                             '--type=json', '-p', patch], capture_output=True, text=True)
    if result.returncode:
        raise ValueError('Cannot update default ServiceAccount for commercial Helm hooks')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--namespace', required=True)
    parser.add_argument('--auth-file', required=True)
    args = parser.parse_args()
    try:
        provision(args.namespace, args.auth_file)
        ensure_default_service_account(args.namespace)
    except (ValueError, OSError) as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
