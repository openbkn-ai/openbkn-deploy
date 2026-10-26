#!/usr/bin/env python3
"""Pause OpenBKN business Deployments while leaving data services available."""
import argparse
import json
import subprocess
import sys
import time


# These Deployments provide persistent data needed during backup and restore.
DATA_DEPLOYMENTS = {'minio', 'bkn-safe-postgres'}
DATA_SUFFIXES = ('-postgres', '-postgresql', '-mariadb', '-mysql', '-redis', '-kafka', '-opensearch', '-minio')


class WorkloadError(Exception):
    pass


def kubectl(namespace, *args):
    command = ['kubectl', '-n', namespace, *args]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode:
        raise WorkloadError(' '.join(command) + ': ' + result.stderr.decode(errors='replace').strip())
    return result.stdout


def get_json(namespace, *args):
    return json.loads(kubectl(namespace, 'get', *args, '-o', 'json'))


def is_data_deployment(deployment):
    name = deployment['metadata']['name']
    if name in DATA_DEPLOYMENTS or name.endswith(DATA_SUFFIXES) or name.startswith(('minio-', 'postgres-')):
        return True
    images = [container.get('image', '').split('/')[-1].split(':')[0].split('@')[0]
              for container in deployment.get('spec', {}).get('template', {}).get('spec', {}).get('containers', [])]
    return any(image in {'minio', 'postgres', 'postgresql', 'mariadb', 'mysql', 'redis', 'kafka', 'opensearch'}
               for image in images)


def owned_replicasets(replicasets, deployment_uids):
    return {item['metadata']['uid'] for item in replicasets
            if any(owner.get('kind') == 'Deployment' and owner.get('uid') in deployment_uids
                   for owner in item['metadata'].get('ownerReferences', []))}


def wait_business_pods_deleted(namespace, deployment_uids, replica_uids, timeout=300):
    deadline = time.monotonic() + timeout
    next_log = time.monotonic() + 20
    while True:
        replica_uids.update(owned_replicasets(get_json(namespace, 'replicasets')['items'], deployment_uids))
        pods = get_json(namespace, 'pods')['items']
        remaining = [pod['metadata']['name'] for pod in pods
                     if any((owner.get('kind') == 'ReplicaSet' and owner.get('uid') in replica_uids)
                            or (owner.get('kind') == 'Deployment' and owner.get('uid') in deployment_uids)
                            for owner in pod['metadata'].get('ownerReferences', []))]
        if not remaining:
            return
        now = time.monotonic()
        if now >= deadline:
            raise WorkloadError('等待业务 Pod 删除超时: ' + ', '.join(remaining))
        if now >= next_log:
            print('等待业务 Pod 删除: ' + ', '.join(remaining), file=sys.stderr, flush=True)
            next_log = now + 20
        time.sleep(2)


def scale_business(namespace, replicas):
    deployments = get_json(namespace, 'deployments')['items']
    chosen = [item for item in deployments if not is_data_deployment(item)]
    if not chosen:
        raise WorkloadError('未发现业务 Deployment')
    deployment_uids = {item['metadata']['uid'] for item in chosen} if replicas == 0 else set()
    replica_uids = (owned_replicasets(get_json(namespace, 'replicasets')['items'], deployment_uids)
                    if replicas == 0 else set())
    names = [item['metadata']['name'] for item in chosen]
    for name in names:
        kubectl(namespace, 'scale', 'deployment/' + name, '--replicas=' + str(replicas))
    if replicas == 0:
        wait_business_pods_deleted(namespace, deployment_uids, replica_uids)
    print(json.dumps({'result': 'stopped' if replicas == 0 else 'started',
                      'namespace': namespace, 'deployments': sorted(names),
                      'replicas': replicas, 'data_services': 'unchanged'}, ensure_ascii=False))


def stop(namespace):
    scale_business(namespace, 0)


def start(namespace):
    scale_business(namespace, 1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('stop', 'start'))
    parser.add_argument('--namespace', default='openbkn')
    args = parser.parse_args()
    try:
        (stop if args.action == 'stop' else start)(args.namespace)
    except (WorkloadError, ValueError, KeyError) as error:
        print('openbkn: ' + str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
