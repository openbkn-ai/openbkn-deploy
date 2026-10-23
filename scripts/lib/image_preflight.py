"""Pre-pull a Recreate release's images before Helm can stop its old replicas.

Dry-run output may contain Secrets: it stays in memory and is never logged.
Probe pods execute no application code and mount no workload data or credentials.
"""
import argparse
import copy
import json
import re
import subprocess
import sys
import time
import uuid

import yaml


class PreflightError(RuntimeError):
    pass


def probe_pull_policy(container):
    policy = container.get('imagePullPolicy')
    if policy == 'Never':
        return 'Never'
    if policy == 'Always':
        return 'Always'
    if policy not in (None, '', 'IfNotPresent'):
        raise PreflightError('unsupported image pull policy')
    # An immutable cached image already satisfies the target workload's pull
    # policy. Forcing Always would unnecessarily join kubelet's serial pull
    # queue and can block a prewarmed core upgrade behind a stalled sample.
    if re.search(r'@sha256:[0-9a-f]{64}$', container['image']):
        return 'IfNotPresent'
    # Mutable tags must still resolve remotely; a stale tag cache cannot prove
    # that the replacement image is available. Explicit Never remains above.
    return 'Always'


def run(args, *, body=None):
    try:
        result = subprocess.run(args, input=body, text=True, capture_output=True, timeout=90)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise PreflightError('command unavailable or timed out: ' + args[0]) from error
    if result.returncode:
        # Never echo Helm's rendered secrets or kubectl's submitted manifests.
        raise PreflightError('command failed: ' + args[0] + ' ' + args[1])
    return result.stdout


def probe_spec(template, node, name, namespace):
    original = template['spec']
    if any((original.get('affinity', {}).get(key) or {}).get(
            'requiredDuringSchedulingIgnoredDuringExecution')
           for key in ('podAffinity', 'podAntiAffinity')):
        raise PreflightError('required pod affinity cannot be safely reproduced by a pull-only probe')
    # Preferred pod terms only score eligible nodes. Preserve them below; the
    # required node-name constraint pins this probe to one current target node.
    spec = {key: copy.deepcopy(original[key]) for key in (
        'nodeSelector', 'affinity', 'tolerations', 'schedulerName', 'runtimeClassName',
        'imagePullSecrets', 'serviceAccountName', 'securityContext',
    ) if key in original}
    # The scheduler must enforce all target constraints AND the current node.
    affinity = spec.setdefault('affinity', {}).setdefault('nodeAffinity', {})
    required = affinity.setdefault('requiredDuringSchedulingIgnoredDuringExecution', {})
    terms = required.setdefault('nodeSelectorTerms', [{}])
    for term in terms:
        term.setdefault('matchFields', []).append({'key': 'metadata.name', 'operator': 'In', 'values': [node]})
    containers = [c for field in ('containers', 'initContainers') for c in original.get(field, [])]
    if not containers:
        raise PreflightError('target workload has no images')
    probes = []
    for i, container in enumerate(containers):
        security = copy.deepcopy(container.get('securityContext', {}))
        security.update({'allowPrivilegeEscalation': False, 'privileged': False,
                         'readOnlyRootFilesystem': True, 'capabilities': {'drop': ['ALL']}})
        security.setdefault('runAsNonRoot', True)
        security.setdefault('seccompProfile', {'type': 'RuntimeDefault'})
        if 'runAsUser' not in security and 'runAsUser' not in spec.get('securityContext', {}):
            security['runAsUser'] = 65532
        probes.append({'name': 'pull-' + str(i), 'image': container['image'],
                       'imagePullPolicy': probe_pull_policy(container), 'command': ['/'], 'securityContext': security,
                       'resources': {'requests': {'cpu': '1m', 'memory': '1Mi'}, 'limits': {'cpu': '10m', 'memory': '16Mi'}}})
    spec.update({'restartPolicy': 'Never', 'automountServiceAccountToken': False,
                 'terminationGracePeriodSeconds': 0, 'containers': probes})
    return {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': name, 'namespace': namespace,
            'labels': {'openbkn.ai/image-preflight': 'true'}}, 'spec': spec}


def images_pulled(pod):
    statuses = {s['name']: s for s in pod.get('status', {}).get('containerStatuses', [])}
    return all(statuses.get(c['name'], {}).get('imageID') for c in pod['spec']['containers'])


def dry_run_args(args):
    result = []
    index = 0
    while index < len(args):
        arg = args[index]
        if arg in ('--output', '-o'):
            index += 2
            continue
        if arg.startswith('--output=') or arg.startswith('--dry-run='):
            index += 1
            continue
        if arg == '--dry-run':
            index += 1
            if index < len(args) and args[index] in ('client', 'server', 'none'):
                index += 1
            continue
        result.append(arg)
        index += 1
    return result + ['--dry-run=server', '--output=json']


def preflight(namespace, release, helm_args, timeout=240, execute=run, clock=time.monotonic, sleep=time.sleep):
    def kubectl(*args, body=None):
        return execute(['kubectl', '-n', namespace, *args], body=body)
    existing = json.loads(kubectl('get', 'deployments', '-o', 'json'))['items']
    existing = {d['metadata']['name']: d for d in existing if
                d['metadata'].get('annotations', {}).get('meta.helm.sh/release-name') == release
                or d['metadata']['name'] == release}
    if not existing:
        return 0
    rendered = json.loads(execute(['helm', *dry_run_args(helm_args)]))
    workloads = [d for d in yaml.safe_load_all(rendered.get('manifest', '')) if d and
                 d.get('kind') == 'Deployment' and d['metadata']['name'] in existing and
                 d.get('spec', {}).get('strategy', {}).get('type') == 'Recreate' and
                 d['spec'].get('replicas', 1) > 0]
    created = []
    try:
        for target in workloads:
            current = existing[target['metadata']['name']]
            labels = current['spec']['selector'].get('matchLabels', {})
            if not labels:
                raise PreflightError('Recreate deployment has no supported pod selector')
            selector = ','.join(k + '=' + v for k, v in sorted(labels.items()))
            pods = json.loads(kubectl('get', 'pods', '-l', selector, '-o', 'json'))['items']
            nodes = sorted({p['spec']['nodeName'] for p in pods if p.get('spec', {}).get('nodeName')})
            if not nodes:
                raise PreflightError('cannot establish current Recreate nodes; old release retained')
            for node in nodes:
                name = 'openbkn-image-preflight-' + uuid.uuid4().hex[:12]
                probe = probe_spec(target['spec']['template'], node, name, namespace)
                # '/' is a directory in every container root filesystem, so
                # exec fails without starting the image's application.
                print('Pre-pull on ' + node + ': ' + ', '.join(c['image'] + ' (' + c['imagePullPolicy'] + ')' for c in probe['spec']['containers']), flush=True)
                created.append(name)
                kubectl('create', '-f', '-', body=json.dumps(probe))
                deadline = clock() + timeout
                while clock() < deadline:
                    observed = json.loads(kubectl('get', 'pod', name, '-o', 'json'))
                    if images_pulled(observed):
                        break
                    sleep(2)
                else:
                    raise PreflightError('image pull or scheduling timed out on ' + node + '; old release retained')
        return len(created)
    finally:
        primary_failure = sys.exc_info()[0] is not None
        cleanup_failures = []
        for name in created:
            try:
                kubectl('delete', 'pod', name, '--ignore-not-found', '--wait=false')
            except PreflightError:
                cleanup_failures.append(name)
        if cleanup_failures:
            print('Could not clean image probe Pods: ' + ', '.join(cleanup_failures), file=sys.stderr)
            if not primary_failure:
                raise PreflightError('image probe cleanup failed; upgrade not started')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--namespace', required=True)
    parser.add_argument('--release', required=True)
    parser.add_argument('--timeout', type=int, default=240)
    parser.add_argument('helm_args', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    try:
        if args.timeout < 1:
            raise PreflightError('image preflight timeout must be positive')
        command = args.helm_args[1:] if args.helm_args[:1] == ['--'] else args.helm_args
        count = preflight(args.namespace, args.release, command, args.timeout)
        print('Image preflight passed (' + str(count) + ' probes).')
    except (PreflightError, ValueError, KeyError, yaml.YAMLError):
        print('Image preflight failed; Helm upgrade was not started. Check node scheduling, registry access and imagePullSecrets.', file=sys.stderr)
        sys.exit(1)
