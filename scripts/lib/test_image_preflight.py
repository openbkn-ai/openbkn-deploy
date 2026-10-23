import copy
import importlib.util
import json
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('image_preflight', Path(__file__).with_name('image_preflight.py'))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

TEMPLATE = {'spec': {'containers': [{'name': 'app', 'image': 'swr.example/core@sha256:abc', 'env': [{'name': 'PASSWORD', 'value': 'secret'}]}], 'volumes': [{'name': 'data'}], 'serviceAccountName': 'core', 'imagePullSecrets': [{'name': 'registry'}], 'nodeSelector': {'arch': 'arm64'}, 'tolerations': [{'key': 'workload', 'operator': 'Exists'}]}}
DEPLOYMENT = {'kind': 'Deployment', 'metadata': {'name': 'core'}, 'spec': {'strategy': {'type': 'Recreate'}, 'selector': {'matchLabels': {'app': 'core'}}, 'template': TEMPLATE}}

class PreflightTest(unittest.TestCase):
    def test_immutable_cached_probe_does_not_join_the_serial_pull_queue(self):
        for policy in (None, 'IfNotPresent', 'Never', 'Always'):
            with self.subTest(policy=policy):
                template = copy.deepcopy(TEMPLATE)
                container = template['spec']['containers'][0]
                container['image'] = 'swr.example/core@sha256:' + 'a' * 64
                if policy is not None:
                    container['imagePullPolicy'] = policy
                probe = m.probe_spec(template, 'node1', 'probe', 'ns')
                self.assertEqual(probe['spec']['containers'][0]['imagePullPolicy'],
                                 policy if policy in ('Never', 'Always') else 'IfNotPresent')

    def test_mutable_tags_still_resolve_remotely_unless_operator_requires_never(self):
        for policy in (None, 'IfNotPresent', 'Always', 'Never'):
            with self.subTest(policy=policy):
                template = copy.deepcopy(TEMPLATE)
                container = template['spec']['containers'][0]
                container['image'] = 'swr.example/core:release'
                if policy is not None:
                    container['imagePullPolicy'] = policy
                probe = m.probe_spec(template, 'node1', 'probe', 'ns')
                self.assertEqual(probe['spec']['containers'][0]['imagePullPolicy'],
                                 'Never' if policy == 'Never' else 'Always')

    def test_probe_copies_constraints_without_credentials_or_application_code(self):
        probe = m.probe_spec(TEMPLATE, 'node1', 'probe1', 'ns')['spec']
        self.assertEqual(probe['imagePullSecrets'], [{'name': 'registry'}])
        self.assertEqual(probe['nodeSelector'], {'arch': 'arm64'})
        self.assertNotIn('volumes', probe)
        self.assertFalse(probe['automountServiceAccountToken'])
        self.assertNotIn('env', probe['containers'][0])
        self.assertEqual(probe['containers'][0]['imagePullPolicy'], 'Always')
        self.assertNotIn('nodeName', probe)
        self.assertEqual(probe['affinity']['nodeAffinity']['requiredDuringSchedulingIgnoredDuringExecution']['nodeSelectorTerms'][0]['matchFields'][0]['values'], ['node1'])

    def test_security_context_is_preserved_and_probe_privileges_are_restricted(self):
        template = copy.deepcopy(TEMPLATE)
        template['spec']['securityContext'] = {'runAsUser': 1001, 'runAsGroup': 1001}
        template['spec']['containers'][0]['securityContext'] = {'runAsUser': 1002, 'runAsNonRoot': True}
        spec = m.probe_spec(template, 'node1', 'probe', 'ns')['spec']
        self.assertEqual(spec['securityContext']['runAsUser'], 1001)
        security = spec['containers'][0]['securityContext']
        self.assertEqual(security['runAsUser'], 1002)
        self.assertEqual(security['capabilities'], {'drop': ['ALL']})
        self.assertFalse(security['allowPrivilegeEscalation'])

    def test_pod_affinity_fails_closed_instead_of_changing_label_semantics(self):
        template = copy.deepcopy(TEMPLATE)
        template['spec']['affinity'] = {'podAntiAffinity': {'requiredDuringSchedulingIgnoredDuringExecution': [{}]}}
        with self.assertRaisesRegex(m.PreflightError, 'affinity'):
            m.probe_spec(template, 'node1', 'probe', 'ns')

    def test_dry_run_normalizes_existing_output_and_dry_run_flags(self):
        self.assertEqual(m.dry_run_args(['upgrade', '--install', 'core', 'chart', '-o', 'yaml', '--dry-run=client', '--output=json']), ['upgrade', '--install', 'core', 'chart', '--dry-run=server', '--output=json'])

    def exercise(self, ready):
        calls, probes = [], {}
        def execute(args, body=None):
            calls.append(args)
            if args[0] == 'helm':
                self.assertIn('--dry-run=server', args)
                return json.dumps({'manifest': json.dumps(DEPLOYMENT)})
            command = args[3:]
            if command[:2] == ['get', 'deployments']:
                return json.dumps({'items': [DEPLOYMENT]})
            if command[:2] == ['get', 'pods']:
                return json.dumps({'items': [{'spec': {'nodeName': 'node1'}}]})
            if command[0] == 'create':
                pod = json.loads(body); probes[pod['metadata']['name']] = pod
                return '{}'
            if command[:2] == ['get', 'pod']:
                pod = copy.deepcopy(probes[command[2]])
                if ready:
                    pod['status'] = {'containerStatuses': [{'name': 'pull-0', 'imageID': 'digest'}]}
                return json.dumps(pod)
            if command[0] == 'delete':
                return '{}'
            self.fail(args)
        ticks = iter([0, 0, 2])
        if ready:
            self.assertEqual(m.preflight('ns', 'core', ['upgrade', '--install', 'core', 'chart'], 1, execute, lambda: next(ticks), lambda _: None), 1)
        else:
            with self.assertRaisesRegex(m.PreflightError, 'timed out'):
                m.preflight('ns', 'core', ['upgrade', '--install', 'core', 'chart'], 1, execute, lambda: next(ticks), lambda _: None)
        self.assertTrue(any('delete' in c for c in calls))
        self.assertFalse(any(c[0] == 'helm' and '--dry-run=server' not in c for c in calls))
        return calls

    def test_pull_success_cleans_probe(self):
        self.exercise(True)

    def test_pull_timeout_cleans_probe_and_never_upgrades(self):
        self.exercise(False)

    def test_cleanup_attempts_all_probes_and_preserves_primary_failure(self):
        for primary in (False, True):
            with self.subTest(primary=primary):
                probes, deleted = {}, []
                def execute(args, body=None):
                    if args[0] == 'helm':
                        return json.dumps({'manifest': json.dumps(DEPLOYMENT)})
                    command = args[3:]
                    if command[:2] == ['get', 'deployments']:
                        return json.dumps({'items': [DEPLOYMENT]})
                    if command[:2] == ['get', 'pods']:
                        return json.dumps({'items': [{'spec': {'nodeName': n}} for n in ('node1', 'node2')]})
                    if command[0] == 'create':
                        pod = json.loads(body)
                        probes[pod['metadata']['name']] = pod
                        return '{}'
                    if command[:2] == ['get', 'pod']:
                        if primary and len(probes) == 2:
                            raise m.PreflightError('original pull failure')
                        pod = copy.deepcopy(probes[command[2]])
                        pod['status'] = {'containerStatuses': [{'name': 'pull-0', 'imageID': 'digest'}]}
                        return json.dumps(pod)
                    if command[0] == 'delete':
                        deleted.append(command[2])
                        if len(deleted) == 1:
                            raise m.PreflightError('delete failure')
                        return '{}'
                    self.fail(args)
                expected = 'original pull failure' if primary else 'cleanup failed'
                with self.assertRaisesRegex(m.PreflightError, expected):
                    m.preflight('ns', 'core', [], execute=execute)
                self.assertEqual(set(deleted), set(probes))
                self.assertEqual(len(deleted), 2)

    def test_empty_status_is_not_success(self):
        self.assertFalse(m.images_pulled({'spec': {'containers': [{'name': 'a'}]}}))

    def test_first_install_does_not_require_existing_nodes(self):
        calls = []
        def execute(args, body=None):
            calls.append(args)
            return '{"items": []}'
        self.assertEqual(m.preflight('ns', 'core', [], execute=execute), 0)
        self.assertEqual(len(calls), 1)

if __name__ == '__main__':
    unittest.main()
