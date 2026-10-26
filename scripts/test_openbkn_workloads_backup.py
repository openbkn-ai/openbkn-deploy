"""Business scale commands leave data service Deployments running."""
import contextlib
import io
import unittest
from unittest import mock

import openbkn_workloads as workloads


def deployment(name, image='example/business:1'):
    return {'metadata': {'name': name, 'uid': 'deploy-' + name},
            'spec': {'template': {'spec': {'containers': [{'image': image}]}}}}


class WorkloadTests(unittest.TestCase):
    def test_stop_and_start_scale_only_business_deployments(self):
        items = [deployment('bkn-backend'), deployment('otelcol-contrib'),
                 deployment('minio', 'minio/minio:latest'),
                 deployment('bkn-safe-postgres', 'postgres:16')]
        def get_json(namespace, kind):
            return {'items': items if kind == 'deployments' else []}
        with mock.patch.object(workloads, 'get_json', side_effect=get_json) as read, \
                mock.patch.object(workloads, 'kubectl') as kubectl, \
                mock.patch.object(workloads, 'wait_business_pods_deleted') as wait, \
                contextlib.redirect_stdout(io.StringIO()):
            workloads.stop('openbkn')
            workloads.start('openbkn')
        self.assertEqual(read.call_count, 3)
        wait.assert_called_once()
        self.assertEqual(kubectl.call_args_list, [
            mock.call('openbkn', 'scale', 'deployment/bkn-backend', '--replicas=0'),
            mock.call('openbkn', 'scale', 'deployment/otelcol-contrib', '--replicas=0'),
            mock.call('openbkn', 'scale', 'deployment/bkn-backend', '--replicas=1'),
            mock.call('openbkn', 'scale', 'deployment/otelcol-contrib', '--replicas=1')])

    def test_no_business_deployment_fails_without_scaling_data(self):
        with mock.patch.object(workloads, 'get_json', return_value={'items': [deployment('minio')]}), \
                mock.patch.object(workloads, 'kubectl') as kubectl:
            with self.assertRaisesRegex(workloads.WorkloadError, '未发现业务'):
                workloads.stop('openbkn')
        kubectl.assert_not_called()

    def test_stop_waits_for_owned_pods_but_not_minio(self):
        replicasets = [{'metadata': {'uid': 'rs-business', 'ownerReferences': [
            {'kind': 'Deployment', 'uid': 'deploy-bkn-backend'}]}}]
        business = {'metadata': {'name': 'bkn-backend-abc', 'ownerReferences': [
            {'kind': 'ReplicaSet', 'uid': 'rs-business'}]}}
        minio = {'metadata': {'name': 'minio-abc', 'ownerReferences': [
            {'kind': 'ReplicaSet', 'uid': 'rs-minio'}]}}
        reads = iter([{'items': replicasets}, {'items': [business, minio]},
                      {'items': replicasets}, {'items': [minio]}])
        with mock.patch.object(workloads, 'get_json', side_effect=lambda *args: next(reads)), \
                mock.patch.object(workloads.time, 'sleep') as sleep:
            workloads.wait_business_pods_deleted('openbkn', {'deploy-bkn-backend'}, {'rs-business'})
        sleep.assert_called_once_with(2)

    def test_stop_times_out_if_business_pod_remains(self):
        business = {'metadata': {'name': 'bkn-backend-abc', 'ownerReferences': [
            {'kind': 'ReplicaSet', 'uid': 'rs-business'}]}}
        with mock.patch.object(workloads, 'get_json', side_effect=[{'items': []}, {'items': [business]}]), \
                mock.patch.object(workloads.time, 'monotonic', side_effect=[10, 10, 11]):
            with self.assertRaisesRegex(workloads.WorkloadError, 'bkn-backend-abc'):
                workloads.wait_business_pods_deleted('openbkn', {'deploy-bkn-backend'}, {'rs-business'}, timeout=1)


if __name__ == '__main__':
    unittest.main()
