# Copyright openbkn.ai
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.
import base64
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import commercial_auth as auth


class CommercialAuthTest(unittest.TestCase):
    def test_temporary_authfile_is_private_and_requires_both_variables(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(os.environ, {'SWR_USER': 'user', 'SWR_PASSWORD': ''}):
                with self.assertRaisesRegex(ValueError, 'both SWR_USER'):
                    auth.create_authfile(tmp, 'registry.local')
            with patch.dict(os.environ, {'SWR_USER': 'user', 'SWR_PASSWORD': 'secret'}):
                file = auth.create_authfile(tmp, 'registry.local/path')
                self.assertEqual(file.stat().st_mode & 0o777, 0o600)
                data = json.loads(file.read_text())
                self.assertEqual(base64.b64decode(data['auths']['registry.local']['auth']), b'user:secret')

    def test_default_service_account_preserves_existing_secrets(self):
        account = {'metadata': {'resourceVersion': '12'}, 'imagePullSecrets': [{'name': 'existing'}]}
        read = subprocess.CompletedProcess([], 0, json.dumps(account), '')
        done = subprocess.CompletedProcess([], 0, '', '')
        with patch.object(auth.subprocess, 'run', side_effect=[read, done]) as run:
            auth.ensure_default_service_account('openbkn')
            command = run.call_args.args[0]
            self.assertEqual(command[-2], '-p')
            ops = json.loads(command[-1])
            self.assertEqual(ops[0]['value'], '12')
            self.assertEqual(ops[1]['value'], [{'name': 'existing'}, {'name': auth.SECRET}])

    def test_provision_uses_stdin_and_refuses_to_replace_different_secret(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, {'SWR_USER': 'user', 'SWR_PASSWORD': 'secret'}):
            file = auth.create_authfile(tmp, 'registry.local')
            empty = subprocess.CompletedProcess([], 0, '', '')
            with patch.object(auth.subprocess, 'run', side_effect=[empty, empty]) as run:
                auth.provision('openbkn', file)
                args, kwargs = run.call_args
                self.assertEqual(args[0], ['kubectl', 'create', '-f', '-'])
                self.assertNotIn('secret', ' '.join(args[0]))
                payload = json.loads(kwargs['input'])
                self.assertEqual(payload['metadata']['namespace'], 'openbkn')
            existing = {'type': 'kubernetes.io/dockerconfigjson', 'data': {'.dockerconfigjson':
                base64.b64encode(b'{"auths":{}}').decode()}}
            with patch.object(auth.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, json.dumps(existing), '')) as run:
                with self.assertRaisesRegex(ValueError, 'differs'):
                    auth.provision('openbkn', file)
                self.assertEqual(run.call_count, 1)


if __name__ == '__main__':
    unittest.main()
