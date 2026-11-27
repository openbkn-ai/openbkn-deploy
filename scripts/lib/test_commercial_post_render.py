# Copyright openbkn.ai
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.
import os
from pathlib import Path
import subprocess
import unittest

import yaml


class PostRenderTest(unittest.TestCase):
    def test_empty_documents_are_removed_and_secret_is_added(self):
        data = '''---
apiVersion: v1
kind: Pod
metadata:
  name: test
spec:
  containers:
    - name: app
      image: registry.local/openbkn-ai-ee/app:tag
---
---
'''
        env = {**os.environ, 'OPENBKN_COMMERCIAL_IMAGE_PREFIX': 'registry.local/openbkn-ai-ee/',
               'OPENBKN_COMMERCIAL_PULL_SECRET': 'openbkn-commercial-pull'}
        result = subprocess.run(['python3', str(Path(__file__).with_name('commercial_post_render.py'))],
                                input=data, text=True, capture_output=True, env=env, check=True)
        self.assertNotIn('--- null', result.stdout)
        documents = list(yaml.safe_load_all(result.stdout))
        self.assertEqual(len(documents), 1)
        self.assertEqual(documents[0]['spec']['imagePullSecrets'], [{'name': 'openbkn-commercial-pull'}])


if __name__ == '__main__':
    unittest.main()
