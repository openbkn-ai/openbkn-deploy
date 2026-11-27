# Copyright openbkn.ai
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.
"""Render commercial values against real source Charts without contacting Kubernetes."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import yaml

DEPLOY = Path(__file__).resolve().parents[2]
CORE = Path(os.environ.get('OPENBKN_CORE_ROOT', DEPLOY.parent))
EE = Path(os.environ.get('OPENBKN_EE_ROOT', CORE / 'openbkn-ee'))
DELIVERY = DEPLOY / 'commercial/0.2.2'


@unittest.skipUnless(shutil.which('helm') and (DEPLOY / 'scripts/lib/commercial_manifest.py').is_file(),
                     'Helm and the matching deployment checkout are required')
class CommercialRenderTest(unittest.TestCase):
    def test_all_commercial_charts_use_offline_images_and_consumer_configuration(self):
        spec = importlib.util.spec_from_file_location('commercial', DEPLOY / 'scripts/lib/commercial_manifest.py')
        commercial = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(commercial)
        spec = importlib.util.spec_from_file_location('runtime', DEPLOY / 'scripts/lib/runtime_profile.py')
        runtime = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(runtime)
        inventory = json.loads((DELIVERY / 'commercial-manifest.json').read_text())
        paths = {
            'bkn-backend': CORE / 'adp/bkn/bkn-backend/helm/bkn-backend',
            'bkn-safe': CORE / 'bkn-safe/charts/bkn-safe',
            'ontology-query': CORE / 'adp/bkn/ontology-query/helm/ontology-query',
            'vega-backend': CORE / 'vega/vega-backend/helm/vega-backend',
            'bkn-studio': CORE / 'bkn-studio/deploy/charts/bkn-studio',
            'agent-observability': CORE / 'bkn-trace/agent-observability/charts/agent-observability',
            'agent-retrieval': CORE / 'adp/context-loader/agent-retrieval/helm/agent-retrieval',
        }
        paths.update({
            'bkn-project': EE / 'adp/bkn/bkn-project/helm/bkn-project',
            'workshop': EE / 'workshop/chart/workshop',
            'bkn-governance': EE / 'governance/chart/bkn-governance',
        })
        for path in paths.values():
            if not (path / 'Chart.yaml').exists():
                self.skipTest('Matching source Chart unavailable: ' + str(path))
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            version = inventory['platformVersion']
            for name, path in paths.items():
                subprocess.run(['helm', 'package', str(path), '--version', version, '--app-version', version, '-d', str(root)],
                               check=True, capture_output=True)
            community = root / 'community.yaml'
            community.write_text(yaml.safe_dump({'kind': 'VersionSet', 'product': 'bkn-foundry', 'version': version, 'releases': {
                name: {'chart': name, 'version': version} for name in paths if inventory['releases'].get(name, {}).get('type') != 'addon'}}))
            _, profile = commercial.compile_plan(DELIVERY / 'commercial-manifest.json', community, root, 'registry.local:5000')
            runtime.write(root / 'profile.json', profile)
            config = yaml.safe_load((DEPLOY / 'conf/config.yaml').read_text())
            config['namespace'] = 'openbkn'
            config.setdefault('depServices', {})['rds'] = {'host': 'mariadb', 'port': 3306, 'user': 'fixture', 'password': 'fixture'}
            config['image'] = {'registry': 'registry.local:5000/openbkn-ai'}
            runtime.write(root / 'config.json', config)
            unsupported = []
            for name, entry in profile['releases'].items():
                values = entry['values']
                values['namespace'] = 'openbkn'
                runtime.write(root / (name + '.values.json'), values)
                args = argparse.Namespace(state=str(root), release=name, helm_args=[
                    'upgrade', '--install', name, str(root / (name + '-' + version + '.tgz')),
                    '-f', str(root / 'config.json'), '-f', str(root / (name + '.values.json'))])
                rendered = subprocess.run(['helm', 'template', *runtime.helm_options(args.helm_args)], capture_output=True, text=True)
                self.assertEqual(rendered.returncode, 0, name + ': ' + rendered.stderr)
                runtime.render(args)
                # Exercise the same post-rendering and validation used with SWR credentials.
                profile['commercialPullSecret'] = 'openbkn-commercial-pull'
                runtime.write(root / 'profile.json', profile)
                args.helm_args += ['--post-renderer', str(DEPLOY / 'scripts/lib/commercial_post_render.py')]
                with patch.dict(os.environ, {'OPENBKN_COMMERCIAL_IMAGE_PREFIX': 'registry.local:5000/openbkn-ai-ee/',
                                             'OPENBKN_COMMERCIAL_PULL_SECRET': 'openbkn-commercial-pull'}):
                    try:
                        runtime.render(args)
                    except ValueError as error:
                        if 'does not attach the commercial image pull Secret' not in str(error):
                            raise
                        unsupported.append(name)
                profile.pop('commercialPullSecret')
                runtime.write(root / 'profile.json', profile)
            self.assertEqual(unsupported, [], 'Charts with unsupported pull Secret handling: ' + ', '.join(unsupported))
            backend = profile['releases']['bkn-backend']['values']
            self.assertNotIn('knowledgeBase', backend)
            safe_env = runtime.envs(profile['releases']['bkn-safe']['values']['extraEnv'])
            self.assertEqual(safe_env['SAFE_WORKSHOP_BASE_URL']['value'], 'http://workshop-svc:13022')
            self.assertNotIn('SAFE_KB_API_BASE_URL', safe_env)
            self.assertEqual(profile['releases']['bkn-project']['values']['migration']['enabled'], True)


if __name__ == '__main__':
    unittest.main()
