#!/usr/bin/env python3
# Copyright openbkn.ai
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.
"""Resolve a commercial development delivery once before installation."""
import argparse
import json
from pathlib import Path
import re
import subprocess
import sys

import yaml
from commercial_bundle import stage_delivery
from commercial_auth import create_authfile


def select_image_tag(tags, branch, line):
    if branch == 'main':
        channels = ['main']
    elif re.fullmatch(r'release/\d+\.\d+\.\d+', branch):
        branch_line = branch.split('/', 1)[1]
        channels = ['release', 'release-' + branch_line]
        if line != branch_line:
            raise ValueError('Chart version does not match release branch: ' + line)
    else:
        raise ValueError('--latest requires main or release/X.Y.Z branch')
    pattern = re.compile(r'^' + re.escape(line) + r'-(?:' + '|'.join(map(re.escape, channels)) +
                         r')\.(\d{14})\.sha[0-9a-f]+$')
    candidates = [(match.group(1), tag) for tag in tags if isinstance(tag, str)
                  and (match := pattern.fullmatch(tag))]
    if not candidates:
        raise ValueError('No timestamped ' + branch + ' image for Chart version ' + line)
    return max(candidates)[1]


def generate(repo, manifest_path, output, registry, community_template=None):
    repo, output = Path(repo), Path(output)
    branch = subprocess.run(['git', '-C', str(repo), 'symbolic-ref', '--quiet', '--short', 'HEAD'],
                            check=True, capture_output=True, text=True).stdout.strip()
    if branch != 'main' and not re.fullmatch(r'release/\d+\.\d+\.\d+', branch):
        raise ValueError('--latest requires main or release/X.Y.Z branch')
    authfile = create_authfile(output, registry)
    delivery = stage_delivery(manifest_path, output / 'commercial')
    manifest = json.loads(delivery.read_text())
    if branch.startswith('release/') and branch.split('/', 1)[1] != manifest['platformVersion']:
        raise ValueError('Commercial manifest platformVersion must match the release branch')
    template_path = Path(community_template) if community_template else repo / 'scripts/bkn-foundry.template.yaml'
    template = yaml.safe_load(template_path.read_text())
    if template.get('kind') != 'VersionSet' or template.get('product') != 'bkn-foundry':
        raise ValueError('A community platform VersionSet is required')
    for name in manifest.get('suspendedReleases', []):
        template['releases'].pop(name, None)
    template['version'] = manifest['platformVersion']
    for item in template['releases'].values():
        item['version'] = 'main'
    addons = []
    for name, item in manifest['releases'].items():
        if item['type'] == 'addon':
            if name in template['releases']:
                raise ValueError('Commercial addon conflicts with community release: ' + name)
            template['releases'][name] = {'chart': item['chart'], 'version': 'main'}
            addons.append(name)
        elif name not in template['releases'] or template['releases'][name].get('chart', name) != item['chart']:
            raise ValueError('Commercial override missing from community template: ' + name)
    template_file = output / 'latest-template.yaml'
    template_file.write_text(yaml.safe_dump(template, sort_keys=False))
    resolved_file = output / 'latest-all.yaml'
    subprocess.run([str(repo / 'scripts/gen-dev-manifest.sh'), '--latest',
                    '--template=' + str(template_file), '--out=' + str(resolved_file)], check=True)
    resolved = yaml.safe_load(resolved_file.read_text())
    for name, item in manifest['releases'].items():
        chart_version = str(resolved['releases'][name]['version'])
        line = chart_version.split('-', 1)[0]
        if line != manifest['platformVersion']:
            raise ValueError('Commercial Chart ' + name + ' does not match manifest platformVersion ' + manifest['platformVersion'])
        ref = registry.rstrip('/') + '/' + item['image']['repository']
        try:
            auth_args = ['--authfile', str(authfile)] if authfile else []
            result = subprocess.run(['skopeo', 'list-tags', *auth_args, 'docker://' + ref],
                                    check=True, capture_output=True, text=True, timeout=120)
            tags = json.loads(result.stdout)['Tags']
            item['image']['tag'] = select_image_tag(tags, branch, line)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, KeyError, json.JSONDecodeError) as error:
            raise ValueError('Cannot resolve image tags for ' + ref + '; check access and registry credentials') from error
        item['chartVersion'] = chart_version
        print(f"  {name}: chart={chart_version} image={ref}:{item['image']['tag']}", file=sys.stderr)
    for name in addons:
        del resolved['releases'][name]
    (output / 'community.yaml').write_text(yaml.safe_dump(resolved, sort_keys=False))
    delivery.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + '\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', required=True)
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--out', required=True)
    parser.add_argument('--registry', required=True)
    parser.add_argument('--community-template')
    args = parser.parse_args()
    try:
        generate(args.repo, args.manifest, args.out, args.registry, args.community_template)
    except (ValueError, OSError, subprocess.CalledProcessError, yaml.YAMLError) as error:
        print('Commercial --latest failed: ' + str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
