#!/usr/bin/env python3
# Copyright openbkn.ai
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.
"""Compile an offline commercial delivery into the existing platform install plan."""
import argparse
import copy
import json
from pathlib import Path
import re
import sys
import tarfile

import yaml

API = 'deploy.openbkn.ai/commercial/v1'
NAME = re.compile(r'[a-z0-9][a-z0-9.-]*\Z')
VERSION = re.compile(r'[0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9.-]+)?\Z')


def merge(base, overlay):
    result = copy.deepcopy(base)
    for key, value in overlay.items():
        if key == 'extraEnv':
            entries = {item['name']: item for item in result.get(key, [])}
            entries.update({item['name']: item for item in value})
            result[key] = list(entries.values())
        elif isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def local_file(root, name):
    if Path(name).is_absolute() or '..' in Path(name).parts:
        raise ValueError('Delivery references cannot be absolute or point outside the delivery bundle')
    path = (root / name).resolve()
    if not path.is_relative_to(root.resolve()) or not path.is_file():
        raise ValueError('Missing delivery file or path outside the delivery bundle: ' + name)
    return path


def compile_plan(manifest_path, community_path, charts, registry, values_path="", missing_charts=None):
    manifest_path = Path(manifest_path)
    root = manifest_path.resolve().parent
    commercial = json.loads(manifest_path.read_text())
    community = yaml.safe_load(Path(community_path).read_text())
    if community.get('product') != 'bkn-foundry' or community.get('kind') != 'VersionSet':
        raise ValueError('A community platform VersionSet is required')
    if commercial.get('apiVersion') != API or commercial.get('kind') != 'CommercialManifest':
        raise ValueError('Unsupported commercial manifest')
    if not registry or '://' in registry or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9.:/-]*', registry):
        raise ValueError('An offline registry host is required')
    version = commercial['platformVersion']
    if not VERSION.fullmatch(version) or community.get('version') != version:
        raise ValueError('Commercial and community platform versions must match')
    profile = json.loads(local_file(root, commercial['runtimeProfile']).read_text())
    profile['platformVersion'] = version
    profile['addons'] = {}  # Commercial services are installed regardless of license edition.
    profile['commercialDelivery'] = True
    suspended = commercial.get('suspendedReleases', [])
    if suspended not in ([], ['kb-api']):
        raise ValueError('Unsupported suspended commercial releases')
    profile['suspendedReleases'] = suspended
    profile['offlineRegistry'] = registry.rstrip('/')
    # The profile is the existing configuration/key contract, not a second image lock.
    for entry in profile['releases'].values():
        entry.get('values', {}).pop('image', None)
    if values_path and not Path(values_path).is_file():
        raise ValueError('Commercial values file does not exist or is not a regular file: ' + str(values_path))
    overrides = yaml.safe_load(Path(values_path).read_text()) if values_path else {}
    if not isinstance(overrides, dict):
        raise ValueError('Commercial values must map release names to values')
    for name in suspended:
        overrides.pop(name, None)
    releases = copy.deepcopy(community['releases'])
    for name in suspended:
        releases.pop(name, None)
        profile['releases'].pop(name, None)
    items = commercial['releases']
    if not isinstance(items, dict) or not items:
        raise ValueError('Commercial releases must not be empty')
    for name, item in items.items():
        if not NAME.fullmatch(name) or not NAME.fullmatch(item['chart']) or not VERSION.fullmatch(item['chartVersion']):
            raise ValueError('Invalid commercial release or Chart version')
        if item['type'] == 'override':
            target = releases.get(name)
            if not target or target.get('chart', name) != item['chart'] or str(target['version']) != item['chartVersion']:
                raise ValueError('Commercial override does not match the community Chart: ' + name)
        elif item['type'] == 'addon':
            if name in releases:
                raise ValueError('Commercial addon conflicts with a platform release: ' + name)
            releases[name] = {'chart': item['chart'], 'version': item['chartVersion']}
        else:
            raise ValueError('Unsupported commercial release type')
        entry = profile['releases'].setdefault(name, {'values': {}})
        for filename in item.get('valuesFiles', []):
            values = yaml.safe_load(local_file(root, filename).read_text())
            entry['values'] = merge(entry['values'], values or {})
        image = item['image']
        if not re.fullmatch(r'openbkn-ai-ee/[A-Za-z0-9._/-]+', image['repository']) or not re.fullmatch(r'[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}', image['tag']):
            raise ValueError('Invalid locked commercial image')
        namespace, repository = image['repository'].split('/', 1)
        entry['values']['image'] = {'registry': registry.rstrip('/') + '/' + namespace,
                                    'repository': repository, 'tag': image['tag']}
    for item in items.values():
        for consumer, filenames in item.get('consumers', {}).items():
            if consumer not in releases:
                raise ValueError('Commercial consumer is not in the install plan: ' + consumer)
            entry = profile['releases'].setdefault(consumer, {'values': {}})
            for filename in filenames:
                entry['values'] = merge(entry['values'], yaml.safe_load(local_file(root, filename).read_text()) or {})
    for name in releases:
        profile['releases'].setdefault(name, {'values': {}})
    if set(overrides) - set(profile['releases']):
        raise ValueError('Commercial values refer to an unmanaged release')
    for name, values in overrides.items():
        if not isinstance(values, dict) or 'image' in values:
            raise ValueError('Commercial values cannot replace locked image coordinates')
        profile['releases'][name]['values'] = merge(profile['releases'][name]['values'], values)
        profile['releases'][name]['environmentValues'] = copy.deepcopy(values)
    if 'kb-api' in profile['releases']:
        profile['releases']['kb-api']['requiredValues'] = [
            'objectStorage.endpoint', 'objectStorage.accessKey', 'objectStorage.secretKey']
    if set(profile['releases']) - set(releases):
        raise ValueError('Runtime profile refers to releases outside the install plan')
    for name, item in releases.items():
        if not NAME.fullmatch(name) or not NAME.fullmatch(item.get('chart', name)) or not VERSION.fullmatch(str(item['version'])):
            raise ValueError('Invalid platform release or Chart version')
        path = Path(charts) / (item.get('chart', name) + '-' + str(item['version']) + '.tgz')
        if not path.is_file():
            if missing_charts is not None:
                missing_charts.append((item.get('chart', name), str(item['version'])))
                continue
            raise ValueError('Offline installation requires local Chart: ' + path.name)
        with tarfile.open(path) as archive:
            metadata = archive.extractfile(item.get('chart', name) + '/Chart.yaml')
            if metadata is None:
                raise ValueError('Chart metadata is missing: ' + path.name)
            chart = yaml.safe_load(metadata.read())
            if chart.get('name') != item.get('chart', name) or str(chart.get('version')) != str(item['version']):
                raise ValueError('Chart metadata does not match the manifest: ' + path.name)
    community['releases'] = releases
    return community, profile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--community', required=True)
    parser.add_argument('--charts', required=True)
    parser.add_argument('--registry', required=True)
    parser.add_argument('--out', required=True)
    parser.add_argument('--values', default='')
    parser.add_argument('--missing-charts', help='Write missing Chart names and locked versions for download')
    args = parser.parse_args()
    try:
        missing = [] if args.missing_charts else None
        community, profile = compile_plan(args.manifest, args.community, args.charts, args.registry, args.values, missing)
        if args.missing_charts:
            Path(args.missing_charts).write_text(''.join(name + '\t' + version + '\n' for name, version in missing))
        directory = Path(args.out)
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        for name, data in [('platform.yaml', yaml.safe_dump(community, sort_keys=False)),
                           ('runtime.json', json.dumps(profile))]:
            path = directory / name
            path.write_text(data)
            path.chmod(0o600)
    except (ValueError, KeyError, TypeError, AttributeError, OSError, tarfile.TarError, yaml.YAMLError) as error:
        message = str(error) if type(error) is ValueError else 'Invalid commercial delivery; check manifest versions, local Charts and values files.'
        print(message, file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
