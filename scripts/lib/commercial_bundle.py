# Copyright openbkn.ai
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.
"""Stage and package commercial deployment data owned by openbkn-deploy."""
import copy
import json
from pathlib import Path
import re
import shutil
import tarfile
import tempfile

from commercial_manifest import compile_plan, local_file, VERSION


def assignments(items, releases):
    result = {}
    for item in items:
        name, separator, value = item.partition('=')
        if not separator or name not in releases or not value or name in result:
            raise ValueError('Expected a unique release=value assignment for a commercial release')
        result[name] = value
    return result


def stage_delivery(manifest_path, destination, version=None, platform_version=None,
                   chart_versions=(), image_tags=()):
    source = Path(manifest_path).resolve()
    root = source.parent
    manifest = json.loads(source.read_text())
    manifest = copy.deepcopy(manifest)
    if version:
        if not VERSION.fullmatch(version):
            raise ValueError('Invalid delivery version')
        manifest['version'] = version
    if platform_version:
        if not VERSION.fullmatch(platform_version):
            raise ValueError('Invalid platform version')
        manifest['platformVersion'] = platform_version
    charts = assignments(chart_versions, manifest['releases'])
    images = assignments(image_tags, manifest['releases'])
    for name, item in manifest['releases'].items():
        if name in charts:
            if not VERSION.fullmatch(charts[name]):
                raise ValueError('Invalid locked Chart version')
            item['chartVersion'] = charts[name]
        if name in images:
            if not re.fullmatch(r'[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}', images[name]):
                raise ValueError('Invalid locked image tag')
            item['image']['tag'] = images[name]
    destination = Path(destination).resolve()
    if destination.exists():
        raise ValueError('Delivery output directory already exists')
    files = {manifest['runtimeProfile'], *manifest.get('examples', [])}
    for item in manifest['releases'].values():
        files.update(item.get('valuesFiles', []))
        for paths in item.get('consumers', {}).values():
            files.update(paths)
    # Resolve all references before writing; delivery must be independent of its source checkout.
    resolved = {name: local_file(root, name) for name in files}
    destination.mkdir(parents=True)
    for name, path in resolved.items():
        target = destination / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)
    profile_path = destination / manifest['runtimeProfile']
    profile = json.loads(profile_path.read_text())
    profile['platformVersion'] = manifest['platformVersion']
    profile_path.write_text(json.dumps(profile, indent=2, ensure_ascii=False) + '\n')
    output = destination / 'commercial-manifest.json'
    output.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + '\n')
    return output


def pack(repo, manifest, community, charts, output):
    """Package local inputs only. Image synchronization remains a separate preparation step."""
    output = Path(output).resolve()
    if output.exists():
        raise ValueError('Archive already exists; choose a new output path')
    with tempfile.TemporaryDirectory(prefix='openbkn-commercial-') as tmp:
        root = Path(tmp) / 'openbkn-commercial'
        root.mkdir()
        commercial = stage_delivery(manifest, root / 'commercial')
        # Validate exact local Chart metadata for the full combined install plan.
        compile_plan(commercial, community, charts, 'offline.invalid:5000')
        repo = Path(repo)
        for name in ('deploy.sh', 'preflight.sh', 'onboard.sh', 'README.md', 'README.zh.md'):
            shutil.copy2(repo / name, root / name)
        for name in ('scripts', 'conf'):
            shutil.copytree(repo / name, root / name,
                            ignore=shutil.ignore_patterns('__pycache__', '*.pyc', '*.bak'))
        shutil.copy2(community, root / 'community.yaml')
        # Include supplied data-service Charts as well as platform and commercial Charts.
        shutil.copytree(charts, root / 'charts', symlinks=False)
        output.parent.mkdir(parents=True, exist_ok=True)
        with tarfile.open(output, 'w:gz') as archive:
            archive.add(root, arcname=root.name)
    return output
