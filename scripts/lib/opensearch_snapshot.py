#!/usr/bin/env python3
"""Merge snapshot support into Helm JSON values without replacing custom arrays."""
import argparse
import copy
import json
from pathlib import PurePosixPath
import re
import sys

REPOSITORY = '/usr/share/opensearch/snapshots'


def merge(values, host_path, image):
    path = PurePosixPath(host_path)
    if (not path.is_absolute() or '..' in path.parts or str(path) != host_path
            or len(path.parts) < 3 or host_path.startswith('/usr/share/opensearch')):
        raise ValueError('快照宿主机目录必须是专用绝对路径，例如 /mnt/openbkn-snapshots')
    # Only return the modified keys: --reuse-values retains everything else.
    result = {key: copy.deepcopy(values.get(key) or ([] if key != 'config' else {}))
              for key in ('config', 'extraVolumes', 'extraVolumeMounts', 'extraInitContainers')}
    if not isinstance(result['config'], dict):
        raise ValueError('config 必须是映射，请手动合并快照配置')
    text = result['config'].get('opensearch.yml', '')
    if not isinstance(text, str):
        raise ValueError('config.opensearch.yml 必须是文本')
    # Preserve existing YAML verbatim. Refuse unfamiliar repo settings rather than
    # rewriting custom YAML with a lossy ad-hoc parser.
    repo = re.search(r'^[ \t]*path\.repo[ \t]*:', text, re.M)
    if repo:
        line = text[repo.start():].splitlines()[0]
        if not re.fullmatch(r'\s*path\.repo\s*:\s*\[\s*[\'\"]?' + re.escape(REPOSITORY)
                            + r'[\'\"]?\s*\]\s*(?:#.*)?', line):
            raise ValueError('已有自定义 path.repo；请手动合并并确保包含 ' + REPOSITORY)
    else:
        text = text.rstrip() + '\npath.repo: ' + json.dumps([REPOSITORY]) + '\n'
    result['config']['opensearch.yml'] = text
    uid = values.get('securityContext', {}).get('runAsUser', 1000)
    gid = values.get('podSecurityContext', {}).get('fsGroup', 1000)
    if type(uid) is not int or type(gid) is not int or uid < 0 or gid < 0:
        raise ValueError('OpenSearch UID/GID 必须是非负整数')
    items = {
        'extraVolumes': {'name': 'snapshot-repo', 'hostPath': {'path': host_path, 'type': 'DirectoryOrCreate'}},
        'extraVolumeMounts': {'name': 'snapshot-repo', 'mountPath': REPOSITORY},
        'extraInitContainers': {
            'name': 'prepare-snapshot-repo', 'image': image, 'imagePullPolicy': 'IfNotPresent',
            'command': ['sh', '-ec', 'chown "$1:$2" /snapshots; chmod 700 /snapshots', 'snapshot-init', str(uid), str(gid)],
            'securityContext': {'runAsUser': 0, 'runAsNonRoot': False, 'allowPrivilegeEscalation': False},
            'resources': {'requests': {'cpu': '10m', 'memory': '16Mi'}},
            'volumeMounts': [{'name': 'snapshot-repo', 'mountPath': '/snapshots'}],
        },
    }
    for key, item in items.items():
        rows = result[key]
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise ValueError(key + ' 必须是列表，请手动合并现有自定义配置')
        existing = [row for row in rows if row.get('name') == item['name']]
        if len(existing) > 1:
            raise ValueError('重复的快照资源名称: ' + item['name'])
        if existing:
            old = existing[0]
            if key == 'extraVolumes' and (old.get('hostPath', {}).get('path') != host_path
                                         or old.get('hostPath', {}).get('type') not in ('Directory', 'DirectoryOrCreate')):
                raise ValueError('已有 snapshot-repo 指向其他存储；拒绝切换快照目录')
            if key == 'extraVolumeMounts' and old != item:
                raise ValueError('已有 snapshot-repo 挂载配置冲突')
            if key == 'extraInitContainers' and old.get('command', [])[:2] != ['sh', '-ec']:
                raise ValueError('已有同名自定义 init 容器，请手动合并')
            rows[rows.index(old)] = item
        else:
            if key == 'extraVolumeMounts' and any(row.get('mountPath') == REPOSITORY for row in rows):
                raise ValueError('快照路径已被其他卷挂载，请手动合并')
            rows.append(item)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host-path', required=True)
    parser.add_argument('--init-image', required=True)
    args = parser.parse_args()
    try:
        print(json.dumps(merge(json.load(sys.stdin), args.host_path, args.init_image)))
    except (ValueError, TypeError, AttributeError) as exc:
        parser.exit(1, 'snapshot: ' + str(exc) + '\n')


if __name__ == '__main__':
    main()
