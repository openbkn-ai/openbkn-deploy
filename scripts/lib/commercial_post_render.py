#!/usr/bin/env python3
# Copyright openbkn.ai
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.
"""Attach the commercial registry Secret to rendered EE pod specifications."""
import os
import sys
import yaml


def inject(value, prefix, name):
    if isinstance(value, dict):
        containers = [container for key in ('containers', 'initContainers', 'ephemeralContainers')
                      for container in value.get(key, []) if isinstance(container, dict)]
        if containers and any(str(container.get('image', '')).startswith(prefix) for container in containers):
            secrets = value.setdefault('imagePullSecrets', [])
            if not isinstance(secrets, list):
                raise ValueError('Pod imagePullSecrets must be a list')
            if not any(isinstance(item, dict) and item.get('name') == name for item in secrets):
                secrets.append({'name': name})
        for item in value.values():
            inject(item, prefix, name)
    elif isinstance(value, list):
        for item in value:
            inject(item, prefix, name)


def main():
    try:
        prefix = os.environ['OPENBKN_COMMERCIAL_IMAGE_PREFIX']
        name = os.environ['OPENBKN_COMMERCIAL_PULL_SECRET']
        # Helm may emit empty documents for conditional templates. Serializing
        # them as `--- null` is rejected when Helm splits upgrade manifests.
        resources = [resource for resource in yaml.safe_load_all(sys.stdin) if resource is not None]
        for resource in resources:
            inject(resource, prefix, name)
        yaml.safe_dump_all(resources, sys.stdout, sort_keys=False, explicit_start=True)
    except (KeyError, TypeError, ValueError, yaml.YAMLError) as error:
        print('Cannot configure commercial image pull Secret: ' + str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
