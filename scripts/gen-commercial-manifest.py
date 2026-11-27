#!/usr/bin/env python3
# Copyright openbkn.ai
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.
"""Create a self-contained commercial delivery from a manifest in openbkn-deploy."""
import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent / 'lib'))
from commercial_bundle import stage_delivery


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--out', required=True, help='New output directory')
    parser.add_argument('--version')
    parser.add_argument('--platform-version')
    parser.add_argument('--chart-version', action='append', default=[])
    parser.add_argument('--image-tag', action='append', default=[])
    args = parser.parse_args()
    try:
        print(stage_delivery(args.manifest, args.out, args.version, args.platform_version,
                             args.chart_version, args.image_tag))
    except (ValueError, OSError, KeyError, TypeError):
        parser.exit(1, 'Cannot generate delivery; check manifest references, locked versions and output directory.\n')


if __name__ == '__main__':
    main()
