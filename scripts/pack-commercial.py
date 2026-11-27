#!/usr/bin/env python3
# Copyright openbkn.ai
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.
"""Package the installer, manifests, values and prepared local Charts for offline delivery."""
import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent / 'lib'))
from commercial_bundle import pack


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--community', required=True)
    parser.add_argument('--charts-dir', required=True)
    parser.add_argument('--out', required=True)
    args = parser.parse_args()
    try:
        print(pack(Path(__file__).resolve().parents[1], args.manifest, args.community,
                   args.charts_dir, args.out))
    except (ValueError, OSError, KeyError, TypeError):
        parser.exit(1, 'Cannot package delivery; check all local Chart versions, manifest references and output path.\n')


if __name__ == '__main__':
    main()
