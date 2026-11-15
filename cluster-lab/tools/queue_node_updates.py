#!/usr/bin/env python3
"""Queue the exact controller release on all currently-online execution nodes."""

import argparse
import json
from pathlib import Path
import sys


PROJECT = Path(__file__).resolve().parents[1]
if str(PROJECT) not in sys.path:
    sys.path.insert(0, str(PROJECT))

from clusterlab.controller import Controller  # noqa: E402


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', required=True, help='controller runtime directory')
    parser.add_argument('--ref', required=True, help='exact Git commit deployed on controller')
    parser.add_argument('--parallel', type=int, default=2)
    args = parser.parse_args()
    service = Controller(args.root)
    try:
        snapshot = service.snapshot()
        online = [node['id'] for node in snapshot['nodes'] if node['status'] != 'offline']
        skipped = [node['name'] for node in snapshot['nodes'] if node['status'] == 'offline']
        result = ({'queued': 0, 'ref': args.ref, 'parallel': args.parallel}
                  if not online else service.request_updates(online, args.ref, args.parallel))
        result['offline_skipped'] = skipped
        print(json.dumps(result, ensure_ascii=False, separators=(',', ':')))
    finally:
        service.close()


if __name__ == '__main__':
    main()
