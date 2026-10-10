#!/usr/bin/env python3
"""Deterministic bounded formal gate. No dependencies beyond Python's stdlib."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

from explorer import SearchLimit, explore
from models import models, negative_controls


def run(max_states=100000):
    baseline = []
    for model in models():
        report, _ = explore(model, max_states)
        baseline.append(report)
        if report['status'] != 'PASS':
            raise AssertionError(json.dumps(report, indent=2))
    controls = []
    for model, expected in negative_controls():
        report, _ = explore(model, max_states)
        if report['status'] != 'COUNTEREXAMPLE' or report['property'] != expected:
            raise AssertionError(f'Insensitive control {model.name}/{model.fault}: {report}')
        controls.append({'fault': model.fault, 'expected_property': expected, **report})
    return {'schema': 'fireweed-formal-v1', 'status': 'PASS', 'baseline': baseline,
            'negative_controls': controls,
            'qualification': {'abstract_models': 'complete within declared domains',
                              'rust_refinement': 'not established by this report',
                              'public_s3': 'independent qualification required',
                              'production_deadline': 'not established by unit service fixture'}}


def provenance(root):
    def git(*args):
        result = subprocess.run(['git', '-C', str(root), *args], capture_output=True, text=True)
        return result.stdout.strip() if result.returncode == 0 else 'unavailable'
    sources = {}
    for parent in ('scripts/formal', 'crates',
                   'docs/helix/02-design/contracts', 'docs/helix/03-test'):
        for path in sorted((root / parent).rglob('*')):
            if path.is_file() and (path.suffix in ('.py', '.rs', '.md') or path.name == 'Cargo.toml'):
                sources[str(path.relative_to(root))] = hashlib.sha256(path.read_bytes()).hexdigest()
    for name in ('Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml', 'scripts/formal/fixtures/replay-smoke.json'):
        path = root / name
        if path.is_file():
            sources[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    return {'head': git('rev-parse', 'HEAD'), 'dirty_status': git('status', '--short'),
            'source_sha256': sources}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--max-states', type=int, default=100000)
    args = parser.parse_args()
    try:
        report = run(args.max_states)
        report['provenance'] = provenance(Path(__file__).resolve().parents[2])
        report['command'] = ['python3', 'scripts/formal/check.py', '--max-states', str(args.max_states)]
    except (SearchLimit, AssertionError) as error:
        report = {'schema': 'fireweed-formal-v1', 'status': 'FAIL', 'error': str(error)}
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
    print(json.dumps({'status': report['status'],
                      'states': sum(r['states'] for r in report.get('baseline', [])),
                      'models': len(report.get('baseline', [])),
                      'negative_controls': len(report.get('negative_controls', [])),
                      'error': report.get('error')}, sort_keys=True))
    return 0 if report['status'] == 'PASS' else 1


if __name__ == '__main__':
    sys.exit(main())
