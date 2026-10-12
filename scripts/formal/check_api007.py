#!/usr/bin/env python3
"""Independent API-007 finite safety gate; no backend correspondence claim."""
import argparse
from collections import deque
import hashlib
import json
import os
import tempfile
import types
from pathlib import Path
import platform
import subprocess
import sys

# Execute the exact bytes whose identities are recorded, avoiding import/hash
# races and stale bytecode caches. The legacy modules themselves are unchanged.
LOADED_SOURCES = {}
for module_name in ('models', 'explorer', 'api007_models'):
    source_path = Path(__file__).resolve().with_name(module_name + '.py')
    source_bytes = source_path.read_bytes()
    LOADED_SOURCES[str(source_path)] = hashlib.sha256(source_bytes).hexdigest()
    module = types.ModuleType(module_name)
    module.__file__ = str(source_path)
    sys.modules[module_name] = module
    exec(compile(source_bytes, str(source_path), 'exec'), module.__dict__)

from api007_models import CONFIGURATIONS, FAULTS, ModelBoundError, Publication
from explorer import SearchLimit, explore, render

MAPPINGS = {
    'F-ACK': {'symbol': 'QueueRecoveryPipeline::recover_checked',
              'source': 'crates/fireweed/src/queue_recovery_pipeline.rs',
              'test': 'owned_claim_publishes_then_replays_original_identity_and_expiry'},
    'F-PREFIX': {'symbol': 'verify_prefix',
                 'source': 'crates/fireweed/src/queue_recovery_pipeline.rs',
                 'test': 'cold_prefix_cached_prefix_and_new_suffix_recover_to_exact_native_target'},
    'F-FENCE': {'symbol': 'resolve_submission',
                'source': 'crates/fireweed/src/queue_recovery_pipeline/mutation.rs',
                'test': 'losing_insert_fence_resolves_winner_without_replanning_or_extra_publication'},
    'F-REPLAY': {'symbol': 'recover_original',
                 'source': 'crates/fireweed/src/queue_recovery_pipeline/mutation.rs',
                 'test': 'missing_original_cache_cannot_duplicate_durable_claim'},
    'F-GROUP': {'symbol': 'CommitExecution',
                'source': 'crates/fireweed/src/queue_recovery_pipeline/commit.rs',
                'test': 'owned_commit_resolves_atomic_original_group_and_rejects_regrouped_retry'},
}
for mapping in MAPPINGS.values():
    mapping['test_source'] = 'crates/fireweed/src/queue_recovery_pipeline/tests.rs'
    mapping['status'] = 'documentary_unexecuted'


def validate_loaded_sources():
    for name, expected in LOADED_SOURCES.items():
        if hashlib.sha256(Path(name).read_bytes()).hexdigest() != expected:
            raise ValueError('loaded executable source differs from provenance: ' + name)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def replay(model, trace, expected=None):
    if not isinstance(trace, list) or not trace:
        raise ValueError('empty or malformed trace')
    initial = {'action': 'Init', 'state': render(model.initial)}
    if canonical(trace[0]) != canonical(initial):
        raise ValueError('invalid initial state')
    current = {model.initial}
    failure = None
    for offset, row in enumerate(trace[1:], 1):
        if not isinstance(row, dict) or set(row) != {'action', 'state'} or not isinstance(row['action'], str):
            raise ValueError('invalid trace row')
        successors = set()
        failures = set()
        for before in current:
            transitions = list(model.transitions(before)) + [('Observe', before)]
            for action, after in transitions:
                if action == row['action'] and canonical(render(after)) == canonical(row['state']):
                    successors.add(after)
                    failures.add(model.violation(before, action, after))
        if not successors or len(failures) != 1:
            raise ValueError('unknown or ambiguous trace transition')
        failure = failures.pop()
        if failure and offset != len(trace) - 1:
            raise ValueError('trace continues after violation')
        current = successors
    if failure != expected:
        raise ValueError('unexpected trace verdict')
    return True


def witness(model, graph, sequence, finish):
    """Shortest graph path completing the required ordered actions and outcome."""
    states, edges, _ = graph
    queue = deque([(0, 0)])
    parents = {(0, 0): None}
    end = None
    while queue:
        node, step = queue.popleft()
        if step == len(sequence) and finish(states[node]):
            end = (node, step)
            break
        for action, dest in edges[node]:
            next_step = step + (step < len(sequence) and action.startswith(sequence[step]))
            key = (dest, next_step)
            if key not in parents:
                parents[key] = ((node, step), action)
                queue.append(key)
    if end is None:
        raise AssertionError('missing completed witness: ' + repr(sequence))
    rows = []
    while parents[end] is not None:
        previous, action = parents[end]
        rows.append({'action': action, 'state': render(states[end[0]])})
        end = previous
    trace = [{'action': 'Init', 'state': render(states[0])}] + list(reversed(rows))
    replay(model, trace)
    return {'required_actions': sequence, 'sha256': digest(trace), 'trace': trace}


WITNESSES = {
    'original-resolution': [
        ('durable_ack', ['Resolve:0:success', 'ServeFresh'], lambda s: s.served and bool(s.acknowledged)),
        ('lost_reply', ['LoseStoredReply:0', 'Resolve:0:success', 'ServeFresh'], lambda s: s.served and bool(s.acknowledged)),
        ('late_rejection', ['TimeoutBeforeStorage:0', 'AcquireWitness:0', 'CASReject:0', 'Reprepare:0', 'Resolve:0:success', 'ServeFresh'], lambda s: s.served and bool(s.acknowledged)),
        ('equal_replay', ['ReplayEqual:0', 'Resolve:0:replayed'], lambda s: s.callers[0].result == 'replayed'),
        ('changed_replay', ['ReplayChanged:0', 'Resolve:0:conflict'], lambda s: s.callers[0].result == 'conflict'),
        ('changed_after_expiry', ['ExplicitRetentionCleanup:0', 'ReplayChanged:0', 'Resolve:0:conflict'], lambda s: s.callers[0].result == 'conflict'),
        ('expired_replay', ['ExplicitRetentionCleanup:0', 'ReplayExpired:0', 'Resolve:0:expired'], lambda s: s.callers[0].result == 'expired'),
        ('lost_cache_refusal', ['Resolve:0:success', 'LoseCachedOriginal', 'ReplayEqual:0', 'CacheHistoryUnknown:0'], lambda s: s.callers[0].result == 'unknown'),
        ('rejected_zero_effects', ['Prepare:0:1', 'CommitApply', 'Resolve:0:success', 'ServeFresh'], lambda s: s.served and bool(s.receipts) and not s.work and not s.workflow and not s.new_work),
    ],
    'background-publication': [
        ('head_conflict', ['Prepare:0:0', 'PrepareGrantedEpoch:1', 'CASStore:1', 'CASReject:0', 'Reprepare:0', 'Resolve:0:success', 'ServeFresh'], lambda s: s.served and len(s.history) == 2),
        ('full_history_late_rejection', ['Prepare:0:0', 'Dispatch:0', 'TimeoutBeforeStorage:0', 'PrepareGrantedEpoch:1', 'CASStore:1', 'Reprepare:0', 'CASStore:2', 'CASReject:0', 'Resolve:0:success', 'ServeFresh'], lambda s: s.served and len(s.history) == 2),
        ('stale_cut_retry', ['Prepare:0:0', 'Dispatch:0', 'CASStore:0', 'ReceiveStored:0', 'AcquireWitness:0', 'PrepareGrantedEpoch:1', 'CASStore:1', 'DiscardStaleCut:0', 'AcquireWitness:0', 'Resolve:0:success', 'ServeFresh'], lambda s: s.served and len(s.history) == 2),
        ('fresh_rebuild', ['Resolve:0:success', 'ResetProjection', 'CommitApply', 'ServeFresh'], lambda s: s.served and bool(s.acknowledged)),
    ],
    'authority-reopen': [
        ('interrupted_apply', ['BeginApply', 'CrashDuringApply', 'Reopen', 'CommitApply', 'Resolve:0:success', 'ServeFresh'], lambda s: s.served and bool(s.acknowledged)),
        ('refuse_then_restore', ['LoseIndex', 'UnknownAuthority:0', 'RestoreAuthenticAuthority', 'CommitApply', 'ServeFresh'], lambda s: s.served and s.callers[0].result == 'unknown' and bool(s.history)),
    ],
}


def run(max_states=100000):
    baselines = []
    for configuration in CONFIGURATIONS:
        model = Publication(configuration)
        report, graph = explore(model, max_states)
        if report['status'] != 'PASS' or not report['complete']:
            raise AssertionError('baseline failure: ' + canonical(report))
        states, edges, _ = graph
        if not any(action != 'Observe' for row in edges for action, _ in row):
            raise AssertionError('zero non-observation edges')
        report['graph_sha256'] = digest({'states': [render(s) for s in states], 'edges': edges})
        report['configuration_sha256'] = digest({'name': configuration, 'bounds': model.bounds, 'assumptions': model.assumptions})
        report['completed_witnesses'] = {name: witness(model, graph, actions, finish)
                                         for name, actions, finish in WITNESSES[configuration]}
        baselines.append(report)
    controls = []
    for fault, (configuration, expected) in FAULTS.items():
        model = Publication(configuration, fault)
        report, _ = explore(model, max_states)
        if report['status'] != 'COUNTEREXAMPLE' or report.get('property') != expected:
            raise AssertionError('insensitive negative control: ' + fault)
        replay(model, report['trace'], expected)
        controls.append({'fault': fault, 'trace_sha256': digest(report['trace']), **report})
    return {'schema': 'fireweed-api007-formal-v1', 'status': 'PASS', 'baseline': baselines,
            'negative_controls': controls, 'production_mappings': MAPPINGS,
            'qualification': {'finite_abstract_safety': 'established within stated domains',
                              'production_mapping': 'documentary_unexecuted',
                              'public_backend_correspondence': 'not run',
                              'excluded': ['catalog/process/cache evidence arbitration', 'independent same-ID callers',
                                           'lease/clock/ABA/key collision', 'budget/progress/deadlines',
                                           'FP4 administration', 'known-batch cached integrity', 'physical durability']}}


def provenance(root):
    sources = {}
    for name in ('scripts/formal', 'crates', 'docs/helix/02-design/contracts', 'docs/helix/03-test'):
        for path in sorted((root / name).rglob('*')):
            if path.is_file() and path.suffix in ('.py', '.rs', '.md', '.toml'):
                sources[str(path.relative_to(root))] = hashlib.sha256(path.read_bytes()).hexdigest()
    for name in ('Cargo.toml', 'Cargo.lock'):
        sources[name] = hashlib.sha256((root / name).read_bytes()).hexdigest()
    def git(*args):
        return subprocess.run(['git', '-C', str(root), *args], check=True, capture_output=True, text=True).stdout.strip()
    return {'head': git('rev-parse', 'HEAD'), 'dirty_status': git('status', '--short'),
            'python': platform.python_version(), 'sources': sources}


def output_path(root, requested):
    target = root / 'target'
    if target.is_symlink():
        raise ValueError('symlink target directory forbidden')
    path = requested if requested.is_absolute() else root / requested
    path = path.resolve()
    allowed = (target / 'api007-formal').resolve()
    if not allowed.is_relative_to(target.resolve()) or not path.is_relative_to(allowed) or path == allowed:
        raise ValueError('output must remain inside repository target/api007-formal')
    return path


def write_report(path, report):
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix='.report-', dir=path.parent)
    try:
        with os.fdopen(descriptor, 'w') as handle:
            handle.write(json.dumps(report, sort_keys=True, indent=2) + '\n')
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--max-states', type=int, default=100000)
    parser.add_argument('--output', type=Path, default=Path('target/api007-formal/report.json'))
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    path = None
    try:
        path = output_path(root, args.output)
        write_report(path, {'schema': 'fireweed-api007-formal-v1', 'status': 'INCOMPLETE'})
        validate_loaded_sources()
        before = provenance(root)
        report = run(args.max_states)
        validate_loaded_sources()
        after = provenance(root)
        if before != after:
            raise ValueError('source changed during verification')
        report['provenance'] = before
        report['provenance_sha256'] = digest(before)
        write_report(path, report)
        print(canonical({'status': 'PASS', 'models': len(report['baseline']), 'negative_controls': len(report['negative_controls'])}))
        return 0
    except (ValueError, AssertionError, SearchLimit, ModelBoundError, OSError, subprocess.CalledProcessError) as error:
        failure = {'schema': 'fireweed-api007-formal-v1', 'status': 'FAIL', 'error': str(error)}
        if path is not None:
            write_report(path, failure)
        print(canonical(failure))
        return 1


if __name__ == '__main__':
    sys.exit(main())
