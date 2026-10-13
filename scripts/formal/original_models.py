"""Independent finite batch-original cache authority and allocation ordering.

One complete immutable source prefix, two unique batch IDs (fresh then known),
one native snapshot. Digest equality abstracts exact byte equality. No runtime,
provider, cryptographic implementation, concurrency or global-row proof.
"""
from dataclasses import dataclass, replace
from itertools import product
from models import Model

# source present, cache present, cache format/fp, caller format/fp,
# cache expiry, length validity, outcome digest, complete bound prefix.
CASES = tuple(product((True, False), (True, False), (0, 1), (0, 1),
                      (0, 1), (0, 1), (0, 1, 2), (0, 1, 2), (0, 1), (True, False)))
FAULTS = {'skip_cut': 'O-CUT', 'cache_as_authority': 'O-META',
          'caller_before_authority': 'O-META', 'trust_expiry': 'O-RESULT',
          'ignore_length': 'O-PAYLOAD', 'payload_before_conflict': 'O-PAYLOAD',
          'ignore_digest': 'O-RESULT', 'publish_fresh_early': 'O-BATCH'}

@dataclass(frozen=True)
class State:
    case: tuple = ()
    phase: str = 'idle'
    result: str = ''
    payload_reads: int = 0
    publications: int = 0

class Originals(Model):
    name = 'api007-original-cache-authority'
    properties = ['O-CUT', 'O-META', 'O-PAYLOAD', 'O-RESULT', 'O-BATCH']
    assumptions = ['complete checked source prefix and fixed native snapshot',
                   'two unique batch IDs; first fresh, second classified original',
                   'FP3/FP4 and fingerprints are injective symbols',
                   'digest agreement means exact opaque byte agreement',
                   'no authoritative receipt-expiry command exists',
                   'final authoritative head freshness remains an adapter obligation',
                   'finite safety only; no fair progress or deadline claim']
    initial = State()
    bounds = {'batch_ids': 2, 'source_receipt': 1, 'case_count': len(CASES),
              'source_prefix_complete_and_bound': [False, True],
              'format_symbols': 2, 'fingerprint_symbols': 2,
              'expiry': ['zero', 'one', 'invalid'],
              'length': ['exact', 'oversized', 'invalid'], 'digest_symbols': 2}
    def __init__(self, fault=None):
        if fault is not None and fault not in FAULTS:
            raise ValueError('unknown original-cache fault')
        self.fault = fault
    def frontier(self, state):
        return state.phase == 'done'
    @staticmethod
    def expected(case):
        # Independent ordered specification, with authority established before
        # caller comparison and no opaque decode required for genuine conflict.
        source, cached, fmt, fp, caller_fmt, caller_fp, expiry, length, digest, bound = case
        if not bound or source != cached:
            return 'unknown'
        if not source:
            return 'fresh'
        if fmt != 0 or fp != 0:
            return 'unknown'
        if caller_fmt != 0 or caller_fp != 0:
            return 'conflict'
        if expiry != 0 or length != 0 or digest != 0:
            return 'unknown'
        return 'replay'
    @staticmethod
    def metadata_valid(case):
        source, cached, fmt, fp, *_ = case
        return source == cached and (not source or (fmt == 0 and fp == 0))
    @staticmethod
    def payload_allowed(case):
        source, cached, fmt, fp, cf, cp, expiry, length, _, bound = case
        return bound and source and cached and fmt == fp == cf == cp == expiry == length == 0
    def metadata_plan(self, case):
        source, cached, fmt, fp, cf, cp, expiry, length, _, _ = case
        if self.fault == 'caller_before_authority' and cached and (cf != fmt or cp != fp):
            return 'finish', 'conflict'
        if self.fault != 'cache_as_authority' and (source != cached or (source and (fmt != 0 or fp != 0))):
            return 'finish', 'unknown'
        if not cached:
            return 'finish', 'fresh'
        if self.fault == 'payload_before_conflict':
            return 'payload', ''
        if cf != fmt or cp != fp:
            return 'finish', 'conflict'
        if expiry != 0:
            return 'finish', 'expired' if self.fault == 'trust_expiry' else 'unknown'
        if length != 0 and self.fault != 'ignore_length':
            return 'finish', 'unknown'
        return 'payload', ''
    def transitions(self, s):
        if s.phase == 'idle':
            for index, case in enumerate(CASES):
                yield 'SelectCase-' + str(index), replace(s, case=case, phase='prefix')
        elif s.phase == 'prefix':
            good = s.case[-1] or self.fault == 'skip_cut'
            yield 'ReadCheckedPrefix', replace(s, phase='metadata' if good else 'finish',
                                               result='' if good else 'unknown')
        elif s.phase == 'metadata':
            phase, result = self.metadata_plan(s.case)
            yield 'ReadCacheMetadata', replace(s, phase=phase, result=result,
                publications=1 if self.fault == 'publish_fresh_early' else 0)
        elif s.phase == 'payload':
            yield 'ReadOpaqueOutcome', replace(s, phase='digest', payload_reads=1)
        elif s.phase == 'digest':
            outcome = 'replay' if s.case[8] == 0 or self.fault == 'ignore_digest' else 'unknown'
            yield 'VerifyExactOutcome', replace(s, phase='finish', result=outcome)
        elif s.phase == 'finish':
            yield 'CompleteClassifications', replace(s, phase='abort' if s.result == 'unknown' else 'publish')
        elif s.phase == 'publish':
            yield 'PublishFreshSibling', replace(s, phase='done', publications=1)
        elif s.phase == 'abort':
            yield 'ReturnUnknown', replace(s, phase='done')
    def violation(self, before, action, after):
        if action == 'ReadCheckedPrefix' and not before.case[-1] and after.phase != 'finish':
            return 'O-CUT'
        if action == 'ReadCacheMetadata' and not self.metadata_valid(before.case) and after.result != 'unknown':
            return 'O-META'
        if action != 'PublishFreshSibling' and after.publications > before.publications:
            return 'O-BATCH'
        if action == 'ReadOpaqueOutcome' and not self.payload_allowed(before.case):
            return 'O-PAYLOAD'
        if action == 'CompleteClassifications' and before.result != self.expected(before.case):
            return 'O-RESULT'
        if action == 'ReturnUnknown' and after.publications != 0:
            return 'O-BATCH'
        return None
