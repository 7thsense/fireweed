"""Independent finite proof-domain/literal-ID binding for one checked source cut.

One literal can exist independently in insert, claim and commit namespaces.
This model covers proof use, including proved absence; metadata/opaque cache
agreement and fresh-head protocols are covered by separate bounded models.
"""
from dataclasses import dataclass, replace
from itertools import product
from models import Model

CASES = tuple(product(range(8), range(3), range(3), range(2), range(2)))
FAULTS = {'skip_domain': 'D-DOMAIN', 'domain_only_if_present': 'D-DOMAIN',
          'skip_literal_id': 'D-ID', 'claim_wrapper_hash': 'D-RESULT'}

@dataclass(frozen=True)
class State:
    case: tuple = ()
    phase: str = 'choose'
    proof: tuple = ()
    reads: int = 0
    result: str = ''
    hash_kind: int = -1

class Domains(Model):
    name = 'api007-original-domain-binding'
    initial = State()
    properties = ['D-DOMAIN', 'D-ID', 'D-RESULT']
    bounds = {'domains': ['insert', 'claim', 'commit'], 'literal_ids': 2,
              'source_presence_masks': 8, 'cases': len(CASES),
              'frontier': 'terminal done; no truncated in-flight protocol'}
    assumptions = ['one complete independently checked immutable source cut',
        'presence mask denotes the same first literal in each separate domain',
        'second literal has no source original',
        'exact receipt bytes map to injective digest-kind symbols',
        'matching claim receipt bytes differ from the full claim wrapper',
        'metadata/caller precedence is covered by the separate originals model',
        'no concurrent head changes, provider/RAM/depth/fairness qualification']
    def __init__(self, fault=None):
        if fault is not None and fault not in FAULTS: raise ValueError('unknown domain fault')
        self.fault = fault
    def frontier(self, state): return state.phase == 'done'
    @staticmethod
    def expected(case):
        mask, tracked, target, literal, caller = case
        if tracked != target or literal != caller: return 'unknown'
        return 'original' if caller == 0 and mask & (1 << target) else 'fresh'
    def transitions(self, state):
        if state.phase == 'choose':
            for index, case in enumerate(CASES):
                yield 'SelectCase-' + str(index), replace(state,case=case,phase='fold')
        elif state.phase == 'fold':
            mask, tracked, target, literal, caller = state.case
            present = literal == 0 and bool(mask & (1 << tracked))
            yield 'FoldExactSourceReceipt', replace(state,proof=(tracked,literal,present),phase='bind')
        elif state.phase == 'bind':
            _, tracked, target, literal, caller = state.case
            domain_ok = tracked == target or self.fault == 'skip_domain' or (self.fault == 'domain_only_if_present' and not state.proof[2])
            id_ok = literal == caller or self.fault == 'skip_literal_id'
            if not domain_ok or not id_ok:
                yield 'RefuseForeignProof', replace(state,phase='done',result='unknown')
            else:
                yield 'ReadNativeOriginalMetadata', replace(state,phase='result',reads=1)
        elif state.phase == 'result':
            kind = 3 if self.fault == 'claim_wrapper_hash' and state.proof[0] == 1 and state.proof[2] else state.proof[0]
            yield 'ReturnSourceClassification', replace(state,phase='done',result='original' if state.proof[2] else 'fresh',hash_kind=kind)
    def violation(self, before, action, after):
        if action == 'ReadNativeOriginalMetadata':
            if before.proof[0] != before.case[2]: return 'D-DOMAIN'
            if before.proof[1] != before.case[4]: return 'D-ID'
        if action == 'ReturnSourceClassification':
            if after.result != self.expected(after.case): return 'D-RESULT'
            if after.result == 'original' and after.hash_kind != after.case[2]: return 'D-RESULT'
        return None
