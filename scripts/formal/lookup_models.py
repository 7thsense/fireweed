"""Finite ordered receipt-proof safety at one independently checked cut.

Aliases are request positions, never duplicate source originals. Scope includes
empty vectors. Digest/metadata and head freshness are separate bounded models.
"""
from dataclasses import dataclass, replace
from itertools import product
from models import Model

VECTORS = ((), (0,), (1,), (0, 1), (1, 0), (0, 0), (0, 1, 0))
CASES = tuple(case for case in product(VECTORS, VECTORS, range(4), range(2), range(2))
              if not case[4] or any(case[2] & (1 << identity) for identity in case[0]))
FAULTS = {'skip_vector': 'L-SCOPE', 'empty_skips_scope': 'L-SCOPE',
          'deduplicate_aliases': 'L-ORDER', 'reverse_results': 'L-ORDER',
          'accept_duplicate_source': 'L-SOURCE'}

@dataclass(frozen=True)
class State:
    case: tuple = ()
    phase: str = 'choose'
    result: tuple = ()
    accepted: bool = False

class Lookups(Model):
    name = 'api007-ordered-receipt-lookup'
    initial = State()
    properties = ['L-SCOPE', 'L-ORDER', 'L-SOURCE']
    bounds = {'literal_ids': 2, 'vectors': len(VECTORS), 'cases': len(CASES),
              'frontier': 'completed decision for every selected case'}
    assumptions = ['complete independently checked immutable source prefix',
        'scope boolean abstracts domain/job/genesis/exact-cut equality',
        'two presence bits denote exact checked source originals',
        'source duplicate boolean denotes a second requested source original',
        'digest and metadata semantics covered by separate originals model',
        'no provider/RAM/fairness/public-backend correspondence claim']
    def __init__(self, fault=None):
        if fault is not None and fault not in FAULTS: raise ValueError('unknown lookup fault')
        self.fault = fault
    def frontier(self, state): return state.phase == 'done'
    @staticmethod
    def expected(case):
        proof, request, mask, foreign, duplicate = case
        if proof != request or foreign or duplicate: return None
        return tuple(identity if mask & (1 << identity) else None for identity in request)
    def transitions(self, state):
        if state.phase == 'choose':
            for index, case in enumerate(CASES):
                yield 'SelectCase-' + str(index), replace(state, case=case, phase='validate')
        elif state.phase == 'validate':
            proof, request, mask, foreign, duplicate = state.case
            vector_ok = proof == request or self.fault == 'skip_vector'
            scope_ok = not foreign
            if self.fault == 'empty_skips_scope' and not request: vector_ok = scope_ok = True
            source_ok = not duplicate or self.fault == 'accept_duplicate_source'
            if not (vector_ok and scope_ok and source_ok):
                yield 'RejectAuthority', replace(state, phase='done')
            else:
                values = tuple(identity if mask & (1 << identity) else None for identity in request)
                if self.fault == 'deduplicate_aliases':
                    values = tuple(identity if mask & (1 << identity) else None for identity in dict.fromkeys(request))
                if self.fault == 'reverse_results': values = tuple(reversed(values))
                yield 'ReturnOrderedResults', replace(state, phase='done', accepted=True, result=values)
    def violation(self, before, action, after):
        if action != 'ReturnOrderedResults': return None
        proof, request, _, foreign, duplicate = after.case
        if proof != request or foreign: return 'L-SCOPE'
        if duplicate: return 'L-SOURCE'
        if after.result != self.expected(after.case): return 'L-ORDER'
        return None
