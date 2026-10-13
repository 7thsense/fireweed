"""Finite state-proof ordering at a checked cut; not production correspondence.

History faults are realizable requested-key predecessor disagreements. Empty
proofs cannot witness requested-key mutations. Digests/sizes are abstract symbols.
"""
from dataclasses import dataclass, replace
from itertools import product
from models import Model

VECTORS = (((),()), ((0,),()), ((0,1),(0,)), ((0,1),(1,0)),
           ((0,1),(0,1)), ((0,1,0),(0,1,0)), ((0,),(0,)))
# Source 0=never seen; 1=deleted with retained HWM; 2=live with HWM.
# HWM and output markers are abstract classes, not concrete executable versions.
# History 1=wrong expected existence with correct predecessor HWM;
# history 2=wrong predecessor HWM with absent expectation.
CASES = tuple(case for case in product(range(len(VECTORS)),range(3),range(3),
    range(2),range(3),range(4),range(2))
    if (case[4]==0 or VECTORS[case[0]][0])
    and (case[4]!=1 or case[1]!=0)
    and (case[4]!=2 or case[1]!=2)
    and (case[2]!=2 or case[1]!=0)
    and (case[5]==0 or case[1]==2)
    and (case[6]==0 or VECTORS[case[0]][1]))
FAULTS = {'skip_expected':'S-HISTORY','skip_prior':'S-HISTORY',
    'skip_vector':'S-SCOPE','empty_skips_scope':'S-SCOPE',
    'skip_metadata':'S-METADATA','skip_body_digest':'S-PAYLOAD',
    'skip_fields_digest':'S-PAYLOAD','cache_inflation_too_large':'S-BUDGET',
    'allocate_before_source_size':'S-BUDGET','reverse_results':'S-ORDER'}

@dataclass(frozen=True)
class State:
    case: tuple = ()
    phase: str = 'choose'
    fetched: bool = False
    result: str = ''
    values: tuple = ()

class StateAuthority(Model):
    name = 'api007-addressed-state-authority'
    initial = State()
    properties = ['S-HISTORY','S-SCOPE','S-METADATA','S-PAYLOAD','S-BUDGET','S-ORDER']
    bounds = {'keys':2,'vector_pairs':len(VECTORS),'source_states':3,'cases':len(CASES),
              'frontier':'completed classification for every selected finite case'}
    assumptions = ['one complete structurally checked source prefix and exact cut',
        'history fault denotes one requested-key command after a known predecessor',
        'expected-existence faults exclude never-seen HWM (wire forbids Some(0))',
        'wrong-HWM faults use absent expectation (wire binds a present expected version to prior HWM)',
        'scope flag abstracts job/queue/collection/genesis/progress mismatch',
        'metadata choice denotes exact cache agreement, wrong presence, or wrong HWM',
        'body/field digest symbols are injective; inflation exceeds source-sized metadata bound',
        'source size flag is authoritative exact public response size, not cached size',
        'allocation-order mutant explores otherwise intact live responses only and retains the final size check',
        'no hash implementation/provider/heap/fairness/cleanup or public-backend correspondence proof']
    def __init__(self,fault=None):
        if fault is not None and fault not in FAULTS: raise ValueError('unknown state-authority fault')
        self.fault=fault
    def frontier(self,state): return state.phase=='done'
    @staticmethod
    def expected(case):
        vector,source,metadata,foreign,history,payload,oversize=case
        proof,request=VECTORS[vector]
        if history or foreign or proof!=request: return 'unknown',()
        if oversize: return 'too_large',()
        if request and (metadata or (source==2 and payload)): return 'unknown',()
        return 'ok',tuple((key,'live-hwm') if source==2 else None for key in request)
    def transitions(self,state):
        if state.phase=='choose':
            for index,case in enumerate(CASES):
                if self.fault=='allocate_before_source_size':
                    vector,source,metadata,foreign,history,payload,_=case
                    proof,request=VECTORS[vector]
                    if source!=2 or metadata or foreign or history or payload or proof!=request or not request: continue
                yield 'SelectCase-'+str(index),replace(state,case=case,phase='history')
            return
        vector,source,metadata,foreign,history,payload,oversize=state.case
        proof,request=VECTORS[vector]
        if state.phase=='history':
            valid=history==0 or (history==1 and self.fault=='skip_expected') or (history==2 and self.fault=='skip_prior')
            if not valid: yield 'RejectSourceHistory',replace(state,phase='done',result='unknown')
            else: yield 'SealStateProof',replace(state,phase='scope')
        elif state.phase=='scope':
            valid=(not foreign and (proof==request or self.fault=='skip_vector'))
            if not request and self.fault=='empty_skips_scope': valid=True
            if not valid: yield 'RejectForeignProof',replace(state,phase='done',result='unknown')
            else: yield 'BindWholeProof',replace(state,phase='size')
        elif state.phase=='size':
            if oversize and self.fault!='allocate_before_source_size':
                yield 'RejectSourceOversize',replace(state,phase='done',result='too_large')
            else: yield 'ReadNativeMetadata',replace(state,phase='metadata')
        elif state.phase=='metadata':
            if request and metadata and self.fault!='skip_metadata':
                yield 'RejectCacheMetadata',replace(state,phase='done',result='unknown')
            elif request and payload==3:
                yield 'RejectCacheInflation',replace(state,phase='done',result='too_large' if self.fault=='cache_inflation_too_large' else 'unknown')
            elif request and (source==2 or metadata):
                yield 'FetchStatePayload',replace(state,phase='payload',fetched=True)
            elif oversize: yield 'RejectFinalOversize',replace(state,phase='done',result='too_large')
            else: yield 'ReturnAbsentPositions',replace(state,phase='done',result='ok',values=tuple(None for _ in request))
        elif state.phase=='payload':
            if metadata or (payload==1 and self.fault!='skip_body_digest') or (payload==2 and self.fault!='skip_fields_digest'):
                yield 'RejectCachePayload',replace(state,phase='done',result='unknown')
            else:
                if oversize:
                    yield 'RejectFinalOversize',replace(state,phase='done',result='too_large')
                    return
                values=tuple((key,'live-hwm') for key in request)
                if self.fault=='reverse_results': values=tuple(reversed(values))
                yield 'ReturnLivePositions',replace(state,phase='done',result='ok',values=values)
    def violation(self,before,action,after):
        if not after.case: return None
        vector,source,metadata,foreign,history,payload,oversize=after.case
        proof,request=VECTORS[vector]
        if action=='SealStateProof' and history: return 'S-HISTORY'
        if action=='BindWholeProof' and (foreign or proof!=request): return 'S-SCOPE'
        if action=='FetchStatePayload':
            if metadata: return 'S-METADATA'
            if oversize: return 'S-BUDGET'
        if after.phase=='done':
            expected,values=self.expected(after.case)
            if after.result!=expected:
                if oversize: return 'S-BUDGET'
                if payload==3 and after.result=='too_large': return 'S-BUDGET'
                return 'S-PAYLOAD'
            if after.result=='ok' and after.values!=values: return 'S-ORDER'
        return None
