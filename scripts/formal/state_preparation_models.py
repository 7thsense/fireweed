"""Independent finite preparation proof: immutable replay and payload knowledge.

Version/field digests are abstract equality symbols. No production correspondence,
provider, heap, unique-index/work integrity, or fairness theorem is claimed.
"""
from dataclasses import dataclass, replace
from itertools import product
from models import Model
# mode: publish/preview; original: absent/present; shape: no-state/Check/Put/Delete;
# binding: exact/foreign; metadata: exact/wrong; fields: exact/wrong/inflated;
# body: exact/missing/inflated. No-state groups have work operations, but no addressed state payload.
CASES = tuple(c for c in product(range(2),range(2),range(4),range(2),range(2),range(3),range(3))
              if c[2] or not any(c[4:]))
FAULTS = {'read_before_original':'P-ORIGINAL','skip_original':'P-ORIGINAL','preview_replays_original':'P-ORIGINAL',
          'skip_binding':'P-SCOPE','empty_skips_binding':'P-SCOPE',
          'skip_metadata':'P-METADATA','skip_fields':'P-FIELDS',
          'require_old_body':'P-KNOWLEDGE','check_loads_fields':'P-KNOWLEDGE',
          'inflation_too_large':'P-BUDGET'}
@dataclass(frozen=True)
class State:
    case: tuple = ()
    phase: str = 'choose'
    result: str = ''
    current_state_touched: bool = False
    fields_loaded: bool = False
    body_loaded: bool = False
class StatePreparation(Model):
    name = 'api007-commit-preview-state-authority'
    initial = State()
    properties = list(dict.fromkeys(FAULTS.values()))
    bounds = {'groups':1,'literal_states':1,'cases':len(CASES),'frontier':'all chosen cases classify completely'}
    assumptions = ['complete checked source prefix and retained tombstone high-water',
        'binding symbol includes exact group/collection/key/queue/genesis/cut/job',
        'fields equality and source-sized metadata are injective abstract symbols',
        'immutable original is valid and complete; preview ignores original identity',
        'Check needs only existence/high-water; Put/Delete need complete old fields',
        'body/field corruption denotes only current cache, never original source',
        'no current-work/unique-owner/index, numeric serializer, provider, heap, cleanup, or fairness proof']
    def __init__(self,fault=None):
        if fault is not None and fault not in FAULTS: raise ValueError('unknown preparation fault')
        self.fault=fault
    def frontier(self,s): return s.phase=='done'
    @staticmethod
    def expected(case):
        preview,original,shape,foreign,metadata,fields,body=case
        if not preview and original: return 'original'
        if foreign or (shape and metadata) or (shape>=2 and fields): return 'unknown'
        return 'prepared'
    def transitions(self,s):
        if s.phase=='choose':
            for i,c in enumerate(CASES): yield 'SelectCase-'+str(i),replace(s,case=c,phase='original')
            return
        preview,original,shape,foreign,metadata,fields,body=s.case
        if s.phase=='original':
            if original and not preview and shape>=2 and self.fault=='read_before_original':
                yield 'FetchOldFieldsBeforeOriginal',replace(s,phase='late-original',current_state_touched=True,fields_loaded=True)
                return
            if original and ((not preview and self.fault!='skip_original') or (preview and self.fault=='preview_replays_original')):
                yield 'ReplayImmutableOriginal',replace(s,phase='done',result='original')
            else: yield 'FreshPreparation',replace(s,phase='binding')
        elif s.phase=='late-original':
            yield 'ReplayImmutableOriginalLate',replace(s,phase='done',result='original')
        elif s.phase=='binding':
            if foreign and self.fault!='skip_binding' and not (not shape and self.fault=='empty_skips_binding'):
                yield 'RejectForeignGroup',replace(s,phase='done',result='unknown')
            else: yield 'BindExactGroup',replace(s,phase='metadata')
        elif s.phase=='metadata':
            s=replace(s,current_state_touched=bool(shape))
            if shape and metadata and self.fault!='skip_metadata':
                yield 'RejectHighwater',replace(s,phase='done',result='unknown')
            elif shape and self.fault=='require_old_body':
                yield 'FetchOldBody',replace(s,phase='body',body_loaded=True)
            elif shape>=2 or (shape==1 and self.fault=='check_loads_fields'):
                if fields==2: yield 'RejectInflatedFields',replace(s,phase='done',result='too_large' if self.fault=='inflation_too_large' else 'unknown')
                else: yield 'FetchOldFields',replace(s,phase='fields',fields_loaded=True)
            else: yield 'PrepareMetadataOnly',replace(s,phase='done',result='prepared')
        elif s.phase=='body':
            yield 'RejectUnusedBody' if body else 'PrepareWithBody',replace(s,phase='done',result='unknown' if body else 'prepared')
        elif s.phase=='fields':
            yield 'RejectFields' if fields and self.fault!='skip_fields' else 'PrepareWithFields',replace(s,phase='done',result='unknown' if fields and self.fault!='skip_fields' else 'prepared')
    def violation(self,before,action,after):
        if not after.case: return None
        preview,original,shape,foreign,metadata,fields,body=after.case
        if original and not preview and after.current_state_touched: return 'P-ORIGINAL'
        if after.body_loaded or (shape<2 and after.fields_loaded): return 'P-KNOWLEDGE'
        if after.phase!='done': return None
        expected=self.expected(after.case)
        if after.result==expected: return None
        if original and ((not preview) or after.result=='original'): return 'P-ORIGINAL'
        if foreign: return 'P-SCOPE'
        if shape and metadata: return 'P-METADATA'
        if after.result=='too_large': return 'P-BUDGET'
        if shape>=2 and fields: return 'P-FIELDS'
        return 'P-KNOWLEDGE'
