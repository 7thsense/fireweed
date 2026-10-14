"""Finite operation-local suffix specification; independent of production code.

One sealed prefix and at most two suffix records. This establishes safety within
these bounds, not implementation correspondence, provider durability or RAM.
"""
from dataclasses import dataclass, replace
from models import Model

FAULTS = {'forget_floor':'C-FLOOR', 'skip_scope':'C-SCOPE',
          'forget_original':'C-DUPLICATE', 'reset_hwm':'C-HWM',
          'use_old_fence':'C-FENCE', 'partial_cut':'C-CUT',
          'relabel_epoch':'C-EPOCH', 'reuse_poison':'C-POISON',
          'suppress_suffix_hwm':'C-DESCRIPTORS', 'drop_final_originals':'C-DESCRIPTORS',
          'forget_retry_floor':'C-FLOOR'}

@dataclass(frozen=True)
class State:
    phase: str = 'choose'
    case: str = ''
    head: int = 2
    epoch: int = 3
    checkpoint: int = 2
    scope: bool = True
    source: bool = True
    originals: tuple = (True, False)
    highwater: int = 4
    historical_epoch: int = 2
    offset: int = 2
    poisoned: bool = False
    acknowledged: bool = False
    acquired: int = 0
    terminal: str = ''
    floor: int = 2
    known_floor: int = 2
    retried: bool = False
    expected_hwm: int = 4
    expected_originals: tuple = (True, False)

class Continuation(Model):
    name = 'api007-batch-local-checked-continuation'
    initial = State()
    bounds = {'prior_records':2, 'suffix_records':2, 'originals':2, 'state_keys':1,
              'continuation_attempts':2, 'frontier':'all cases finish Unknown or verified cut'}
    properties = sorted(set(FAULTS.values()))
    assumptions = ['immutable sealed prefix in one owning operation',
                   'atomic fresh acquisition and complete individually checked records',
                   'opaque original/body/field digests; wire correctness checked separately',
                   'finite safety only; no liveness, RAM or storage-provider claim']
    def __init__(self, fault=None):
        if fault is not None and fault not in FAULTS: raise ValueError('unknown continuation fault')
        self.fault = fault
    def frontier(self,s): return s.phase == 'done'
    def transitions(self,s):
        if self.frontier(s): return
        if s.phase == 'choose':
            for case in ('empty','suffix','deleted-recreated','duplicate','rewind','vanished',
                         'wrong-scope','wrong-source','missing-projection','behind-projection','poison',
                         'retry-valid','retry-rewind'):
                yield 'Choose:'+case, replace(s,phase='acquire',case=case,
                    head=0 if case=='vanished' else 1 if case=='rewind' else 4 if case=='deleted-recreated' else 3 if case in ('suffix','duplicate','poison','retry-valid','retry-rewind') else 2,
                    checkpoint=0 if case=='missing-projection' else 1 if case=='behind-projection' else 2,
                    scope=case!='wrong-scope',source=case!='wrong-source',
                    expected_hwm=6 if case=='deleted-recreated' else 4,
                    expected_originals=(True,True) if case in ('suffix','retry-valid','retry-rewind') else (True,False))
        elif s.phase == 'acquire':
            bad_scope=not s.scope or not s.source
            if bad_scope and self.fault!='skip_scope':
                yield 'RejectScope',replace(s,phase='done',terminal='Unknown');return
            if s.head < s.floor and self.fault!='forget_floor':
                yield 'RejectKnownRegression',replace(s,phase='done',terminal='Unknown');return
            acquired=s.epoch if self.fault=='use_old_fence' else s.epoch+1
            yield 'AcquireFresh',replace(s,phase='scan',acquired=acquired,epoch=acquired,
                offset=0 if s.checkpoint<2 or s.head<s.floor else s.floor,
                originals=(False,False) if self.fault=='forget_original' else s.originals,
                highwater=0 if self.fault=='reset_hwm' else s.highwater)
        elif s.phase == 'scan':
            if s.offset<s.head:
                duplicate=s.case=='duplicate' and s.offset==2
                poison=s.case=='poison' and s.offset==2
                if (duplicate and s.originals[0]) or poison:
                    yield 'RejectSuffix',replace(s,phase='done',terminal='Unknown',poisoned=True)
                    if self.fault=='reuse_poison':
                        yield 'ReusePartialPoison',replace(s,phase='verify',poisoned=True,offset=s.head)
                    return
                yield 'FoldRecord',replace(s,offset=s.offset+1,
                    originals=(True,True) if s.case in ('suffix','retry-valid','retry-rewind') and s.offset==2 else s.originals,
                    highwater=s.highwater if self.fault=='suppress_suffix_hwm' else max(s.highwater,5 if s.case=='deleted-recreated' and s.offset==2 else 6 if s.case=='deleted-recreated' and s.offset==3 else s.highwater),
                    historical_epoch=3 if s.offset>=2 else s.historical_epoch)
                if self.fault=='partial_cut':
                    yield 'SkipRemaining',replace(s,phase='verify')
            else:
                yield 'VerifyCheckpoint',replace(s,phase='verify')
        elif s.phase=='verify':
            if s.case in ('retry-valid','retry-rewind') and not s.retried:
                yield 'CoveredReplayFailure',replace(s,phase='acquire',retried=True,
                    known_floor=s.head,floor=2 if self.fault=='forget_retry_floor' else s.head,
                    head=2 if s.case=='retry-rewind' else s.head)
            else:
                yield 'ResolveCovered',replace(s,phase='done',acknowledged=True,terminal='Covered',
                    originals=(False,False) if self.fault=='drop_final_originals' else s.originals,
                    historical_epoch=s.acquired if self.fault=='relabel_epoch' else s.historical_epoch)
    def violation(self,before,action,after):
        if action=='AcquireFresh':
            if before.head<before.known_floor: return 'C-FLOOR'
            if not before.scope or not before.source: return 'C-SCOPE'
            if after.acquired<=before.epoch: return 'C-FENCE'
            if after.highwater<before.highwater: return 'C-HWM'
        if action=='FoldRecord' and before.case=='duplicate' and before.offset==2: return 'C-DUPLICATE'
        if action=='ResolveCovered':
            if after.poisoned: return 'C-POISON'
            if after.offset!=after.head: return 'C-CUT'
            if after.case=='empty' and after.historical_epoch!=2: return 'C-EPOCH'
            if after.highwater!=before.expected_hwm or after.originals!=before.expected_originals: return 'C-DESCRIPTORS'
        return None

# Separate binding model: the existing continuation graph remains unchanged.
SINGLE_FAULTS = {'wrong_domain':'S-DECLARATION', 'wrong_id':'S-DECLARATION',
                 'wrong_count':'S-DECLARATION', 'wrong_owner':'S-DECLARATION',
                 'wrong_presence':'S-PRESENCE', 'forget_present':'S-PRESENCE'}

@dataclass(frozen=True)
class SingleState:
    phase: str = 'choose'
    case: str = ''
    domain: int = 0
    expected_domain: int = 0
    id_matches: bool = True
    count: int = 1
    owner_matches: bool = True
    descriptor_present: bool = False
    old_present: bool = False
    resumed_present: bool = False
    touched_authority: bool = False
    terminal: str = ''

class SingleOriginalBinding(Model):
    name = 'api007-single-original-continuation-binding'
    initial = SingleState()
    bounds = {'domains':2, 'requested_originals':1, 'declaration_count_cases':[0,1,2],
              'frontier':'complete accepted or Unknown binding'}
    properties = ['S-DECLARATION','S-PRESENCE']
    assumptions = ['sealed digest/cut checked by the shared continuation model',
                   'finite exact-declaration binding only; no wire/provider correspondence']
    def __init__(self, fault=None):
        if fault is not None and fault not in SINGLE_FAULTS: raise ValueError('unknown single fault')
        self.fault=fault
    def frontier(self,s): return s.phase=='done'
    def transitions(self,s):
        if self.frontier(s): return
        if s.phase=='choose':
            for domain in (0,1):
                for case in ('present','absent','domain','id','empty','multiple','owner','presence'):
                    yield f'Choose:{domain}:{case}', replace(s,phase='bind',case=case,
                        domain=1-domain if case=='domain' else domain,expected_domain=domain,
                        id_matches=case!='id',count=0 if case=='empty' else 2 if case=='multiple' else 1,
                        owner_matches=case!='owner',descriptor_present=case in ('present','presence'),
                        old_present=case=='present')
        elif s.phase=='bind':
            rejected=(s.domain!=s.expected_domain and self.fault!='wrong_domain') or (not s.id_matches and self.fault!='wrong_id') or (s.count!=1 and self.fault!='wrong_count') or (not s.owner_matches and self.fault!='wrong_owner') or (s.old_present!=s.descriptor_present and self.fault!='wrong_presence')
            if rejected:
                yield 'RejectBeforeAuthority',replace(s,phase='done',terminal='Unknown')
            else:
                yield 'ResumeChecked',replace(s,phase='done',terminal='Covered',touched_authority=True,
                    resumed_present=False if self.fault=='forget_present' else s.old_present)
    def violation(self,before,action,after):
        if after.touched_authority or after.terminal=='Covered':
            if before.domain!=before.expected_domain or not before.id_matches or before.count!=1 or not before.owner_matches: return 'S-DECLARATION'
            if before.old_present!=before.descriptor_present or after.resumed_present!=before.descriptor_present: return 'S-PRESENCE'
        return None
