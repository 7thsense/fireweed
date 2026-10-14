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
