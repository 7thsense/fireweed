"""Independent finite ordinary-pack preparation model, not publication authority.

Three logical groups, one addressed state/work key, one unique tuple, abstract
frame lengths. Oracles describe sequential results independently of faults.
Provider/RAM/wire correctness and root publication/retry are not established.
"""
from dataclasses import dataclass, replace
from models import Model

FAULTS = {'skip_boundaries':'P-ORDER','hydrate_before_originals':'P-ORIGINAL',
          'reset_overlay':'P-SEQUENTIAL','mutate_rejected':'P-SEQUENTIAL',
          'reload_unique_owner':'P-UNIQUE','suppress_required_fields':'P-FIELDS',
          'fetch_unused_body':'P-KNOWLEDGE','omit_wrapper':'P-LIMIT',
          'continue_after_overflow':'P-SPLIT','resample_prefix':'P-CLOCK',
          'sort_logical_outputs':'P-ORDER','unsorted_physical':'P-CANONICAL',
          'ack_provisional':'P-ACK','resource_as_size':'P-ADMISSION',
          'drop_physical':'P-CORRESPONDENCE','bad_receipt_outcomes':'P-RECEIPTS',
          'reset_emitted_state':'P-STATE','leak_overflow_state':'P-STATE'}

@dataclass(frozen=True)
class State:
    phase: str = 'choose'
    case: str = ''
    step: int = 0
    version: int = 1
    live: bool = True
    owner: str = 'A'
    admitted: bool = False
    classified: bool = False
    hydrated: bool = False
    fields_loaded: bool = False
    receipts: tuple = ()
    clocks: tuple = ()
    calls: int = 0
    record_bytes: int = 0
    physical: tuple = ()
    terminal: str = ''
    acknowledged: bool = False
    accepted_version: int = 1
    accepted_live: bool = True
    accepted_owner: str = 'A'
    accepted_receipts: tuple = ()
    accepted_clocks: tuple = ()

CASES = ('normal','aba','unique','check-union','corrupt-fields','check-only','known',
         'fresh-prefix','noncontiguous','barrier','invalid-admin','split-shrinking',
         'wrapper-short','admission-failure')
INVALID = {'fresh-prefix','noncontiguous','barrier','invalid-admin'}
IDS = ('z','a','β')
EXPECTED = {'normal':(2,'VersionConflict',3),'aba':(2,'Check',3),
            'unique':('Release','Claim','UniqueConflict')}

class PackPreparation(Model):
    name='api007-ordinary-pack-preparation'
    initial=State()
    bounds={'groups':3,'addressed_keys':1,'unique_tuples':1,'clock_samples':3,
            'bytes':'abstract explicit frame lengths plus ten-byte wrapper',
            'frontier':'all inputs end Prepared, SingleGroup, Reclassify, Invalid or Unknown'}
    properties=sorted(set(FAULTS.values()))
    assumptions=['sealed exact full-batch source proof and schema; separate authority models',
        'native cache reads verified for the declared fields; opaque field/receipt digests',
        'three-group sequential semantics explicitly enumerated in independent oracles',
        'no root publication/retry, wire encoding, provider durability, unbounded concurrency or RAM claim']
    def __init__(self,fault=None):
        if fault is not None and fault not in FAULTS: raise ValueError('unknown pack preparation fault')
        self.fault=fault
    def frontier(self,s): return s.phase=='done'
    def transitions(self,s):
        if self.frontier(s): return
        if s.phase=='choose':
            for case in CASES: yield 'Choose:'+case,replace(s,case=case,phase='validate')
        elif s.phase=='validate':
            if s.case in INVALID and self.fault!='skip_boundaries':
                yield 'RejectExecutionBoundary',replace(s,phase='done',terminal='Invalid')
            elif s.case=='admission-failure':
                yield 'RejectAdmission',replace(s,phase='done',terminal='SingleGroup' if self.fault=='resource_as_size' else 'Unknown')
            else: yield 'Admit',replace(s,phase='classify',admitted=True)
        elif s.phase=='classify':
            if self.fault=='hydrate_before_originals':
                yield 'HydrateBeforeOriginals',replace(s,hydrated=True,phase='hydrate')
            elif s.case=='known':
                yield 'ClassifyKnown',replace(s,classified=True,phase='done',terminal='Reclassify')
            else: yield 'ClassifyWholeBatch',replace(s,classified=True,phase='hydrate')
        elif s.phase=='hydrate':
            if self.fault=='fetch_unused_body' and s.case=='check-only':
                yield 'FetchUnusedBody',replace(s,phase='done',terminal='Unknown')
            elif s.case=='corrupt-fields' and self.fault!='suppress_required_fields':
                yield 'RejectCorruptRequiredFields',replace(s,phase='done',terminal='Unknown')
            else:
                yield 'HydrateReadUnion',replace(s,phase='plan',hydrated=True,
                    fields_loaded=s.case!='check-only' and self.fault!='suppress_required_fields')
        elif s.phase=='plan':
            version=1 if self.fault=='reset_overlay' else s.version
            live=s.live;owner=s.owner
            if s.case=='aba':
                if s.step==0: version+=1;live=False;outcome=version
                elif s.step==1: outcome='Check'
                else: version+=1;live=True;outcome=version
            elif s.case=='unique':
                if self.fault=='reload_unique_owner': owner='A'
                if s.step==0: owner='';version=2;outcome='Release'
                elif s.step==1:
                    outcome='Claim' if not owner else 'UniqueConflict'
                    if not owner: owner='B'
                else: outcome='UniqueConflict' if owner else 'Claim'
            elif s.case in ('check-union','corrupt-fields'):
                if s.step in (0,2): outcome='Check'
                else: version+=1;outcome=version
            elif s.case=='check-only': outcome='Check'
            elif s.case=='split-shrinking':
                version+=1;outcome=version
                if s.step==2: live=False
            else:
                expected=(1,1,2)[s.step]
                if version==expected: version+=1;outcome=version
                else:
                    outcome='VersionConflict'
                    if self.fault=='mutate_rejected': version+=1
            yield 'PlanGroup',replace(s,phase='measure',version=version,live=live,owner=owner,
                receipts=s.receipts+((IDS[s.step],outcome),),clocks=s.clocks+(s.calls,),calls=s.calls+1)
        elif s.phase=='measure':
            frame=(120,190,65)[s.step] if s.case=='split-shrinking' else 50+20*(s.step+1)
            complete=frame+10
            cap=130 if s.case=='split-shrinking' else 79 if s.case=='wrapper-short' else 1000
            measured=frame if self.fault=='omit_wrapper' else complete
            if measured>cap and self.fault!='continue_after_overflow':
                clocks=s.accepted_clocks;calls=s.calls
                if self.fault=='resample_prefix' and clocks: clocks=(calls,)+clocks[1:];calls+=1
                yield 'SplitWholeGroup',replace(s,phase='emit',accepted_clocks=clocks,calls=calls,
                    terminal='Prepared' if s.accepted_receipts else 'SingleGroup')
            else:
                yield 'AcceptPrefix',replace(s,record_bytes=complete,step=s.step+1,
                    accepted_version=s.version,accepted_live=s.live,accepted_owner=s.owner,
                    accepted_receipts=s.receipts,accepted_clocks=s.clocks,
                    phase='emit' if s.step==2 else 'plan',terminal='Prepared' if s.step==2 else '')
        elif s.phase=='emit':
            receipts=tuple(sorted(s.accepted_receipts)) if self.fault=='sort_logical_outputs' else s.accepted_receipts
            if self.fault=='bad_receipt_outcomes': receipts=tuple((identity,'BAD') for identity,_ in receipts)
            physical=receipts if self.fault=='unsorted_physical' else tuple(sorted(receipts))
            if self.fault=='drop_physical': physical=()
            version=s.accepted_version
            if self.fault=='reset_emitted_state': version=1
            if self.fault=='leak_overflow_state' and s.case=='split-shrinking': version=s.version
            yield 'ReturnPreparation',replace(s,phase='done',receipts=receipts,physical=physical,
                version=version,live=s.accepted_live,owner=s.accepted_owner,clocks=s.accepted_clocks,
                acknowledged=self.fault=='ack_provisional')
    def violation(self,before,action,after):
        if action=='Admit' and before.case in INVALID: return 'P-ORDER'
        if action=='RejectAdmission' and after.terminal!='Unknown': return 'P-ADMISSION'
        if action=='HydrateBeforeOriginals' and not before.classified: return 'P-ORIGINAL'
        if action=='FetchUnusedBody': return 'P-KNOWLEDGE'
        if action=='PlanGroup':
            if before.case in ('check-union','corrupt-fields') and before.step==1 and not before.fields_loaded: return 'P-FIELDS'
            if before.case in ('normal','aba'):
                expected=EXPECTED[before.case][before.step]
                if after.receipts[-1][1]!=expected: return 'P-SEQUENTIAL'
                if before.case=='normal' and before.step==1 and after.version!=2: return 'P-SEQUENTIAL'
            if before.case=='unique' and after.receipts[-1][1]!=EXPECTED['unique'][before.step]: return 'P-UNIQUE'
        if action=='ReturnPreparation':
            if after.acknowledged: return 'P-ACK'
            if tuple(identity for identity,_ in after.receipts)!=IDS[:len(after.receipts)]: return 'P-ORDER'
            if after.physical!=tuple(sorted(after.physical)): return 'P-CANONICAL'
            if after.clocks!=tuple(range(len(after.receipts))): return 'P-CLOCK'
            if after.case=='split-shrinking' and len(after.receipts)!=1: return 'P-SPLIT'
            if after.case=='wrapper-short' and (after.terminal!='SingleGroup' or after.record_bytes): return 'P-LIMIT'
            expected_outcomes = EXPECTED.get(after.case,
                ('Check',2,'Check') if after.case in ('check-union','corrupt-fields') else
                ('Check','Check','Check') if after.case=='check-only' else
                (2,) if after.case=='split-shrinking' else
                () if after.case=='wrapper-short' else EXPECTED['normal'])
            expected_receipts=tuple(zip(IDS,expected_outcomes))
            if after.receipts!=expected_receipts: return 'P-RECEIPTS'
            if after.physical!=tuple(sorted(expected_receipts)): return 'P-CORRESPONDENCE'
            expected_version=2 if after.case in ('unique','check-union','corrupt-fields','split-shrinking') else 1 if after.case in ('check-only','wrapper-short') else 3
            expected_owner='B' if after.case=='unique' else 'A'
            if (after.version,after.live,after.owner)!=(expected_version,True,expected_owner): return 'P-STATE'
        return None
