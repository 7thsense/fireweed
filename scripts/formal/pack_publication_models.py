"""Finite Root protocol: atomic physical packs, immutable logical originals.

Three independent addressed effects and enumerated provider/race schedules.
This establishes only the bounded protocol, not wire/RAM/provider durability.
"""
from dataclasses import dataclass, replace
from models import Model

CASES=('success','lost-reply','absent','middle-match','middle-conflict','outside-match',
       'fence-only','split','regression','replay-failure','single-conflict','single-expiry')
FAULTS={'provisional_ack':'R-ACK','old_fence_retry':'R-FENCE','forget_floor':'R-FLOOR',
        'forget_outside':'R-SIBLINGS',
        'consumed_means_absent':'R-EFFECT','lose_single_cut':'R-FLOOR',
        'duplicate_effect':'R-EFFECT','sort_outputs':'R-ORDER','drop_receipt':'R-RECEIPTS','bypass_final_resolution':'R-ACK','noncontiguous_pack':'R-PACK','drop_physical':'R-SOURCE','drop_source_originals':'R-SOURCE'}
IDS=('z','a','β')

@dataclass(frozen=True)
class State:
    phase:str='choose'
    case:str=''
    cut:int=2
    floor:int=2
    epoch:int=1
    prepared_epoch:int=1
    known:tuple=(False,False,False)
    finalized:tuple=('','','')
    effects:tuple=(0,0,0)
    submitted:tuple=(0,1,2)
    physical:tuple=()
    fresh_plan:bool=True
    acknowledged:bool=False
    output:tuple=()
    terminal:str=''
    round:int=0
    checked_cut:int=2
    known_floor:int=2
    source_known:tuple=(False,False,False)
    confirmed:tuple=(False,False,False)
    confirmed_head:int=2

class PackPublication(Model):
    name='api007-root-pack-publication'
    initial=State()
    bounds={'logical_groups':3,'initial_cut':2,'physical_packs':3,
            'provider_schedules':list(CASES),'retry':'one representative replay failure and fresh continuation'}
    properties=sorted(set(FAULTS.values()))
    assumptions=['complete-batch source proof/classification validated by separate models',
       'independent addressed effects; receipts are abstract opaque source-verified values',
       'atomic conditional publication per physical pack; enumerated ambiguous replies and races',
       'stale-overlay semantic replanning is covered separately; this model proves only publication protocol',
       'no universal fairness/deadlines, wire/RAM/provider implementation or throughput claim']
    def __init__(self,fault=None):
        if fault is not None and fault not in FAULTS: raise ValueError('unknown publication fault')
        self.fault=fault
    def frontier(self,s): return s.phase=='done'
    def transitions(self,s):
        if self.frontier(s): return
        if s.phase=='choose':
            for case in CASES: yield 'Choose:'+case,replace(s,case=case,phase='prepare')
        elif s.phase=='prepare':
            submitted=(0,) if s.case=='split' else (0,1) if s.case=='outside-match' else (0,1,2)
            yield 'PrepareActualPrefix',replace(s,phase='publish',submitted=submitted,
                acknowledged=False)
        elif s.phase=='publish':
            known=list(s.known);effects=list(s.effects);physical=s.physical;cut=s.cut
            if s.round or s.case in ('success','lost-reply','split','regression','replay-failure'):
                for p in s.submitted: known[p]=True;effects[p]+=1
                physical+=(s.submitted,);cut+=1
            elif s.case in ('middle-match','middle-conflict','outside-match','single-conflict','single-expiry'):
                p=2 if s.case=='outside-match' else 1 if s.case.startswith('middle') else 0
                known[p]=True;effects[p]+=1;physical+=((p,),);cut+=1
            phase='ack' if s.round and self.fault=='bypass_final_resolution' else 'recover'
            finalized=s.finalized
            if phase=='ack': finalized=tuple('Committed' if p in s.submitted else outcome for p,outcome in enumerate(finalized))
            yield 'ConditionalPublication',replace(s,phase=phase,known=tuple(known),effects=tuple(effects),physical=() if self.fault=='drop_physical' else physical,cut=cut,round=s.round+1,finalized=finalized,acknowledged=self.fault=='provisional_ack',source_known=(False,False,False) if self.fault=='drop_source_originals' else tuple(known))
        elif s.phase=='recover':
            target=1 if s.case=='regression' and s.round==1 else s.cut
            if target<s.floor:
                if self.fault=='forget_floor':
                    yield 'ColdRegression',replace(s,phase='replay',cut=target,floor=target,known=(False,False,False),epoch=2)
                else: yield 'RejectRegression',replace(s,phase='done',terminal='Unknown')
            elif s.case=='replay-failure' and s.round==1:
                yield 'RecoverThenReplayFailure',replace(s,phase='retry',floor=target,epoch=s.epoch+1,checked_cut=target,known_floor=max(s.known_floor,target))
            else: yield 'RecoverCompleteCut',replace(s,phase='replay',floor=target,epoch=s.epoch if self.fault=='old_fence_retry' else s.epoch+1,checked_cut=target,known_floor=max(s.known_floor,target))
        elif s.phase=='retry':
            target=1 if self.fault=='forget_floor' else s.cut
            yield 'ContinueFromCheckedFloor',replace(s,phase='replay',cut=target,floor=target,epoch=s.epoch+1,checked_cut=target,known_floor=max(s.known_floor,target))
        elif s.phase=='replay':
            finalized=list(s.finalized)
            for p,known in enumerate(s.known):
                if known:
                    if self.fault=='forget_outside' and p not in s.submitted: continue
                    finalized[p]='Conflict' if (s.case=='middle-conflict' and p==1) or (s.case=='single-conflict' and p==0) else 'Expired' if s.case=='single-expiry' and p==0 else 'Committed'
            yield 'ClassifyEveryUnresolvedSibling',replace(s,phase='head',finalized=tuple(finalized),confirmed=s.known)
        elif s.phase=='head':
            yield 'ConfirmFreshReplayHead',replace(s,phase='resolve',confirmed_head=s.cut)
        elif s.phase=='resolve':
            if s.case.startswith('single-') and self.fault=='lose_single_cut':
                yield 'DiscardClassificationCut',replace(s,phase='replan',floor=0)
            elif all(s.finalized): yield 'AllOriginalsKnown',replace(s,phase='ack')
            else:
                yield 'FenceAbsentThenReplan',replace(s,phase='replan',fresh_plan=True)
        elif s.phase=='replan':
            missing=tuple(p for p in range(3) if not s.finalized[p])
            first=missing[0]
            limit=1 if s.case=='split' else 3
            selected=[]
            for p in range(first, min(3,first+limit)):
                if s.finalized[p]: break
                selected.append(p)
            if self.fault=='noncontiguous_pack': selected=list(missing)
            if self.fault=='consumed_means_absent' and any(s.finalized): selected=list(range(3))
            if self.fault=='duplicate_effect': selected=selected+selected[:1]
            yield 'PrepareEarliestContiguousRemainder',replace(s,phase='publish',submitted=tuple(selected),prepared_epoch=s.epoch)
        elif s.phase=='ack':
            output=tuple(zip(IDS,s.finalized))
            if self.fault=='sort_outputs': output=tuple(sorted(output))
            if self.fault=='drop_receipt': output=output[:-1]
            yield 'AcknowledgeSourceReceipts',replace(s,phase='done',terminal='Success',acknowledged=True,output=output)
    def violation(self,before,action,after):
        if after.cut<before.known_floor or after.floor<before.known_floor or after.known_floor<before.known_floor: return 'R-FLOOR'
        if after.acknowledged and (after.phase!='done' or not all(after.finalized) or after.checked_cut!=after.cut or after.confirmed_head!=after.cut or after.confirmed!=after.source_known): return 'R-ACK'
        counts=tuple(sum(record.count(p) for record in after.physical) for p in range(3))
        if counts!=after.effects or tuple(n>0 for n in counts)!=after.source_known: return 'R-SOURCE'
        if any(n>1 for n in after.effects): return 'R-EFFECT'
        if action=='PrepareActualPrefix' and after.acknowledged: return 'R-ACK'
        if action in ('ColdRegression','ContinueFromCheckedFloor') and after.cut<before.floor: return 'R-FLOOR'
        if action=='FenceAbsentThenReplan':
            if before.epoch<=before.prepared_epoch: return 'R-FENCE'
            if any(known and not after.finalized[p] for p,known in enumerate(after.known)): return 'R-SIBLINGS'
        if action=='ConditionalPublication' and after.round>1 and any(a+1!=b for a,b in zip(before.submitted,before.submitted[1:])): return 'R-PACK'
        if after.acknowledged:
            if after.checked_cut!=after.cut or after.confirmed!=after.source_known: return 'R-ACK'
            if tuple(p for p,_ in after.output)!=IDS:
                return 'R-ORDER' if len(after.output)==3 else 'R-RECEIPTS'
            expected=('Expired' if after.case=='single-expiry' else 'Conflict' if after.case=='single-conflict' else 'Committed',
                      'Conflict' if after.case=='middle-conflict' else 'Committed','Committed')
            if after.output!=tuple(zip(IDS,expected)) or after.effects!=(1,1,1): return 'R-RECEIPTS'
        return None
