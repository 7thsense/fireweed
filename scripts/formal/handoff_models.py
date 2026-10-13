"""Finite same-operation handoff specification; no production imports.

Three initially fresh IDs, one foreign publication, one fence and one raced
same-ID original. Each recovery reads a full prefix; one adverse outcome can lack a newer fence.
Opaque receipt agreement is assumed here and checked by the originals model.
"""
from dataclasses import dataclass, replace
from models import Model

FAULTS = {'skip_head': 'H-HANDOFF', 'wrong_position': 'H-SCOPE',
          'partial_prefix': 'H-CUT', 'ignore_epoch': 'H-CAS',
          'provisional_response': 'H-RESOLVE', 'discard_batch_scope': 'H-SCOPE',
          'unfenced_reprepare': 'H-FENCE', 'ignore_head': 'H-CAS'}

@dataclass(frozen=True)
class State:
    group: int = 0
    head: int = 0
    epoch: int = 1
    originals: tuple = (False, False, False)
    phase: str = 'read'
    cut: tuple = ()  # head, acquired fence, full ordered original presence
    complete: bool = False
    scope: int = 0
    position: int = -1
    prepared_head: int = -1
    prepared_epoch: int = -1
    submitted_epoch: int = -1
    acknowledged: tuple = ()
    recoveries: int = 0
    foreign: bool = False
    fenced: bool = False
    raced: bool = False
    non_newer: bool = False

class Handoff(Model):
    name = 'api007-same-operation-batch-handoff'
    initial = State()
    bounds = {'groups':3,'foreign_publications':1,'extra_fences':1,'raced_originals':1,'non_newer_fence_results':1,
              'frontier':'terminal done states; no environmental-bound truncation of started protocol'}
    properties = ['H-HANDOFF','H-SCOPE','H-CUT','H-CAS','H-RESOLVE','H-FENCE']
    assumptions = ['unique literal IDs in one admitted operation, address and genesis',
        'full ordered source receipt descriptors at every acquired cut',
        'native original bytes agree with source; separate originals model covers corruption',
        'source head and fence CAS are atomic',
        'one checked post-submission prefix may lack a newer acquired fence',
        'no process crash, allocation failure, provider durability or fairness claim']
    def __init__(self, fault=None):
        if fault is not None and fault not in FAULTS:
            raise ValueError('unknown handoff fault')
        self.fault = fault
    def frontier(self,s):
        return s.phase == 'done'
    def transitions(self,s):
        if self.frontier(s): return
        if not s.foreign:
            yield 'ForeignPublication', replace(s,head=s.head+1,foreign=True)
        if not s.fenced:
            yield 'CompetingFence', replace(s,epoch=s.epoch+1,fenced=True)
        if not s.raced and not s.originals[s.group]:
            original=list(s.originals);original[s.group]=True
            yield 'RacedOriginal',replace(s,head=s.head+1,epoch=s.epoch+1,originals=tuple(original),raced=True)
        if s.phase in ('read','post-read'):
            post=s.phase=='post-read'
            if post and not s.non_newer and s.submitted_epoch >= 0:
                yield 'PostReadWithoutNewFence', replace(s,cut=(s.head,s.submitted_epoch,s.originals),
                    complete=True,scope=0,recoveries=s.recoveries+1,phase='resolve',non_newer=True)
            cut=(s.head,s.epoch+1,s.originals)
            yield 'PostRecoverFullPrefix' if post else 'RecoverFullPrefix', replace(s,
                epoch=s.epoch+1,cut=cut,complete=self.fault!='partial_prefix',scope=0,
                recoveries=s.recoveries+1,phase='resolve' if post else 'choose')
        elif s.phase=='choose':
            if s.cut[0]!=s.head and self.fault!='skip_head':
                yield 'DiscardStaleHandoff',replace(s,cut=(),complete=False,phase='read')
            else:
                yield 'BindDeclaredPosition',replace(s,phase='prepare',
                    position=(s.group+1)%3 if self.fault=='wrong_position' else s.group,
                    scope=1 if self.fault=='discard_batch_scope' and s.group else s.scope)
        elif s.phase=='prepare':
            if s.cut[2][s.group]:
                yield 'KnownOriginal',replace(s,phase='post-read',submitted_epoch=-1)
            else:
                yield 'PrepareNative',replace(s,phase='submit',prepared_head=s.cut[0],prepared_epoch=s.cut[1])
        elif s.phase=='submit':
            accepted=(s.prepared_head==s.head or self.fault=='ignore_head') and (s.prepared_epoch==s.epoch or self.fault=='ignore_epoch')
            original=list(s.originals)
            if accepted: original[s.group]=True
            yield 'ConditionalPublish' if accepted else 'ConditionalReject',replace(s,
                head=s.head+int(accepted),originals=tuple(original),submitted_epoch=s.prepared_epoch,
                phase='provisional' if accepted and self.fault=='provisional_response' else 'post-read')
        elif s.phase=='provisional':
            yield 'ReturnProvisional',replace(s,phase='done',acknowledged=s.acknowledged+(s.group,))
        elif s.phase=='resolve':
            if s.cut[0]!=s.head:
                yield 'DiscardStaleReplay',replace(s,cut=(),complete=False,phase='post-read')
            elif s.cut[2][s.group]:
                yield 'ResolveOriginal',replace(s,group=s.group+1,phase='done' if s.group==2 else 'choose',
                    acknowledged=s.acknowledged+(s.group,),position=-1)
            elif s.submitted_epoch>=0 and (s.cut[1]>s.submitted_epoch or self.fault=='unfenced_reprepare'):
                yield 'ReprepareFencedAbsent',replace(s,phase='choose',position=-1)
            else:
                yield 'ReturnUnknown',replace(s,phase='done')
    def violation(self,before,action,after):
        if action=='BindDeclaredPosition':
            if not before.complete: return 'H-CUT'
            if before.cut[0]!=before.head: return 'H-HANDOFF'
            if after.scope!=0 or after.position!=before.group: return 'H-SCOPE'
        if action=='ConditionalPublish':
            if before.prepared_head!=before.head or before.prepared_epoch!=before.epoch:
                return 'H-CAS'
            if before.originals[before.group]: return 'H-CAS'
        if action in ('ResolveOriginal','ReturnProvisional'):
            if not before.complete or before.cut[0]!=before.head or not before.cut[2][before.group]:
                return 'H-RESOLVE'
            if after.acknowledged!=tuple(range(after.group)): return 'H-RESOLVE'
        if action=='ReprepareFencedAbsent':
            if not before.complete or before.cut[2][before.group] or before.cut[1]<=before.submitted_epoch:
                return 'H-FENCE'
        return None
