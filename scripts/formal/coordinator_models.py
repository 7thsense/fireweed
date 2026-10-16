"""Finite orchestration over immutable provider fixtures; no Rust refinement proof."""
from dataclasses import dataclass, replace
from models import Model

IDS = ('z', 'a', 'm')  # Deliberately different from wire identity order.
@dataclass(frozen=True)
class Fact:
    fingerprint: int = 7
    offset: int = 1
    revision: int = 0
    receipt: str = "outcome-v1"

@dataclass(frozen=True)
class Cut:
    facts: tuple = (None, None)
    epoch: int = 0
    through: int = 1
    capability: str = ''  # Opaque actual acquire capability, not a numeric epoch.
    current: bool = True
    preparation_current: bool = True
    read_error: int = -1
    preparation_error: bool = False
    conversion_error: int = -1
    bad_outcome: int = -1
    reply: str = 'stored'  # stored, durable-content, unknown
    capture: int = -1
    acquire: int = -1

@dataclass(frozen=True)
class Case:
    cuts: tuple
    expected: str = 'complete'
    admin: tuple = ()
    abandon: bool = False
    contention: int = 6

EMPTY = (None, None)
FIRST = (Fact(), None)
BOTH = (Fact(), Fact())
BASE = (Cut(acquire=1), Cut(epoch=1, capability='writer-A', capture=2),
        Cut(BOTH, epoch=1, through=2))
CASES = {
    'shared_prefix': Case(BASE),
    'all_known': Case((Cut(BOTH, through=2),)),
    'different_receipt_content_same_fingerprint': Case((Cut((Fact(receipt='Committed:state1:work1'),Fact(receipt='Rejected:VersionConflict')),through=2),)),
    'invalid_authenticated_outcome_refuses_output': Case((Cut(BOTH, through=2, bad_outcome=1),), expected='unknown'),
    'stale_conversion_error_retains_fixed_observations': Case((Cut(BOTH, through=2, current=False, conversion_error=0, capture=1), Cut(BOTH, epoch=1, through=2))),
    'known_conflict_and_fresh': Case((Cut((Fact(99), None), through=2, acquire=1),
        Cut((Fact(99), None), epoch=1, through=2, capability='writer-A', capture=2),
        Cut((Fact(99), Fact(offset=2)), epoch=1, through=3))),
    'actual_durable_content': Case((BASE[0], replace(BASE[1], reply='durable-content'), BASE[2])),
    'unknown_reply_but_originals': Case((BASE[0], replace(BASE[1], reply='unknown'), BASE[2])),
    'ambiguous_absence_new_actual_acquire': Case((BASE[0], replace(BASE[1], reply='unknown'),
        Cut(epoch=1, acquire=3), Cut(epoch=2, capability='writer-B', capture=4),
        Cut(BOTH, epoch=2, through=2))),
    'numeric_epoch_without_capability': Case((BASE[0], replace(BASE[1], reply='unknown'),
        Cut(epoch=1, acquire=3), Cut(epoch=2)), expected='unknown'),
    'same_epoch_actual_acquire_is_insufficient': Case((BASE[0], replace(BASE[1], reply='unknown'),
        Cut(epoch=1, acquire=3), Cut(epoch=1, capability='writer-B')), expected='unknown'),
    'stored_missing_second_original': Case((BASE[0], BASE[1], Cut(FIRST, epoch=1, through=2)), expected='unknown'),
    'durable_content_conflicting_second_original': Case((BASE[0], replace(BASE[1], reply='durable-content'),
        Cut((Fact(), Fact(99)), epoch=1, through=2)), expected='unknown'),
    'stale_preparation_error_becomes_replay': Case((BASE[0],
        replace(BASE[1], preparation_error=True, preparation_current=False), BASE[2])),
    'stale_read_error_then_replay': Case((Cut(BOTH, through=2, current=False, read_error=1, capture=1), Cut(BOTH, epoch=1, through=2))),
    'partial_observation_sticky_despite_stale_head': Case((Cut(FIRST, through=2, current=False, read_error=1, capture=1),
        Cut((Fact(revision=1), None), epoch=1, through=2, current=False, capture=2), Cut(BOTH, epoch=1, through=2)), expected='unknown'),
    'known_presence_cannot_disappear': Case((Cut(FIRST, through=2, current=False, capture=1),
        Cut(epoch=1, through=2, current=False, capture=2), Cut(BOTH, epoch=1, through=2)), expected='unknown'),
    'first_preparation_refusal': Case((BASE[0], replace(BASE[1], preparation_error=True)), expected='too-large'),
    'fresh_admin_blocks_ordinary_sibling': Case((Cut(acquire=1), Cut(epoch=1, capability='writer-A')), expected='invalid', admin=(1,)),
    'abandoned_caller_owned_task_drains': Case(BASE, expected='drained', abandon=True),
    'two_prefixes_reset_only_confirmed_progress': Case((
        Cut((None, None, None), acquire=1),
        Cut((None, None, None), epoch=1, capability='writer-A', capture=2),
        Cut((Fact(), Fact(), None), epoch=1, through=2, acquire=3),
        Cut((Fact(), Fact(), None), epoch=2, through=2, capability='writer-B', capture=4),
        Cut((Fact(), Fact(), Fact(offset=2)), epoch=2, through=3)), contention=3),
    'fresh_known_fresh_barrier': Case((
        Cut((None,Fact(),None), through=2, acquire=1),
        Cut((None,Fact(),None), epoch=1, through=2, capability='writer-A', capture=2),
        Cut((Fact(offset=2),Fact(),None), epoch=1, through=3, acquire=3),
        Cut((Fact(offset=2),Fact(),None), epoch=2, through=3, capability='writer-B', capture=4),
        Cut((Fact(offset=2),Fact(),Fact(offset=3)), epoch=2, through=4))),
    'ambiguous_prefix_competitor_resolves_subset': Case((BASE[0],replace(BASE[1],reply='unknown'),
        Cut((Fact(99),None),epoch=2,through=2,acquire=3),
        Cut((Fact(99),None),epoch=3,through=2,capability='writer-C',capture=4),
        Cut((Fact(99),Fact(offset=2)),epoch=3,through=3))),
    'pending_late_fixed_error_cannot_acquire': Case((BASE[0],BASE[1],
        Cut(FIRST,epoch=1,through=2,read_error=1,acquire=3),
        Cut(BOTH,epoch=2,through=2,capability='writer-B')),expected='unknown'),
    'pending_sticky_change_despite_stale_head': Case((Cut(FIRST,through=2,acquire=1),
        Cut(FIRST,epoch=1,through=2,capability='writer-A',capture=2),
        Cut((Fact(revision=1),None),epoch=2,through=3,current=False,capture=3,acquire=3),
        Cut(BOTH,epoch=3,through=3,capability='writer-B')),expected='unknown'),
    'second_preparation_refusal_after_prefix': Case((
        Cut((None, None, None), acquire=1),
        Cut((None, None, None), epoch=1, capability='writer-A', capture=2),
        Cut((Fact(), Fact(), None), epoch=1, through=2, acquire=3),
        Cut((Fact(), Fact(), None), epoch=2, through=2, capability='writer-B', preparation_error=True)), expected='unknown'),
}
FAULTS = {
    'coordinator_forget_partial_observation': 'CO-CONTINUITY',
    'coordinator_numeric_fence_only': 'CO-AUTHORITY',
    'coordinator_durable_omission_reprepare': 'CO-AUTHORITY',
    'coordinator_skip_submitted_member': 'CO-OWNER',
    'coordinator_release_pending_early': 'CO-OWNER',
    'coordinator_cancel_owned_task': 'CO-OWNER',
    'coordinator_refund_work': 'CO-WORK',
    'coordinator_replace_ledger': 'CO-WORK',
    'coordinator_reset_without_progress': 'CO-PROGRESS',
    'coordinator_no_progress_reset': 'CO-PROGRESS',
    'coordinator_premature_output': 'CO-OUTPUT',
    'coordinator_swap_caller_order': 'CO-OUTPUT',
    'coordinator_stored_is_ack': 'CO-OUTPUT',
    'coordinator_stale_error_escape': 'CO-HISTORY',
    'coordinator_forget_durability_latch': 'CO-OWNER',
    'coordinator_erase_unknown_history': 'CO-HISTORY',
    'coordinator_admin_prefix': 'CO-AUTHORITY',
    'coordinator_replace_declaration': 'CO-OUTPUT',
    'coordinator_skip_outcome_validation': 'CO-OUTPUT',
    'coordinator_forget_converted_members': 'CO-OUTPUT',
    'coordinator_substitute_receipt_token': 'CO-OUTPUT',
    'coordinator_return_without_dto_owner': 'CO-OWNER',
    'coordinator_release_borrowed_output': 'CO-OWNER',
    'coordinator_acquire_after_failed_classification': 'CO-AUTHORITY',
    'coordinator_acquire_after_sticky_contradiction': 'CO-AUTHORITY',
}
PREFIX_ORACLE = None
@dataclass(frozen=True)
class State:
    case: str = ''
    phase: str = 'choose'
    cut: int = -1
    index: int = 0
    member: int = 0
    converted: tuple = ()
    observed: tuple = ()
    classes: tuple = ()
    sticky: bool = False
    read_error: bool = False
    confirmed: bool = False
    prepared: tuple = ()
    pending: tuple = ()
    pending_cut: int = -1
    durable: bool = False
    possible: bool = False
    owner: bool = False
    caller: bool = True
    job: bool = False
    known: int = 0
    attempts: int = 0
    work: int = 0
    ledger: str = 'one-ledger'
    declarations: tuple = ()
    output: tuple = ()
    output_owner: bool = False
    result: str = ''
    events: tuple = ()

def stamp(position, fact):
    return None if fact is None else (2, IDS[position], 3, fact.fingerprint,
        fact.offset, fact.revision, position * 10, 4, 13)

def status(fact): return 'absent' if fact is None else ('present' if fact.fingerprint == 7 else 'conflict')

class Coordinator(Model):
    name = 'api007-native-commit-coordinator'
    initial = State()
    properties = sorted(set(FAULTS.values()))
    bounds = {'caller_positions':3, 'physical_prefixes':2, 'relative_epochs':3, 'cases':len(CASES)}
    assumptions = ['immutable authenticated cuts and complete fixed facts supplied by ports',
        'capability names denote actual acquire, never an inferred epoch',
        'SDK atomic publication and resolution are trusted port contracts',
        'DTO and publication physical retirement contracts and compiler ownership are assumed',
        'finite orchestration only; no Rust refinement, physical memory arithmetic or performance proof',
        'missing/conflicting post-durable facts are result-port contradictions, not valid provider history']
    def __init__(self, fault=None):
        if fault is not None and fault not in FAULTS: raise ValueError('unknown coordinator fault')
        self.fault = fault
    def frontier(self,s): return s.phase == 'done'
    def transitions(self,s):
        if s.phase == 'choose':
            for name,c in CASES.items():
                yield 'ChooseCoordinator-'+name, replace(s,case=name,phase='admit')
            return
        c=CASES[s.case];cut=c.cuts[s.cut] if s.cut>=0 else None
        def emit(word,arg=None,phase=None,**fields):
            f=self.fault
            if f=='coordinator_forget_partial_observation' and word=='capture' and any(s.observed): fields['observed']=(None,)*len(s.observed)
            if f=='coordinator_skip_submitted_member' and word=='submit': fields['pending']=fields['pending'][:1]
            if f=='coordinator_release_pending_early' and word=='submit': fields['owner']=False
            if f=='coordinator_cancel_owned_task' and word=='cancel': fields['job']=False;fields['owner']=False
            if f=='coordinator_refund_work' and word=='capture' and s.work: fields['work']=0
            if f=='coordinator_replace_ledger' and word=='capture': fields['ledger']='new-ledger'
            if f=='coordinator_reset_without_progress' and word=='confirm' and fields.get('attempts',s.attempts)>0: fields['attempts']=0
            if f=='coordinator_no_progress_reset' and word=='confirm' and fields.get('known',s.known)>s.known: fields['attempts']=s.attempts
            if f=='coordinator_swap_caller_order' and word=='output': fields['output']=tuple(reversed(fields['output']));arg=fields['output']
            if f=='coordinator_forget_durability_latch' and word=='submit': fields['possible']=False
            if f=='coordinator_replace_declaration' and word=='admit': fields['declarations']=('foreign',)
            if f=='coordinator_forget_converted_members' and word=='confirm': fields['converted']=()
            if f=='coordinator_substitute_receipt_token' and word=='output' and fields.get('output'):
                fields['output']=tuple(value[:-1]+('foreign-token',) for value in fields['output']);arg=fields['output']
            if f=='coordinator_return_without_dto_owner' and word=='output': fields['output_owner']=False;fields['job']=False
            if f=='coordinator_release_borrowed_output' and word=='borrow-output': fields['output_owner']=False;fields['job']=False
            events=s.events+((word,arg),)
            if f=='coordinator_erase_unknown_history' and word=='error': events=((word,arg),)
            return word,replace(s,phase=phase or s.phase,events=events,**fields)
        def error(kind='unknown'):
            result='unknown' if s.possible else kind
            return emit('error',result,phase='done',result=result,owner=False,job=False,pending=(),prepared=(),converted=())
        def capture(kind,target):
            if target<0 or target>=len(c.cuts): return error()
            new=c.cuts[target]
            if kind=='acquire' and not new.capability: return error()
            if s.attempts>=c.contention: return error()
            return emit('capture',(kind,target),phase='point',cut=target,index=0,member=0,converted=(),classes=(),confirmed=False,
                read_error=False,prepared=(),attempts=s.attempts+1,work=s.work+1)
        if s.phase=='admit':
            yield emit('admit',phase='initial',job=True,observed=(None,)*len(c.cuts[0].facts),declarations=IDS[:len(c.cuts[0].facts)])
        elif s.phase=='initial': yield capture('initial',0)
        elif s.phase=='point':
            if s.index==len(cut.facts): yield emit('classified',phase='convert')
            elif s.index==cut.read_error: yield emit('read-error',s.index,phase='confirm',read_error=True,work=s.work+1)
            else:
                fact=cut.facts[s.index];observed=list(s.observed);next_stamp=stamp(s.index,fact)
                sticky=s.sticky or (observed[s.index] is not None and observed[s.index]!=next_stamp)
                if next_stamp is not None and observed[s.index] is None: observed[s.index]=next_stamp
                yield emit('point',s.index,index=s.index+1,classes=s.classes+(status(fact),),observed=tuple(observed),sticky=sticky,work=s.work+1)
        elif s.phase=='convert':
            if s.member==len(cut.facts): yield emit('conversion-complete',phase='confirm')
            elif s.member==cut.conversion_error or (s.member==cut.bad_outcome and self.fault!='coordinator_skip_outcome_validation'):
                yield emit('conversion-error',s.member,phase='confirm',read_error=True,work=s.work+1)
            else:
                token=None if cut.facts[s.member] is None else ('RequestConflict' if s.classes[s.member]=='conflict' else cut.facts[s.member].receipt)
                yield emit('converted',(s.member,token),member=s.member+1,converted=s.converted+((s.member,token),),work=s.work+1)
        elif s.phase=='confirm':
            known=sum(x!='absent' for x in s.classes)
            progress=cut.current and not s.read_error and not s.sticky and known>s.known
            yield emit('confirm',phase='decide',confirmed=cut.current,known=known if progress else s.known,
                attempts=0 if progress else s.attempts,work=s.work+1)
        elif s.phase=='decide':
            if s.sticky:
                if s.pending and self.fault=='coordinator_acquire_after_sticky_contradiction':yield capture('acquire',cut.acquire)
                else:yield error()
            elif not s.confirmed:
                if s.read_error and self.fault=='coordinator_stale_error_escape': yield error('too-large')
                else: yield capture('recapture',cut.capture)
            elif s.read_error:
                if s.pending and self.fault=='coordinator_acquire_after_failed_classification':yield capture('acquire',cut.acquire)
                else:yield error('unknown' if cut.bad_outcome>=0 else 'too-large')
            elif self.fault=='coordinator_premature_output':
                yield emit('output',phase='done',output=(),result='complete',job=False,owner=False)
            elif s.pending:
                complete=all(s.classes[p]!='absent' for p in s.pending)
                bad=s.durable and (not complete or any(s.classes[p]=='conflict' for p in s.pending))
                if bad and self.fault!='coordinator_durable_omission_reprepare': yield error()
                elif not s.durable and not complete and not (cut.capability and cut.epoch>c.cuts[s.pending_cut].epoch):
                    if self.fault=='coordinator_numeric_fence_only': yield emit('release',phase='fresh',owner=False,pending=())
                    else: yield capture('acquire',cut.acquire)
                else: yield emit('release',phase='fresh',owner=False,pending=())
            else: yield emit('ready',phase='fresh')
        elif s.phase=='fresh':
            if all(x!='absent' for x in s.classes):
                tokens=dict(s.converted)
                answer=tuple((IDS[p],s.classes[p],stamp(p,fact),tokens[p]) for p,fact in enumerate(cut.facts)) if s.caller else ()
                yield emit('output',answer,phase='returned' if s.caller else 'done',output=answer,
                    result='complete' if s.caller else 'drained',job=s.caller,output_owner=s.caller,converted=())
            elif any(s.classes[p]=='absent' for p in c.admin) and self.fault!='coordinator_admin_prefix': yield error('invalid')
            elif not cut.capability: yield capture('acquire',cut.acquire)
            else:
                first=s.classes.index('absent');positions=[]
                for p in range(first,min(first+2,len(s.classes))):
                    if s.classes[p]!='absent':break
                    positions.append(p)
                yield emit('prepare',tuple(positions),phase='prepared',prepared=tuple(positions),work=s.work+1)
        elif s.phase=='prepared':
            yield emit('prepare-confirm',phase='submit-or-retry',confirmed=cut.preparation_current,work=s.work+1)
        elif s.phase=='submit-or-retry':
            if not s.confirmed: yield capture('recapture',cut.capture)
            elif cut.preparation_error: yield error('too-large')
            else:
                yield emit('submit',cut.reply,phase='post-submit',pending=s.prepared,pending_cut=s.cut,
                    owner=True,possible=True,durable=cut.reply in ('stored','durable-content'),prepared=(),work=s.work+1)
        elif s.phase=='post-submit':
            if self.fault=='coordinator_stored_is_ack': yield emit('output',phase='done',result='complete',job=False,owner=False)
            elif c.abandon and s.caller: yield emit('cancel',phase='shutdown-wait',caller=False)
            else: yield capture('recapture',cut.capture)
        elif s.phase=='shutdown-wait': yield emit('shutdown-pending',phase='post-submit')
        elif s.phase=='returned':yield emit('borrow-output',phase='drop-output')
        elif s.phase=='drop-output':yield emit('drop-output',phase='done',output=(),output_owner=False,job=False)
    def violation(self,before,action,s):
        if before.case and (s.case!=before.case or s.events[:len(before.events)]!=before.events): return 'CO-HISTORY'
        if not s.case: return None if s==self.initial else 'CO-OUTPUT'
        if s.case not in CASES: return 'CO-HISTORY'
        if PREFIX_ORACLE is None: raise ValueError('captured coordinator oracle missing')
        return PREFIX_ORACLE(s,CASES[s.case])
