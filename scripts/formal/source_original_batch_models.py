"""Finite batch-original composition; no Rust/provider/performance proof.

Receipt fixtures and fixed observations are separate immutable inputs. Events
record logical composition, not physical bytes, cryptographic implementations or
allocation arithmetic. Pending C ownership is abstracted by its independent port.
"""
from dataclasses import dataclass, replace
from models import Model

@dataclass(frozen=True)
class Receipt:
    offset: int
    domain: int
    literal: str
    fingerprint: int
    checksum: int
    start: int
    length: int
    digest: int
    format: int = 3
    outcome: str = 'equal-bytes'

A = Receipt(1, 2, ' a\0α ', 7, 11, 10, 4, 13)
B = Receipt(1, 2, ' b\0β ', 7, 11, 20, 4, 13)
D = Receipt(2, 0, 'd', 8, 12, 10, 4, 13)

@dataclass(frozen=True)
class Request:
    domain: int
    literal: str
    fact: Receipt | None
    expected: int | None = 7  # None is Lookup, not an implicit conflict bypass.
    expected_format: int = 3
    declaration: bool = True
    fixed: bool = True
    point_cut: str = 'A'

RA = Request(2, A.literal, A)
RB = Request(2, B.literal, B)
RD = Request(0, D.literal, D, None)
ABSENT = Request(2, 'missing', None)
CONFLICT = replace(RA, expected=99)

@dataclass(frozen=True)
class Case:
    requests: tuple = (RA, RB)
    receipts: tuple = (A, B, D)
    mode: str = 'stream'
    cut: str = 'A'
    admission: bool = True
    point_error: int = -1
    conversion_error: int = -1
    cancel_offset: int = -1
    read_error: int = -1

CASES = {
    'empty': Case(requests=()),
    'shared_frame_reverse': Case(requests=(RB, RA)),
    'mixed': Case(requests=(RD, CONFLICT, ABSENT, RA)),
    'duplicate': Case(requests=(RA, RA)),
    'reverse_offsets': Case(requests=(RD, RA)),
    'retained': Case(requests=(RD, RA), mode='retain'),
    'absence_conflict': Case(requests=(ABSENT, CONFLICT)),
    'lookup_conflicting_fingerprint': Case(requests=(replace(RA, expected=None),)),
    'other_equal_numeric_cut': Case(cut='B', requests=(replace(RA, point_cut='B'),)),
    'foreign_point': Case(requests=(replace(RA, point_cut='B'),)),
    'late_declaration_error': Case(requests=(RA, replace(RB, declaration=False))),
    'late_point_error': Case(point_error=1),
    'late_fixed_error': Case(requests=(RA, replace(RB, fixed=False))),
    'admission_error': Case(admission=False),
    'late_conversion_error': Case(requests=(RA, RD), conversion_error=1),
    'cancel_second': Case(requests=(RA, RD), cancel_offset=2),
    'late_read_error': Case(requests=(RA,RD),read_error=2),
    'retained_late_read_error': Case(requests=(RA,RD),mode='retain',read_error=2),
    'late_nonpresent_conversion_error': Case(requests=(ABSENT,CONFLICT),conversion_error=1),
    'retained_cancel_second': Case(requests=(RA, RD), mode='retain', cancel_offset=2),
    **{'second_alias_'+field: Case(requests=(RA, replace(RA, expected=None, fact=replace(A, **{field:value}))))
       for field,value in [('start',20),('digest',99),('fingerprint',99),('format',4)]},
    'equally_wrong_aliases': Case(requests=(replace(RA, fact=replace(A,start=20)),)*2),
    'same_bytes_other_identity': Case(requests=(replace(RA, fact=B),)),
    'wrong_checksum': Case(requests=(replace(RA, fact=replace(A,checksum=99)),)),
    'claim_historical': Case(requests=(Request(1,'claim',replace(A,domain=1,literal='claim')),), receipts=(replace(A,domain=1,literal='claim'),)),
    'wrong_length': Case(requests=(replace(RA, fact=replace(A,length=3)),)),
    'late_frame_verification_error': Case(requests=(RA, replace(RD,fact=replace(D,digest=99)))),
}
FAULTS = {
    'original_batch_swap_equal_span_token': 'BO-OUTPUT',
    'original_batch_replace_observation': 'BO-OUTPUT',
    'original_batch_premature_output': 'BO-OUTPUT',
    'original_batch_representative_only': 'BO-HISTORY',
    'original_batch_skip_alias': 'BO-HISTORY',
    'original_batch_conflict_hydration': 'BO-HISTORY',
    'original_batch_conflict_upgrade': 'BO-OUTPUT',
    'original_batch_early_dispatch': 'BO-HISTORY',
    'original_batch_wrong_position': 'BO-OUTPUT',
    'original_batch_repeat_frame': 'BO-HISTORY',
    'original_batch_prefix_return': 'BO-HISTORY',
    'original_batch_early_frame_release': 'BO-OWNER',
    'original_batch_early_container_release': 'BO-OWNER',
    'original_batch_surviving_dto': 'BO-OWNER',
    'original_batch_refund_work': 'BO-WORK',
    'original_batch_erase_history': 'BO-HISTORY',
    'original_batch_accept_wrong_aliases': 'BO-HISTORY',
    'original_batch_replace_cut': 'BO-CUT',
    'original_batch_skip_barrier': 'BO-HISTORY',
    'original_batch_unknown_launders_verification': 'BO-HISTORY',
}

@dataclass(frozen=True)
class State:
    case: str = ''
    phase: str = 'choose'
    index: int = 0
    group: int = 0
    member: int = 0
    events: tuple = ()
    observations: tuple = ()
    classes: tuple = ()
    verified: tuple = ()
    points: int = 0
    reads: int = 0
    containers: bool = False
    pending: tuple = ()
    frames: tuple = ()
    dtos: tuple = ()
    values: tuple = ()
    returned: tuple = ()
    result: str = ''
    cut: str = ''
    ledger: str = 'input-ledger'

def observation(fact):
    if fact is None: return None
    return (fact.offset,fact.domain,fact.literal,fact.format,fact.fingerprint,
        fact.checksum,fact.start,fact.length,fact.digest)

def value_for(request,status):
    stamp=observation(request.fact)
    verified=(request.fact.outcome,stamp) if status=='present' else None
    return (status,request.domain,request.literal,stamp,verified)

PREFIX_ORACLE = None  # Bound to the captured oracle by source_group_gate.

class SourceOriginalBatch(Model):
    name = 'api007-source-original-batch-resolution'
    initial = State()
    properties = sorted(set(FAULTS.values()))
    bounds = {'requests':4, 'frames':2, 'opaque_cuts':2, 'cases':len(CASES)}
    assumptions = ['immutable valid recovered capsule borrowed throughout one invocation',
        'A and B can have equal numeric heads but are different opaque cuts',
        'C port supplies checked owned frames; cancellation retains physical ownership until retirement',
        'full literal identities and exact receipt facts are independent immutable fixtures',
        'abstract one point/read ticket per dispatch; no allocation arithmetic or actual hash computation',
        'finite documentary composition only; no Rust refinement, provider durability or performance proof',
        'no current lease eligibility, freshness across recapture, publication or ACK authority']
    def __init__(self, fault=None):
        if fault is not None and fault not in FAULTS: raise ValueError('unknown original batch fault')
        self.fault = fault
    def frontier(self,s): return s.phase == 'done'
    def transitions(self,s):
        if s.phase == 'choose':
            if self.fault=='original_batch_premature_output':
                yield 'UnselectedOutput',replace(s,phase='done',result='batch',returned=(('forged',),))
            for name,c in CASES.items():
                yield 'ChooseOriginalBatch-'+name, replace(s,case=name,phase='admit',cut=c.cut)
            return
        c=CASES[s.case]
        def emit(word,arg=-1,phase=None,**fields):
            event=(word,arg)
            f=self.fault
            if f=='original_batch_early_dispatch' and word=='declare': event=('dispatch',1)
            if f=='original_batch_conflict_hydration' and word=='classify' and arg[1]=='conflict': event=('dispatch',1)
            if f=='original_batch_skip_barrier' and word=='classify' and arg==(0,'present'): event=('dispatch',1)
            if f=='original_batch_representative_only' and word=='verify' and s.case=='duplicate' and arg==1: event=('verify',0)
            if f=='original_batch_skip_alias' and word=='verify' and s.case=='duplicate' and arg==1: event=('convert',1)
            if f=='original_batch_repeat_frame' and word=='dispatch' and s.reads: event=('dispatch',1)
            if f=='original_batch_prefix_return' and word=='convert' and s.case=='reverse_offsets': event=('return',-1)
            if f=='original_batch_accept_wrong_aliases' and word=='fail' and s.case=='equally_wrong_aliases': event=('verify',0)
            if f=='original_batch_unknown_launders_verification' and word=='verify': event=('fail',-1)
            events=s.events+(event,)
            if f=='original_batch_erase_history' and word=='fail': events=(event,)
            if f=='original_batch_early_frame_release' and word=='read': fields['frames']=()
            if f=='original_batch_early_container_release' and word=='dispatch': fields['containers']=False
            if f=='original_batch_surviving_dto' and word=='fail' and s.dtos: fields['dtos']=s.dtos
            if f=='original_batch_refund_work' and word=='fail' and s.points: fields['points']=0
            if f=='original_batch_replace_cut' and word=='admit': fields['cut']='B' if c.cut=='A' else 'A'
            if f=='original_batch_replace_observation' and word=='classify' and arg[1]!='absent':
                fields['observations']=s.observations+(('forged',),)
            if f=='original_batch_swap_equal_span_token' and word=='return' and s.case=='shared_frame_reverse':
                left,right=fields['returned']
                fields['returned']=(left[:-1]+((left[-1][0],right[-1][1]),),right[:-1]+((right[-1][0],left[-1][1]),))
            if f=='original_batch_wrong_position' and word=='return': fields['returned']=tuple(reversed(fields['returned']))
            if f=='original_batch_conflict_upgrade' and word=='return' and s.case=='mixed':
                fields['returned']=tuple(('present',)+v[1:] if v[0]=='conflict' else v for v in fields['returned'])
            return word+'-'+str(arg), replace(s,events=events,phase=phase or s.phase,**fields)
        def fail():
            return emit('fail',phase='done',containers=False,frames=(),dtos=(),values=(),result='unknown')
        if s.phase=='admit':
            if not c.admission: yield fail()
            else: yield emit('admit',phase='declare',containers=True)
        elif s.phase=='declare':
            if s.index==len(c.requests): yield emit('declared',phase='point',index=0)
            elif not c.requests[s.index].declaration: yield fail()
            else: yield emit('declare',s.index,index=s.index+1)
        elif s.phase=='point':
            if s.index==len(c.requests): yield emit('classified',phase='nonpresent',index=0)
            else: yield emit('point',s.index,phase='classify',points=s.points+1)
        elif s.phase=='classify':
            r=c.requests[s.index]
            if c.point_error==s.index or not r.fixed or r.point_cut!=c.cut: yield fail()
            else:
                status='absent' if r.fact is None else ('conflict' if r.expected is not None and (r.expected,r.expected_format)!=(r.fact.fingerprint,r.fact.format) else 'present')
                yield emit('classify',(s.index,status),phase='point',classes=s.classes+(status,),observations=s.observations+(observation(r.fact),),index=s.index+1)
        elif s.phase=='nonpresent':
            positions=[i for i,status in enumerate(s.classes) if status!='present'] if c.mode=='stream' else []
            if s.index==len(positions): yield emit('groups',phase='dispatch',group=0)
            else:
                i=positions[s.index];r=c.requests[i]
                if c.conversion_error==i: yield fail()
                else: yield emit('convert',i,index=s.index+1,dtos=s.dtos+(i,),values=s.values+((i,value_for(r,s.classes[i])),))
        else:
            offsets=sorted({r.fact.offset for i,r in enumerate(c.requests) if i<len(s.classes) and s.classes[i]=='present'})
            if s.phase=='dispatch':
                if s.group==len(offsets):
                    values=dict(s.values) if c.mode=='stream' else {
                        i:value_for(r,status)
                        for i,(r,status) in enumerate(zip(c.requests,s.classes))}
                    yield emit('return',phase='borrow',returned=tuple(values[i] for i in range(len(c.requests))),result='batch')
                else:
                    offset=offsets[s.group]
                    yield emit('dispatch',offset,phase='read',pending=(offset,),reads=s.reads+1)
            elif s.phase=='read':
                offset=offsets[s.group]
                if c.read_error==offset:
                    yield emit('read-error',offset,phase='error',pending=())
                elif c.cancel_offset==offset:
                    yield emit('cancel',phase='retire',containers=False,frames=(),dtos=(),values=(),result='cancelled')
                else: yield emit('read',offset,phase='verify',pending=(),frames=s.frames+(offset,),member=0)
            elif s.phase in ('verify','convert'):
                offset=offsets[s.group]
                positions=[i for i,r in enumerate(c.requests) if s.classes[i]=='present' and r.fact.offset==offset]
                if s.member==len(positions):
                    if s.phase=='verify' and c.mode=='stream': yield emit('verified-group',offset,phase='convert',member=0)
                    elif c.mode=='stream': yield emit('release',offset,phase='dispatch',group=s.group+1,frames=tuple(f for f in s.frames if f!=offset))
                    else: yield emit('retained',offset,phase='dispatch',group=s.group+1)
                else:
                    i=positions[s.member];r=c.requests[i]
                    if s.phase=='verify':
                        actual=next((v for v in c.receipts if (v.offset,v.domain,v.literal)==(offset,r.domain,r.literal)),None)
                        if actual is None or r.fact!=actual: yield fail()
                        else: yield emit('verify',i,member=s.member+1,verified=s.verified+(i,))
                    elif c.conversion_error==i: yield fail()
                    else: yield emit('convert',i,member=s.member+1,dtos=s.dtos+(i,),values=s.values+((i,value_for(r,'present')),))
            elif s.phase=='error': yield fail()
            elif s.phase=='borrow': yield emit('borrow',phase='drop')
            elif s.phase=='drop': yield emit('drop',phase='done',containers=False,frames=(),dtos=(),values=())
            elif s.phase=='retire': yield emit('retire',phase='done',pending=())
    def violation(self,before,action,s):
        if before.case:
            if s.case!=before.case or s.events[:len(before.events)]!=before.events: return 'BO-HISTORY'
            if s.points<before.points or s.reads<before.reads: return 'BO-WORK'
        if not s.case: return None if s==self.initial else 'BO-OUTPUT'
        if s.case not in CASES: return 'BO-HISTORY'
        # Separate immutable-fixture oracle: never consult transition phases or
        # producer grouping/representative validation to determine expected facts.
        if PREFIX_ORACLE is None: raise ValueError('captured original batch oracle missing')
        return PREFIX_ORACLE(s,CASES[s.case])
