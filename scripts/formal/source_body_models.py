"""Bounded source-body driver. No Rust refinement, durability or speed proof."""
from dataclasses import dataclass, replace
from models import Model

@dataclass(frozen=True)
class Command:
    key: str
    domain: str
    start: int
    body: tuple
    insertion: int
    version: int = 1
@dataclass(frozen=True)
class Region:
    domain: str
    start: int
    body: tuple
@dataclass(frozen=True)
class Frame:
    offset: int
    pin: int
    commands: tuple
    regions: tuple = ()
@dataclass(frozen=True)
class Evidence:
    offset: int
    start: int
    end: int
    digest: tuple
    cut: str = 'original'
    job: str = 'job'
@dataclass(frozen=True)
class Observation:
    key: str
    status: str
    version: int
    insertion: int
    highwater: int
    evidence: Evidence | None
@dataclass(frozen=True)
class Declaration:
    key: str
    body: bool = True
@dataclass(frozen=True)
class Manifest:
    result: str
    returned: tuple
    work: tuple
    reads: tuple
    walks: tuple
    hashes: tuple
@dataclass(frozen=True)
class Case:
    frames: tuple
    declarations: tuple
    observations: tuple
    expected: Manifest
    stop: str = ''
    unreadable: tuple = ()

INITIAL_WORK=(20,100,4,20,200)
FRAMES=(Frame(1,11,(Command('a','work',10,(1,2),1),Command('b','work',20,(1,2),2),
                   Command('state','state',40,(1,2),0)),(Region('receipt',50,(1,2)),)),
        Frame(2,11,(Command('c','work',30,(7,8,9),3),)))
A=Observation('a','live',1,1,1,Evidence(1,10,12,(1,2)))
B=Observation('b','live',1,2,1,Evidence(1,20,22,(1,2)))
C=Observation('c','live',1,3,1,Evidence(2,30,33,(7,8,9)))
DECLS=tuple(Declaration(k) for k in ('a','a','b','c'))
ROWS=(('a',1,1,(1,2)),('b',1,2,(1,2)),('c',1,3,(7,8,9)))
SUCCESS=Manifest('image',ROWS,(17,91,2,15,186),(1,2),(1,2),('a','b','c'))
BASE=Case(FRAMES,DECLS,(A,A,B,C),SUCCESS)
EARLY=Manifest('unknown',(),(17,91,3,18,195),(1,),(),())
BAD_A=replace(EARLY,walks=(1,),hashes=('a',))
BAD_B=replace(BAD_A,hashes=('a','b'))
NO_C=Manifest('image',ROWS[:2],(17,91,3,18,195),(1,),(1,),('a','b'))
CASES={'shared_aliases':BASE,
 'metadata_preserve':replace(BASE,observations=(replace(A,version=2,highwater=2),replace(A,version=2,highwater=2),B,C),expected=replace(SUCCESS,returned=(('a',2,1,(1,2)),*ROWS[1:]))),
 'empty_body':replace(BASE,frames=(FRAMES[0],replace(FRAMES[1],commands=(replace(FRAMES[1].commands[0],body=()),))),observations=(A,A,B,replace(C,evidence=replace(C.evidence,end=30,digest=()))),expected=replace(SUCCESS,returned=(*ROWS[:2],('c',1,3,())))),
 'authenticated_absence':replace(BASE,observations=(A,A,B,Observation('c','absent',0,0,0,None)),expected=NO_C),
 'authenticated_tombstone':replace(BASE,observations=(A,A,B,Observation('c','tombstone',0,0,4,None)),expected=NO_C),
 'omitted_observation':replace(BASE,observations=(A,A,B),expected=Manifest('unknown',(),(17,91,4,20,200),(),(),())),
 'metadata_only_unreadable_body':replace(BASE,declarations=tuple(Declaration(k,False) for k in ('a','a','b','c')),unreadable=(1,2),expected=Manifest('image',tuple((k,v,i,None) for k,v,i,_ in ROWS),(17,91,4,20,200),(),(),())),
 'second_cap':replace(BASE,stop='cap',expected=Manifest('unknown',(),(17,91,2,18,195),(1,2),(1,),('a','b'))),
 'second_hydration_failure':replace(BASE,stop='hydrate',expected=replace(SUCCESS,result='unknown',returned=(),walks=(1,),hashes=('a','b'))),
 'second_copy_refusal':replace(BASE,stop='copy',expected=replace(SUCCESS,result='unknown',returned=())),
 'second_cancel_retry':replace(BASE,stop='cancel',expected=replace(SUCCESS,work=(14,82,0,10,172),reads=(1,2,1,2),walks=(1,1,2),hashes=('a','b','a','b','c'))),
 'unsupported_selector':replace(BASE,stop='selector',expected=Manifest('unknown',(),INITIAL_WORK,(),(),()))}
for name,change in {
 'same_bytes_wrong_command':{'start':20,'end':22},
 'interior_valid_digest':{'start':11,'end':12,'digest':(2,)},
 'state_interval_substitution':{'start':40,'end':42},
 'receipt_interval_substitution':{'start':50,'end':52},
 'wrong_digest':{'digest':(9,9)},
 'foreign_equal_numeric_cut':{'cut':'other'},'foreign_job':{'job':'other'},
}.items():
    expected=EARLY if name in ('foreign_equal_numeric_cut','foreign_job') else BAD_A
    CASES[name]=replace(BASE,observations=(A,replace(A,evidence=replace(A.evidence,**change)),B,C),expected=expected)
CASES['unreadable_required_body']=replace(BASE,unreadable=(1,),expected=Manifest('unknown',(),(17,91,3,20,200),(1,),(),()))
CASES['new_insertion_incarnation']=replace(BASE,observations=(A,replace(A,insertion=3),B,C),expected=BAD_A)
CASES['future_command_version']=replace(BASE,frames=(replace(FRAMES[0],commands=(replace(FRAMES[0].commands[0],version=2),*FRAMES[0].commands[1:])),FRAMES[1]),expected=BAD_A)
CASES['alias_metadata']=replace(BASE,observations=(A,replace(A,version=2,highwater=2),B,C),expected=BAD_A)
CASES['wrong_frame_pin']=replace(BASE,frames=(replace(FRAMES[0],pin=12),FRAMES[1]),expected=EARLY)
CASES['later_command_bad_interval']=replace(BASE,observations=(A,A,replace(B,evidence=replace(B.evidence,start=40,end=42)),C),expected=BAD_B)
# Checked frames remain canonical (a,c); requests deliberately ask c first.
late=Command('a','work',10,(1,2),1)
CASES['wrong_frame_missing_member']=replace(BASE,observations=(A,replace(A,evidence=replace(A.evidence,offset=2)),B,C),expected=replace(SUCCESS,result='unknown',returned=()))
CASES['actual_frame_order']=replace(BASE,frames=(FRAMES[0],replace(FRAMES[1],commands=(late,FRAMES[1].commands[0]))),declarations=tuple(Declaration(k) for k in ('c','a','a','b')),observations=(C,replace(A,evidence=Evidence(2,10,12,(1,2))),replace(A,evidence=Evidence(2,10,12,(1,2))),B),expected=replace(SUCCESS,hashes=('b','a','c')))
CASES['mixed_body_scope']=replace(BASE,declarations=(Declaration('a',False),Declaration('a',False),Declaration('b',False),Declaration('c')),unreadable=(1,),expected=Manifest('image',(('a',1,1,None),('b',1,2,None),ROWS[2]),(17,91,3,17,191),(2,),(2,),('c',)))

FAULTS={
 'body_skip_alias':'B-MEMBER','body_accept_foreign_cut':'B-MEMBER',
 'body_wrong_provisional_then_unknown':'B-CONTENT','body_fake_loaded':'B-CONTENT',
 'body_wrong_alias_view':'B-CONTENT','body_unfunded_copy':'B-COPY',
 'body_copy_before_admission':'B-COPY','body_refund_cancel':'B-WORK',
 'body_replace_ledger':'B-WORK','body_refund_cap':'B-WORK',
 'body_keep_first_frame':'B-OWNER','body_cancel_releases_physical':'B-OWNER',
 'body_return_early_release':'B-OWNER','body_release_borrowed_image':'B-OWNER',
 'body_erase_history':'B-HISTORY','body_rehash_alias':'B-COUNT',
 'body_rewalk_alias':'B-COUNT','body_reread_alias':'B-COUNT',
 'body_omit_request':'B-OBSERVATION','body_absence_from_missing':'B-OBSERVATION',
 'body_broaden_body_scope':'B-COUNT','body_selector_accept':'B-MEMBER','body_lose_tombstone_hwm':'B-OBSERVATION',
}
PREFIX_ORACLE=None
@dataclass(frozen=True)
class State:
    case: str=''
    phase: str='choose'
    events: tuple=()
    declarations: tuple=()
    observed: tuple=()
    frames: tuple=()
    frame_index: int=0
    scan: int=0
    matched: tuple=()
    views: tuple=()
    verified: tuple=()
    index: int=0
    image: tuple=()
    loaded: tuple=()
    copies: int=0
    funded: str=''
    owner: bool=False
    physical: tuple=()
    pending: bool=False
    work: tuple=INITIAL_WORK
    ledger: str='one'
    reads: tuple=()
    walks: tuple=()
    hashes: tuple=()
    returned: tuple=()
    result: str=''
    retry: bool=False

class SourceBodies(Model):
    name='api007-addressed-body-materialization'
    bounds={'frames':2,'keys':3,'aliases':4,'retries':1,'cases':len(CASES),'frontier':'return borrow drop or refusal physical retirement'}
    properties=sorted(set(FAULTS.values()))
    assumptions=['authenticated observations are explicit live/absent/tombstone full-key facts; omission is not absence',
        'checked physical carriers bind original opaque cut/job/offset; immutable frame fixtures include state/receipt intervals',
        'frame pin represents the embedded publication definition pin; digest is symbolic complete-byte identity',
        'symbolic intervals preserve command order and nonoverlap; absolute framing lengths/canonical codecs are external premises',
        'coordinator campaign supplies freshness and publication authority; no publication is modeled here',
        'batch overlays are deferred to the existing batch campaign and real Local/S3 contract controls',
        'logical read/walk/hash bounds are not physical traffic, heap-size, durability, Rust refinement or performance proof']
    initial=State()
    def __init__(self,fault=None):
        if fault is not None and fault not in FAULTS:raise ValueError('unknown body fault')
        self.fault=fault
    def frontier(self,s):return s.phase=='done'
    def transitions(self,s):
        if not s.case:
            for name in CASES:yield 'ChooseBody-'+name,replace(s,case=name,phase='metadata')
            return
        if s.phase=='done':return
        c=CASES[s.case]
        offset=s.frames[s.frame_index] if s.frames and s.frame_index<len(s.frames) else 0
        frame=next((f for f in c.frames if f.offset==offset),None)
        members=[(p,o) for p,(d,o) in enumerate(zip(c.declarations,c.observations)) if d.body and o is not None and o.status=='live' and o.evidence.offset==offset]
        def emit(word,arg=None,phase=None,**fields):
            n=replace(s,phase=phase or s.phase,events=s.events+((word,arg),),**fields);fault=self.fault
            if fault=='body_wrong_provisional_then_unknown' and word=='copy':n=replace(n,image=n.image[:-1]+((arg,(99,)),))
            if fault=='body_fake_loaded' and word=='metadata':n=replace(n,loaded=('a',))
            if fault=='body_wrong_alias_view' and word=='match':n=replace(n,views=n.views[:-1]+((99,'a',(99,)),))
            if fault=='body_unfunded_copy' and word=='fund':n=replace(n,copies=s.copies)
            if fault=='body_refund_cancel' and word=='cancel':n=replace(n,work=INITIAL_WORK)
            if fault=='body_refund_cap' and word=='refuse' and arg=='cap':n=replace(n,work=INITIAL_WORK)
            if fault=='body_replace_ledger' and word=='metadata':n=replace(n,ledger='new')
            if fault=='body_keep_first_frame' and word=='release':n=replace(n,physical=s.physical)
            if fault=='body_cancel_releases_physical' and word=='cancel':n=replace(n,physical=())
            if fault=='body_return_early_release' and word=='return':n=replace(n,owner=False)
            if fault=='body_release_borrowed_image' and word=='borrow':n=replace(n,owner=False)
            if fault=='body_erase_history' and word=='cancel':n=replace(n,events=((word,arg),))
            if fault=='body_rehash_alias' and word=='hash':n=replace(n,hashes=n.hashes+(arg,))
            if fault=='body_rewalk_alias' and word=='walk':n=replace(n,walks=n.walks+(offset,))
            if fault=='body_reread_alias' and word=='attempt':n=replace(n,reads=n.reads+(offset,))
            if fault=='body_omit_request' and word=='metadata':n=replace(n,declarations=n.declarations[:-1])
            if fault=='body_lose_tombstone_hwm' and word=='metadata' and s.case=='authenticated_tombstone':n=replace(n,observed=n.observed[:-1]+(('c','tombstone',0,0,0),))
            return word,n
        def refused(reason):return emit('refuse',reason,phase='done',image=(),loaded=(),copies=0,funded='',owner=False,physical=(),pending=False,observed=(),declarations=(),frames=(),frame_index=0,scan=0,index=0,matched=(),views=(),verified=(),result='unknown')
        if s.phase=='metadata':
            if c.stop=='selector' and self.fault!='body_selector_accept':yield refused('selector');return
            work=list(s.work);work[0]-=3;work[1]-=9
            yield emit('metadata',phase='observe',work=tuple(work),declarations=c.declarations,owner=True)
        elif s.phase=='observe':
            valid=len(c.observations)==len(c.declarations) and all(o is not None and d.key==o.key and o.status in ('live','absent','tombstone') for d,o in zip(c.declarations,c.observations))
            if not valid:
                if self.fault=='body_absence_from_missing':yield emit('observations',(),phase='return')
                else:yield refused('observations')
            else:
                observed=tuple(dict.fromkeys((o.key,o.status,o.version,o.insertion,o.highwater) for o in c.observations))
                frames=tuple(sorted({o.evidence.offset for d,o in zip(c.declarations,c.observations) if (d.body or (self.fault=='body_broaden_body_scope' and any(x.body for x in c.declarations))) and o.status=='live'}))
                yield emit('observations',phase='attempt' if frames else 'return',observed=observed,frames=frames)
        elif s.phase=='attempt':
            work=list(s.work);work[2]-=1
            yield emit('attempt',offset,phase='cap',work=tuple(work),reads=s.reads+(offset,))
        elif s.phase=='cap':
            if c.stop=='cap' and offset==2:yield refused('cap')
            elif frame is None or offset in c.unreadable:yield refused('record')
            else:yield emit('cap-ok',offset,phase='hydrate')
        elif s.phase=='hydrate':
            work=list(s.work);r,b=(2,5) if offset==1 else (3,9);work[3]-=r;work[4]-=b
            yield emit('hydrate',offset,phase='io',work=tuple(work),physical=(offset,),pending=True)
        elif s.phase=='io':
            if offset==2 and c.stop=='cancel' and not s.retry:yield emit('cancel',phase='retire',image=(),loaded=(),copies=0,funded='',owner=False,observed=(),declarations=(),frames=(),frame_index=0,scan=0,index=0,matched=(),views=(),verified=(),result='cancelled')
            elif offset==2 and c.stop=='hydrate':yield refused('hydrate')
            else:yield emit('complete',offset,phase='scope',pending=False)
        elif s.phase=='retire':yield emit('retire',phase='retry',physical=(),pending=False)
        elif s.phase=='retry':yield emit('retry',phase='metadata',frames=(),frame_index=0,scan=0,index=0,retry=True,observed=(),declarations=(),result='')
        elif s.phase=='scope':
            valid=frame.pin==11 and all(o.evidence.job=='job' and (o.evidence.cut=='original' or self.fault=='body_accept_foreign_cut') for _,o in members)
            if not valid:yield refused('scope')
            else:yield emit('walk',offset,phase='scan',walks=s.walks+(offset,),scan=0,matched=(),views=())
        elif s.phase=='scan':
            if s.scan==len(frame.commands):
                if any(o.key not in s.matched for _,o in members):yield refused('membership')
                else:yield emit('verified',phase='copy',verified=tuple(dict.fromkeys(o.key for _,o in members)),index=0)
            else:
                cmd=frame.commands[s.scan]
                if cmd.domain!='work' or not any(o.key==cmd.key for _,o in members):yield emit('skip',s.scan,scan=s.scan+1)
                else:yield emit('hash',cmd.key,phase='match',hashes=s.hashes+(cmd.key,))
        elif s.phase=='match':
            cmd=frame.commands[s.scan];aliases=[(p,o) for p,o in members if o.key==cmd.key];valid=cmd.key not in s.matched;views=[];first=None
            for p,o in aliases:
                views.append((p,o.key,cmd.body))
                if self.fault=='body_skip_alias' and first is not None:continue
                e=o.evidence
                valid &= (e.start==cmd.start and e.end==cmd.start+len(cmd.body) and e.digest==cmd.body and o.insertion==cmd.insertion and o.version>=cmd.version)
                if first is not None:valid &= (o.version,o.insertion,e)==first
                first=(o.version,o.insertion,e)
            if not valid:yield refused('membership')
            else:yield emit('match',cmd.key,phase='scan',scan=s.scan+1,matched=s.matched+(cmd.key,),views=s.views+tuple(views))
        elif s.phase=='copy':
            if s.index==len(s.verified):
                next_frame=s.frame_index+1
                yield emit('release',offset,phase='attempt' if next_frame<len(s.frames) else 'return',physical=(),frame_index=next_frame,scan=0,index=0,matched=(),views=(),verified=())
            elif c.stop=='copy' and offset==2:yield refused('copy')
            else:
                key=s.verified[s.index];body=next(body for _,k,body in s.views if k==key)
                if self.fault=='body_copy_before_admission':yield emit('copy',key,index=s.index+1,image=s.image+((key,body),),loaded=s.loaded+(key,))
                else:yield emit('fund',key,phase='allocate',funded=key,copies=s.copies+len(body))
        elif s.phase=='allocate':
            key=s.funded;body=next(body for _,k,body in s.views if k==key)
            yield emit('copy',key,phase='copy',funded='',index=s.index+1,image=s.image+((key,body),),loaded=s.loaded+(key,))
        elif s.phase=='return':
            bodies=dict(s.image);rows=tuple((k,v,i,bodies.get(k)) for k,status,v,i,_ in sorted(s.observed) if status=='live')
            yield emit('return',rows,phase='borrow',returned=rows,result='image')
        elif s.phase=='borrow':yield emit('borrow',phase='drop')
        elif s.phase=='drop':yield emit('drop',phase='done',image=(),loaded=(),copies=0,owner=False,observed=(),declarations=(),returned=())
    def violation(self,before,action,s):
        if before.case and (s.case!=before.case or s.events[:len(before.events)]!=before.events):return 'B-HISTORY'
        if not s.case:return None if s==self.initial else 'B-HISTORY'
        if PREFIX_ORACLE is None:raise ValueError('captured body oracle missing')
        return PREFIX_ORACLE(s,CASES[s.case])
