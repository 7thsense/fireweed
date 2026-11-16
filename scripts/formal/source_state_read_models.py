"""Finite direct-state materialization; no Rust/provider/heap refinement claim.

Commands, observations, caller declarations and literal expected DTOs are separate.
Digests are injective value tokens; sizes are abstract units, not Postcard bytes.
"""
from dataclasses import dataclass, replace
from models import Model

@dataclass(frozen=True)
class Command:
    collection: str
    key: str
    hwm: int
    start: int
    value: tuple
    domain: str = 'state'
@dataclass(frozen=True)
class Frame:
    offset: int
    commands: tuple
    pin: int = 11  # The one embedded publication pin.
    epoch: int = 1
    cut: str = 'A'
    job: str = 'job'
    queue: str = 'q'
    receipts: tuple = ((50,(1,2)),)
@dataclass(frozen=True)
class Observation:
    identity: tuple
    status: str
    hwm: int = 0
    offset: int = 0
    start: int = 0
    end: int = 0
    digest: tuple = ()
    count: int = 0
    complete: bool = True
    cut: str = 'A'
    job: str = 'job'
@dataclass(frozen=True)
class Case:
    request: tuple
    observations: tuple
    fields: tuple
    frames: tuple
    expected: tuple = ()
    outcome: str = 'ok'
    limit: int = 1000
    scope: bool = True
    admission: bool = True
    stop: str = ''
    cut: str = 'A'
    through: int = 3

A=('s','a'); B=('s','b'); C=('s','c'); R=('r','a')
FIELDS=((A,(('n',1),('tag','a'))),(B,()),(C,(('n',3),)),(R,(('n',1),)))
FRAMES=(Frame(1,(Command('','work',1,4,(1,2),'work'),Command('r','a',1,10,(1,2)),
                   Command('s','a',1,20,(1,2)),Command('s','b',1,30,()))),
        Frame(2,(Command('s','c',1,20,(7,8,9)),),epoch=2))
OA=Observation(A,'live',1,1,20,22,(1,2),2)
OB=Observation(B,'live',1,1,30,30,(),0)
OC=Observation(C,'live',1,2,20,23,(7,8,9),1)
RA=('s','a',1,FIELDS[0][1],(1,2)); RB=('s','b',1,(),()); RC=('s','c',1,FIELDS[2][1],(7,8,9))
BASE=Case((C,A,A,B),(OC,OA,OA,OB),FIELDS,FRAMES,(RC,RA,RA,RB))

def bad(**changes): return replace(BASE,outcome='unknown',expected=(),**changes)
def one(observation,**changes):
    return bad(request=(observation.identity,),observations=(observation,),**changes)
def mutate_frame(index,**changes):
    frames=list(FRAMES);frames[index]=replace(frames[index],**changes);return tuple(frames)
def public_size(rows):
    return 1+sum(1 if row is None else 4+len(row[0])+len(row[1])+len(row[4])+
                 sum(len(name)+1+len(str(value)) for name,value in row[3]) for row in rows)
assert public_size(BASE.expected)==51
CASES={
    'grouped_reversed_aliases':BASE,
    'empty':replace(BASE,request=(),observations=(),expected=(),limit=1),
    'foreign_empty':bad(request=(),observations=(),scope=False),
    'admission':bad(admission=False),
    'absent':replace(BASE,request=(('s','missing'),),observations=(Observation(('s','missing'),'absent'),),expected=(None,)),
    'tombstone':replace(BASE,request=(A,),observations=(Observation(A,'tombstone',2),),expected=(None,)),
    'empty_value_fields':replace(BASE,request=(B,),observations=(OB,),expected=(RB,)),
    'equal_bytes_other_collection':one(replace(OA,identity=R,start=20,end=22,count=1)),
    'equal_bytes_other_key':one(replace(OA,identity=('s','z')),fields=FIELDS+((('s','z'),FIELDS[0][1]),)),
    'foreign_cut':bad(observations=(OC,OA,replace(OA,cut='B'),OB)),
    'foreign_job':one(replace(OA,job='other')),
    'foreign_queue':bad(frames=mutate_frame(0,queue='other')),
    'foreign_pin':bad(frames=mutate_frame(0,pin=12)),
    'future_epoch':bad(frames=mutate_frame(0,epoch=3)),
    'missing_observation':bad(observations=(OC,OA,OA)),
    'error_observation':bad(observations=(OC,replace(OA,status='error'),OA,OB)),
    'offset_zero':one(replace(OA,offset=0)),
    'offset_future':one(replace(OA,offset=3)),
    'incomplete_fields':one(replace(OA,complete=False)),
    'field_count':one(replace(OA,count=1)),
    'field_type':one(OA,fields=((A,(('n','wrong'),('tag','a'))),)),
    'work_substitution':one(replace(OA,start=4,end=6)),
    'receipt_substitution':one(replace(OA,start=50,end=52)),
    'interior_span':one(replace(OA,start=21,end=22,digest=(2,))),
    'wrong_digest':one(replace(OA,digest=(9,9))),
    'historical_below':one(replace(OA,hwm=2)),
    'historical_above':one(OA,frames=mutate_frame(0,commands=tuple(replace(c,hwm=2) if (c.collection,c.key)==A else c for c in FRAMES[0].commands))),
    'second_bad_alias':bad(observations=(OC,OA,replace(OA,start=10,end=12),OB)),
    'C1_read_failure':bad(stop='read1'),
    'C2_read_failure':bad(stop='read2'),
    'late_copy_failure':bad(stop='copy2'),
    'C2_bad_span':bad(observations=(replace(OC,start=21),OA,OA,OB)),
    'C2_bad_hwm':bad(observations=(replace(OC,hwm=2),OA,OA,OB)),
    'C2_bad_pin':bad(frames=mutate_frame(1,pin=12)),
    'exact_cap':replace(BASE,limit=51),
    'cap_minus_one':replace(BASE,limit=50,outcome='too_large',expected=()),
}
recreated_frames=mutate_frame(1,commands=(Command('s','a',3,10,(1,2)),)+FRAMES[1].commands)
recreated=replace(OA,hwm=3,offset=2,start=10,end=12)
CASES['recreated']=replace(BASE,request=(A,),observations=(recreated,),frames=recreated_frames,expected=(('s','a',3,FIELDS[0][1],(1,2)),))
CASES['old_incarnation']=one(replace(OA,hwm=3),frames=recreated_frames)
CASES['equal_live_bytes']=replace(BASE,request=(B,A),observations=(replace(OB,end=32,digest=(1,2)),OA),
    frames=mutate_frame(0,commands=tuple(replace(x,value=(1,2)) if (x.collection,x.key)==B else x for x in FRAMES[0].commands)),
    expected=(('s','b',1,(),(1,2)),RA))
CASES['other_collection_positive']=replace(BASE,request=(R,),observations=(replace(OA,identity=R,start=10,end=12,count=1),),expected=(('r','a',1,FIELDS[3][1],(1,2)),))
CASES['independent_cut_B']=replace(BASE,cut='B',observations=tuple(replace(o,cut='B') for o in BASE.observations),frames=tuple(replace(f,cut='B') for f in FRAMES))

for status in ('absent','tombstone'):
    observation=Observation(A,status,2 if status=='tombstone' else 0)
    for scope,value in (('cut','B'),('job','other')):
        CASES['foreign_'+status+'_'+scope]=one(replace(observation,**{scope:value}))
CASES['tombstone_zero_hwm']=one(Observation(A,'tombstone'))
CASES['tombstone_fields']=one(Observation(A,'tombstone',2,count=1))
CASES['absent_retained_hwm']=one(Observation(A,'absent',1))

FAULTS={'ss_first_group_only':'SV-MEMBER','ss_skip_cut':'SV-METADATA','ss_skip_job':'SV-METADATA','ss_empty_scope':'SV-SCOPE','ss_skip_observation':'SV-METADATA',
    'ss_skip_field_finish':'SV-FIELDS','ss_skip_field_count':'SV-FIELDS','ss_skip_field_type':'SV-FIELDS',
    'ss_skip_record_scope':'SV-MEMBER','ss_skip_identity':'SV-MEMBER','ss_skip_span':'SV-MEMBER',
    'ss_skip_digest':'SV-MEMBER','ss_old_hwm_le':'SV-MEMBER','ss_skip_alias':'SV-MEMBER',
    'ss_read_per_alias':'SV-GROUP','ss_copy_unfunded':'SV-COPY','ss_early_credit_release':'SV-OWNER',
    'ss_reorder':'SV-ORDER','ss_early_return':'SV-RETURN','ss_skip_size':'SV-SIZE',
    'ss_refund_failure':'SV-WORK','ss_keep_unknown_prefix':'SV-OWNER'}

@dataclass(frozen=True)
class State:
    case: str = ''
    phase: str = 'choose'
    index: int = 0
    group: int = 0
    events: tuple = ()
    rows: tuple = ()
    funded: tuple = ()
    physical: tuple = ()
    work: int = 4
    owner: bool = False
    output: tuple = ()
    result: str = ''

def unique(c): return tuple(dict.fromkeys(o.identity for o in c.observations if o.status=='live'))
def groups(c): return tuple(sorted({o.offset for o in c.observations if o.status=='live'}))
def metadata_ok(c,fault=None):
    return len(c.request)==len(c.observations) and all(o.identity==key and
        (o.cut==c.cut or fault=='ss_skip_cut') and (o.job=='job' or fault=='ss_skip_job') and
        (o.status=='live' and o.hwm>0 and 0<o.offset<c.through or
         o.status in ('absent','tombstone') and o.hwm>=(1 if o.status=='tombstone' else 0) and
         (o.status!='absent' or o.hwm==0) and o.count==o.offset==o.start==o.end==0 and not o.digest)
        for key,o in zip(c.request,c.observations))
def fields_ok(c,key,fault):
    fields=dict(c.fields).get(key,())
    for o in c.observations:
        if o.identity!=key or o.status!='live':continue
        if not o.complete and fault!='ss_skip_field_finish':return False
        if o.count!=len(fields) and fault!='ss_skip_field_count':return False
    return fault=='ss_skip_field_type' or all((name=='n' and type(value) is int) or (name=='tag' and type(value) is str) for name,value in fields)
def commands(c,offset):return next(f for f in c.frames if f.offset==offset).commands
def member_ok(c,offset,fault):
    if fault=='ss_first_group_only' and offset==2:return True
    frame=next((f for f in c.frames if f.offset==offset),None)
    if frame is None:return False
    obs=[o for o in c.observations if o.status=='live' and o.offset==offset]
    if fault!='ss_skip_record_scope' and (frame.pin!=11 or frame.epoch>2 or frame.cut!=c.cut or frame.queue!='q' or frame.job!='job' or any(o.cut!=c.cut or o.job!='job' for o in obs)):return False
    seen=set()
    for o in obs:
        if fault=='ss_skip_alias' and o.identity in seen:continue
        seen.add(o.identity)
        found=[x for x in frame.commands if x.domain=='state' and ((x.collection,x.key)==o.identity or fault=='ss_skip_identity')]
        if fault=='ss_skip_identity':found=[x for x in found if x.start==o.start]
        if len(found)!=1:return False
        x=found[0]
        if x.hwm!=o.hwm and not (fault=='ss_old_hwm_le' and x.hwm<=o.hwm):return False
        if fault!='ss_skip_span' and (x.start,x.start+len(x.value))!=(o.start,o.end):return False
        if fault!='ss_skip_digest' and x.value!=o.digest:return False
    return True

def emit(s,word,arg,phase,**changes):
    return word+'-'+str(len(s.events)),replace(s,phase=phase,events=s.events+((word,arg),),**changes)
class SourceStateRead(Model):
    name='api007-source-state-read'
    initial=State()
    properties=sorted(set(FAULTS.values())|{'SV-HISTORY'})
    bounds={'cases':len(CASES),'live_identities':3,'caller_positions':4,'physical_frames':2,'fields_per_record':2,'frontier':'complete output drop or complete refusal per fixture'}
    assumptions=['qualified original-bound header port supplies the persisted definition pin',
        'qualified original-bound C port supplies physical membership/work/retirement',
        'complete field-range cut/job transport is an assumed qualified port',
        'canonical immutable symbolic command intervals and injective digest tokens',
        'public response size measured after materialization in abstract units',
        'no Rust/codec/hash/exact heap/provider/freshness/shutdown/performance theorem']
    def __init__(self,fault=None):
        if fault is not None and fault not in FAULTS:raise ValueError('unknown source-state fault')
        self.fault=fault
    def frontier(self,s):return s.phase=='done'
    def transitions(self,s):
        if s.phase=='choose':
            for name in CASES:yield 'ChooseState-'+name,replace(s,case=name,phase='bind',events=(('select',name),))
            return
        c=CASES[s.case];fault=self.fault
        def refuse(reason,result='unknown'):
            changes=dict(rows=(),funded=(),physical=(),owner=False,output=(),result=result)
            if fault=='ss_refund_failure' and reason=='read':changes['work']=s.work+1
            if fault=='ss_keep_unknown_prefix' and s.rows:
                changes.update(rows=s.rows,funded=s.funded,owner=s.owner)
            return emit(s,'refuse',(reason,result),'done',**changes)
        if s.phase=='bind':
            if not c.scope or not c.admission:
                if not (fault=='ss_empty_scope' and not c.request):yield refuse('scope');return
            yield emit(s,'bind',None,'header',owner=True)
        elif s.phase=='header':yield emit(s,'header',11,'metadata')
        elif s.phase=='metadata':
            if not metadata_ok(c,fault) and fault!='ss_skip_observation':yield refuse('metadata');return
            yield emit(s,'metadata',None,'fields')
        elif s.phase=='fields':
            keys=unique(c)
            if s.index==len(keys):yield emit(s,'fields_done',None,'read',index=0);return
            key=keys[s.index]
            if not fields_ok(c,key,fault):yield refuse('fields');return
            yield emit(s,'fields',key,'fields',index=s.index+1)
        elif s.phase=='read':
            offsets=groups(c)
            if fault=='ss_read_per_alias' and s.group==1:
                yield emit(s,'read',offsets[0],'record',work=s.work-1);return
            if fault=='ss_early_return' and s.group==1 and s.group<len(offsets):
                yield emit(s,'fund_output',len(c.request),'assemble',funded=s.funded+('output',));return
            if s.group==len(offsets):yield emit(s,'fund_output',len(c.request),'assemble',funded=s.funded+('output',));return
            offset=offsets[s.group]
            yield emit(s,'read',offset,'record',work=s.work-1)
        elif s.phase=='record':
            offset=groups(c)[s.group]
            if c.stop=='read'+str(offset):yield refuse('read');return
            yield emit(s,'retain',offset,'verify',physical=(offset,))
        elif s.phase=='verify':
            offset=groups(c)[s.group]
            if not member_ok(c,offset,fault):yield refuse('membership');return
            yield emit(s,'verify',offset,'fund',index=0)
        elif s.phase=='fund':
            offset=groups(c)[s.group]
            keys=tuple(dict.fromkeys(o.identity for o in c.observations if o.status=='live' and o.offset==offset))
            if s.index==len(keys):yield emit(s,'retire',offset,'read',physical=(),group=s.group+1,index=0);return
            if c.stop=='copy2' and offset==2:yield refuse('copy');return
            key=keys[s.index]
            if fault=='ss_copy_unfunded':
                command=next(x for x in commands(c,offset) if (x.collection,x.key)==key and x.domain=='state')
                row=(key[0],key[1],command.hwm,dict(c.fields).get(key,()),command.value)
                yield emit(s,'copy',(key,row),'after_copy',rows=s.rows+((key,row),),index=s.index+1);return
            yield emit(s,'fund',key,'copy',funded=s.funded+(key,))
        elif s.phase=='copy':
            offset=groups(c)[s.group]
            key=tuple(dict.fromkeys(o.identity for o in c.observations if o.status=='live' and o.offset==offset))[s.index]
            command=next((x for x in commands(c,offset) if (x.collection,x.key)==key and x.domain=='state'),None)
            row=(key[0],key[1],command.hwm,dict(c.fields).get(key,()),command.value) if command else ()
            yield emit(s,'copy',(key,row),'after_copy',rows=s.rows+((key,row),),index=s.index+1)
        elif s.phase=='after_copy':
            if fault=='ss_early_credit_release':yield emit(s,'drop_credit',s.rows[-1][0],'fund',funded=s.funded[:-1]);return
            yield emit(s,'continue',None,'fund')
        elif s.phase=='assemble':
            rows=dict(s.rows);output=tuple(rows.get(key) for key in c.request)
            if fault=='ss_reorder':output=tuple(reversed(output))
            yield emit(s,'assemble',output,'size',output=output)
        elif s.phase=='size':
            if public_size(s.output)>c.limit and fault!='ss_skip_size':yield refuse('size','too_large');return
            yield emit(s,'return',s.output,'drop',result='ok')
        elif s.phase=='drop':yield emit(s,'drop',None,'done',owner=False,rows=(),funded=(),output=())
    def violation(self,before,action,after):
        if before.case and before.case!=after.case:return 'SV-HISTORY'
        if not after.case:return None if after==self.initial else 'SV-HISTORY'
        if after.events[:len(before.events)]!=before.events:return 'SV-HISTORY'
        if before!=after and len(after.events)!=len(before.events)+1:return 'SV-HISTORY'
        return PREFIX_ORACLE(after,CASES[after.case])
