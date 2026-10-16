"""Bounded candidate-production traces, not Rust refinement or performance proof.

The producer compiles an AST to postfix instructions and streams typed postings.
The gate separately interprets immutable records and the original AST. Source
index faithfulness, canonical codecs, and authentic source ports are premises.
The abstract field n is prefix-declared and accepts all five scalar types;
fixed declared-field restrictions are separately exercised by Rust tests.
"""
from dataclasses import dataclass, replace
from models import Model

KINDS=('integer','float','boolean','text','bytes')
# Full identities include collection. A terminal work item remains in U.
RECORDS=((('work','a'),True,False,(('n','integer',1),)),
         (('work','b'),True,False,()),
         (('work','c'),True,True,(('n','text','different'),)),
         (('other','a'),True,False,(('n','integer',1),)))
EQ=('eq','n','integer',1); PRESENT=('present','n',True)
@dataclass(frozen=True)
class Case:
    expression: tuple
    expected: tuple
    records: tuple = RECORDS
    collection: str = 'work'
    cut: str = 'A'
    producer: str = 'P'
    mode: str = 'success'
    outcome: str = 'ok'
    job: str = 'job'
    budget: str = 'budgetArcA'
    pin: str = 'definitionA'
    clause: int = 0
    capacity: tuple = (100,100,100)
    root: int = 64

A=('work','a'); B=('work','b'); C=('work','c'); O=('other','a')
CASES={
    'equal':Case(EQ,(A,)),
    'three_distinct_fold_children':Case(('any',EQ,('present','n',False),('eq','n','text','different')),(A,B,C)),
    'work_exact':Case(EQ,(A,),capacity=(2,1,1)),
    'work_row_short':Case(EQ,(),capacity=(1,1,1),outcome='unknown'),
    'work_copy_short':Case(EQ,(),capacity=(2,0,1),outcome='unknown'),
    'work_step_short':Case(EQ,(),capacity=(2,1,0),outcome='unknown'),
    'algebra_work_exact':Case(('not',EQ),(B,C),capacity=(7,4,5)),
    'algebra_work_short':Case(('not',EQ),(),capacity=(7,4,4),outcome='unknown'),
    'root_exact':Case(EQ,(A,),root=2),
    'root_copy_short':Case(EQ,(),root=1,outcome='unknown'),
    'root_manifest_short':Case(EQ,(),root=0,outcome='unknown'),
    'same_kind_occurrences':Case(('all',EQ,EQ),(A,)),
    'multi_range':Case(('in','n','integer',(1,2)),(A,C),records=(RECORDS[0],RECORDS[1],(C,True,True,(('n','integer',2),)))),
    'identical_empty_clause0':Case(('any',),()),
    'identical_empty_clause1':Case(('any',),(),clause=1),
    'independent_budget_and_pin':Case(EQ,(A,),budget='budgetArcB',pin='definitionB'),
    'present_all_types':Case(PRESENT,(A,C)),
    'missing':Case(('present','n',False),(B,)),
    'not_equal':Case(('not',EQ),(B,C)),
    'not_present':Case(('not',PRESENT),(B,)),
    'all_empty':Case(('all',),(A,B,C)),
    'any_empty':Case(('any',),()),
    'nested':Case(('not',('any',EQ,('present','n',False))),(C,)),
    'intersection':Case(('all',('not',EQ),PRESENT),(C,)),
    'duplicate_occurrences':Case(('all',EQ,EQ,PRESENT),(A,)),
    'decisive_empty':Case(('all',('any',),EQ),()),
    'other_collection':Case(EQ,(O,),collection='other'),
    'independent_cut':Case(EQ,(A,),cut='B',producer='Q'),
    'independent_producer_same_cut':Case(EQ,(A,),producer='Q'),
    'empty_source':Case(('not',EQ),(),records=()),
    'tombstone':Case(('all',),(B,C),records=((A,False,False,()),)+RECORDS[1:]),
    'unsupported_decisive':Case(('all',('any',),('unsupported',)),(),outcome='unknown'),
    'late_write_failure':Case(PRESENT,(),mode='write_failure',outcome='unknown'),
    'direct_cancel':Case(PRESENT,(),mode='cancel',outcome='unknown'),
    'accepted_caller_abort':Case(PRESENT,(A,C),mode='caller_abort'),
    'unsupported_keys':Case(('keys',(('work','missing'),)),(),outcome='unknown'),
    'late_algebra_failure':Case(('all',('not',EQ),PRESENT),(),mode='algebra_failure',outcome='unknown'),
    'late_algebra_cancel':Case(('all',('not',EQ),PRESENT),(),mode='algebra_cancel',outcome='unknown'),
    'early_algebra_failure':Case(('all',('not',EQ),PRESENT),(),mode='early_algebra_failure',outcome='unknown'),
    'early_algebra_cancel':Case(('all',('not',EQ),PRESENT),(),mode='early_algebra_cancel',outcome='unknown'),
    'algebra_then_source_failure':Case(('all',('not',EQ),PRESENT),(),mode='write_failure',outcome='unknown'),
}
for kind,value in zip(KINDS,(2,2.5,True,'v',b'v')):
    CASES['presence_'+kind]=Case(PRESENT,(A,),records=((A,True,False,(('n',kind,value),)),))

FAULTS={
    'sel_omit_completion':'SEL-COMPLETE',
    'sel_seal_pending_write':'SEL-WRITE',
    'sel_foreign_cut':'SEL-SCOPE',
    'sel_foreign_producer':'SEL-SCOPE',
    'sel_exchange_occurrence':'SEL-COMPLETE',
    'sel_eligible_universe':'SEL-MEMBERS',
    'sel_omit_presence_kind':'SEL-COMPLETE',
    'sel_unsupported_shortcut':'SEL-SUPPORT',
    'sel_partial_grant':'SEL-WORK',
    'sel_refund_failure':'SEL-WORK',
    'sel_early_owner_release':'SEL-OWNER',
    'sel_bad_algebra_input':'SEL-ALGEBRA',
    'sel_wrong_result':'SEL-MEMBERS',
    'sel_unknown_prefix':'SEL-RETURN',
    'sel_abort_drops_operation':'SEL-OWNER',
    'sel_raw_nonexistent_keys':'SEL-SUPPORT',
    'sel_omit_empty_builder':'SEL-COMPLETE',
    'sel_same_kind_certificate':'SEL-SCOPE',
    'sel_foreign_leaf_proof':'SEL-SCOPE',
    'sel_same_balance_foreign_budget':'SEL-SCOPE',
    'sel_wrong_empty_clause':'SEL-SCOPE',
    'sel_unfunded_copy':'SEL-ROOT',
    'sel_unfunded_manifest':'SEL-ROOT',
    'sel_omit_algebra_debit':'SEL-WORK',
    'sel_partial_denial':'SEL-WORK',
    'sel_spent_grant_denial':'SEL-RETURN',
}

def compile_program(expression):
    tag,*args=expression
    if tag in ('all','any'):
        return sum((compile_program(child) for child in args),[])+[(tag,len(args))]
    if tag=='not':return compile_program(args[0])+[('not',)]
    return [expression]

def context(c):return (c.cut,c.producer,c.job,c.budget,c.pin,7,c.clause)

def stream(c,leaf,kind):
    rows=[]
    expected_kind=kind[0] if leaf[0]=='in' else kind
    expected_value=kind[1] if leaf[0]=='in' else (leaf[3] if leaf[0]=='eq' else None)
    for identity,live,terminal,fields in c.records:
        if not live or identity[0]!=c.collection:continue
        if leaf[0]=='universe':rows.append(identity);continue
        for name,field_kind,value in fields:
            if name==leaf[1] and field_kind==expected_kind and (leaf[0]=='present' or value==expected_value):
                rows.append(identity)
    return tuple(sorted(set(rows)))

def scan_leaf(c,fault,leaf_id,leaf):
    yield ('manifest',(leaf_id,leaf))
    kinds=('record',) if leaf[0]=='universe' else (tuple((leaf[2],v) for v in leaf[3]) if leaf[0]=='in' else ((leaf[2],) if leaf[0]=='eq' else KINDS))
    if fault=='sel_omit_presence_kind' and leaf[0]=='present':kinds=kinds[:-1]
    rows=set()
    for kind in kinds:
        scope=(context(c),leaf_id,kind)
        if fault=='sel_foreign_cut':scope=(('foreign',*scope[0][1:]),*scope[1:])
        if fault=='sel_foreign_producer':scope=((scope[0][0],'foreign',*scope[0][2:]),*scope[1:])
        yield ('open',scope)
        for identity in stream(c,leaf,kind):
            if fault=='sel_eligible_universe' and leaf[0]=='universe' and next(r[2] for r in c.records if r[0]==identity):continue
            yield ('grant',(1,1,1))
            if fault!='sel_unfunded_copy':yield ('fund_copy',1)
            yield ('copy',identity)
            yield ('verify',identity)
            yield ('write_pending',identity)
            if c.mode=='caller_abort':yield ('caller_abort',None)
            if c.mode in ('write_failure','cancel') and leaf[0]=='present' and identity==C:
                yield ('unknown',c.mode);yield ('retire',None);yield ('coordinator_drop',None);return
            if fault=='sel_seal_pending_write':yield ('seal',(leaf_id,tuple(sorted(rows))));return
            yield ('write_done',identity)
            rows.add(identity)
        yield ('grant',(1,0,0))
        yield ('terminal_empty',kind)
        if fault!='sel_omit_completion':yield ('finish',(context(c),leaf_id,kind,('cursor',leaf_id,kind)))
    proof_id=leaf_id
    if fault=='sel_exchange_occurrence' and isinstance(leaf_id,int):proof_id=leaf_id+1
    value=tuple(sorted(rows))
    yield ('seal',(context(c),proof_id,value))
    return value

def raw_protocol(c,fault=None):
    """Producer-side finite event driver. Outputs are computed from postings."""
    code=compile_program(c.expression)
    yield ('bind',context(c))
    supported=all(node[0] in ('eq','in','present','all','any','not') for node in code)
    if not supported and fault!='sel_unsupported_shortcut' and not (fault=='sel_raw_nonexistent_keys' and c.expression[0]=='keys'):
        yield ('unknown','unsupported');yield ('retire',None);yield ('coordinator_drop',None);return
    if fault=='sel_raw_nonexistent_keys' and c.expression[0]=='keys':
        yield ('return',c.expression[1]);return
    if fault!='sel_unfunded_manifest':yield ('fund_manifest',len(code))
    yield ('admit',len(code))
    needs_u=any(node[0]=='not' or node==('all',0) or (node[0]=='present' and not node[2]) for node in code)
    sets={};stack=[]
    if needs_u:
        sets['U']=yield from scan_leaf(c,fault,'U',('universe',))
        if sets['U'] is None:return
    if any(node==('any',0) for node in code) and fault!='sel_omit_empty_builder':
        yield ('empty_builder',context(c))
        yield ('empty_seal',(context(c),'empty',()))
    for node_index,node in enumerate(code):
        tag=node[0]
        if tag in ('eq','in','present'):
            value=yield from scan_leaf(c,fault,node_index,node)
            if value is None:return
            sets[node_index]=value
            if tag=='present' and not node[2]:
                value=tuple(sorted(set(sets['U'])-set(value)))
                yield ('algebra',('difference',sets['U'],sets[node_index],value))
            stack.append(value)
        elif tag=='not':
            right=stack.pop();value=tuple(sorted(set(sets['U'])-set(right)))
            yield ('algebra',('difference',sets['U'],right,value));stack.append(value)
            if c.mode in ('early_algebra_failure','early_algebra_cancel'):
                yield ('unknown',c.mode);yield ('retire',None);yield ('coordinator_drop',None);return
        elif tag in ('all','any'):
            count=node[1];operands=stack[-count:] if count else []
            if count:del stack[-count:]
            operands.reverse()  # Rust pops the rightmost child first.
            value=operands[0] if operands else (sets['U'] if tag=='all' else ())
            for right in operands[1:]:
                op='intersection' if tag=='all' else 'union'
                output=tuple(sorted(set(value)&set(right) if tag=='all' else set(value)|set(right)))
                yield ('algebra',(op,value,right,output));value=output
                if c.mode in ('algebra_failure','algebra_cancel'):
                    yield ('unknown',c.mode);yield ('retire',None);yield ('coordinator_drop',None);return
            stack.append(value)
        else:stack.append(())  # only reachable under unsupported-shortcut mutant
    yield ('return',stack[-1]);yield ('retire',None);yield ('coordinator_drop',None)

def protocol(c,fault=None):
    work=list(c.capacity);root=c.root;funded=0
    for word,arg in raw_protocol(c,fault):
        demands=[('algebra_grant',(1,0,1)),(word,arg)] if word=='algebra' else [(word,arg)]
        for event,value in demands:
            if event in ('grant','algebra_grant'):
                if any(a<b for a,b in zip(work,value)):
                    yield ('unknown',('budget',value));yield ('retire',None);yield ('coordinator_drop',None);return
                work=[a-b for a,b in zip(work,value)]
            elif event in ('fund_manifest','fund_copy'):
                if root<value:
                    yield ('unknown',('root',event,value));yield ('retire',None);yield ('coordinator_drop',None);return
                root-=value;funded+=value
            elif event=='write_done':root+=1;funded-=1
            elif event=='retire':root+=funded;funded=0
            yield (event,value)

@dataclass(frozen=True)
class State:
    case: str = ''
    events: tuple = ()
    owner: bool = False  # producer ports and scratch
    capsule: bool = False  # actual capsule retained by external coordinator
    builders: tuple = ()
    copies: tuple = ()
    pending: tuple = ()
    sets: tuple = ()
    work: tuple = (100,100,100)
    root: int = 64
    funded: int = 0
    output: tuple = ()
    result: str = ''
    done: bool = False

PREFIX_ORACLE=None
class SourceSelection(Model):
    name='api007-source-selection'
    initial=State()
    properties=['SEL-SCOPE','SEL-SUPPORT','SEL-WORK','SEL-WRITE','SEL-COMPLETE',
                'SEL-MEMBERS','SEL-ALGEBRA','SEL-OWNER','SEL-RETURN','SEL-ROOT']
    bounds={'records_per_fixture':4,'collections':2,'field_types':5,'cuts':2,
            'numeric_head':7,'original_programs':len(CASES),'schedule':'finite named event schedules'}
    assumptions=['canonical codecs and authentic source ports',
                 'coupled producer index faithfulness to immutable records',
                 'immutable scratch set operations return exact full-identity algebra',
                 'abstract costs, not allocator/RSS, SDK retirement or Rust refinement']
    def __init__(self,fault=None):self.fault=fault
    def transitions(self,s):
        if not s.case:
            for name,c in CASES.items():yield 'ChooseSelection-'+name,replace(s,case=name,work=c.capacity,root=c.root)
            return
        if s.done:return
        events=list(protocol(CASES[s.case],self.fault))
        if len(s.events)>=len(events):return
        word,arg=events[len(s.events)]
        if self.fault=='sel_same_kind_certificate' and word=='finish':
            kind=arg[2];base=kind[0] if isinstance(kind,tuple) else kind
            prior=[v for w,v in events[:len(s.events)] if w=='finish' and (v[2][0] if isinstance(v[2],tuple) else v[2])==base]
            if prior:arg=prior[0]
        if self.fault=='sel_foreign_leaf_proof' and word=='seal':
            scope,leaf,value=arg;arg=((('foreign',)+scope[1:]),leaf,value)
        if self.fault=='sel_same_balance_foreign_budget' and word=='finish':
            scope,leaf,kind,cursor=arg;arg=(scope[:3]+('foreignArcSameBalances',)+scope[4:],leaf,kind,cursor)
        if self.fault=='sel_wrong_empty_clause' and word=='empty_seal':
            scope,leaf,value=arg;arg=(scope[:-1]+(scope[-1]+1,),leaf,value)
        if self.fault=='sel_spent_grant_denial' and s.case=='algebra_work_exact' and word=='algebra':word,arg='unknown',('budget',(1,0,1))
        if self.fault=='sel_bad_algebra_input' and word=='algebra':
            op,left,right,output=arg;arg=(op,(),right,output)
        if self.fault=='sel_wrong_result' and word=='return':arg=(('work','nonexistent'),)
        changes={'events':s.events+((word,arg),)}
        if word=='bind':changes.update(owner=True,capsule=True)
        elif word in ('grant','algebra_grant'):
            debit=(arg[0],0,0) if self.fault=='sel_partial_grant' else ((0,0,0) if self.fault=='sel_omit_algebra_debit' and word=='algebra_grant' else arg)
            changes['work']=tuple(a-b for a,b in zip(s.work,debit))
        elif word in ('fund_manifest','fund_copy'):changes.update(root=s.root-arg,funded=s.funded+arg)
        elif word=='empty_builder':changes['builders']=('empty',)
        elif word=='empty_seal':changes.update(builders=(),sets=s.sets+(arg,))
        elif word=='copy':changes['copies']=s.copies+(arg,)
        elif word=='write_pending':changes['pending']=(arg,)
        elif word=='write_done':changes.update(pending=(),copies=(),root=s.root+1,funded=s.funded-1)
        elif word=='seal':changes['sets']=s.sets+(arg,)
        elif word=='return':changes.update(output=arg,result='ok')
        elif word=='unknown':
            changes.update(result='unknown',output=())
            if self.fault=='sel_refund_failure':changes['work']=CASES[s.case].capacity
            if self.fault=='sel_partial_denial' and isinstance(arg,tuple) and arg[0]=='budget':changes['work']=(s.work[0]-1,*s.work[1:])
            if self.fault=='sel_unknown_prefix':changes['output']=(A,)
        elif word=='retire':changes.update(owner=False,builders=(),copies=(),pending=(),sets=(),output=(),root=s.root+s.funded,funded=0)
        elif word=='coordinator_drop':changes.update(capsule=False,done=True)
        if self.fault=='sel_early_owner_release' and word=='copy':changes['capsule']=False
        if self.fault=='sel_abort_drops_operation' and word=='caller_abort':changes['owner']=False
        yield word,replace(s,**changes)
    def violation(self,before,action,after):
        if not before.case and after.case:
            if before!=self.initial or after.case not in CASES or after!=replace(self.initial,case=after.case,work=CASES[after.case].capacity,root=CASES[after.case].root):return 'SEL-SCOPE'
        if before.case and before.case!=after.case:return 'SEL-SCOPE'
        if after.events[:len(before.events)]!=before.events:return 'SEL-SCOPE'
        if before.case and before!=after and len(after.events)!=len(before.events)+1:return 'SEL-SCOPE'
        if not after.case:return None if after==self.initial else 'SEL-SCOPE'
        return PREFIX_ORACLE(after,CASES[after.case])
