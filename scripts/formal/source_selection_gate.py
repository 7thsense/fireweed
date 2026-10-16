"""Independent record/AST interpreter and strict event-prefix verifier.

No expected member or algebra input comes from a produced set, compiled plan,
completion flag, or producer helper. Model state is checked against replay of
ordinary events at every prefix, including failures and operation retirement.
"""
TYPES=('integer','float','boolean','text','bytes')

def reference(c):
    live={identity:fields for identity,alive,terminal,fields in c.records
          if alive and identity[0]==c.collection}
    universe=set(live)
    def evaluate(expr):
        tag,*args=expr
        if tag=='in':return {key for key,fields in live.items() if any(name==args[0] and kind==args[1] and value in args[2] for name,kind,value in fields)}
        if tag=='eq':return {key for key,fields in live.items() if (args[0],args[1],args[2]) in fields}
        if tag=='present':
            present={key for key,fields in live.items() if any(name==args[0] for name,kind,value in fields)}
            return present if args[1] else universe-present
        if tag=='not':return universe-evaluate(args[0])
        if tag=='all':
            result=universe.copy()
            for child in args:result &= evaluate(child)
            return result
        if tag=='any':
            result=set()
            for child in args:result |= evaluate(child)
            return result
        raise ValueError('unsupported original request')
    return tuple(sorted(evaluate(c.expression)))

def original_manifest(c):
    """Derive occurrence identities from original AST; never read producer code."""
    instructions=[];needs_universe=False;algebra=[];timeline=[]
    live={identity:fields for identity,alive,terminal,fields in c.records
          if alive and identity[0]==c.collection}
    u=tuple(sorted(live))
    def visit(expr):
        nonlocal needs_universe
        tag,*args=expr
        if tag in ('all','any'):
            children=[visit(child) for child in args]
            children.reverse()  # Independent AST evaluation, actual stack-pop schedule.
            if not children and tag=='all':needs_universe=True
            value=children[0] if children else (u if tag=='all' else ())
            for right in children[1:]:
                op='intersection' if tag=='all' else 'union'
                output=tuple(sorted(set(value)&set(right) if tag=='all' else set(value)|set(right)))
                algebra.append((op,value,right,output));timeline.append(('algebra',len(algebra)-1));value=output
            instructions.append((tag,len(args)));return value
        if tag=='not':
            needs_universe=True;right=visit(args[0]);value=tuple(sorted(set(u)-set(right)))
            algebra.append(('difference',u,right,value));timeline.append(('algebra',len(algebra)-1));instructions.append(('not',));return value
        if tag not in ('eq','in','present'):raise ValueError('unsupported original request')
        timeline.append(('leaf',len(instructions)))
        instructions.append(expr)
        members=tuple(sorted(key for key,fields in live.items()
            if ((args[0],args[1],args[2]) in fields if tag=='eq' else any(name==args[0] and kind==args[1] and value in args[2] for name,kind,value in fields) if tag=='in'
                else any(name==args[0] for name,kind,value in fields))))
        if tag=='present' and not args[1]:
            needs_universe=True;value=tuple(sorted(set(u)-set(members)))
            algebra.append(('difference',u,members,value));timeline.append(('algebra',len(algebra)-1));return value
        return members
    visit(c.expression)
    manifest=[]
    if needs_universe:
        manifest.append(('U',('universe',),('record',),u));timeline.insert(0,('leaf','U'))
    for index,node in enumerate(instructions):
        if node[0] not in ('eq','in','present'):continue
        rows=tuple(sorted(key for key,fields in live.items() if
            ((node[1],node[2],node[3]) in fields if node[0]=='eq' else any(name==node[1] and kind==node[2] and value in node[3] for name,kind,value in fields) if node[0]=='in'
             else any(name==node[1] for name,kind,value in fields))))
        manifest.append((index,node,tuple((node[2],v) for v in node[3]) if node[0]=='in' else (node[2],) if node[0]=='eq' else TYPES,rows))
    if ('any',0) in instructions:timeline.insert(1 if needs_universe else 0,('empty',None))
    return manifest,algebra,len(instructions),timeline

def prefix_violation(s,c):
    try:manifest,operations,count,timeline=original_manifest(c);supported=True
    except ValueError:manifest=[];operations=[];count=0;timeline=[];supported=False
    scope=(c.cut,c.producer,c.job,c.budget,c.pin,7,c.clause)
    mode='bind';owner=False;capsule=False;builders=();copies=();pending=();sets=[];work=list(c.capacity);root=c.root;funded=0
    output=();result='';done=False;admitted=False;leaf_index=0;kind_index=0
    rows=set();visited=[];current=None;verified=False;grant=None;algebra_index=0
    empty=False;finished=False;aborted=False;dispatch=0;algebra_granted=False
    for word,arg in s.events:
        if done:return 'SEL-RETURN'
        if word=='bind':
            if mode!='bind' or arg!=scope:return 'SEL-SCOPE'
            owner=True;capsule=True;mode='support'
        elif word=='fund_manifest':
            if not supported:return 'SEL-SUPPORT'
            if mode!='support' or arg!=count or root<arg:return 'SEL-ROOT'
            root-=arg;funded+=arg;mode='admit'
        elif word=='admit':
            if not supported:return 'SEL-SUPPORT'
            if mode!='admit' or arg!=count or funded!=count:return 'SEL-ROOT'
            admitted=True;mode='manifest'
        elif word=='empty_builder':
            if mode!='manifest' or dispatch>=len(timeline) or timeline[dispatch]!=('empty',None):return 'SEL-COMPLETE'
            if arg!=scope:return 'SEL-SCOPE'
            builders=('empty',);mode='empty_seal'
        elif word=='empty_seal':
            if mode!='empty_seal' or builders!=('empty',):return 'SEL-COMPLETE'
            if arg!=(scope,'empty',()):return 'SEL-SCOPE'
            builders=();sets.append(arg);dispatch+=1;mode='manifest'
        elif word=='manifest':
            if mode!='manifest' or not admitted or leaf_index>=len(manifest):return 'SEL-COMPLETE'
            identity,expr,kinds,members=manifest[leaf_index]
            if arg!=(identity,expr) or dispatch>=len(timeline) or timeline[dispatch]!=('leaf',identity):return 'SEL-COMPLETE'
            rows=set();kind_index=0;mode='open'
        elif word=='open':
            if mode!='open' or leaf_index>=len(manifest):return 'SEL-COMPLETE'
            identity,expr,kinds,members=manifest[leaf_index]
            if kind_index>=len(kinds):return 'SEL-COMPLETE'
            if arg!=(scope,identity,kinds[kind_index]):return 'SEL-SCOPE'
            visited=[];empty=False;finished=False;mode='grant'
        elif word=='grant':
            if mode!='grant':return 'SEL-WORK'
            if arg not in ((1,1,1),(1,0,0)):return 'SEL-WORK'
            work=[a-b for a,b in zip(work,arg)]
            if min(work)<0:return 'SEL-WORK'
            grant=arg;mode='row' if arg==(1,1,1) else 'terminal'
        elif word=='fund_copy':
            if mode!='row' or arg!=1 or root<1:return 'SEL-ROOT'
            root-=1;funded+=1;mode='funded_row'
        elif word=='copy':
            if mode!='funded_row':return 'SEL-ROOT'
            if mode!='funded_row' or grant!=(1,1,1) or copies or pending:return 'SEL-WRITE'
            identity,expr,kinds,members=manifest[leaf_index]
            expected=[]
            range_kind=kinds[kind_index][0] if expr[0]=='in' else kinds[kind_index]
            range_value=kinds[kind_index][1] if expr[0]=='in' else (expr[3] if expr[0]=='eq' else None)
            for key,alive,terminal,fields in c.records:
                if not alive or key[0]!=c.collection:continue
                if expr[0]=='universe' or any(name==expr[1] and kind==range_kind
                    and (expr[0]=='present' or value==range_value) for name,kind,value in fields):expected.append(key)
            expected=sorted(set(expected))
            if len(visited)>=len(expected) or arg!=expected[len(visited)]:return 'SEL-MEMBERS'
            current=arg;copies=(arg,);verified=False;mode='verify'
        elif word=='verify':
            if mode!='verify' or arg!=current:return 'SEL-MEMBERS'
            verified=True;mode='write'
        elif word=='write_pending':
            if mode!='write' or not verified or arg!=current or not copies:return 'SEL-WRITE'
            pending=(arg,);mode='pending'
        elif word=='caller_abort':
            if mode!='pending' or c.mode!='caller_abort':return 'SEL-OWNER'
            aborted=True  # Caller disappearance does not drop the operation.
        elif word=='write_done':
            if mode!='pending' or pending!=(arg,) or arg!=current:return 'SEL-WRITE'
            visited.append(arg);rows.add(arg);pending=();copies=();root+=1;funded-=1;mode='grant'
        elif word=='terminal_empty':
            if mode!='terminal' or grant!=(1,0,0) or pending or copies:return 'SEL-WRITE'
            identity,expr,kinds,members=manifest[leaf_index]
            if arg!=kinds[kind_index]:return 'SEL-COMPLETE'
            range_kind=arg[0] if expr[0]=='in' else arg
            range_value=arg[1] if expr[0]=='in' else (expr[3] if expr[0]=='eq' else None)
            expected={key for key,alive,terminal,fields in c.records if alive and key[0]==c.collection
                and (expr[0]=='universe' or any(name==expr[1] and kind==range_kind
                    and (expr[0]=='present' or value==range_value) for name,kind,value in fields))}
            if set(visited)!=expected:return 'SEL-MEMBERS'
            empty=True;mode='finish'
        elif word=='finish':
            identity,expr,kinds,members=manifest[leaf_index]
            if mode!='finish' or not empty:return 'SEL-COMPLETE'
            if arg!=(scope,identity,kinds[kind_index],('cursor',identity,kinds[kind_index])):return 'SEL-SCOPE'
            finished=True;kind_index+=1;mode='open' if kind_index<len(kinds) else 'seal'
        elif word=='seal':
            if pending or copies:return 'SEL-WRITE'
            if mode!='seal' or not finished:return 'SEL-COMPLETE'
            identity,expr,kinds,members=manifest[leaf_index]
            if len(arg)!=3 or arg[0]!=scope:return 'SEL-SCOPE'
            if arg[1:]!=(identity,members) or tuple(sorted(rows))!=members:return 'SEL-COMPLETE'
            sets.append(arg);leaf_index+=1;dispatch+=1;mode='manifest'
        elif word=='algebra_grant':
            if mode!='manifest' or dispatch>=len(timeline) or timeline[dispatch]!=('algebra',algebra_index):return 'SEL-ALGEBRA'
            if algebra_granted or arg!=(1,0,1) or any(a<b for a,b in zip(work,arg)):return 'SEL-WORK'
            work=[a-b for a,b in zip(work,arg)];algebra_granted=True
        elif word=='algebra':
            if mode!='manifest' or dispatch>=len(timeline) or timeline[dispatch]!=('algebra',algebra_index):return 'SEL-COMPLETE'
            if not algebra_granted:return 'SEL-WORK'
            if algebra_index>=len(operations) or arg!=operations[algebra_index]:return 'SEL-ALGEBRA'
            algebra_index+=1;dispatch+=1;algebra_granted=False
        elif word=='return':
            if not supported:return 'SEL-SUPPORT'
            if mode!='manifest' or dispatch!=len(timeline) or leaf_index!=len(manifest) or algebra_index!=len(operations):return 'SEL-COMPLETE'
            if c.outcome!='ok' or arg!=reference(c) or arg!=c.expected:return 'SEL-MEMBERS'
            output=arg;result='ok';mode='retire'
        elif word=='unknown':
            valid=(arg=='unsupported' and not supported and mode=='support'
                or arg in ('write_failure','cancel') and c.mode==arg and mode=='pending' and current==('work','c') and manifest[leaf_index][1][0]=='present'
                or arg in ('algebra_failure','algebra_cancel') and c.mode==arg and mode=='manifest'
                    and leaf_index==len(manifest) and algebra_index==len(operations)
                or arg in ('early_algebra_failure','early_algebra_cancel') and c.mode==arg and mode=='manifest'
                    and algebra_index==1 and leaf_index<len(manifest))
            if isinstance(arg,tuple) and arg[0]=='root':
                _,event,demand=arg
                valid=(event=='fund_manifest' and mode=='support' and supported and demand==count and root<demand
                    or event=='fund_copy' and mode=='row' and demand==1 and root<1)
            if isinstance(arg,tuple) and arg[0]=='budget':
                demand=arg[1];wanted=None
                if mode=='manifest' and dispatch<len(timeline) and timeline[dispatch]==('algebra',algebra_index):wanted=(1,0,1)
                elif mode=='grant':
                    _,expr,kinds,_=manifest[leaf_index];token=kinds[kind_index]
                    kind=token[0] if expr[0]=='in' else token
                    value=token[1] if expr[0]=='in' else (expr[3] if expr[0]=='eq' else None)
                    expected={key for key,alive,terminal,fields in c.records if alive and key[0]==c.collection and
                        (expr[0]=='universe' or any(n==expr[1] and k==kind and (expr[0]=='present' or v==value) for n,k,v in fields))}
                    wanted=(1,1,1) if len(visited)<len(expected) else (1,0,0)
                valid=not algebra_granted and demand==wanted and any(a<b for a,b in zip(work,demand))
            if not valid:return 'SEL-RETURN'
            result='unknown';output=();mode='retire'
        elif word=='retire':
            if mode!='retire' or not owner:return 'SEL-OWNER'
            owner=False;builders=();copies=();pending=();sets=[];output=();root+=funded;funded=0;mode='coordinator'
        elif word=='coordinator_drop':
            if mode!='coordinator' or owner or not capsule:return 'SEL-OWNER'
            capsule=False;done=True
        else:return 'SEL-RETURN'
        if not capsule and not done:return 'SEL-OWNER'
    if s.work!=tuple(work):return 'SEL-WORK'
    if (s.root,s.funded)!=(root,funded) or root<0:return 'SEL-ROOT'
    if s.owner!=owner or s.capsule!=capsule:return 'SEL-OWNER'
    if (s.builders,s.copies,s.pending)!=(builders,copies,pending):return 'SEL-WRITE'
    if s.sets!=tuple(sets):return 'SEL-COMPLETE'
    if s.output!=output or s.result!=result or s.done!=done:return 'SEL-RETURN'
    if done and (result!=c.outcome or (c.mode=='caller_abort' and not aborted)):return 'SEL-RETURN'
    return None

# Literal completed costs: rows = visited identities + terminal empty pages;
# copied units and scratch writes = visited identities. No byte/RSS claim.
COSTS={
    'work_exact':(2,1,1),'work_row_short':(1,1,1),'work_copy_short':(0,0,0),'work_step_short':(0,0,0),
    'algebra_work_exact':(7,4,5),'algebra_work_short':(6,4,4),
    'root_exact':(2,1,1),'root_copy_short':(1,1,1),'root_manifest_short':(0,0,0),
    'three_distinct_fold_children':(18,7,10),
    'equal':(2,1,1),'same_kind_occurrences':(5,2,3),'multi_range':(4,2,2),
    'identical_empty_clause0':(0,0,0),'identical_empty_clause1':(0,0,0),'independent_budget_and_pin':(2,1,1),'present_all_types':(7,2,2),'missing':(12,5,6),
    'not_equal':(7,4,5),'not_present':(12,5,6),'all_empty':(4,3,3),
    'any_empty':(0,0,0),'nested':(16,6,9),'intersection':(15,6,8),
    'duplicate_occurrences':(13,4,6),'decisive_empty':(3,1,2),
    'other_collection':(2,1,1),'independent_cut':(2,1,1),'independent_producer_same_cut':(2,1,1),
    'empty_source':(3,0,1),'tombstone':(3,2,2),
    'unsupported_decisive':(0,0,0),'late_write_failure':(5,2,2),
    'direct_cancel':(5,2,2),'accepted_caller_abort':(7,2,2),
    'unsupported_keys':(0,0,0),'late_algebra_failure':(15,6,8),'late_algebra_cancel':(15,6,8),
    'early_algebra_failure':(7,4,5),'early_algebra_cancel':(7,4,5),
    'algebra_then_source_failure':(12,6,7),
    **{'presence_'+kind:(6,1,1) for kind in TYPES},
}
def completed(s,c):
    returns=tuple(arg for word,arg in s.events if word=='return')
    costs=COSTS[s.case]
    return (s.done and s.result==c.outcome and not s.owner and not s.copies and not s.pending
        and not s.capsule and not s.builders and not s.sets and not s.output and s.work==tuple(a-n for a,n in zip(c.capacity,costs))
        and s.root==c.root and s.funded==0
        and returns==((c.expected,) if c.outcome=='ok' else ()) and prefix_violation(s,c) is None)

def run_campaign(helper,model,max_states):
    if set(COSTS)!=set(model.CASES):raise AssertionError('literal selection cost coverage required')
    for c in model.CASES.values():
        if len(c.records)>4 or len({r[0][0] for r in c.records})>2:raise AssertionError('selection bound exceeded')
        if c.outcome=='ok' and reference(c)!=c.expected:raise AssertionError('literal original-request membership mismatch')
    spec=model.SourceSelection();report,graph=helper.explore(spec,max_states)
    if report['status']!='PASS' or not report['complete']:raise AssertionError('incomplete selection exploration: '+str(report))
    states,edges,_=graph
    report['graph_sha256']=helper.digest({'states':[helper.render(s) for s in states],'edges':edges})
    report['completed_witnesses']={name:helper.witness(spec,graph,['ChooseSelection-'+name],
        lambda s,name=name,c=c:s.case==name and completed(s,c)) for name,c in model.CASES.items()}
    controls=[]
    for fault,expected in model.FAULTS.items():
        mutant=model.SourceSelection(fault);failure,_=helper.explore(mutant,max_states)
        if failure['status']!='COUNTEREXAMPLE' or failure.get('property')!=expected:
            raise AssertionError('insensitive or misclassified selection mutant '+fault+': '+str(failure))
        helper.replay(mutant,failure['trace'],expected)
        controls.append({'fault':fault,'trace_sha256':helper.digest(failure['trace']),**failure})
    return report,controls
