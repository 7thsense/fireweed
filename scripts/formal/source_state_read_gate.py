"""Independent immutable-state interpreter; no producer phase proves safety."""
READS={
    'grouped_reversed_aliases':(1,2),'empty':(),'foreign_empty':(),'admission':(),
    'absent':(),'tombstone':(),'empty_value_fields':(1,),
    'equal_bytes_other_collection':(1,),'equal_bytes_other_key':(1,),
    'foreign_cut':(),'foreign_job':(),'foreign_queue':(1,),'foreign_pin':(1,),
    'future_epoch':(1,),'missing_observation':(),'error_observation':(),
    'offset_zero':(),'offset_future':(),'incomplete_fields':(),'field_count':(),
    'field_type':(),'work_substitution':(1,),'receipt_substitution':(1,),
    'interior_span':(1,),'wrong_digest':(1,),'historical_below':(1,),
    'historical_above':(1,),'second_bad_alias':(1,),'C1_read_failure':(1,),
    'C2_bad_span':(1,2),'C2_bad_hwm':(1,2),'C2_bad_pin':(1,2),
    'C2_read_failure':(1,2),'late_copy_failure':(1,2),'exact_cap':(1,2),
    'cap_minus_one':(1,2),'recreated':(2,),'old_incarnation':(1,),
    'foreign_absent_cut':(),'foreign_absent_job':(),'foreign_tombstone_cut':(),'foreign_tombstone_job':(),
    'tombstone_zero_hwm':(),'tombstone_fields':(),'absent_retained_hwm':(),
    'independent_cut_B':(1,2),'equal_live_bytes':(1,),'other_collection_positive':(1,),
}
def validate_fixtures(model):
    if set(READS)!=set(model.CASES):raise AssertionError('literal state cost manifests required')
    for name,c in model.CASES.items():
        if (len(c.request)>4 or len(c.frames)>2 or len({key[0] for key in c.request})>1
                or len({o.identity for o in c.observations if o.status=='live'})>3
                or len({x.collection for f in c.frames for x in f.commands if x.domain=='state'})>2):
            raise AssertionError('state bounds exceeded')
        if len(dict(c.fields))!=len(c.fields):raise AssertionError('duplicate field map identities')
        if len({f.offset for f in c.frames})!=len(c.frames):raise AssertionError('duplicate physical offset')
        for frame in c.frames:
            keys=[(x.collection,x.key) for x in frame.commands]
            if keys!=sorted(set(keys)):raise AssertionError('noncanonical immutable command identities')
            spans=sorted([(x.start,x.start+len(x.value)) for x in frame.commands]+[(start,start+len(value)) for start,value in frame.receipts])
            if any(a<0 or b<a for a,b in spans) or any(left[1]>right[0] for left,right in zip(spans,spans[1:])):raise AssertionError('overlapping immutable regions')
        if any(len(fields)>2 or [n for n,_ in fields]!=sorted(set(n for n,_ in fields)) for _,fields in c.fields):raise AssertionError('noncanonical immutable field maps')
        if c.outcome=='ok' and len(c.expected)!=len(c.request):raise AssertionError('literal output manifest shape differs')

def prefix_violation(s,c):
    if s.case not in READS:return 'SV-HISTORY'
    mode='select';owner=False;header=False;metadata=False;done_fields=[];reads=[];verified=[]
    physical=();rows=[];funded=[];output=();result='';group=0;work=4
    wanted=[]
    for key,o in zip(c.request,c.observations):
        if o.status=='live' and key not in wanted:wanted.append(key)
    offsets=sorted({o.offset for o in c.observations if o.status=='live'})
    def obs_valid():
        if len(c.observations)!=len(c.request):return False
        for key,o in zip(c.request,c.observations):
            if o.identity!=key or o.cut!=c.cut or o.job!='job':return False
            if o.status=='live':
                if o.hwm<=0 or not 0<o.offset<c.through:return False
            elif o.status in ('absent','tombstone'):
                if o.hwm<0 or (o.status=='absent' and o.hwm!=0) or (o.status=='tombstone' and o.hwm==0):return False
                if any((o.count,o.offset,o.start,o.end,o.digest)):return False
            else:return False
        return True
    def field_valid(key):
        values=dict(c.fields).get(key,())
        if any(not ((name=='n' and type(v) is int) or (name=='tag' and type(v) is str)) for name,v in values):return False
        return all(o.complete and o.count==len(values) for o in c.observations if o.identity==key and o.status=='live')
    def members(offset):return [o for o in c.observations if o.status=='live' and o.offset==offset]
    def frame(offset):return next((f for f in c.frames if f.offset==offset),None)
    def exact(offset):
        f=frame(offset)
        if f is None or f.pin!=11 or f.epoch>2 or f.queue!='q' or f.cut!=c.cut or f.job!='job':return False
        for o in members(offset):
            if o.cut!=c.cut or o.job!='job':return False
            matching=[cmd for cmd in f.commands if cmd.domain=='state' and (cmd.collection,cmd.key)==o.identity]
            if len(matching)!=1:return False
            command=matching[0]
            if (command.hwm,command.start,command.start+len(command.value),command.value)!=(o.hwm,o.start,o.end,o.digest):return False
        return True
    def group_keys():return list(dict.fromkeys(o.identity for o in members(physical[0]))) if physical else []
    def expected_row(key):
        observation=next(o for o in c.observations if o.identity==key and o.status=='live')
        command=next(x for x in frame(observation.offset).commands if x.domain=='state' and (x.collection,x.key)==key)
        return (key[0],key[1],command.hwm,dict(c.fields).get(key,()),command.value)
    def expected_output():
        table=dict(rows)
        return tuple(table.get(key) for key in c.request)
    def measure(values):
        size=1
        for row in values:
            if row is None:size+=1;continue
            collection,key,version,fields,value=row
            size+=4+len(collection)+len(key)+len(value)
            for name,field in fields:size+=len(name)+1+len(str(field))
        return size
    for word,arg in s.events:
        if mode=='done':return 'SV-HISTORY'
        if word=='select':
            if mode!='select' or arg!=s.case:return 'SV-HISTORY'
            mode='bind'
        elif word=='bind':
            if mode!='bind' or not c.scope or not c.admission:return 'SV-SCOPE'
            owner=True;mode='header'
        elif word=='header':
            if mode!='header' or not owner or arg!=11:return 'SV-SCOPE'
            header=True;mode='metadata'
        elif word=='metadata':
            if mode!='metadata' or not header or not obs_valid():return 'SV-METADATA'
            metadata=True;mode='fields'
        elif word=='fields':
            if mode!='fields' or not metadata or len(done_fields)>=len(wanted) or arg!=wanted[len(done_fields)] or not field_valid(arg):return 'SV-FIELDS'
            done_fields.append(arg)
        elif word=='fields_done':
            if mode!='fields' or done_fields!=wanted:return 'SV-FIELDS'
            mode='read'
        elif word=='read':
            if mode!='read' or physical or group>=len(offsets) or arg!=offsets[group] or arg in reads:return 'SV-GROUP'
            if done_fields!=wanted:return 'SV-FIELDS'
            reads.append(arg);work-=1;mode='record'
        elif word=='retain':
            if mode!='record' or arg!=reads[-1] or c.stop=='read'+str(arg):return 'SV-GROUP'
            physical=(arg,);mode='verify'
        elif word=='verify':
            if mode!='verify' or physical!=(arg,) or not exact(arg):return 'SV-MEMBER'
            verified.append(arg);mode='copy'
        elif word=='fund':
            if mode!='copy' or not physical or physical[0] not in verified or arg not in group_keys() or arg in funded or (c.stop=='copy2' and physical==(2,)):return 'SV-COPY'
            funded.append(arg);mode='allocate'
        elif word=='copy':
            key,row=arg
            if mode!='allocate' or key not in funded or key in dict(rows):return 'SV-COPY'
            if row!=expected_row(key):return 'SV-MEMBER'
            rows.append((key,row));mode='continue'
        elif word=='continue':
            if mode!='continue':return 'SV-HISTORY'
            mode='copy'
        elif word=='retire':
            if mode!='copy' or physical!=(arg,) or any(k not in dict(rows) for k in group_keys()):return 'SV-OWNER'
            physical=();group+=1;mode='read'
        elif word=='fund_output':
            if mode!='read' or group!=len(offsets) or physical or arg!=len(c.request):return 'SV-RETURN'
            funded.append('output');mode='assemble'
        elif word=='assemble':
            if mode!='assemble' or 'output' not in funded:return 'SV-COPY'
            if arg!=expected_output():return 'SV-ORDER'
            output=arg;mode='size'
        elif word=='return':
            if mode!='size' or physical or group!=len(offsets):return 'SV-RETURN'
            if measure(output)>c.limit:return 'SV-SIZE'
            if arg!=output or arg!=c.expected or c.outcome!='ok':return 'SV-ORDER'
            result='ok';mode='drop'
        elif word=='drop':
            if mode!='drop' or not owner or 'output' not in funded:return 'SV-OWNER'
            owner=False;rows=[];funded=[];output=();mode='done'
        elif word=='refuse':
            reason,classification=arg
            allowed=(reason=='scope' and mode=='bind' and (not c.scope or not c.admission)
                or reason=='metadata' and mode=='metadata' and not obs_valid()
                or reason=='fields' and mode=='fields' and len(done_fields)<len(wanted) and not field_valid(wanted[len(done_fields)])
                or reason=='read' and mode=='record' and c.stop=='read'+str(reads[-1])
                or reason=='membership' and mode=='verify' and not exact(physical[0])
                or reason=='copy' and mode=='copy' and physical==(2,) and c.stop=='copy2'
                or reason=='size' and mode=='size' and measure(output)>c.limit)
            if not allowed:return 'SV-HISTORY'
            if classification!=('too_large' if reason=='size' else 'unknown'):return 'SV-HISTORY'
            owner=False;rows=[];funded=[];physical=();output=();result=classification;mode='done'
        elif word=='drop_credit':return 'SV-OWNER'
        else:return 'SV-HISTORY'
    if s.work!=work or work<0:return 'SV-WORK'
    if (s.owner,s.physical,s.funded)!=(owner,physical,tuple(funded)):return 'SV-OWNER'
    if (s.rows,s.output)!=(tuple(rows),output):return 'SV-COPY'
    if s.result!=result or (s.phase=='done')!=(mode=='done'):return 'SV-HISTORY'
    if mode=='done' and (result!=c.outcome or tuple(reads)!=READS[s.case]):return 'SV-HISTORY'
    return None

def completed(s,c):
    returns=tuple(arg for word,arg in s.events if word=='return')
    return (s.phase=='done' and s.result==c.outcome and returns==((c.expected,) if c.outcome=='ok' else ())
        and s.work==4-len(READS[s.case]) and not s.owner and s.rows==s.funded==s.physical==s.output==()
        and prefix_violation(s,c) is None)

def run_campaign(helper,model,max_states):
    validate_fixtures(model)
    spec=model.SourceStateRead();report,graph=helper.explore(spec,max_states)
    if report['status']!='PASS' or not report['complete']:raise AssertionError('incomplete state read campaign: '+str(report))
    states,edges,_=graph
    report['graph_sha256']=helper.digest({'states':[helper.render(s) for s in states],'edges':edges})
    report['completed_witnesses']={name:helper.witness(spec,graph,['ChooseState-'+name],lambda s,name=name,c=c:s.case==name and completed(s,c)) for name,c in model.CASES.items()}
    controls=[]
    for fault,expected in model.FAULTS.items():
        mutant=model.SourceStateRead(fault);failure,_=helper.explore(mutant,max_states)
        if failure['status']!='COUNTEREXAMPLE' or failure.get('property')!=expected:raise AssertionError('insensitive or misclassified state read mutant '+fault+': '+str(failure))
        helper.replay(mutant,failure['trace'],expected)
        controls.append({'fault':fault,'trace_sha256':helper.digest(failure['trace']),**failure})
    return report,controls
