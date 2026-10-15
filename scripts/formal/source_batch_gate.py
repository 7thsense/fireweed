"""Independent complete-history oracle for speculative batch composition."""
def reference_history(c, model):
    # Relational specification over full source records. It never consults the
    # implementation overlay, loaded probes, packing flags or transition labels.
    rows={0:(1,0,1,1),1:(1,1,1,1)}
    highwater=[1,1]; state=(1,1); state_hwm=1; sequence=2
    snapshots=[]; outcomes=[]
    for group in c.groups:
        is_state=group.action.startswith('state_')
        present=state if is_state else rows.get(group.key)
        version=present[0] if present else 0
        valid=version==group.expect
        if group.action=='put':
            valid=valid and not any(key!=group.key and row[1]==group.unique for key,row in rows.items())
        if not valid: outcome=('rejected',0)
        elif group.action=='state_check': outcome=('committed',0)
        elif is_state:
            state_hwm+=1
            state=None if group.action=='state_delete' else (state_hwm,group.number)
            outcome=('committed',state_hwm)
        else:
            highwater[group.key]+=1
            if group.action=='delete': rows.pop(group.key,None)
            elif group.action=='put':
                sequence+=int(group.key not in rows)
                rows[group.key]=(highwater[group.key],group.unique,group.payload,1)
            else:
                row=rows[group.key]
                rows[group.key]=(highwater[group.key],row[1],row[2],group.number)
            outcome=('committed',highwater[group.key])
        image=model.Image(tuple(model.Work(*rows[key]) if key in rows else None for key in range(2)),
            tuple(highwater),state,state_hwm,sequence)
        snapshots.append(image);outcomes.append(outcome)
    if tuple(zip(snapshots,outcomes))!=c.expected:
        raise AssertionError('literal batch fixtures disagree with independent relational history')
    return tuple(snapshots),tuple(outcomes)

def reference_size(image,count):
    # Fold a canonical write-set against the immutable initial image. Tombstones
    # carry highwaters even when the final live row disappeared by coalescing.
    commands=[]
    source=((1,0,1,1),(1,1,1,1))
    for key,row in enumerate(image.work):
        after=None if row is None else (row.version,row.unique,row.payload,row.number)
        if after!=source[key] or image.highwater[key]!=1:
            commands.append(2 if row is None else row.payload)
    if image.state!=(1,1) or image.state_highwater!=1: commands.append(2)
    frame=2+sum(commands)+count
    return frame,frame+2

def expected_batch(c, model):
    history,outcomes=reference_history(c,model)
    words=[];loaded=set();owners=[None,None];fields=();clocks=();attempts=[]
    image=model.INITIAL;histories=();results=();index=0;candidate=0;best=0;retained=0
    def finish(result,returned=()):
        return dict(phase='done',events=tuple(words),before=model.INITIAL,image=image,
            owners=tuple(owners),loaded=tuple(sorted(loaded)),fields=fields,index=index,
            histories=histories,outcomes=results,clocks=clocks,attempts=tuple(attempts),
            candidate=candidate,best=best,retained=retained,binding=('A','D','P','J'),
            plan_live=False,output_live=False,output_credit=False,result=result,
            returned=returned,acknowledged=False)
    def error(result='unknown'):
        words.append((result,-1));return finish(result)
    if not c.admission: return error()
    words.append(('admit',-1))
    if not c.response_ok: return error('invalid')
    words.append(('validate',-1))
    if c.source_failure=='fields': return error()
    fields=tuple(sorted({('s' if group.action.startswith('state_') else 'w',group.key,name)
        for group in c.groups for name in group.fields}))
    words.append(('fields',-1))
    for i,group in enumerate(c.groups):
        if c.source_failure=='probe-'+str(i): return error()
        # The oracle's loaded domain comes from the current full relational image,
        # never from source restoration or implementation probe/owner containers.
        probes=set()
        if group.action in ('put','delete'):
            if image.work[group.key] is not None: probes.add(image.work[group.key].unique)
            if group.action=='put': probes.add(group.unique)
        loaded|=probes
        for unique in loaded:
            owners[unique]=next((key for key,row in enumerate(image.work)
                if row is not None and row.unique==unique),None)
        words.extend((('probe',i),('clock',i),('plan',i)))
        clocks+=(i,);index=i+1
        image=history[i];histories=history[:index];results=outcomes[:index]
        for unique in loaded:
            owners[unique]=next((key for key,row in enumerate(image.work)
                if row is not None and row.unique==unique),None)
    total=len(c.groups);candidate=total
    if not c.output_admission: return error()
    def fits(n):
        frame,record=reference_size(history[n-1],n)
        return frame<=c.frame and record<=c.record
    words.append(('encode',total));attempts.append(total)
    if fits(total): retained=best=total
    else:
        for n in range(1,total):
            candidate=n;words.append(('encode',n));attempts.append(n)
            if not fits(n): break
            best=n
        retained=best
        if not retained: return error('too-large')
    if c.requested_binding!=('A','D','P','J'): return error()
    words.extend((('seal',retained),('borrow',-1),('drop',-1)))
    returned=(history[retained-1],tuple(range(c.offset,c.offset+retained)),
        tuple(sorted((c.ids[i],outcomes[i]) for i in range(retained))),('A','D','P','J'))
    return finish('sealed',returned)

def completed_batch_witness(s,c,model):
    expected=expected_batch(c,model)
    return all(getattr(s,key)==value for key,value in expected.items())

def run_batch_campaign(helper,model,max_states):
    spec=model.SourceBatchComposition()
    report,graph=helper.explore(spec,max_states)
    if report['status']!='PASS' or not report['complete']:
        raise AssertionError('incomplete source batch composition baseline: '+str(report))
    states,edges,_=graph
    report['graph_sha256']=helper.digest({'states':[helper.render(s) for s in states],'edges':edges})
    report['completed_witnesses']={}
    for name,case in model.BATCH_CASES.items():
        report['completed_witnesses'][name]=helper.witness(spec,graph,['SelectBatch-'+name],
            lambda s,name=name,case=case:s.case==name and completed_batch_witness(s,case,model))
    controls=[]
    for fault,expected in model.BATCH_FAULTS.items():
        mutant=model.SourceBatchComposition(fault)
        failure,_=helper.explore(mutant,max_states)
        if failure['status']!='COUNTEREXAMPLE' or failure.get('property')!=expected:
            raise AssertionError('insensitive or misclassified source batch mutant '+fault+': '+str({key:failure.get(key) for key in ('status','property','complete','states')}))
        helper.replay(mutant,failure['trace'],expected)
        controls.append({'fault':fault,'trace_sha256':helper.digest(failure['trace']),**failure})
    return report,controls
