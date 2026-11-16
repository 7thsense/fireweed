"""Independent immutable-frame interpreter, including every provisional prefix."""
def prefix_violation(s,c):
    # All expected content comes from declared keys and immutable complete frame
    # commands. Producer verified/loaded flags never establish correctness.
    mode='metadata';decls=();observed=();offsets=();fi=scan=index=0
    matched=[];views=[];verified=[];image=[];loaded=[];copies=0;funded=''
    owner=False;physical=();pending=False;work=[20,100,4,20,200]
    reads=[];walks=[];hashes=[];returned=();result='';retry=False
    def frame():return next((f for f in c.frames if f.offset==offsets[fi]),None)
    def members():
        return [(p,o) for p,(d,o) in enumerate(zip(c.declarations,c.observations)) if d.body and o is not None and o.status=='live' and o.evidence.offset==offsets[fi]]
    def scope_valid():
        return frame().pin==11 and all(o.evidence.cut=='original' and o.evidence.job=='job' for _,o in members())
    def matching_valid(command):
        aliases=[o for _,o in members() if o.key==command.key]
        for o in aliases:
            e=o.evidence
            if (e.start,e.end,e.digest,o.insertion)!=(command.start,command.start+len(command.body),command.body,command.insertion):return False
            if o.version<command.version:return False
            if (o.version,o.insertion,e)!=(aliases[0].version,aliases[0].insertion,aliases[0].evidence):return False
        return bool(aliases) and command.key not in matched
    def requested_keys():return list(dict.fromkeys(o.key for _,o in members()))
    def required_command():return frame().commands[scan]
    def view_body(key):
        # Re-derive from actual full-key command membership, not supplied views.
        commands=[x for x in frame().commands if x.key==key and x.domain=='work']
        return commands[0].body if len(commands)==1 else None
    for word,arg in s.events:
        if mode=='done':return 'B-HISTORY'
        if word=='metadata':
            if mode!='metadata':return 'B-HISTORY'
            if c.stop=='selector':return 'B-MEMBER'
            decls=c.declarations;owner=True;work[0]-=3;work[1]-=9;mode='observe'
        elif word=='observations':
            if mode!='observe':return 'B-OBSERVATION'
            if len(c.observations)!=len(c.declarations):return 'B-OBSERVATION'
            for declaration,obs in zip(c.declarations,c.observations):
                if obs is None or obs.key!=declaration.key or obs.status not in ('live','absent','tombstone'):return 'B-OBSERVATION'
            observed=tuple(dict.fromkeys((o.key,o.status,o.version,o.insertion,o.highwater) for o in c.observations))
            offsets=tuple(sorted({o.evidence.offset for d,o in zip(c.declarations,c.observations) if d.body and o.status=='live'}))
            mode='attempt' if offsets else 'return'
        elif word=='attempt':
            if mode!='attempt' or arg!=offsets[fi]:return 'B-COUNT'
            if physical:return 'B-OWNER'
            reads.append(arg);work[2]-=1;mode='cap'
        elif word=='cap-ok':
            if mode!='cap' or arg!=offsets[fi] or (arg==2 and c.stop=='cap') or frame() is None or arg in c.unreadable:return 'B-WORK'
            mode='hydrate'
        elif word=='hydrate':
            if mode!='hydrate' or arg!=offsets[fi] or physical:return 'B-OWNER'
            n,b=(2,5) if arg==1 else (3,9);work[3]-=n;work[4]-=b
            physical=(arg,);pending=True;mode='io'
        elif word=='complete':
            if mode!='io' or arg!=offsets[fi] or (arg==2 and (c.stop=='hydrate' or (c.stop=='cancel' and not retry))):return 'B-HISTORY'
            pending=False;mode='scope'
        elif word=='walk':
            if mode!='scope' or arg!=offsets[fi] or not scope_valid():return 'B-MEMBER'
            walks.append(arg);scan=0;matched=[];views=[];mode='scan'
        elif word=='skip':
            if mode!='scan' or scan>=len(frame().commands) or arg!=scan:return 'B-COUNT'
            cmd=required_command()
            if cmd.domain=='work' and cmd.key in requested_keys():return 'B-MEMBER'
            scan+=1
        elif word=='hash':
            if mode!='scan' or scan>=len(frame().commands):return 'B-COUNT'
            cmd=required_command()
            if cmd.domain!='work' or cmd.key!=arg or arg not in requested_keys():return 'B-MEMBER'
            hashes.append(arg);mode='match'
        elif word=='match':
            if mode!='match' or arg!=required_command().key or not matching_valid(required_command()):return 'B-MEMBER'
            cmd=required_command()
            views.extend((p,o.key,cmd.body) for p,o in members() if o.key==arg)
            matched.append(arg);scan+=1;mode='scan'
        elif word=='verified':
            if mode!='scan' or scan!=len(frame().commands) or any(k not in matched for k in requested_keys()):return 'B-MEMBER'
            verified=requested_keys();index=0;mode='copy'
        elif word=='fund':
            if mode!='copy' or index>=len(verified) or arg!=verified[index] or funded:return 'B-COPY'
            if offsets[fi]==2 and c.stop=='copy':return 'B-COPY'
            funded=arg;copies+=len(view_body(arg));mode='allocate'
        elif word=='copy':
            if mode!='allocate' or arg!=funded:return 'B-COPY'
            body=view_body(arg)
            if body is None:return 'B-CONTENT'
            image.append((arg,body));loaded.append(arg);index+=1;funded='';mode='copy'
        elif word=='release':
            if mode!='copy' or index!=len(verified) or arg!=offsets[fi] or pending:return 'B-OWNER'
            physical=();fi+=1;scan=index=0;matched=[];views=[];verified=[]
            mode='attempt' if fi<len(offsets) else 'return'
        elif word=='cancel':
            if mode!='io' or offsets[fi]!=2 or c.stop!='cancel' or retry:return 'B-HISTORY'
            image=[];loaded=[];copies=0;funded='';owner=False;decls=();observed=();offsets=();fi=scan=index=0;matched=[];views=[];verified=[];result='cancelled';mode='retire'
        elif word=='retire':
            if mode!='retire' or not pending:return 'B-OWNER'
            physical=();pending=False;mode='retry'
        elif word=='retry':
            if mode!='retry' or retry or physical:return 'B-WORK'
            retry=True;offsets=();fi=scan=index=0;decls=();observed=();result='';mode='metadata'
        elif word=='refuse':
            invalid_obs=len(c.observations)!=len(c.declarations) or any(o is None or d.key!=o.key or o.status not in ('live','absent','tombstone') for d,o in zip(c.declarations,c.observations))
            allowed=(arg=='selector' and mode=='metadata' and c.stop=='selector'
                or arg=='observations' and mode=='observe' and invalid_obs
                or arg=='cap' and mode=='cap' and offsets[fi]==2 and c.stop=='cap'
                or arg=='record' and mode=='cap' and (frame() is None or offsets[fi] in c.unreadable)
                or arg=='hydrate' and mode=='io' and offsets[fi]==2 and c.stop=='hydrate'
                or arg=='scope' and mode=='scope' and not scope_valid()
                or arg=='membership' and mode=='match' and not matching_valid(required_command())
                or arg=='membership' and mode=='scan' and scan==len(frame().commands) and any(k not in matched for k in requested_keys())
                or arg=='copy' and mode=='copy' and offsets[fi]==2 and c.stop=='copy')
            if not allowed:return 'B-HISTORY'
            image=[];loaded=[];copies=0;funded='';owner=False;physical=();pending=False;decls=();observed=();offsets=();fi=scan=index=0;matched=[];views=[];verified=[];result='unknown';mode='done'
        elif word=='return':
            if mode!='return' or physical or not owner:return 'B-OWNER'
            bodies=dict(image);expected=tuple((k,v,i,bodies.get(k)) for k,status,v,i,_ in sorted(observed) if status=='live')
            if arg!=expected:return 'B-CONTENT'
            returned=expected;result='image';mode='borrow'
        elif word=='borrow':
            if mode!='borrow':return 'B-HISTORY'
            mode='drop'
        elif word=='drop':
            if mode!='drop':return 'B-OWNER'
            image=[];loaded=[];copies=0;owner=False;observed=();decls=();returned=();mode='done'
        else:return 'B-HISTORY'
    if s.ledger!='one' or tuple(work)!=s.work or min(work)<0:return 'B-WORK'
    if (s.declarations,s.observed)!=(decls,observed):return 'B-OBSERVATION'
    if (s.reads,s.walks,s.hashes)!=(tuple(reads),tuple(walks),tuple(hashes)):return 'B-COUNT'
    if (s.image,s.loaded,s.views)!=(tuple(image),tuple(loaded),tuple(views)):return 'B-CONTENT'
    if s.copies!=copies:return 'B-OWNER' if mode in ('borrow','drop') else 'B-COPY'
    if s.funded!=funded:return 'B-COPY'
    if (s.owner,s.physical,s.pending)!=(owner,physical,pending):return 'B-OWNER'
    if s.frames!=offsets:return 'B-COUNT'
    if (s.frames,s.frame_index,s.scan,s.matched,s.verified,s.index,s.retry,s.returned,s.result)!=(offsets,fi,scan,tuple(matched),tuple(verified),index,retry,returned,result):return 'B-HISTORY'
    if (s.phase=='done')!=(mode=='done'):return 'B-HISTORY'
    return None

def completed_witness(s,c):
    # Literal fixture manifests are independent of both transition code and the
    # prefix interpreter; exact results/costs are required even for Unknown.
    e=c.expected
    returns=tuple(arg for word,arg in s.events if word=='return')
    return (s.phase=='done' and s.result==e.result and (e.result=='image' or e.returned==()) and s.work==e.work
        and (s.reads,s.walks,s.hashes)==(e.reads,e.walks,e.hashes)
        and returns==((e.returned,) if e.result=='image' else ())
        and s.image==s.loaded==s.physical==s.views==() and s.copies==0 and not s.owner and not s.pending
        and prefix_violation(s,c) is None)

def run_campaign(helper,model,max_states):
    spec=model.SourceBodies();report,graph=helper.explore(spec,max_states)
    if report['status']!='PASS' or not report['complete']:raise AssertionError('incomplete body baseline: '+str(report))
    states,edges,_=graph;report['graph_sha256']=helper.digest({'states':[helper.render(s) for s in states],'edges':edges})
    report['completed_witnesses']={name:helper.witness(spec,graph,['ChooseBody-'+name],lambda s,name=name,c=c:s.case==name and completed_witness(s,c)) for name,c in model.CASES.items()}
    controls=[]
    for fault,expected in model.FAULTS.items():
        mutant=model.SourceBodies(fault);failure,_=helper.explore(mutant,max_states)
        if failure['status']!='COUNTEREXAMPLE' or failure.get('property')!=expected:raise AssertionError('insensitive or misclassified body mutant '+fault+': '+str(failure))
        helper.replay(mutant,failure['trace'],expected);controls.append({'fault':fault,'trace_sha256':helper.digest(failure['trace']),**failure})
    return report,controls
