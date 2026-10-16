"""Independent immutable-fixture event oracle for bounded coordinator composition."""
def prefix_violation(s,c):
    # Never use producer phases, observations, classes, counters or helper
    # functions to derive expected storage facts or output identities.
    ids=('z','a','m')[:len(c.cuts[0].facts)]
    observed=[];classes=[];sticky=False;cut_index=-1;index=0
    pending=();pending_cut=-1;durable=False;possible=False;owner=False
    job=False;caller=True;known=attempts=work=0;prepared=();output=();output_owner=False;result=''
    confirmed=False;read_error=False;member=0;converted=();declarations=();mode='admit'
    def full_stamp(p,fact):
        return None if fact is None else (2,ids[p],3,fact.fingerprint,fact.offset,fact.revision,p*10,4,13)
    def kind(fact):return 'absent' if fact is None else ('present' if fact.fingerprint==7 else 'conflict')
    def unresolved():return any(classes[p]=='absent' for p in pending)
    def durable_bad():return durable and (unresolved() or any(classes[p]=='conflict' for p in pending))
    def capture_target():
        if mode=='initial':return 'initial',0
        if mode=='post':return 'recapture',c.cuts[cut_index].capture
        if mode=='submit' and not confirmed:return 'recapture',c.cuts[cut_index].capture
        if mode=='decide' and not sticky and not confirmed:return 'recapture',c.cuts[cut_index].capture
        if mode=='decide' and confirmed and not sticky and not read_error and pending and not durable and unresolved():
            cut=c.cuts[cut_index]
            if not (cut.capability and cut.epoch>c.cuts[pending_cut].epoch):return 'acquire',cut.acquire
        if mode=='fresh' and not any(classes[p]=='absent' for p in c.admin) and not all(v!='absent' for v in classes) and not c.cuts[cut_index].capability:
            return 'acquire',c.cuts[cut_index].acquire
        return None
    for word,arg in s.events:
        if mode=='done':return 'CO-HISTORY'
        if word=='admit':
            if mode!='admit':return 'CO-HISTORY'
            job=True;declarations=ids;observed=[None]*len(ids);mode='initial'
        elif word=='capture':
            expected=capture_target()
            if expected is None or arg!=expected:return 'CO-AUTHORITY'
            tag,target=arg
            if not 0<=target<len(c.cuts) or attempts>=c.contention:return 'CO-HISTORY'
            next_cut=c.cuts[target]
            if tag=='acquire' and not next_cut.capability:return 'CO-AUTHORITY'
            if cut_index>=0:
                old=c.cuts[cut_index]
                if next_cut.epoch<old.epoch or next_cut.through<old.through:return 'CO-CONTINUITY'
            cut_index=target;index=0;member=0;converted=();classes=[];confirmed=False;read_error=False;prepared=()
            attempts+=1;work+=1;mode='point'
        elif word=='point':
            cut=c.cuts[cut_index]
            if mode!='point' or arg!=index or index>=len(ids) or index==cut.read_error:return 'CO-HISTORY'
            value=full_stamp(index,cut.facts[index])
            if observed[index] is not None and observed[index]!=value:sticky=True
            if observed[index] is None and value is not None:observed[index]=value
            classes.append(kind(cut.facts[index]));index+=1;work+=1
        elif word=='read-error':
            if mode!='point' or arg!=index or index!=c.cuts[cut_index].read_error:return 'CO-HISTORY'
            read_error=True;work+=1;mode='confirm'
        elif word=='classified':
            if mode!='point' or index!=len(ids):return 'CO-HISTORY'
            mode='convert'
        elif word=='converted':
            cut=c.cuts[cut_index]
            if mode!='convert' or member>=len(ids):return 'CO-OUTPUT'
            if member in (cut.conversion_error,cut.bad_outcome):return 'CO-OUTPUT'
            # The original resolver port supplies a checked result bound to this
            # member/cut; its lower-level verification is campaign eight's contract.
            token=None if cut.facts[member] is None else ('RequestConflict' if cut.facts[member].fingerprint!=7 else cut.facts[member].receipt)
            if arg!=(member,token):return 'CO-OUTPUT'
            converted+=((member,token),);member+=1;work+=1
        elif word=='conversion-error':
            cut=c.cuts[cut_index]
            if mode!='convert' or arg!=member or member not in (cut.conversion_error,cut.bad_outcome):return 'CO-OUTPUT'
            read_error=True;work+=1;mode='confirm'
        elif word=='conversion-complete':
            if mode!='convert' or tuple(p for p,_ in converted)!=tuple(range(len(ids))):return 'CO-OUTPUT'
            mode='confirm'
        elif word=='confirm':
            if mode!='confirm':return 'CO-HISTORY'
            confirmed=c.cuts[cut_index].current;work+=1
            count=sum(v!='absent' for v in classes)
            if confirmed and not read_error and not sticky and count>known:known=count;attempts=0
            mode='decide'
        elif word=='ready':
            if mode!='decide' or sticky or not confirmed or read_error or pending:return 'CO-HISTORY'
            mode='fresh'
        elif word=='release':
            if mode!='decide' or sticky or not confirmed or read_error or not pending:return 'CO-AUTHORITY'
            cut=c.cuts[cut_index]
            if durable_bad():return 'CO-AUTHORITY'
            if not durable and unresolved() and not (cut.capability and cut.epoch>c.cuts[pending_cut].epoch):return 'CO-AUTHORITY'
            pending=();owner=False;mode='fresh'
        elif word=='prepare':
            if mode!='fresh' or not confirmed or sticky or pending:return 'CO-AUTHORITY'
            cut=c.cuts[cut_index]
            if not cut.capability or any(classes[p]=='absent' for p in c.admin):return 'CO-AUTHORITY'
            if all(v!='absent' for v in classes):return 'CO-AUTHORITY'
            first=next(p for p,fact in enumerate(cut.facts) if fact is None)
            selected=[]
            for p in range(first,min(first+2,len(ids))):
                if cut.facts[p] is not None:break
                selected.append(p)
            if arg!=tuple(selected):return 'CO-AUTHORITY'
            prepared=arg;work+=1;mode='prepared'
        elif word=='prepare-confirm':
            if mode!='prepared':return 'CO-HISTORY'
            confirmed=c.cuts[cut_index].preparation_current;work+=1;mode='submit'
        elif word=='submit':
            if mode!='submit' or not confirmed or c.cuts[cut_index].preparation_error or not prepared:return 'CO-AUTHORITY'
            if arg!=c.cuts[cut_index].reply:return 'CO-AUTHORITY'
            pending=prepared;pending_cut=cut_index;prepared=();owner=True;possible=True
            durable=arg in ('stored','durable-content');work+=1;mode='post'
        elif word=='cancel':
            if mode!='post' or not c.abandon or not caller:return 'CO-HISTORY'
            caller=False;mode='shutdown'
        elif word=='shutdown-pending':
            if mode!='shutdown' or not owner or not job:return 'CO-OWNER'
            mode='post'
        elif word=='output':
            if mode!='fresh' or not confirmed or sticky or read_error or pending or len(classes)!=len(ids):return 'CO-OUTPUT'
            cut=c.cuts[cut_index]
            if any(fact is None for fact in cut.facts) or tuple(p for p,_ in converted)!=tuple(range(len(ids))):return 'CO-OUTPUT'
            output=tuple((literal,kind(cut.facts[p]),full_stamp(p,cut.facts[p]),
                'RequestConflict' if cut.facts[p].fingerprint!=7 else cut.facts[p].receipt) for p,literal in enumerate(ids)) if caller else ()
            if arg!=output:return 'CO-OUTPUT'
            result='complete' if caller else 'drained';job=caller;output_owner=caller;converted=();mode='returned' if caller else 'done'
        elif word=='borrow-output':
            if mode!='returned' or not output_owner or not job:return 'CO-OWNER'
            mode='drop-output'
        elif word=='drop-output':
            if mode!='drop-output' or not output_owner:return 'CO-OWNER'
            output=();output_owner=False;job=False;mode='done'
        elif word=='error':
            allowed=False;reason='unknown'
            if mode=='decide' and sticky:allowed=True
            elif mode=='decide' and confirmed and read_error:allowed=True;reason='unknown' if c.cuts[cut_index].bad_outcome>=0 else 'too-large'
            elif mode=='decide' and confirmed and pending and durable_bad():allowed=True
            elif mode=='fresh' and any(classes[p]=='absent' for p in c.admin):allowed=True;reason='invalid'
            elif mode=='submit' and confirmed and c.cuts[cut_index].preparation_error:allowed=True;reason='too-large'
            else:
                target=capture_target()
                if target:
                    tag,p=target
                    allowed=p<0 or p>=len(c.cuts) or attempts>=c.contention or (tag=='acquire' and not c.cuts[p].capability)
            if not allowed:return 'CO-HISTORY'
            result='unknown' if possible else reason
            if arg!=result:return 'CO-OWNER'
            owner=False;job=False;pending=();prepared=();converted=();mode='done'
        else:return 'CO-HISTORY'
    if (tuple(observed),sticky)!=(s.observed,s.sticky):return 'CO-CONTINUITY'
    if (pending,pending_cut,durable,possible,owner,job,caller,output_owner)!=(s.pending,s.pending_cut,s.durable,s.possible,s.owner,s.job,s.caller,s.output_owner):return 'CO-OWNER'
    if (work,'one-ledger')!=(s.work,s.ledger):return 'CO-WORK'
    if (known,attempts)!=(s.known,s.attempts):return 'CO-PROGRESS'
    if (output,result,declarations,converted,member)!=(s.output,s.result,s.declarations,s.converted,s.member):return 'CO-OUTPUT'
    if (cut_index,index,tuple(classes),confirmed,read_error,prepared)!=(s.cut,s.index,s.classes,s.confirmed,s.read_error,s.prepared):return 'CO-HISTORY'
    if mode=='done' and result!=c.expected:return 'CO-OUTPUT'
    if (s.phase=='done')!=(mode=='done'):return 'CO-HISTORY'
    return None

def completed_witness(s,c):return s.phase=='done' and s.result==c.expected and prefix_violation(s,c) is None

def run_campaign(helper,model,max_states):
    spec=model.Coordinator();report,graph=helper.explore(spec,max_states)
    if report['status']!='PASS' or not report['complete']:raise AssertionError('incomplete coordinator baseline: '+str(report))
    states,edges,_=graph
    report['graph_sha256']=helper.digest({'states':[helper.render(s) for s in states],'edges':edges})
    report['completed_witnesses']={name:helper.witness(spec,graph,['ChooseCoordinator-'+name],
        lambda s,name=name,c=c:s.case==name and completed_witness(s,c)) for name,c in model.CASES.items()}
    controls=[]
    for fault,expected in model.FAULTS.items():
        mutant=model.Coordinator(fault);failure,_=helper.explore(mutant,max_states)
        if failure['status']!='COUNTEREXAMPLE' or failure.get('property')!=expected:
            raise AssertionError('insensitive or misclassified coordinator mutant '+fault+': '+str(failure))
        helper.replay(mutant,failure['trace'],expected)
        controls.append({'fault':fault,'trace_sha256':helper.digest(failure['trace']),**failure})
    return report,controls
