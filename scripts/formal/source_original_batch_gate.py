"""Independent immutable-fixture and complete-event oracle for batch originals."""

def reference(c):
    # This oracle neither invokes producer transitions nor consumes their classes,
    # grouping, verified-members, phase labels or representative equality.
    words=[];statuses=[];returned=()
    def fail(): return tuple(words+[('fail',-1)]),()
    if not c.admission: return fail()
    words.append(('admit',-1))
    for i,r in enumerate(c.requests):
        if not r.declaration: return fail()
        words.append(('declare',i))
    words.append(('declared',-1))
    for i,r in enumerate(c.requests):
        words.append(('point',i))
        if c.point_error==i or not r.fixed or r.point_cut!=c.cut: return fail()
        kind='absent'
        if r.fact is not None:
            kind='present' if r.expected is None or (r.expected,r.expected_format)==(r.fact.fingerprint,r.fact.format) else 'conflict'
        statuses.append(kind);words.append(('classify',(i,kind)))
    words.append(('classified',-1))
    if c.mode=='stream':
        for i,kind in enumerate(statuses):
            if kind=='present': continue
            if c.conversion_error==i: return fail()
            words.append(('convert',i))
    words.append(('groups',-1))
    # Relationally select all eligible physical offsets from immutable requests.
    offsets={r.fact.offset for r,kind in zip(c.requests,statuses) if kind=='present'}
    for physical in sorted(offsets):
        words.append(('dispatch',physical))
        if physical==c.read_error:
            words.append(('read-error',physical));return fail()
        if physical==c.cancel_offset:
            return tuple(words+[('cancel',-1),('retire',-1)]),()
        words.append(('read',physical))
        selected=[]
        for i,(r,kind) in enumerate(zip(c.requests,statuses)):
            if kind!='present' or r.fact.offset!=physical: continue
            selected.append(i)
            candidates=[v for v in c.receipts if v.offset==physical and v.domain==r.domain and v.literal==r.literal]
            if len(candidates)!=1: return fail()
            actual=candidates[0]
            # Explicit full fields, independently of the producer's dataclass
            # equality and the first/representative alias's observation.
            for field in ('offset','domain','literal','format','fingerprint','checksum','start','length','digest','outcome'):
                if getattr(r.fact,field)!=getattr(actual,field): return fail()
            words.append(('verify',i))
        if c.mode=='stream':
            words.append(('verified-group',physical))
            for i in selected:
                if c.conversion_error==i: return fail()
                words.append(('convert',i))
            words.append(('release',physical))
        else: words.append(('retained',physical))
    output=[]
    for r,kind in zip(c.requests,statuses):
        stamp=None if r.fact is None else tuple(getattr(r.fact,f) for f in
            ('offset','domain','literal','format','fingerprint','checksum','start','length','digest'))
        verified=None
        if kind=='present':
            actual=next(v for v in c.receipts if v.offset==r.fact.offset and v.domain==r.domain and v.literal==r.literal)
            token=tuple(getattr(actual,f) for f in
                ('offset','domain','literal','format','fingerprint','checksum','start','length','digest'))
            verified=(actual.outcome,token)
        output.append((kind,r.domain,r.literal,stamp,verified))
    returned=tuple(output)
    words.extend((('return',-1),('borrow',-1),('drop',-1)))
    return tuple(words),returned

def prefix_violation(s,c):
    if s.cut!=c.cut or s.ledger!='input-ledger': return 'BO-CUT'
    words,output=reference(c)
    if len(s.events)>len(words) or words[:len(s.events)]!=s.events: return 'BO-HISTORY'
    containers=False;pending=();frames=();dtos=();values=();classes=();verified=()
    result='';returned=();points=reads=0;observations=()
    for word,arg in s.events:
        if word=='admit': containers=True
        elif word=='point': points+=1
        elif word=='classify':
            classes+=(arg[1],);fact=c.requests[arg[0]].fact
            observations+=(None if fact is None else tuple(getattr(fact,f) for f in
                ('offset','domain','literal','format','fingerprint','checksum','start','length','digest')),)
        elif word=='dispatch': reads+=1;pending=(arg,)
        elif word=='read': pending=();frames+=(arg,)
        elif word=='read-error': pending=()
        elif word=='verify': verified+=(arg,)
        elif word=='convert':
            r=c.requests[arg];kind=classes[arg];verified_outcome=None
            if kind=='present':
                actual=next(v for v in c.receipts if v.offset==r.fact.offset and v.domain==r.domain and v.literal==r.literal)
                token=tuple(getattr(actual,f) for f in
                    ('offset','domain','literal','format','fingerprint','checksum','start','length','digest'))
                verified_outcome=(actual.outcome,token)
            value=(kind,r.domain,r.literal,observations[arg],verified_outcome)
            dtos+=(arg,);values+=((arg,value),)
        elif word=='release': frames=tuple(f for f in frames if f!=arg)
        elif word=='return': result='batch';returned=output
        elif word in ('fail','cancel','drop'):
            containers=False;frames=();dtos=();values=()
            if word=='fail': result='unknown'
            elif word=='cancel': result='cancelled'
        elif word=='retire': pending=()
    if (s.points,s.reads)!=(points,reads): return 'BO-WORK'
    if (s.containers,s.pending,s.frames,s.dtos)!=(containers,pending,frames,dtos): return 'BO-OWNER'
    if (s.classes,s.verified)!=(classes,verified): return 'BO-HISTORY'
    if (s.values,s.returned,s.result,s.observations)!=(values,returned,result,observations): return 'BO-OUTPUT'
    if s.phase=='done' and len(s.events)!=len(words): return 'BO-HISTORY'
    return None

def completed_witness(s,c):
    words,_=reference(c)
    return s.phase=='done' and s.events==words and prefix_violation(s,c) is None

def run_campaign(helper,model,max_states):
    spec=model.SourceOriginalBatch()
    report,graph=helper.explore(spec,max_states)
    if report['status']!='PASS' or not report['complete']:
        raise AssertionError('incomplete source original batch baseline: '+str(report))
    states,edges,_=graph
    report['graph_sha256']=helper.digest({'states':[helper.render(s) for s in states],'edges':edges})
    report['completed_witnesses']={}
    for name,c in model.CASES.items():
        report['completed_witnesses'][name]=helper.witness(spec,graph,['ChooseOriginalBatch-'+name],
            lambda s,name=name,c=c:s.case==name and completed_witness(s,c))
    controls=[]
    for fault,expected in model.FAULTS.items():
        mutant=model.SourceOriginalBatch(fault)
        failure,_=helper.explore(mutant,max_states)
        if failure['status']!='COUNTEREXAMPLE' or failure.get('property')!=expected:
            raise AssertionError('insensitive or misclassified original batch mutant '+fault+': '+str(failure))
        helper.replay(mutant,failure['trace'],expected)
        controls.append({'fault':fault,'trace_sha256':helper.digest(failure['trace']),**failure})
    return report,controls
