"""Independent immutable-fixture witness oracle for the sixth source campaign."""
def expected_original(case):
    c=case
    words=['validate']
    if not (c.scope and c.limits and c.domain<=2 and bool(c.literal.strip())): return 'unknown',tuple(words)
    if not c.admission: return 'unknown',tuple(words+['return-unknown'])
    words+=['admit']
    if c.semantic<1: return 'unknown',tuple(words+['return-unknown'])
    words+=['point-grant']
    if not c.point_ok: return 'unknown',tuple(words+['point-error'])
    words+=['point']
    if c.point_cut!='original' or not c.point_key: return 'unknown',tuple(words+['return-unknown'])
    words+=['point-verified']
    if c.present and c.fixed!='valid': return 'unknown',tuple(words+['return-unknown'])
    words+=['fixed']
    if not c.present: return 'absent',tuple(words+['return-absent','borrow','drop'])
    if c.mode=='replay' and not c.matches: return 'conflict',tuple(words+['return-conflict'])
    words+=['matching']
    if c.records<1: return 'unknown',tuple(words+['return-unknown'])
    words+=['record-grant']
    if not c.record_ok: return 'unknown',tuple(words+['record-error'])
    words+=['record-read']
    for word,valid in [('pin',c.pin),('member',c.member),('span',c.span=='exact'),('digest',c.digest),('decode',c.decode),('binding',c.domain!=1 or c.claim_binding)]:
        if not valid: return 'unknown',tuple(words+['return-unknown'])
        words.append(word)
    return 'present',tuple(words+['return-present','borrow','drop'])

def completed_original_witness(s,c):
    result,words=expected_original(c)
    success=result in ('present','absent')
    returned=(result,c.origin,c.domain,c.literal,'input-ledger') if success else ()
    observed=(('B' if c.origin=='A' else 'A') if c.point_cut=='foreign' else c.origin) if 'point' in words else ''
    key=((c.domain,c.literal) if c.point_key else (c.domain,'prefix')) if 'point' in words else ()
    return (s.phase=='done' and s.result==result and s.events==words and s.active==c.origin
        and s.ledger=='input-ledger' and s.semantic==c.semantic-words.count('point-grant')
        and s.records==c.records-words.count('record-grant') and s.point_cut==observed and s.point_key==key
        and s.record_cut==(c.origin if 'record-grant' in words else '')
        and s.external_capsule and not s.capsule_borrow and not s.identity_credit and not s.record_live
        and s.returned==returned and s.output==() and s.borrowed==success)

def run_original_campaign(helper,model,max_states):
    spec=model.SourceOriginalResolution()
    report,graph=helper.explore(spec,max_states)
    if report['status']!='PASS' or not report['complete']:
        raise AssertionError('incomplete source original resolution baseline')
    states,edges,_=graph
    report['graph_sha256']=helper.digest({'states':[helper.render(s) for s in states],'edges':edges})
    report['completed_witnesses']={}
    for name,case in model.ORIGINAL_CASES.items():
        report['completed_witnesses'][name]=helper.witness(spec,graph,['SelectOriginal-'+name],
            lambda s,name=name,case=case: s.case==name and completed_original_witness(s,case))
    controls=[]
    for fault,expected in model.ORIGINAL_FAULTS.items():
        mutant=model.SourceOriginalResolution(fault)
        failure,_=helper.explore(mutant,max_states)
        if failure['status']!='COUNTEREXAMPLE' or failure.get('property')!=expected:
            raise AssertionError('insensitive source original mutant '+fault)
        helper.replay(mutant,failure['trace'],expected)
        controls.append({'fault':fault,'trace_sha256':helper.digest(failure['trace']),**failure})
    return report,controls
