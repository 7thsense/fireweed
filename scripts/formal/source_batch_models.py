"""Finite speculative batch composition; independent of Rust and providers.

No original freshness, predecessor resolution, CAS, durable ACK, canonical byte
encoding, allocation arithmetic or performance theorem is asserted here.
"""
from dataclasses import dataclass, replace
from models import Model

@dataclass(frozen=True)
class Work:
    version: int
    unique: int
    payload: int = 1
    number: int = 1

@dataclass(frozen=True)
class Image:
    work: tuple = (Work(1, 0), Work(1, 1))
    highwater: tuple = (1, 1)
    state: tuple | None = (1, 1)
    state_highwater: int = 1
    sequence: int = 2

@dataclass(frozen=True)
class Group:
    action: str
    key: int = 0
    expect: int = 1  # zero means absent
    unique: int = 0
    payload: int = 1
    number: int = 1
    fields: tuple = ()  # immutable ghost read-set declaration

@dataclass(frozen=True)
class BatchCase:
    groups: tuple
    expected: tuple  # literal independent ghost images/outcomes per position
    ids: tuple = ('z', 'a', 'm')
    offset: int = 0
    response_ok: bool = True
    admission: bool = True
    output_admission: bool = True
    source_failure: str = ''
    frame: int = 64
    record: int = 66
    requested_binding: tuple = ('A', 'D', 'P', 'J')

INITIAL = Image()
DELETED_A = replace(INITIAL, work=(None, Work(1, 1)), highwater=(2, 1))
TRANSFERRED = replace(DELETED_A, work=(None, Work(2, 0)), highwater=(2, 2))
PATCHED_A = replace(INITIAL, work=(Work(2, 0, number=7), Work(1, 1)), highwater=(2, 1))
PATCHED_BOTH = replace(PATCHED_A, work=(Work(2, 0, number=7), Work(2, 1)), highwater=(2, 2))
LARGE_A = replace(INITIAL, work=(Work(2, 0, 8), Work(1, 1)), highwater=(2, 1))
LARGE_BOTH = replace(LARGE_A, work=(Work(2, 0, 8), Work(2, 1, 8)), highwater=(2, 2))
TOMBSTONE_A = replace(INITIAL, work=(None, Work(1, 1)), highwater=(3, 1))
STATE_DELETED = replace(INITIAL, state=None, state_highwater=2)
STATE_RECREATED = replace(INITIAL, state=(3, 9), state_highwater=3)
DELETE_A = Group('delete', fields=('u',))
REPLACE_B = Group('put', key=1, unique=0, fields=('u',))
PATCH_A = Group('number', number=7, fields=('n',))
KEEP_B = Group('put', key=1, unique=1, fields=('u',))
BIG_A = Group('put', payload=8, fields=('u',))
BIG_B = Group('put', key=1, unique=1, payload=8, fields=('u',))
C = lambda version: ('committed', version)
R = ('rejected', 0)
TRANSFER_CASE = BatchCase((DELETE_A, REPLACE_B), ((DELETED_A, C(2)), (TRANSFERRED, C(2))))
FIELD_CASE = BatchCase((PATCH_A, KEEP_B), ((PATCHED_A, C(2)), (PATCHED_BOTH, C(2))))
LARGE_CASE = BatchCase((BIG_A, BIG_B), ((LARGE_A, C(2)), (LARGE_BOTH, C(2))), frame=12, record=32)
BATCH_CASES = {
    'release_then_transfer': TRANSFER_CASE,
    'rejection_preserves_overlay': BatchCase((REPLACE_B, PATCH_A), ((INITIAL, R), (PATCHED_A, C(2)))),
    'per_identity_fields_and_late_probe': FIELD_CASE,
    'state_delete_recreate_check': BatchCase((Group('state_delete', fields=('u', 'n')),
        Group('state_put', expect=0, number=9, fields=('u', 'n')), Group('state_check', expect=3)),
        ((STATE_DELETED, C(2)), (STATE_RECREATED, C(3)), (STATE_RECREATED, C(0)))),
    'physical_prefix': LARGE_CASE,
    'nonmonotonic_full_run': BatchCase((BIG_A, Group('delete', expect=2, fields=('u',))),
        ((LARGE_A, C(2)), (TOMBSTONE_A, C(3))), frame=7, record=9),
    'outer_exact': BatchCase((PATCH_A,), ((PATCHED_A, C(2)),), frame=20, record=6),
    'outer_one_short': BatchCase((PATCH_A,), ((PATCHED_A, C(2)),), frame=20, record=5),
    'first_group_too_large': BatchCase((BIG_A,), ((LARGE_A, C(2)),), frame=3, record=64),
    'aggregate_response_refusal': replace(TRANSFER_CASE, response_ok=False),
    'declaration_admission_refusal': replace(FIELD_CASE, admission=False),
    'record_admission_refusal': replace(LARGE_CASE, output_admission=False),
    'field_source_failure': replace(FIELD_CASE, source_failure='fields'),
    'late_probe_source_failure': replace(FIELD_CASE, source_failure='probe-1'),
    'caller_position_offset': replace(TRANSFER_CASE, offset=2),
    'work_delete_recreate_sequence': BatchCase((DELETE_A,Group('put',expect=0,fields=('u',))),
        ((DELETED_A,C(2)),(replace(INITIAL,work=(Work(3,0),Work(1,1)),highwater=(3,1),sequence=3),C(3)))),
    'state_check_unrelated_work_patch': BatchCase((Group('state_check'),PATCH_A),
        ((INITIAL,C(0)),(PATCHED_A,C(2)))),
    **{'foreign_'+name: replace(FIELD_CASE, requested_binding=binding)
       for name, binding in (('equal_head_cut', ('B','D','P','J')),
                            ('declaration', ('A','E','P','J')),
                            ('definition', ('A','D','Q','J')),
                            ('job', ('A','D','P','K')))},
}
BATCH_FAULTS = {
    'batch_skip_aggregate_validation': 'SB-VALIDATE',
    'batch_cross_key_fields': 'SB-FIELDS', 'batch_omit_fields': 'SB-FIELDS',
    'batch_reload_released_owner': 'SB-OVERLAY', 'batch_forget_loaded_absence': 'SB-OVERLAY',
    'batch_reset_overlay': 'SB-OVERLAY', 'batch_reset_highwater': 'SB-HWM',
    'batch_mutate_rejection': 'SB-REJECT', 'batch_early_clock': 'SB-CLOCK',
    'batch_resample_clock': 'SB-CLOCK', 'batch_skip_full_run_first': 'SB-PACK',
    'batch_ignore_outer_limit': 'SB-LIMIT', 'batch_suffix_effects': 'SB-PACK',
    'batch_suffix_receipts': 'SB-PACK', 'batch_mutate_beforeimage': 'SB-BEFORE',
    'batch_accept_foreign_binding': 'SB-BIND', 'batch_rebind_output_cut': 'SB-BIND',
    'batch_release_output_credit': 'SB-OWNER', 'batch_speculative_ack': 'SB-ACK',
    'batch_source_as_size_fallback': 'SB-SOURCE', 'batch_admission_as_size_fallback': 'SB-SOURCE',
    'batch_erase_history_on_error': 'SB-PREFIX', 'batch_reorder_groups': 'SB-ORDER',
    'batch_reset_insertion_sequence': 'SB-OVERLAY',
    'batch_validate_before_admission': 'SB-VALIDATE', 'batch_premature_unknown': 'SB-PREFIX',
    'batch_skip_required_probe': 'SB-OVERLAY', 'batch_foreign_seal_as_unknown': 'SB-BIND',
    'batch_erase_sealed_result': 'SB-PACK', 'batch_wrong_drop_index': 'SB-PREFIX',
}

def declared_fields(c):
    return tuple(sorted({('s' if g.action.startswith('state_') else 'w', g.key, field)
                         for g in c.groups for field in g.fields}))

def image_owners(image):
    return tuple(next((key for key, row in enumerate(image.work)
                       if row is not None and row.unique == unique), None) for unique in range(2))

def implementation_fields(c):
    names = set()
    for g in c.groups:
        fields = ('u','n') if g.action in ('state_put','state_delete') else (
            ('n',) if g.action=='number' else ('u',) if g.action in ('put','delete') else ())
        names.update(('s' if g.action.startswith('state_') else 'w',g.key,f) for f in fields)
    return tuple(sorted(names))

def implementation_probe(g, image):
    if g.action not in ('put', 'delete'): return ()
    old = image.work[g.key]
    return tuple(sorted(({old.unique} if old else set()) | ({g.unique} if g.action=='put' else set())))

def implementation_plan(g, image, owners, force=False):
    is_state = g.action.startswith('state_')
    old = image.state if is_state else image.work[g.key]
    actual = (old[0] if is_state else old.version) if old else 0
    valid = actual == g.expect
    if g.action=='put': valid &= owners[g.unique] in (None,g.key)
    if not valid and not force: return image, owners, R
    if g.action=='state_check': return image, owners, C(0)
    if is_state:
        version=image.state_highwater+1
        changed=replace(image,state_highwater=version,
            state=None if g.action=='state_delete' else (version,g.number))
        return changed, owners, C(version) if valid else R
    rows=list(image.work); highwater=list(image.highwater); version=highwater[g.key]+1
    highwater[g.key]=version; ownership=list(owners)
    if old is not None and g.action in ('delete','put'): ownership[old.unique]=None
    if g.action=='delete': rows[g.key]=None
    elif g.action=='put':
        rows[g.key]=Work(version,g.unique,g.payload); ownership[g.unique]=g.key
    else: rows[g.key]=replace(old,version=version,number=g.number)
    return replace(image,work=tuple(rows),highwater=tuple(highwater),
        sequence=image.sequence+int(g.action=='put' and old is None)),tuple(ownership),C(version) if valid else R

def encoded_sizes(before, after, count):
    body=0
    for key,row in enumerate(after.work):
        if row!=before.work[key] or after.highwater[key]!=before.highwater[key]: body+=2 if row is None else row.payload
    if after.state!=before.state or after.state_highwater!=before.state_highwater: body+=2
    return 2+body+count,4+body+count

@dataclass(frozen=True)
class BatchState:
    case: str = ''
    phase: str = 'choose'
    events: tuple = ()
    before: Image = INITIAL
    image: Image = INITIAL
    owners: tuple = (None,None)
    loaded: tuple = ()
    fields: tuple = ()
    index: int = 0
    histories: tuple = ()
    outcomes: tuple = ()
    clocks: tuple = ()
    attempts: tuple = ()
    candidate: int = 0
    best: int = 0
    retained: int = 0
    binding: tuple = ('A','D','P','J')
    plan_live: bool = False
    output_live: bool = False
    output_credit: bool = False
    result: str = ''
    returned: tuple = ()
    acknowledged: bool = False

class SourceBatchComposition(Model):
    name='api007-source-batch-composition'
    bounds={'groups':3,'work_identities':2,'state_identities':1,'unique_tuples':2,
            'opaque_equal_numeric_cuts':['A','B'],'fixture_cases':len(BATCH_CASES)}
    properties=list(dict.fromkeys(BATCH_FAULTS.values()))
    assumptions=['finite speculative composition only; no original freshness or preceding-position resolution',
        'no Rust refinement, canonical bytes, allocator arithmetic, conditional publication, provider durability or performance']
    initial=BatchState()
    def __init__(self,fault=None): self.fault=fault
    def transitions(self,s):
        if not s.case:
            for name in BATCH_CASES: yield 'SelectBatch-'+name,replace(s,case=name,phase='admit')
            return
        c=BATCH_CASES[s.case]; fault=self.fault
        def step(label,phase,event=None,**fields):
            return label,replace(s,phase=phase,events=s.events+((event,) if event else ()),**fields)
        def fail(kind='unknown'):
            events=s.events+((kind,-1),)
            if fault=='batch_erase_history_on_error': events=((kind,-1),)
            return 'Refuse-'+kind,replace(s,phase='done',events=events,result=kind,plan_live=False)
        if s.phase=='validate':
            if not c.response_ok and fault!='batch_skip_aggregate_validation': yield fail('invalid')
            else: yield step('ValidateWholeEnvelope','fields',('validate',-1))
        elif s.phase=='admit':
            if fault=='batch_validate_before_admission': yield step('ValidateWithoutFunding','validate',('validate',-1))
            elif not c.admission: yield fail()
            else: yield step('AdmitDeclarations','validate',('admit',-1),plan_live=True)
        elif s.phase=='fields':
            if fault=='batch_premature_unknown': yield fail()
            elif c.source_failure=='fields':
                if fault=='batch_source_as_size_fallback': yield step('SourceFailureAsSize','encode',('source-error',-1),candidate=len(c.groups))
                else: yield fail()
            else:
                fields=implementation_fields(c)
                if fault=='batch_cross_key_fields':
                    fields=tuple(sorted({('s' if g.action.startswith('state_') else 'w',g.key,f) for g in c.groups for _,_,f in fields}))
                if fault=='batch_omit_fields': fields=fields[:-1]
                events=s.events+(('fields',-1),)
                clocks=s.clocks
                if fault=='batch_early_clock': events+=(('clock',0),); clocks+=(0,)
                yield 'LoadUnionByIdentity',replace(s,phase='probe',events=events,fields=fields,clocks=clocks)
        elif s.phase=='probe':
            i=s.index
            if c.source_failure=='probe-'+str(i):
                if fault=='batch_source_as_size_fallback': yield step('ProbeFailureAsSize','encode',('source-error',i),candidate=len(c.groups))
                else: yield fail()
            else:
                image=replace(s.image,work=INITIAL.work,state=INITIAL.state,sequence=INITIAL.sequence) if fault=='batch_reset_overlay' and i else s.image
                if fault=='batch_reset_highwater' and i: image=replace(image,highwater=INITIAL.highwater,state_highwater=INITIAL.state_highwater)
                loaded=set(s.loaded); owners=list(s.owners)
                probes=() if fault=='batch_skip_required_probe' else implementation_probe(c.groups[i],image)
                for unique in probes:
                    if unique not in loaded or fault=='batch_reload_released_owner': owners[unique]=image_owners(INITIAL)[unique]
                    loaded.add(unique)
                yield step('LoadLateUniqueProbes','clock',('probe',i),image=image,loaded=tuple(sorted(loaded)),owners=tuple(owners))
        elif s.phase=='clock':
            yield step('SampleGroupClock','plan',('clock',s.index),clocks=s.clocks+(s.index,))
        elif s.phase=='plan':
            i=s.index
            if fault=='batch_reorder_groups' and len(c.groups)>1 and not i: i=1
            image,owners,outcome=implementation_plan(c.groups[i],s.image,s.owners,fault=='batch_mutate_rejection')
            if fault=='batch_reset_insertion_sequence': image=replace(image,sequence=s.image.sequence)
            loaded=() if fault=='batch_forget_loaded_absence' and c.groups[i].action=='delete' else s.loaded
            histories=s.histories+(image,); outcomes=s.outcomes+(outcome,)
            next_index=s.index+1; finished=next_index==len(c.groups)
            yield step('PlanSharedGroup','encode' if finished else 'probe',('plan',i),
                index=next_index,image=image,owners=owners,loaded=loaded,histories=histories,outcomes=outcomes,
                before=image if fault=='batch_mutate_beforeimage' else s.before,
                candidate=(1 if fault=='batch_skip_full_run_first' else len(c.groups)) if finished else 0)
        elif s.phase=='encode':
            if not c.output_admission:
                if fault=='batch_admission_as_size_fallback': yield step('AdmissionFailureAsSize','encode-result',('admission-fallback',-1))
                else: yield fail()
            else:
                n=s.candidate
                events=s.events+(('encode',n),); clocks=s.clocks
                if fault=='batch_resample_clock': events+=(('clock',len(c.groups)-1),); clocks+=(len(c.groups)-1,)
                yield 'EncodeRetainedCandidate',replace(s,phase='encode-result',events=events,
                    attempts=s.attempts+(n,),clocks=clocks)
        elif s.phase=='encode-result':
            n=s.candidate
            frame,record=encoded_sizes(s.before,s.histories[n-1],n)
            fits=frame<=c.frame and (record<=c.record or fault=='batch_ignore_outer_limit')
            if fits:
                if n==len(c.groups) or n==len(c.groups)-1:
                    yield step('KeepFittingPrefix','bind',retained=n,best=n)
                else: yield step('KeepPrefixTryNext','encode',candidate=n+1,best=n)
            elif not s.best and n==len(c.groups) and len(c.groups)>1:
                yield step('FullRunTooLarge','encode',candidate=1)
            elif s.best: yield step('DiscardOverflowSuffix','bind',retained=s.best)
            else: yield fail('too-large')
        elif s.phase=='bind':
            if c.requested_binding!=('A','D','P','J') and fault=='batch_foreign_seal_as_unknown':
                yield 'ForeignSealThenUnknown',replace(s,phase='done',result='unknown',plan_live=False,
                    output_live=True,output_credit=True,events=s.events+(('seal',s.retained),('unknown',-1)))
            elif c.requested_binding!=('A','D','P','J') and fault!='batch_accept_foreign_binding': yield fail()
            else:
                n=s.retained; effect=s.histories[-1] if fault=='batch_suffix_effects' else s.histories[n-1]
                count=len(c.groups) if fault=='batch_suffix_receipts' else n
                positions=tuple(range(c.offset,c.offset+count))
                receipts=tuple(sorted(zip(c.ids[:count],s.outcomes[:count])))
                binding=('B','D','P','J') if fault=='batch_rebind_output_cut' else s.binding
                returned=(effect,positions,receipts,binding)
                yield step('SealOwnedSpeculativeRecord','borrow',('seal',n),returned=returned,result='sealed',
                    plan_live=False,output_live=True,output_credit=fault!='batch_release_output_credit',
                    binding=binding,acknowledged=fault=='batch_speculative_ack')
        elif s.phase=='borrow':
            if fault=='batch_erase_sealed_result':
                yield 'EraseSealedResult',replace(s,retained=0,returned=(),result='',phase='probe',
                    plan_live=True,output_live=False,output_credit=False)
            else: yield step('BorrowRecord','drop',('borrow',-1))
        elif s.phase=='drop':
            if fault=='batch_wrong_drop_index': yield step('WrongIndexDrop','done',('drop',0))
            else: yield step('DropRecord','done',('drop',-1),output_live=False,output_credit=False)
    def violation(self,before,action,s):
        if before.case:
            if s.case!=before.case or s.events[:len(before.events)]!=before.events: return 'SB-PREFIX'
            if not set(before.loaded)<=set(s.loaded): return 'SB-OVERLAY'
        if not s.case: return None
        c=BATCH_CASES[s.case]; events=s.events
        if s.before!=INITIAL: return 'SB-BEFORE'
        if s.acknowledged: return 'SB-ACK'
        if any(tag in ('source-error','admission-fallback') for tag,_ in events): return 'SB-SOURCE'
        if not c.response_ok and any(tag in ('validate','fields','probe','clock','plan','encode','seal') for tag,_ in events): return 'SB-VALIDATE'
        if c.source_failure=='fields' and ('fields',-1) in events: return 'SB-SOURCE'
        if c.source_failure.startswith('probe-') and ('probe',int(c.source_failure[-1])) in events: return 'SB-SOURCE'
        if not c.output_admission and any(tag=='encode' for tag,_ in events): return 'SB-SOURCE'
        if s.fields!=(declared_fields(c) if ('fields',-1) in events else ()): return 'SB-FIELDS'
        if s.loaded!=tuple(sorted(set(s.loaded))) or not set(s.loaded)<={0,1}: return 'SB-OVERLAY'
        planned=[i for tag,i in events if tag=='plan']; clocks=[i for tag,i in events if tag=='clock']
        if planned!=list(range(len(planned))) or s.index!=len(planned): return 'SB-ORDER'
        if tuple(clocks)!=s.clocks or clocks!=list(range(len(clocks))) or len(clocks)>len(c.groups): return 'SB-CLOCK'
        total=len(c.groups)
        def fits(n):
            frame,record=encoded_sizes(INITIAL,c.expected[n-1][0],n)
            return frame<=c.frame and record<=c.record
        expected_attempts=[total]; expected_retained=total if fits(total) else 0
        if not expected_retained:
            for n in range(1,total):
                expected_attempts.append(n)
                if not fits(n): break
                expected_retained=n
        seen=set(); terminal=False; seen_encodes=[]; required_loaded=set()
        global_tags={'validate','admit','fields','borrow','drop','unknown','invalid','too-large'}
        group_tags={'probe','clock','plan'}; prefix_tags={'encode','seal'}
        allowed=global_tags|group_tags|prefix_tags
        for tag,i in events:
            if terminal or tag not in allowed or (tag,i) in seen: return 'SB-PREFIX'
            if type(i) is not int or (tag in global_tags and i!=-1) or (tag in group_tags and not 0<=i<total) or (tag in prefix_tags and not 1<=i<=total): return 'SB-PREFIX'
            if any(t=='seal' for t,_ in seen) and tag not in ('borrow','drop'): return 'SB-PREFIX'
            if tag=='seal' and c.requested_binding!=('A','D','P','J'): return 'SB-BIND'
            if tag=='admit' and not c.admission: return 'SB-VALIDATE'
            if tag=='validate' and ('admit',-1) not in seen: return 'SB-VALIDATE'
            if tag=='fields' and not {('admit',-1),('validate',-1)}<=seen: return 'SB-VALIDATE'
            if tag=='probe' and ('fields',-1) not in seen: return 'SB-VALIDATE'
            if tag=='seal' and (seen_encodes!=expected_attempts or i!=expected_retained or not i): return 'SB-PACK'
            if tag=='borrow' and not any(t=='seal' for t,_ in seen): return 'SB-OWNER'
            if tag=='drop' and ('borrow',-1) not in seen: return 'SB-OWNER'
            if tag=='clock' and ('probe',i) not in seen: return 'SB-CLOCK'
            if tag=='plan' and ('clock',i) not in seen: return 'SB-CLOCK'
            if tag=='probe' and i!=len([1 for t,_ in seen if t=='plan']): return 'SB-ORDER'
            if tag=='probe':
                group=c.groups[i]; previous=INITIAL if not i else c.expected[i-1][0]
                if group.action in ('put','delete'):
                    if previous.work[group.key] is not None: required_loaded.add(previous.work[group.key].unique)
                    if group.action=='put': required_loaded.add(group.unique)
            if tag=='encode':
                if len([1 for t,_ in seen if t=='plan'])!=total or not {('admit',-1),('validate',-1),('fields',-1)}<=seen: return 'SB-PACK'
                seen_encodes.append(i)
            if tag in ('unknown','invalid','too-large','drop'): terminal=True
            seen.add((tag,i))
        if set(s.loaded)!=required_loaded: return 'SB-OVERLAY'
        if len(s.histories)!=len(planned) or len(s.outcomes)!=len(planned): return 'SB-PREFIX'
        for i,(image,outcome) in enumerate(zip(s.histories,s.outcomes)):
            expected,decision=c.expected[i]
            if outcome==R and image!=(INITIAL if not i else c.expected[i-1][0]): return 'SB-REJECT'
            if image.highwater!=expected.highwater or image.state_highwater!=expected.state_highwater: return 'SB-HWM'
            if image!=expected or outcome!=decision: return 'SB-OVERLAY'
        expected_image=INITIAL if not planned else c.expected[len(planned)-1][0]
        if s.image.highwater!=expected_image.highwater or s.image.state_highwater!=expected_image.state_highwater: return 'SB-HWM'
        if s.image!=expected_image: return 'SB-OVERLAY'
        expected_owners=image_owners(expected_image)
        if any(s.owners[i]!=expected_owners[i] for i in s.loaded): return 'SB-OVERLAY'
        if s.attempts!=tuple(expected_attempts[:len(s.attempts)]): return 'SB-PACK'
        if s.phase=='encode' and (len(s.attempts)>=len(expected_attempts) or s.candidate!=expected_attempts[len(s.attempts)]): return 'SB-PACK'
        if s.phase=='encode-result' and (not s.attempts or s.candidate!=s.attempts[-1]): return 'SB-PACK'
        if s.phase in ('bind','borrow','drop') and s.retained and not fits(s.retained): return 'SB-LIMIT'
        if s.phase in ('bind','borrow','drop') and (s.retained!=expected_retained or len(s.attempts)!=len(expected_attempts)): return 'SB-PACK'
        if tuple(i for tag,i in events if tag=='encode')!=s.attempts: return 'SB-PREFIX'
        if s.binding!=('A','D','P','J'): return 'SB-BIND'
        if s.returned:
            n=s.retained
            if not n or n>len(c.groups): return 'SB-PACK'
            effect,positions,receipts,binding=s.returned
            if effect!=c.expected[n-1][0] or positions!=tuple(range(c.offset,c.offset+n)) or receipts!=tuple(sorted((c.ids[i],c.expected[i][1]) for i in range(n))): return 'SB-PACK'
            frame,record=encoded_sizes(INITIAL,effect,n)
            if frame>c.frame or record>c.record: return 'SB-LIMIT'
            if binding!=('A','D','P','J') or c.requested_binding!=binding or s.binding!=binding: return 'SB-BIND'
            if not c.output_admission: return 'SB-SOURCE'
        seal_counts=[i for tag,i in events if tag=='seal']
        sealed=bool(seal_counts)
        if sealed and (len(seal_counts)!=1 or seal_counts[0]!=s.retained): return 'SB-PACK'
        dropped=('drop',-1) in seen
        if s.output_live!=(sealed and not dropped) or s.output_credit!=s.output_live: return 'SB-OWNER'
        if s.plan_live!=(('admit',-1) in seen and not sealed and s.phase!='done'): return 'SB-OWNER'
        if sealed!=bool(s.returned) or sealed!=(s.result=='sealed'): return 'SB-PREFIX'
        if terminal and (s.plan_live or s.output_live or s.output_credit): return 'SB-OWNER'
        if sealed:
            expected_phase='done' if dropped else 'drop' if ('borrow',-1) in seen else 'borrow'
            if s.phase!=expected_phase: return 'SB-PREFIX'
        if (s.phase=='done')!=terminal: return 'SB-PREFIX'
        if s.result and not sealed and not terminal: return 'SB-PREFIX'
        if terminal:
            terminal_kind=events[-1][0]
            if s.result!=('sealed' if terminal_kind=='drop' else terminal_kind): return 'SB-PREFIX'
            if s.result=='unknown':
                allowed_failure=(not c.admission and ('admit',-1) not in seen)
                allowed_failure|=(c.source_failure=='fields' and ('validate',-1) in seen and ('fields',-1) not in seen)
                allowed_failure|=(c.source_failure.startswith('probe-') and ('fields',-1) in seen
                    and len(planned)==int(c.source_failure[-1]))
                allowed_failure|=(not c.output_admission and len(planned)==total and not s.attempts)
                allowed_failure|=(c.requested_binding!=('A','D','P','J') and expected_retained>0
                    and s.retained==expected_retained and len(s.attempts)==len(expected_attempts))
                if not allowed_failure: return 'SB-PREFIX'
            if s.result=='invalid' and (c.response_ok or ('admit',-1) not in seen): return 'SB-PREFIX'
            if s.result=='too-large' and (expected_retained or s.attempts!=tuple(expected_attempts)): return 'SB-PACK'
        return None
