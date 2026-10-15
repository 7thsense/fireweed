"""Finite source-plan and speculative preparation safety, independent of Rust/providers."""
from dataclasses import dataclass, replace
from itertools import product
from models import Model

PROFILES = ('mixed', 'mixed_check_corrupt', 'selected_overcount', 'state_check',
            'state_put', 'state_short', 'state_incomplete', 'future_sequence',
            'future_body', 'future_state', 'swap')
SCOPES = ('same', 'request', 'schema', 'job', 'key', 'cut_aba', 'retarget')
OWNERS = ('absent', 'release', 'reclaim', 'swap', 'conflict', 'missing_release')
FAILURES = ('none', 'budget', 'field', 'unique', 'cancel')
CASES = tuple(c for c in product(PROFILES, SCOPES, OWNERS, (False, True), FAILURES, (False, True))
              if (c[0] not in ('state_check', 'future_state') or c[2] == 'absent')
              and (c[2] != 'swap' or c[0] == 'swap'))
FAULTS = {'validation_before_admission': 'G-ADMISSION', 'skip_scope': 'G-BIND',
          'skip_key': 'G-KEY', 'recapture_fields': 'G-CUT',
          'read_check_fields': 'G-COVERAGE', 'skip_selected_count': 'G-FIELDS',
          'skip_complete_count': 'G-FIELDS', 'skip_complete_finish': 'G-FIELDS',
          'skip_source_bounds': 'G-BOUNDS', 'retarget_capsule': 'G-BIND',
          'clock_before_probes': 'G-CLOCK', 'missing_release_success': 'G-OUTCOME',
          'source_error_as_absence': 'G-SOURCE', 'seal_after_cancel': 'G-SEAL',
          'sequential_swap': 'G-OUTCOME', 'ignore_surviving_owner': 'G-OUTCOME',
          'read_before_admission': 'G-PREFIX', 'skip_evidence': 'G-SEAL', 'skip_seal': 'G-SEAL'}

# Immutable ghost declarations. A/B name addressed keys; root-0/root-1 are
# distinct opaque captures with identical numeric epoch/through values.
def required_fields(case):
    profile = case[0]
    if profile in ('state_check', 'future_state'): return ()
    return ('A', 'B') if profile == 'swap' else ('B',)

def needs_unique(case): return case[0] not in ('state_check', 'future_state')
def bounds_bad(case): return case[0] in ('future_sequence', 'future_body', 'future_state')
def fields_bad(case): return case[0] in ('selected_overcount', 'state_short', 'state_incomplete')
def lease_rejects(case): return case[5] and case[0] in ('mixed', 'mixed_check_corrupt')

def unique_facts(case):
    if not needs_unique(case): return (None, None), (), ()
    return {
        'absent': ((None, None), (), ((0, 'B'),)),
        'release': (('B', None), ((0, 'B'),), ()),
        'reclaim': (('B', None), ((0, 'B'),), ((0, 'B'),)),
        'swap': (('A', 'B'), ((0, 'A'), (1, 'B')), ((0, 'B'), (1, 'A'))),
        'conflict': (('A', None), (), ((0, 'B'),)),
        'missing_release': ((None, None), ((0, 'B'),), ()),
    }[case[2]]

def ghost_unique(case):
    # Relational oracle: survivors plus all claims, independent of execution order.
    initial, releases, claims = unique_facts(case)
    if any(initial[slot] != owner for slot, owner in releases): return 'unknown', ()
    surviving = {(slot, owner) for slot, owner in enumerate(initial)
                 if owner is not None and (slot, owner) not in releases}
    final = surviving | set(claims)
    if any(len({owner for at, owner in final if at == slot}) > 1 for slot in range(2)):
        return 'rejected', ()
    return 'committed', tuple(next((owner for at, owner in final if at == slot), None) for slot in range(2))

def planner_unique(case, fault):
    # Abstract mutable planner effects; deliberately separate from ghost oracle.
    initial, releases, claims = unique_facts(case)
    owners = list(initial)
    for slot, owner in releases:
        if owners[slot] != owner and fault != 'missing_release_success': return 'unknown', ()
    batches = [(releases, claims)]
    if fault == 'sequential_swap':
        batches = [(tuple(row for row in releases if row[1] == key),
                    tuple(row for row in claims if row[1] == key)) for key in ('A', 'B')]
    for drops, adds in batches:
        for slot, _ in drops: owners[slot] = None
        for slot, owner in adds:
            if owners[slot] not in (None, owner) and fault != 'ignore_surviving_owner': return 'rejected', ()
            owners[slot] = owner
    return 'committed', tuple(owners)

def expected(case):
    """Declarative ghost classification, never derived from implementation flags."""
    profile, scope, owner, early_reject, failure, _ = case
    if failure == 'budget' or scope in ('request', 'schema', 'job', 'key'): return 'unknown'
    if bounds_bad(case): return 'unknown'
    if required_fields(case) and (scope == 'cut_aba' or fields_bad(case) or failure == 'field'): return 'unknown'
    if needs_unique(case) and failure == 'unique': return 'unknown'
    if failure == 'cancel': return 'cancelled'
    if scope == 'retarget': return 'unknown'
    # Existing shared planner checks version/lease business rejection before
    # validating an intended release. Absence is not intrinsically an I/O error.
    if early_reject or lease_rejects(case): return 'rejected'
    return ghost_unique(case)[0]

@dataclass(frozen=True)
class State:
    case: tuple = ()
    phase: str = 'choose'
    admitted: bool = False
    allocated: bool = False
    fixed_read: bool = False
    bound: bool = False
    key_bound: bool = False
    metadata_valid: bool = False
    metadata_accepted: bool = False
    fields: tuple = ()
    field_root: str = 'root-0'
    fields_valid: bool = True
    source_ok: bool = True
    fields_done: bool = False
    unique_done: bool = False
    clock: int = 0
    environment_time: int = 0
    sampled_time: int = -1
    retargeted: bool = False
    sealed: bool = False
    cancelled: bool = False
    result: str = ''
    final_owners: tuple = ()

class SourceGroup(Model):
    name = 'api007-source-group-preimage'
    initial = State()
    properties = sorted(set(FAULTS.values()))
    bounds = {'addressed_keys': 2, 'opaque_roots': 2, 'equal_numeric_heads': True,
              'cases': len(CASES), 'clock_samples': 1, 'frontier': 'terminal classification of every case'}
    assumptions = ['one immutable authenticated source map; facts and canonical decoders are ghost inputs',
        'root-0 and root-1 differ despite identical numeric epoch/through',
        'ordinary groups exclude administrative selection and required old-body replacement',
        'mixed profiles declare Check(A) and Patch(B); swap profile mutates both keys',
        'selected and complete field membership are distinct evidence contracts',
        'source error, exhausted work or incomplete range is never authenticated absence',
        'owner classes independently denote release/reclaim, two-owner swap, surviving conflict and missing release',
        'one lease can expire during the final required unique probe',
        'shared planner version/lease rejection precedes intended release validation',
        'finite safety only; no Rust refinement, exact heap arithmetic, publication, provider durability or performance proof']
    def __init__(self, fault=None):
        if fault is not None and fault not in FAULTS: raise ValueError('unknown source-group fault')
        self.fault = fault
    def frontier(self, state): return state.phase == 'done'
    def transitions(self, s):
        if s.phase == 'choose':
            for i, case in enumerate(CASES): yield 'SelectCase-'+str(i), replace(s, case=case, phase='admit')
            return
        profile, scope, owner, early, failure, expires = s.case
        done = lambda **kw: replace(s, phase='done', **kw)
        if s.phase == 'admit':
            if self.fault == 'read_before_admission':
                yield 'ReadBeforeAdmission', replace(s, fixed_read=True, phase='fixed')
            elif self.fault == 'validation_before_admission':
                yield 'AllocateValidationBeforeAdmission', replace(s, phase='validate', allocated=True)
            elif failure == 'budget': yield 'RejectAdmission', done(result='unknown')
            else: yield 'AdmitStructuralScratch', replace(s, admitted=True, phase='validate')
        elif s.phase == 'validate':
            if scope in ('request', 'schema', 'job') and self.fault != 'skip_scope':
                yield 'RejectForeignDeclarations', done(allocated=True, result='unknown')
            else: yield 'ValidateDeclarations', replace(s, allocated=True, bound=scope not in ('request', 'schema', 'job'), phase='fixed')
        elif s.phase == 'fixed':
            if self.fault == 'skip_evidence':
                result, owners = planner_unique(s.case, None)
                yield 'BypassSourceEvidence', done(sealed=True, result=result, final_owners=owners)
            elif scope == 'key' and self.fault != 'skip_key': yield 'RejectForeignFullKey', done(fixed_read=True, result='unknown')
            elif bounds_bad(s.case) and self.fault != 'skip_source_bounds':
                yield 'RejectFutureSourceReference', done(fixed_read=True, key_bound=True, result='unknown')
            else: yield 'AcceptFixedMetadata', replace(s, fixed_read=True, key_bound=scope != 'key', metadata_valid=not bounds_bad(s.case), metadata_accepted=True, phase='fields')
        elif s.phase == 'fields':
            fields = required_fields(s.case)
            if self.fault == 'read_check_fields' and profile in ('mixed', 'mixed_check_corrupt'):
                fields = ('A', 'B')
            if fields and scope == 'cut_aba' and self.fault != 'recapture_fields':
                yield 'RejectForeignFieldCut', done(result='unknown')
            elif fields and failure == 'field' and self.fault != 'source_error_as_absence':
                yield 'RejectFieldReadFailure', done(result='unknown')
            elif profile == 'selected_overcount' and self.fault != 'skip_selected_count':
                yield 'RejectSelectedOvercount', done(result='unknown')
            elif profile == 'state_short' and self.fault != 'skip_complete_count':
                yield 'RejectCompleteCount', done(result='unknown')
            elif profile == 'state_incomplete' and self.fault != 'skip_complete_finish':
                yield 'RejectIncompleteRange', done(result='unknown')
            else:
                yield 'AcceptRequiredFields', replace(s, fields=fields, fields_done=True,
                    field_root='root-1' if fields and scope == 'cut_aba' else 'root-0',
                    fields_valid=not fields_bad(s.case), source_ok=not (fields and failure == 'field'), phase='unique')
        elif s.phase == 'unique':
            if failure == 'cancel':
                yield 'CancelPartialPreimage', done(cancelled=True, sealed=self.fault == 'seal_after_cancel', result='cancelled')
            elif self.fault == 'clock_before_probes' and needs_unique(s.case):
                yield 'SampleClockBeforeProbe', replace(s, clock=1, sampled_time=s.environment_time, phase='clock')
            elif needs_unique(s.case) and failure == 'unique' and self.fault != 'source_error_as_absence':
                yield 'RejectUniqueReadFailure', done(result='unknown')
            else: yield 'CompleteSourceProbes', replace(s, unique_done=True,
                source_ok=not (needs_unique(s.case) and failure == 'unique'),
                environment_time=2 if expires and needs_unique(s.case) else 0, phase='seal')
        elif s.phase == 'seal':
            if scope == 'retarget' and self.fault != 'retarget_capsule': yield 'RejectRetargetedRequest', done(result='unknown')
            else: yield 'BindPlanDeclarations', replace(s, retargeted=scope == 'retarget', phase='clock')
        elif s.phase == 'clock':
            yield 'SampleClock', replace(s, clock=s.clock+1, sampled_time=s.environment_time, phase='plan')
        elif s.phase == 'plan':
            if early or (lease_rejects(s.case) and s.sampled_time >= 2): result, owners = 'rejected', ()
            else: result, owners = planner_unique(s.case, self.fault)
            yield 'RunSharedPlanner', done(sealed=result in ('committed', 'rejected') and self.fault != 'skip_seal', result=result, final_owners=owners)
    def violation(self, before, action, s):
        if s.allocated and not s.admitted: return 'G-ADMISSION'
        if not s.case: return None
        if s.fixed_read and s.case[1] in ('request', 'schema', 'job'): return 'G-BIND'
        if s.metadata_accepted and (s.case[1] == 'key' or not s.key_bound): return 'G-KEY'
        if s.metadata_accepted and (not s.metadata_valid or bounds_bad(s.case)): return 'G-BOUNDS'
        if not set(s.fields).issubset(required_fields(s.case)): return 'G-COVERAGE'
        if s.field_root != 'root-0': return 'G-CUT'
        if s.fields_done and (not s.fields_valid or fields_bad(s.case)): return 'G-FIELDS'
        if s.fields_done and set(s.fields) != set(required_fields(s.case)): return 'G-COVERAGE'
        if not s.source_ok or (s.fields_done and required_fields(s.case) and s.case[4] == 'field') or (s.unique_done and needs_unique(s.case) and s.case[4] == 'unique'): return 'G-SOURCE'
        if s.retargeted: return 'G-BIND'
        any_proof = s.fixed_read or s.metadata_accepted or s.fields_done or s.unique_done or s.clock or s.sealed or s.result in ('committed', 'rejected')
        if any_proof and (not s.admitted or not s.bound): return 'G-PREFIX'
        if (s.fields or s.fields_done or s.unique_done or s.clock) and not (s.fixed_read and s.metadata_accepted and s.metadata_valid and s.key_bound): return 'G-PREFIX'
        if s.clock and (not s.fields_done or not s.unique_done or s.clock != 1): return 'G-CLOCK'
        if s.cancelled and s.sealed: return 'G-SEAL'
        planned = s.result in ('committed', 'rejected')
        complete = s.admitted and s.bound and s.fixed_read and s.metadata_accepted and s.metadata_valid and s.key_bound and s.fields_done and s.unique_done and s.field_root == 'root-0' and not s.retargeted and s.clock == 1 and not s.cancelled
        if (planned and not s.sealed) or ((s.sealed or planned) and not complete): return 'G-SEAL'
        if s.phase == 'done' and s.result != expected(s.case): return 'G-OUTCOME'
        if s.result == 'committed' and s.final_owners != ghost_unique(s.case)[1]: return 'G-OUTCOME'
        if s.sealed and (s.clock != 1 or s.cancelled or s.result not in ('committed', 'rejected')): return 'G-SEAL'

# A separate bounded boundary model starts with a legitimately sealed plan.
# It proves no Rust refinement, codec fidelity, provider CAS or durability.
PUBLICATION_CASES = tuple(product(('committed', 'rejected'),
    ('same', 'cut', 'definition', 'job'), ('none', 'canonical', 'frame', 'record')))
PUBLICATION_FAULTS = {
    'canonical_before_admission': 'P-ADMISSION',
    'release_plan_before_encoding': 'P-LIFETIME',
    'replace_declarations': 'P-BIND',
    'skip_cut_binding': 'P-PREPARE',
    'skip_definition_binding': 'P-PREPARE',
    'skip_job_binding': 'P-PREPARE',
    'debit_before_binding': 'P-PREFIX',
    'prepare_acknowledges': 'P-ACK',
    'skip_allocation_prefix': 'P-PREFIX',
}

@dataclass(frozen=True)
class PublicationState:
    case: tuple = ()
    phase: str = 'choose'
    plan_live: bool = True
    canonical_admitted: bool = False
    canonical_allocated: bool = False
    frame_admitted: bool = False
    frame_allocated: bool = False
    record_admitted: bool = False
    record_allocated: bool = False
    original_declarations: bool = True
    encoded: bool = False
    binding_verified: bool = False
    compiler_work: bool = False
    prepared: bool = False
    acknowledged: bool = False

class SourceGroupPublication(Model):
    name = 'api007-source-group-encoding-preparation'
    initial = PublicationState()
    properties = sorted(set(PUBLICATION_FAULTS.values()))
    bounds = {'cases': len(PUBLICATION_CASES), 'opaque_roots': 2,
              'equal_numeric_heads': True, 'frontier': 'terminal preparation or refusal'}
    assumptions = ['input is a sealed committed or rejected source-group plan',
        'cut mismatch includes equal numeric epoch/through with distinct opaque roots',
        'canonical, frame and record admission are abstract nonzero funding guards',
        'encoding retains original queue/request/definition/epoch/sequence/before-image/receipt',
        'preparation performs no submit, refresh, durable resolution or acknowledgement',
        'finite abstract safety only; no Rust refinement, exact sizing, codec fidelity or provider durability']
    def __init__(self, fault=None):
        if fault is not None and fault not in PUBLICATION_FAULTS: raise ValueError('unknown publication fault')
        self.fault = fault
    def frontier(self, s): return s.phase == 'done'
    def transitions(self, s):
        if s.phase == 'choose':
            for i, case in enumerate(PUBLICATION_CASES):
                yield 'SelectPublication-'+str(i), replace(s, case=case, phase='canonical')
            return
        outcome, binding, failure = s.case
        if s.phase == 'canonical':
            if self.fault == 'skip_allocation_prefix':
                yield 'ForgePreparedWithoutBuffers', replace(s, encoded=True, binding_verified=True, compiler_work=True, prepared=True, phase='done')
            elif self.fault == 'canonical_before_admission':
                yield 'UnfundedCanonicalization', replace(s, canonical_allocated=True, phase='frame')
            elif failure == 'canonical': yield 'RefuseCanonicalAdmission', replace(s, phase='done')
            else: yield 'AdmitThenCanonicalize', replace(s, canonical_admitted=True, canonical_allocated=True,
                plan_live=self.fault != 'release_plan_before_encoding', phase='frame')
        elif s.phase == 'frame':
            if failure == 'frame': yield 'RefuseFrameAdmission', replace(s, phase='done')
            else: yield 'AdmitThenEncodeFrame', replace(s, frame_admitted=True, frame_allocated=True,
                original_declarations=self.fault != 'replace_declarations', phase='record')
        elif s.phase == 'record':
            if failure == 'record': yield 'RefuseRecordAdmission', replace(s, phase='done')
            else: yield 'AdmitThenEncodeRecord', replace(s, record_admitted=True,
                record_allocated=True, encoded=True, phase='binding')
        elif s.phase == 'binding':
            if self.fault == 'debit_before_binding':
                yield 'CompilerBeforeBinding', replace(s, compiler_work=True, phase='prepare')
            elif binding != 'same' and self.fault != 'skip_'+binding+'_binding':
                yield 'RefuseForeignBinding', replace(s, phase='done')
            else: yield 'VerifyOriginalBinding', replace(s, binding_verified=True, phase='prepare')
        elif s.phase == 'prepare':
            yield 'CompileAndPrepareWithoutSubmit', replace(s, compiler_work=True, prepared=True,
                acknowledged=self.fault == 'prepare_acknowledges', phase='done')
    def violation(self, before, action, s):
        if ((s.canonical_allocated and not s.canonical_admitted)
            or (s.frame_allocated and not s.frame_admitted)
            or (s.record_allocated and not s.record_admitted)): return 'P-ADMISSION'
        canonical = s.canonical_allocated and s.canonical_admitted
        frame = canonical and s.frame_allocated and s.frame_admitted
        record = frame and s.record_allocated and s.record_admitted
        if s.frame_allocated and not canonical: return 'P-PREFIX'
        if s.record_allocated and not frame: return 'P-PREFIX'
        if (s.encoded or s.binding_verified or s.compiler_work or s.prepared) and not record: return 'P-PREFIX'
        if s.canonical_allocated and not s.plan_live: return 'P-LIFETIME'
        if not s.original_declarations: return 'P-BIND'
        if s.binding_verified and s.case[1] != 'same': return 'P-PREPARE'
        if s.compiler_work and not (s.encoded and s.binding_verified): return 'P-PREFIX'
        if s.acknowledged: return 'P-ACK'
        if s.prepared and not (s.encoded and s.binding_verified and s.compiler_work): return 'P-PREFIX'

# Independent record-read ledger/ownership abstraction. Units deliberately are
# NOT bytes or SDK calls: the Rust/provider correspondence remains documentary.
READ_CASES = tuple(product(('same', 'foreign'), (0, 1, 2), (1, 2, 3, 4),
                           (4, 5, 9, 10),
                           ('success', 'cancel', 'provider', 'missing', 'malformed', 'root')))
READ_FAULTS = {'read_skip_scope': 'R-SCOPE', 'read_refund_attempt': 'R-WORK',
    'read_partial_reads': 'R-WORK', 'read_partial_bytes': 'R-WORK',
    'read_early_hydrate': 'R-PREFIX', 'read_refund_cancel': 'R-WORK',
    'read_early_root_release': 'R-OWNER', 'read_payload_only': 'R-OWNER',
    'read_reset_semantic': 'R-SEMANTIC', 'read_forged_alias': 'R-AUTHORITY', 'read_skip_alias_drop': 'R-OWNER'}

@dataclass(frozen=True)
class ReadState:
    case: tuple = ()
    phase: str = 'choose'
    index: int = 0
    attempts: int = 0
    reads: int = 0
    body: int = 0
    semantic: int = 7
    credit: int = 0
    events: tuple = ()
    acknowledged: bool = False

class SourceRecordRead(Model):
    name = 'api007-source-record-read'
    initial = ReadState()
    properties = sorted(set(READ_FAULTS.values()) | {'R-AUTHORITY', 'R-ACK'})
    bounds = {'reads_sharing_ledger': 2, 'required_reads': 2, 'required_body': 5,
              'frame': 9, 'payload': 3, 'cases': len(READ_CASES),
              'frontier': 'two completed calls and all aliases dropped'}
    assumptions = ['immutable initial grants; independent event-prefix balance oracle',
        'one original capture; same/foreign binding is an immutable ghost input',
        'first call has selected outcome; second retries success with the SAME ledger',
        'second call starts after first physical retirement; overlapping concurrent retries are excluded',
        'planning attempt precedes hydration grant; grant precedes Root admission',
        'caller cancellation and physical retirement are distinct transitions',
        'successful aliases retain a frame larger than payload after physical retirement',
        'finite abstract ledger/ownership safety only; no Rust refinement, exact heap accounting, provider durability or performance proof']
    def __init__(self, fault=None):
        if fault is not None and fault not in READ_FAULTS: raise ValueError('unknown record-read fault')
        self.fault = fault
    def frontier(self, s): return s.phase == 'done'
    def transitions(self, s):
        if s.phase == 'choose':
            for i, case in enumerate(READ_CASES):
                yield 'SelectRead-'+str(i), replace(s, case=case, attempts=case[1],
                    reads=case[2], body=case[3], phase='validate')
            return
        outcome = s.case[4] if s.index == 0 else 'success'
        def step(label, phase, event=None, **kw):
            new_events = (event,) if isinstance(event, str) else (event or ())
            events = s.events + tuple((kind, s.index) for kind in new_events)
            return label, replace(s, phase=phase, events=events, **kw)
        if s.phase == 'validate':
            if s.case[0] == 'foreign' and self.fault != 'read_skip_scope':
                yield step('RefuseForeign', 'next')
            elif s.attempts == 0: yield step('ExhaustedAttempts', 'next')
            else: yield step('DispatchPlan', 'plan', 'plan', attempts=s.attempts-1)
        elif s.phase == 'plan':
            if outcome == 'missing' and self.fault == 'read_forged_alias':
                yield step('ForgeRecordFromMissingAuthority', 'next', 'alias')
            elif outcome == 'missing':
                yield step('MissingAuthority', 'next', attempts=s.attempts +
                    int(self.fault == 'read_refund_attempt'))
            elif self.fault == 'read_early_hydrate':
                yield step('HydrateBeforeGrant', 'next', 'hydrate')
            else: yield step('SealedPlanRequirements', 'grant')
        elif s.phase == 'grant':
            if s.reads < 2 or s.body < 5:
                yield step('RefuseAtomicGrant', 'next',
                    reads=s.reads-int(self.fault == 'read_partial_reads' and s.reads >= 2)*2,
                    body=s.body-int(self.fault == 'read_partial_bytes' and s.body >= 5)*5)
            else: yield step('SpendAtomicGrant', 'root', 'grant', reads=s.reads-2, body=s.body-5)
        elif s.phase == 'root':
            if outcome == 'root': yield step('RefuseRootAfterWorkSpent', 'next')
            else: yield step('FundWholeFrame', 'hydrate', 'fund',
                credit=s.credit+(3 if self.fault == 'read_payload_only' else 9))
        elif s.phase == 'hydrate':
            yield step('StartPhysicalHydration', 'cancel' if outcome == 'cancel' else 'retire', 'hydrate')
        elif s.phase == 'cancel':
            yield step('CancelCallerPhysicalStillRunning', 'retire',
                ('cancel','release') if self.fault == 'read_early_root_release' else 'cancel',
                reads=s.reads + (2 if self.fault == 'read_refund_cancel' else 0),
                body=s.body + (5 if self.fault == 'read_refund_cancel' else 0),
                credit=s.credit-(9 if self.fault == 'read_early_root_release' else 0))
        elif s.phase == 'retire':
            if outcome == 'success':
                yield step('PhysicalRetiresAliasSurvives', 'next', ('retire', 'alias'))
            else: yield step('PhysicalRetiresWithoutRecord', 'next', ('retire', 'release'), credit=s.credit-9)
        elif s.phase == 'next':
            if s.index == 0:
                yield step('RetrySameLedger', 'validate', index=1,
                    semantic=8 if self.fault == 'read_reset_semantic' else s.semantic)
            else: yield step('FinishCalls', 'drop')
        elif s.phase == 'drop':
            aliases = [i for event, i in s.events if event == 'alias'
                       and ('release', i) not in s.events]
            if aliases and self.fault == 'read_skip_alias_drop':
                yield step('PrematureCompletionWithLiveAlias', 'done')
            elif aliases:
                i = aliases[0]
                yield 'DropRetainedAlias', replace(s, credit=s.credit-9,
                    events=s.events+(('drop_alias', i), ('release', i)))
            else: yield step('AllOwnersRetired', 'done')
    def violation(self, before, action, s):
        if not s.case: return None
        # Expected balances come only from immutable grants and actual historical
        # operations, never from remaining counters or transition action labels.
        # Validate ordered SAME-CALL prefixes before trusting events as ghost
        # accounting inputs. A fabricated alias or release cannot justify itself.
        prefixes = {0: set(), 1: set()}
        for event, i in s.events:
            if i not in prefixes: return 'R-PREFIX'
            seen = prefixes[i]
            outcome = s.case[4] if i == 0 else 'success'
            if event in seen: return 'R-PREFIX'
            required = {'plan': (), 'grant': ('plan',), 'fund': ('plan','grant'),
                'hydrate': ('plan','grant','fund'), 'cancel': ('hydrate',),
                'retire': ('hydrate',), 'alias': ('plan','grant','fund','hydrate','retire'),
                'drop_alias': ('alias',), 'release': ('retire',)}
            if event not in required: return 'R-PREFIX'
            if event == 'alias' and (outcome != 'success' or 'cancel' in seen
                    or not set(required[event]) <= seen): return 'R-AUTHORITY'
            if event == 'release' and ('retire' not in seen
                    or ('alias' in seen and 'drop_alias' not in seen)): return 'R-OWNER'
            if not set(required[event]) <= seen: return 'R-PREFIX'
            if event == 'cancel' and 'retire' in seen: return 'R-PREFIX'
            if event == 'alias' and 'release' in seen: return 'R-AUTHORITY'
            seen.add(event)
        plans = [i for event, i in s.events if event == 'plan']
        grants = [i for event, i in s.events if event == 'grant']
        if s.case[0] != 'same' and s.events: return 'R-SCOPE'
        if (s.attempts, s.reads, s.body) != (s.case[1]-len(plans),
                s.case[2]-2*len(grants), s.case[3]-5*len(grants)): return 'R-WORK'
        if min(s.attempts, s.reads, s.body) < 0: return 'R-WORK'
        for event, i in s.events:
            if event == 'hydrate' and (('grant', i) not in s.events
                    or ('fund', i) not in s.events): return 'R-PREFIX'
        funded = {i for event, i in s.events if event == 'fund'}
        released = {i for event, i in s.events if event == 'release'}
        if s.credit != 9*len(funded-released): return 'R-OWNER'
        if s.phase == 'done' and (s.index != 1 or s.credit != 0
                or funded-released): return 'R-OWNER'
        if s.semantic != 7: return 'R-SEMANTIC'
        for event, i in s.events:
            outcome = s.case[4] if i == 0 else 'success'
            if event == 'alias' and outcome != 'success': return 'R-AUTHORITY'
        if s.acknowledged: return 'R-ACK'

# Root-level coupled Genesis arbitration. SDK CAS and scoped immutable reads are
# assumptions; reply delivery is deliberately independent of committed history.
GENESIS_FACTS = ('genesis', 'sequence', 'pending', 'leased', 'completed', 'failed')
GENESIS_A = ('a', 'pin-a', 'schema', 'secret-a', GENESIS_FACTS)
GENESIS_B = ('b', 'pin-b', 'schema', 'secret-b', GENESIS_FACTS)
GENESIS_LEGACY = ('b', 'pin-b', 'schema', 'secret-b', ())
@dataclass(frozen=True)
class ProvisionCase:
    initial: str = 'empty'
    accepted: str = 'a'
    reply: str = 'stored'
    advance: str = 'none'
    corrupt: str = 'none'
    incoming_schema: str = 'schema'
    incoming_pin: str = 'pin-a'
    known: bool = False
    capacity: int = 2
    preflight: bool = True
    retries: int = 0
    retry_winner: bool = False

PROVISION_CASES = {
    'own_stored': ProvisionCase(),
    'own_lost_reply': ProvisionCase(reply='lost'),
    'competitor_wins_rejected_own': ProvisionCase(accepted='b', reply='rejected'),
    'advanced_capture': ProvisionCase(initial='covered', advance='before'),
    'append_during_adoption': ProvisionCase(initial='covered', advance='during'),
    'equivalent_encoding_other_secret': ProvisionCase(initial='covered'),
    'exact_adoption_only': ProvisionCase(initial='covered', capacity=1),
    'preflight_refusal': ProvisionCase(capacity=0),
    'foreign_job': ProvisionCase(preflight=False),
    'known_missing': ProvisionCase(initial='missing', known=True),
    'known_empty': ProvisionCase(known=True),
    'legacy_nonempty': ProvisionCase(initial='legacy'),
    'post_submit_empty': ProvisionCase(accepted='none', reply='lost'),
    'delayed_commit_after_empty': ProvisionCase(accepted='delayed', reply='lost'),
    'work_spent_before_unknown': ProvisionCase(capacity=1),
    'bounded_empty_retries': ProvisionCase(retries=7),
    'winner_during_acquisition_retry': ProvisionCase(accepted='b',reply='none',capacity=1,retries=1,retry_winner=True),
    'valid_schema_conflict': ProvisionCase(initial='covered', incoming_schema='different'),
    **{'corrupt_'+kind: ProvisionCase(initial='covered', corrupt=kind,
        incoming_schema='different') for kind in ('missing', 'span', 'pin', 'cut', 'key')},
}
PROVISION_FAULTS = {
    'genesis_bare': 'GP-COUPLED', 'genesis_promote_legacy': 'GP-INITIAL',
    'genesis_post_submit_retry': 'GP-REPLACE', 'genesis_reset_ledger': 'GP-WORK',
    'genesis_skip_cut': 'GP-PROOF', 'genesis_skip_span': 'GP-PROOF',
    'genesis_skip_pin': 'GP-PROOF', 'genesis_skip_key': 'GP-PROOF',
    'genesis_candidate_secret': 'GP-WINNER', 'genesis_conflict_before_proof': 'GP-ORDER',
    'genesis_reject_advanced': 'GP-OUTCOME', 'genesis_compare_incoming_pin': 'GP-OUTCOME',
    'genesis_require_write_allowance': 'GP-OUTCOME', 'genesis_skip_preflight': 'GP-ADMISSION',
    'genesis_drop_original': 'GP-ORIGINAL',
    'genesis_reset_known': 'GP-INITIAL', 'genesis_clear_latch': 'GP-ORIGINAL',
    'genesis_ignore_retry_winner': 'GP-INITIAL', 'genesis_reuse_empty': 'GP-BOUND',
    'genesis_fake_return': 'GP-ORIGINAL', 'genesis_refence_winner': 'GP-INITIAL',
}
def provision_initial(case):
    return (GENESIS_B,) if case.initial == 'covered' else ((GENESIS_LEGACY,) if case.initial == 'legacy' else ())
def provision_expected(case):
    # Independent fixture history: never infer authority from reply or planner flags.
    if not case.preflight or case.capacity == 0: return 'unknown', ()
    if case.initial == 'legacy' or (case.known and case.initial in ('empty','missing')):
        return 'unknown', ()
    if case.initial != 'covered' and not case.retry_winner and (case.accepted in ('none','delayed') or case.capacity < 2):
        return 'unknown', ()
    if case.corrupt != 'none': return 'unknown', ()
    winner = GENESIS_B if case.initial == 'covered' or case.retry_winner or case.accepted == 'b' else GENESIS_A
    if case.incoming_schema != winner[2]: return 'conflict', ()
    return 'success', winner

@dataclass(frozen=True)
class ProvisionState:
    case: str = ''
    phase: str = 'choose'
    history: tuple = ()
    snapshot: tuple = ()
    events: tuple = ()
    remaining: int = 0
    retry_left: int = 0
    fences: int = 0
    possible_submit: bool = False
    original_live: bool = False
    point_cut: str = 'original'
    record_cut: str = 'original'
    full_key: bool = True
    full_span: bool = True
    stored_pin: str = ''
    proof: bool = False
    result: str = ''
    returned: tuple = ()

class CoupledGenesisProvision(Model):
    name = 'api007-coupled-genesis-provision'
    initial = ProvisionState()
    properties = sorted(set(PROVISION_FAULTS.values()) | {'GP-PREFIX', 'GP-BOUND'})
    bounds = {'candidate_identities':2, 'genesis_slots':1, 'later_appends':1,
        'namespace_families':1, 'acquisition_attempts':8, 'cases':len(PROVISION_CASES),
        'frontier':'classified return after immutable winner adoption or explicit refusal'}
    assumptions = ['SDK conditional publication is atomic; exact scoped immutable reads are authentic',
        'actual committed history and reply delivery are independent fixture inputs',
        'one original namespace family; opaque cuts can have equal numeric heads',
        'one work dimension abstracts nonrefunding shared grants; separate record-read model covers reads/bytes',
        'six fact names abstract canonical compiler correspondence, not actual decoding/hash arithmetic',
        'one later append occurs before capture OR during adoption; concurrent actor scheduling is bounded',
        'finite abstract Root arbitration only; no Rust refinement, provider proof, heap/RSS or performance proof']
    def __init__(self, fault=None):
        if fault is not None and fault not in PROVISION_FAULTS: raise ValueError('unknown Genesis fault')
        self.fault = fault
    def frontier(self, s): return s.phase == 'done'
    def transitions(self, s):
        if s.phase == 'choose':
            for name, case in PROVISION_CASES.items():
                yield 'SelectGenesis-'+name, replace(s, case=name, history=provision_initial(case),
                    remaining=case.capacity, retry_left=case.retries, phase='preflight')
            return
        c = PROVISION_CASES[s.case]
        def step(label, phase, event=None, **kw):
            return label, replace(s, phase=phase, events=s.events+((event,) if event else ()), **kw)
        def done(label, result='unknown', **kw): return step(label,'done',result=result,**kw)
        if s.phase == 'preflight':
            if self.fault == 'genesis_skip_preflight': yield step('OpenWithoutAdmission','observe','open')
            elif not c.preflight or c.capacity == 0: yield done('RejectPreflight')
            elif self.fault == 'genesis_require_write_allowance' and c.capacity < 2:
                yield done('RequireUnneededWriteAllowance')
            else: yield step('ValidatePreflight','open','preflight')
        elif s.phase == 'open': yield step('OpenSameNamespace','observe','open')
        elif s.phase == 'observe':
            if c.initial == 'legacy':
                if self.fault == 'genesis_promote_legacy':
                    yield step('PromoteLegacy','capture','promote',history=(GENESIS_B,))
                else: yield done('RefuseNonemptyUninitialized')
            elif s.history:
                if self.fault == 'genesis_refence_winner':
                    yield 'RefenceExistingWinner',replace(s,phase='capture',fences=s.fences+1,
                        events=s.events+('empty','acquire'))
                else: yield step('ObserveExistingWinner','capture')
            elif c.known and self.fault != 'genesis_reset_known': yield done('RefuseKnownMissingOrEmpty')
            elif self.fault == 'genesis_reuse_empty' and c.retries:
                yield 'CacheEmptyTwice',replace(s,phase='acquire',events=s.events+('empty','empty'))
            else: yield step('AuthenticateEmpty','acquire','empty')
        elif s.phase == 'acquire':
            yield step('SingleAcquireAttempt','retry' if s.retry_left else 'prepare','acquire',fences=s.fences+1)
        elif s.phase == 'retry':
            if c.retry_winner:
                yield step('FreshAuthenticatedWinner',
                    'acquire' if self.fault == 'genesis_ignore_retry_winner' else 'capture',
                    'observe-winner',history=(GENESIS_B,),retry_left=0)
            elif self.fault == 'genesis_reuse_empty':
                yield step('ReusePriorEmptyObservation','acquire',retry_left=s.retry_left-1)
            else: yield step('FreshAuthenticatedEmpty','acquire','empty',retry_left=s.retry_left-1)
        elif s.phase == 'prepare':
            yield step('CompileCoupledOriginal','submit','grant-write',remaining=s.remaining-1,original_live=True)
        elif s.phase == 'submit':
            yield step('LatchPossibleSubmission','cas','possible',possible_submit=True)
        elif s.phase == 'cas':
            history = ()
            if c.accepted in ('a','b'):
                winner = GENESIS_A if c.accepted == 'a' else GENESIS_B
                history = (winner if self.fault != 'genesis_bare' else (*winner[:4],()),)
            yield step('ActualConditionalOutcome','reply','cas',history=history)
        elif s.phase == 'reply':
            label, after = step('DeliverReply-'+c.reply,'capture','reply-'+c.reply,
                original_live=self.fault not in ('genesis_drop_original','genesis_clear_latch'),
                possible_submit=self.fault != 'genesis_clear_latch')
            if self.fault == 'genesis_fake_return':
                after = replace(after,original_live=False,events=after.events+('return-unknown',))
            yield label, after
        elif s.phase == 'capture':
            if self.fault == 'genesis_post_submit_retry' and s.possible_submit:
                yield step('UnsafeReplacementFence','capture','acquire',fences=s.fences+1)
            else:
                history = s.history+(('later',),) if s.history and c.advance == 'before' else s.history
                yield step('CaptureOneImmutableCut','read','capture',history=history,snapshot=history,
                    remaining=c.capacity if self.fault == 'genesis_reset_ledger' else s.remaining)
        elif s.phase == 'read':
            if not s.snapshot:
                if c.accepted == 'delayed':
                    yield step('ReturnUnknownBeforePhysicalSettlement','settling','return-unknown',
                        result='unknown',original_live=False)
                else: yield done('EmptyAfterPossibleSubmit')
            elif s.remaining == 0: yield done('ReadWorkExhaustedAfterStoredGenesis')
            else: yield step('GrantAdoptionOnly','fact','grant-read',remaining=s.remaining-1,
                history=s.history+(('later',),) if c.advance == 'during' else s.history)
        elif s.phase == 'settling':
            winner = GENESIS_A if self.fault != 'genesis_bare' else (*GENESIS_A[:4],())
            yield done('PhysicalStorageAfterReturnedUnknown',history=(winner,),event='late-storage')
        elif s.phase == 'fact':
            if c.corrupt == 'missing': yield done('CoveredGenesisFactMissing')
            else: yield step('ReadBoundGenesisPoint','record','point',point_cut='foreign' if c.corrupt == 'cut' else 'original',
                full_key=c.corrupt != 'key',full_span=c.corrupt != 'span',
                stored_pin='bad-pin' if c.corrupt == 'pin' else s.snapshot[0][1])
        elif s.phase == 'record': yield step('HydrateOriginalOffsetZero','validate','record')
        elif s.phase == 'validate':
            if self.fault == 'genesis_conflict_before_proof' and c.incoming_schema != s.snapshot[0][2]:
                yield done('ConflictBeforeStoredProof','conflict')
            elif ((s.point_cut != s.record_cut and self.fault != 'genesis_skip_cut')
                  or (not s.full_key and self.fault != 'genesis_skip_key')
                  or (not s.full_span and self.fault != 'genesis_skip_span')
                  or (s.stored_pin != s.snapshot[0][1] and self.fault != 'genesis_skip_pin')):
                yield done('RejectInvalidStoredCorrespondence')
            else: yield step('VerifyFullStoredGenesis','schema','verified',proof=True)
        elif s.phase == 'schema':
            if self.fault == 'genesis_reject_advanced' and len(s.history)>1: yield done('RejectAdvancedWinner')
            elif (c.incoming_schema != s.snapshot[0][2] or
                (self.fault == 'genesis_compare_incoming_pin' and c.incoming_pin != s.snapshot[0][1])):
                yield done('ValidatedSchemaConflict','conflict')
            else: yield done('ReturnOriginalWinner','success',returned=GENESIS_A if self.fault == 'genesis_candidate_secret' else s.snapshot[0])
    def violation(self, before, action, s):
        if not s.case: return None
        c = PROVISION_CASES[s.case]
        if s.events and 'preflight' not in s.events: return 'GP-ADMISSION'
        if 'open' in s.events and (not c.preflight or c.capacity == 0): return 'GP-ADMISSION'
        if s.remaining != c.capacity-s.events.count('grant-write')-s.events.count('grant-read') or s.remaining<0:
            return 'GP-WORK'
        initial = provision_initial(c)
        if initial and s.history[:len(initial)] != initial: return 'GP-INITIAL'
        if initial and (s.fences or any(event in s.events
                for event in ('empty','grant-write','possible','cas'))): return 'GP-INITIAL'
        if sum(entry[0] in ('a','b') for entry in s.history)>1: return 'GP-INITIAL'
        if c.known and c.initial in ('missing','empty') and (s.history != initial
                or s.fences or any(event in s.events for event in ('grant-write','possible','cas'))): return 'GP-INITIAL'
        if c.retry_winner and s.history and (s.fences>1
                or any(event in s.events for event in ('grant-write','possible','cas'))): return 'GP-INITIAL'
        if any(entry[0] in ('a','b') and entry != GENESIS_LEGACY and entry[4] != GENESIS_FACTS for entry in s.history):
            return 'GP-COUPLED'
        # A new legacy-shaped own record is invalid even if return is Unknown.
        if not initial and any(entry[0] in ('a','b') and entry[4] != GENESIS_FACTS for entry in s.history):
            return 'GP-COUPLED'
        if 'possible' in s.events and any(event in ('acquire','grant-write')
                for event in s.events[s.events.index('possible')+1:]): return 'GP-REPLACE'
        if s.fences != s.events.count('acquire') or s.fences>8 or s.fences>s.events.count('empty'): return 'GP-BOUND'
        if s.possible_submit != ('possible' in s.events): return 'GP-ORIGINAL'
        if 'return-unknown' in s.events:
            after_return = s.events[s.events.index('return-unknown')+1:]
            if (c.accepted != 'delayed' or s.result != 'unknown' or s.returned or s.snapshot
                or s.phase not in ('settling','done') or 'capture' not in s.events
                or any(event != 'late-storage' for event in after_return)
                or (s.phase == 'settling' and (s.history or after_return))
                or (s.phase == 'done' and after_return != ('late-storage',))): return 'GP-ORIGINAL'
        if 'possible' in s.events and s.phase != 'done' and 'return-unknown' not in s.events and not s.original_live:
            return 'GP-ORIGINAL'
        seen = set()
        empties = acquisitions = 0
        fresh_empty = False
        prerequisites = {'open':('preflight',), 'empty':('open',),
            'grant-write':('acquire',), 'possible':('grant-write',), 'cas':('possible',),
            'capture':('open',), 'grant-read':('capture',), 'point':('grant-read',),
            'record':('point',), 'verified':('record',), 'return-unknown':('capture',), 'late-storage':('return-unknown',)}
        for event in s.events:
            if event == 'empty':
                empties += 1
                fresh_empty = True
            if event == 'acquire':
                acquisitions += 1
                if not fresh_empty or acquisitions > empties: return 'GP-BOUND'
                fresh_empty = False
            if event in prerequisites and not set(prerequisites[event]) <= seen: return 'GP-PREFIX'
            if event not in ('empty','acquire') and event in seen: return 'GP-PREFIX'
            seen.add(event)
        if s.history and s.history[0][0] == 'a' and not initial and any(
                event not in s.events for event in ('grant-write','possible','cas')): return 'GP-PREFIX'
        if s.snapshot and s.snapshot != s.history[:len(s.snapshot)]: return 'GP-PREFIX'
        authentic = bool(s.snapshot and s.snapshot[0][4] == GENESIS_FACTS and c.corrupt == 'none'
            and s.point_cut == s.record_cut == 'original' and s.full_key and s.full_span
            and s.stored_pin == s.snapshot[0][1])
        if s.proof and not authentic: return 'GP-PROOF'
        required = ('preflight','open','capture','grant-read','point','record','verified')
        if s.result == 'conflict' and (not s.proof or not authentic or any(event not in s.events for event in required)):
            return 'GP-ORDER'
        if s.result == 'success':
            if not authentic or not s.proof or any(event not in s.events for event in required): return 'GP-PROOF'
            if s.returned != s.snapshot[0]: return 'GP-WINNER'
        if s.phase == 'done' and (s.result,s.returned) != provision_expected(c): return 'GP-OUTCOME'
