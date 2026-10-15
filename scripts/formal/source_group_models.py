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
