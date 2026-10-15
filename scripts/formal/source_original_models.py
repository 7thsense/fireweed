"""Finite F composition from a valid E capsule and C owned-record/error port.

This does not model physical I/O, canonical encodings, hash implementations,
allocation arithmetic, Rust refinement, provider durability or performance.
"""
from dataclasses import dataclass, replace
from models import Model

@dataclass(frozen=True)
class OriginalCase:
    origin: str = 'A'
    domain: int = 2
    literal: str = 'r\0α '
    mode: str = 'replay'
    scope: bool = True
    limits: bool = True
    admission: bool = True
    semantic: int = 1
    records: int = 1
    point_ok: bool = True
    point_cut: str = 'original'
    point_key: bool = True
    present: bool = True
    fixed: str = 'valid'
    matches: bool = True
    record_ok: bool = True
    pin: bool = True
    member: bool = True
    span: str = 'exact'
    digest: bool = True
    decode: bool = True
    claim_binding: bool = True

ORIGINAL_CASES = {
    'commit': OriginalCase(),
    'insert': OriginalCase(domain=0),
    'claim': OriginalCase(domain=1),
    'lookup': OriginalCase(mode='lookup',matches=False),
    'equal_head_other_original': OriginalCase(origin='B'),
    'absence': OriginalCase(present=False,records=0),
    'conflict_no_record_capacity': OriginalCase(matches=False,records=0),
    'conflict_missing_record': OriginalCase(matches=False,record_ok=False),
    'foreign_scope': OriginalCase(scope=False),
    'invalid_limits': OriginalCase(limits=False),
    'blank_literal': OriginalCase(literal=' \t'),
    'invalid_domain': OriginalCase(domain=3),
    'identity_one_short': OriginalCase(admission=False),
    'point_work_zero': OriginalCase(semantic=0),
    'record_work_zero': OriginalCase(records=0),
    'point_error': OriginalCase(point_ok=False),
    'foreign_equal_head_point': OriginalCase(point_cut='foreign'),
    'wrong_full_key': OriginalCase(point_key=False),
    'foreign_absence': OriginalCase(present=False,point_cut='foreign'),
    **{name: OriginalCase(fixed=name,matches=False) for name in ('offset_zero','future_offset','bad_format','trailing')},
    'missing_record': OriginalCase(record_ok=False),
    'wrong_publication_pin': OriginalCase(pin=False),
    'wrong_claim_pin': OriginalCase(domain=1,pin=False),
    'wrong_receipt_identity': OriginalCase(member=False),
    'other_equal_outcome': OriginalCase(span='other_equal'),
    'valid_subspan': OriginalCase(span='subspan'),
    'wrong_digest': OriginalCase(digest=False),
    'invalid_outcome': OriginalCase(decode=False),
    'invalid_claim_binding': OriginalCase(domain=1,claim_binding=False),
}
ORIGINAL_FAULTS = {
    'original_accept_foreign_cut':'OR-CUT', 'original_accept_prefix_key':'OR-KEY',
    'original_skip_fixed':'OR-FIXED', 'original_hydrate_conflict':'OR-PRECEDENCE',
    'original_skip_pin':'OR-PIN', 'original_skip_member':'OR-MEMBER',
    'original_skip_span':'OR-SPAN', 'original_skip_digest':'OR-DIGEST',
    'original_skip_decode':'OR-DECODE', 'original_skip_claim_binding':'OR-CLAIM',
    'original_retarget_id':'OR-IDENTITY', 'original_replace_capsule':'OR-CUT',
    'original_replace_ledger':'OR-LEDGER', 'original_early_record_release':'OR-OWNER',
    'original_premature_result':'OR-PREFIX', 'original_refund_work':'OR-WORK',
    'original_rewind_work':'OR-WORK', 'original_invent_expiry':'OR-OUTCOME',
    'original_record_error_as_absence':'OR-OUTCOME', 'original_skip_admission':'OR-ADMISSION',
    'original_complete_without_drop':'OR-OWNER', 'original_accept_record_error':'OR-RECORD',
    'original_accept_point_error':'OR-SOURCE',
    'original_retire_success_as_error':'OR-PREFIX', 'original_conflict_without_authority':'OR-PREFIX',
}

@dataclass(frozen=True)
class OriginalState:
    case: str = ''
    phase: str = 'choose'
    events: tuple = ()
    active: str = ''
    ledger: str = 'input-ledger'
    semantic: int = 0
    records: int = 0
    point_cut: str = ''
    point_key: tuple = ()
    record_cut: str = ''
    external_capsule: bool = True
    capsule_borrow: bool = True
    identity_credit: bool = False
    record_live: bool = False
    result: str = ''
    output: tuple = ()
    returned: tuple = ()
    borrowed: bool = False

class SourceOriginalResolution(Model):
    name = 'api007-source-original-resolution'
    initial = OriginalState()
    properties = sorted(set(ORIGINAL_FAULTS.values()) | {'OR-CAPSULE'})
    bounds = {'capsules':2,'point_reads':1,'record_reads':1,'cases':len(ORIGINAL_CASES),
              'scope':'one F invocation; same-ledger retry is qualified by runtime and the independent C campaign'}
    assumptions = ['input E capsule is already valid and externally retained throughout',
        'A/B are distinct immutable captures with equal numeric heads; full domain/literal keys are distinct',
        'C supplies a checked owned record or error under the invoked capsule and shared ledger',
        'one abstract ticket per point/record attempt; C owns its separate physical read/byte accounting',
        'record definition pin is the actual embedded Publication/Claim-command pin, not a second FWQL field',
        'receipt membership, span, digest, structure and Claim command-to-lease binding are independent fixture facts',
        'no expiry fact or current lease validity/public claim response is inferred',
        'finite documentary composition only; no Rust refinement, physical I/O, heap/RSS or performance proof']
    def __init__(self,fault=None):
        if fault is not None and fault not in ORIGINAL_FAULTS: raise ValueError('unknown original resolution fault')
        self.fault=fault
    def frontier(self,s): return s.phase=='done'
    def transitions(self,s):
        if s.phase=='choose':
            for name,c in ORIGINAL_CASES.items():
                yield 'SelectOriginal-'+name,replace(s,case=name,phase='validate',active=c.origin,semantic=c.semantic,records=c.records)
            return
        c=ORIGINAL_CASES[s.case]
        def step(label,phase,event,**fields):
            return label,replace(s,phase=phase,events=s.events+(event,),**fields)
        def fail(label,event='return-unknown',result='unknown'):
            fields={'result':result,'identity_credit':False,'record_live':False,'capsule_borrow':False}
            if self.fault=='original_refund_work': fields.update(semantic=c.semantic,records=c.records)
            return step(label,'done',event,**fields)
        def output(kind):
            literal='replacement' if self.fault=='original_retarget_id' else c.literal
            bound=(kind,s.active,c.domain,literal,'input-ledger')
            return step('ReturnBound'+kind,'borrow','return-'+kind,result=kind,output=bound,returned=bound)
        if s.phase=='validate':
            if self.fault=='original_conflict_without_authority' and c.present and not c.matches:
                yield fail('ConflictWithoutEvidence',result='conflict')
                return
            valid=c.scope and c.limits and c.domain<=2 and bool(c.literal.strip())
            if not valid: yield fail('RefuseConstructor','validate')
            else: yield step('ValidateConstructor','admit','validate')
        elif s.phase=='admit':
            if not c.admission and self.fault!='original_skip_admission': yield fail('RefuseIdentityAdmission')
            elif self.fault=='original_premature_result': yield output('present')
            else: yield step('AdmitIdentity','point-grant','admit',identity_credit=True)
        elif s.phase=='point-grant':
            if s.semantic<1: yield fail('RefusePointWork')
            else: yield step('GrantPoint','point','point-grant',semantic=s.semantic-1,
                active=('B' if c.origin=='A' else 'A') if self.fault=='original_replace_capsule' else s.active,
                ledger='replacement' if self.fault=='original_replace_ledger' else s.ledger)
        elif s.phase=='point':
            if not c.point_ok and self.fault!='original_accept_point_error': yield fail('PointReadFailed','point-error')
            else: yield step('ObservePoint','point-verify','point',
                point_cut=('B' if c.origin=='A' else 'A') if c.point_cut=='foreign' else s.active,
                point_key=(c.domain,c.literal) if c.point_key else (c.domain,'prefix'))
        elif s.phase=='point-verify':
            if s.point_cut!=c.origin and self.fault!='original_accept_foreign_cut': yield fail('RefuseForeignPoint')
            elif s.point_key!=(c.domain,c.literal) and self.fault!='original_accept_prefix_key': yield fail('RefuseOtherKey')
            else: yield step('VerifyExactPoint','fixed','point-verified')
        elif s.phase=='fixed':
            if c.present and c.fixed!='valid' and self.fault!='original_skip_fixed': yield fail('RefuseMalformedFixedFact')
            elif self.fault=='original_rewind_work':
                yield 'RewindPointAndRefund',replace(s,phase='admit',events=('validate',),semantic=c.semantic,point_cut='',point_key=())
            else: yield step('VerifyFixedAuthority','classify','fixed')
        elif s.phase=='classify':
            if not c.present: yield output('absent')
            elif c.mode=='replay' and not c.matches and self.fault!='original_hydrate_conflict': yield fail('ReturnConflict','return-conflict','conflict')
            elif self.fault=='original_invent_expiry': yield fail('InventExpiry',result='expired')
            else: yield step('RequireRecord','record-grant','matching')
        elif s.phase=='record-grant':
            if s.records<1: yield fail('RefuseRecordWork')
            else: yield step('GrantRecord','record','record-grant',records=s.records-1,record_cut=s.active)
        elif s.phase=='record':
            if not c.record_ok and self.fault!='original_accept_record_error':
                if self.fault=='original_record_error_as_absence': yield output('absent')
                else: yield fail('RecordReadFailed','record-error')
            else: yield step('RetainCheckedRecord','pin','record-read',record_live=self.fault!='original_early_record_release')
        elif s.phase in ('pin','member','span','digest','decode','binding'):
            stage=s.phase
            valid={'pin':c.pin,'member':c.member,'span':c.span=='exact','digest':c.digest,'decode':c.decode,'binding':c.domain!=1 or c.claim_binding}[stage]
            fault={'pin':'original_skip_pin','member':'original_skip_member','span':'original_skip_span','digest':'original_skip_digest','decode':'original_skip_decode','binding':'original_skip_claim_binding'}[stage]
            if not valid and self.fault!=fault: yield fail('Refuse'+stage)
            else:
                successor={'pin':'member','member':'span','span':'digest','digest':'decode','decode':'binding','binding':'output'}[stage]
                yield step('Verify'+stage,successor,stage)
        elif s.phase=='output': yield output('present')
        elif s.phase=='borrow': yield step('ReadBorrowedOutcome','drop','borrow',borrowed=True)
        elif s.phase=='drop':
            if self.fault=='original_retire_success_as_error':
                yield step('SubstituteErrorForSuccessfulDrop','done','return-unknown',output=(),identity_credit=False,record_live=False,capsule_borrow=False)
            elif self.fault=='original_complete_without_drop':
                yield 'FinishWithoutDrop',replace(s,phase='done',output=(),identity_credit=False,record_live=False,capsule_borrow=False)
            else: yield step('DropFResult','done','drop',output=(),identity_credit=False,record_live=False,capsule_borrow=False)
    def violation(self,before,action,s):
        if before.case:
            if s.case!=before.case: return 'OR-PREFIX'
            if s.semantic>before.semantic or s.records>before.records: return 'OR-WORK'
            if s.events[:len(before.events)]!=before.events: return 'OR-PREFIX'
        if not s.case: return None
        c=ORIGINAL_CASES[s.case]
        if not s.external_capsule: return 'OR-CAPSULE'
        if s.active!=c.origin: return 'OR-CUT'
        if s.ledger!='input-ledger': return 'OR-LEDGER'
        if s.semantic!=c.semantic-s.events.count('point-grant') or s.records!=c.records-s.events.count('record-grant') or min(s.semantic,s.records)<0: return 'OR-WORK'
        requirements={'admit':('validate',),'point-grant':('admit',),'point':('point-grant',),
            'point-error':('point-grant',),'point-verified':('point',),'fixed':('point-verified',),
            'matching':('fixed',),'record-grant':('matching',),'record-read':('record-grant',),
            'record-error':('record-grant',),'pin':('record-read',),'member':('pin',),'span':('member',),
            'digest':('span',),'decode':('digest',),'binding':('decode',),'return-present':('binding',),
            'return-absent':('fixed',),'return-conflict':('fixed',),'borrow':(), 'drop':('borrow',),
            'return-unknown':(), 'validate':()}
        seen=set()
        terminal_events={'return-unknown','return-conflict','point-error','record-error','drop'}
        for event in s.events:
            if seen & terminal_events: return 'OR-PREFIX'
            if event in terminal_events-{'drop'} and seen & {'return-present','return-absent','borrow'}: return 'OR-PREFIX'
            if event not in requirements or event in seen or not set(requirements[event])<=seen: return 'OR-PREFIX'
            if event=='borrow' and not {'return-present','return-absent'}&seen: return 'OR-PREFIX'
            seen.add(event)
        for result,event in (('present','return-present'),('absent','return-absent'),('conflict','return-conflict')):
            if s.result==result and event not in seen: return 'OR-PREFIX'
            if event in seen and s.result!=result: return 'OR-OUTCOME'
        if seen & {'return-unknown','point-error','record-error'} and s.result!='unknown': return 'OR-OUTCOME'
        if 'drop' in seen and s.result not in ('present','absent'): return 'OR-OUTCOME'
        if 'admit' in seen and not c.admission: return 'OR-ADMISSION'
        if 'point-grant' in seen and not (c.scope and c.limits and c.domain<=2 and bool(c.literal.strip())): return 'OR-PREFIX'
        if ('point' in seen and not c.point_ok) or ('point-error' in seen and c.point_ok): return 'OR-SOURCE'
        if ('record-read' in seen and not c.record_ok) or ('record-error' in seen and c.record_ok): return 'OR-RECORD'
        if 'point' in seen:
            observed=('B' if c.origin=='A' else 'A') if c.point_cut=='foreign' else c.origin
            if s.point_cut!=observed: return 'OR-CUT'
            if s.point_key!=((c.domain,c.literal) if c.point_key else (c.domain,'prefix')): return 'OR-KEY'
        if 'point-verified' in seen:
            if not c.point_ok or s.point_cut!=c.origin: return 'OR-CUT'
            if s.point_key!=(c.domain,c.literal): return 'OR-KEY'
        if 'fixed' in seen and c.present and c.fixed!='valid': return 'OR-FIXED'
        if 'matching' in seen and (not c.present or (c.mode=='replay' and not c.matches)): return 'OR-PRECEDENCE'
        if 'return-absent' in seen and c.present: return 'OR-OUTCOME'
        if 'return-conflict' in seen and (not c.present or c.mode!='replay' or c.matches): return 'OR-PRECEDENCE'
        if 'record-grant' in seen and s.record_cut!=c.origin: return 'OR-CUT'
        for event,valid,prop in (('pin',c.pin,'OR-PIN'),('member',c.member,'OR-MEMBER'),('span',c.span=='exact','OR-SPAN'),('digest',c.digest,'OR-DIGEST'),('decode',c.decode,'OR-DECODE'),('binding',c.domain!=1 or c.claim_binding,'OR-CLAIM')):
            if event in seen and not valid: return prop
        done=s.phase=='done'
        invalid_constructor=not (c.scope and c.limits and c.domain<=2 and bool(c.literal.strip()))
        retired=bool(seen & terminal_events) or ('validate' in seen and invalid_constructor)
        if s.identity_credit!=('admit' in seen and not retired): return 'OR-OWNER'
        if s.record_live!=('record-read' in seen and not retired): return 'OR-OWNER'
        if s.capsule_borrow!= (not retired): return 'OR-OWNER'
        if done!=retired: return 'OR-PREFIX'
        valid_scope=c.scope and c.limits and c.domain<=2 and bool(c.literal.strip()) and c.admission and c.semantic>0
        expected='unknown'
        if valid_scope and c.point_ok and c.point_cut=='original' and c.point_key and (not c.present or c.fixed=='valid'):
            if not c.present: expected='absent'
            elif c.mode=='replay' and not c.matches: expected='conflict'
            elif c.records>0 and c.record_ok and c.pin and c.member and c.span=='exact' and c.digest and c.decode and (c.domain!=1 or c.claim_binding): expected='present'
        if s.result and s.result!=expected: return 'OR-OUTCOME'
        returned=(expected,c.origin,c.domain,c.literal,'input-ledger') if expected in ('absent','present') else ()
        returned_event=('return-present' in seen or 'return-absent' in seen)
        if (s.returned or s.output or s.result in ('present','absent')) and not returned_event: return 'OR-PREFIX'
        if returned_event and (not s.returned or (not done and s.output!=s.returned)): return 'OR-OWNER'
        if s.returned and s.returned!=returned: return 'OR-IDENTITY'
        if s.output and (s.output!=returned or not s.identity_credit or not s.capsule_borrow or (s.result=='present' and not s.record_live)): return 'OR-OWNER'
        if s.borrowed!=('borrow' in seen): return 'OR-PREFIX'
        if done and (s.result!=expected or s.output or s.returned!=returned or s.borrowed!=(expected in ('absent','present'))): return 'OR-OUTCOME'
