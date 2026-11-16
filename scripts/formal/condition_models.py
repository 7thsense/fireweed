"""Independent bounded positive Check predicates, atomic cuts and original replay.

Lease validity refers to fresh Tlease sampled after reading the metadata cut;
time passage after preparation does not resample that invocation's decision.
No negative-claim proof, Snorri classifier or Rust refinement is asserted.
"""
from dataclasses import dataclass, replace
from models import Model

CONFIGURATIONS = tuple((mode, state) for mode in ('checks-only', 'mixed')
                       for state in ('present', 'absent', 'tombstoned'))
FAULTS = {
    'omit_state': 'C-PREDICATE', 'omit_claim': 'C-PREDICATE',
    'check_version': 'C-NO-MUTATION', 'check_sequence': 'C-NO-MUTATION',
    'partial_rejection': 'C-ATOMIC', 'stale_cut': 'C-CUT',
    'missing_receipt': 'C-RECEIPT', 'reevaluate_replay': 'C-REPLAY',
}


@dataclass(frozen=True)
class State:
    epoch: int = 1
    head: int = 0
    clock: int = 0
    work_version: int = 1
    token: int = 0
    state_present: bool = True
    state_hwm: int = 1
    business_version: int = 1
    business_value: int = 0
    new_rows: int = 0
    sequence: int = 0
    environmental_steps: int = 0
    invocation: int = 0
    phase: str = 'idle'
    body: int = 0
    cut: tuple = ()
    lease_time: int = -1
    planned: str = ''
    receipt: tuple = ()  # full body, outcome, exact modified versions
    observations: tuple = ()


class Conditions(Model):
    properties = ['C-PREDICATE', 'C-CUT', 'C-NO-MUTATION', 'C-ATOMIC', 'C-RECEIPT', 'C-REPLAY']
    assumptions = [
        'one group identity, two sequential invocations; canonical body symbols are injective',
        'checked work is leased with expiry1; version1 and token0 are the issued credential',
        'positive state Version(1) or Absent only; exact expiry and wrong token/version modeled',
        'mixed group stages a disjoint business state update before its final state Check; new work follows it',
        'fresh Tlease sampled after metadata cut; later clock passage alone does not fence that cut',
        'durable atomic epoch/head CAS and immutable receipt storage are physical assumptions',
        'state delete/recreate preserves a monotonic per-key version high-water mark',
        'finite safety only; no fairness, deadlines, storage capacity or provider durability proof',
    ]

    def __init__(self, configuration, fault=None):
        if configuration not in CONFIGURATIONS or (fault is not None and fault not in FAULTS):
            raise ValueError('unknown condition configuration or fault')
        self.mode, self.state_kind = configuration
        self.name = 'api007-conditions-' + '-'.join(configuration)
        self.fault = fault
        self.expected_state = 1 if self.state_kind == 'present' else 0
        self.initial = State(state_present=self.state_kind == 'present',
                             state_hwm=0 if self.state_kind == 'absent' else 1)
        self.bounds = {'group_identities': 1, 'invocations': 2, 'canonical_bodies': 2,
                       'environmental_mutations': 2, 'epoch': 2,
                       'state_version_hwm': 3, 'work_version': 3,
                       'clock': [0, 1], 'lease_expiry': 1,
                       'check_keys': 2, 'mutation_keys': 2,
                       'frontier': 'environmental mutation domain OR invocation domain exhausted; pending protocol remains enabled'}

    def frontier(self, state):
        return state.environmental_steps == 2 or state.invocation == 2

    @staticmethod
    def metadata(s):
        return (s.epoch, s.head, s.work_version, s.token, s.state_present, s.state_hwm)

    def decision(self, cut, lease_time, fault=None):
        _, _, work_version, token, present, hwm = cut
        if fault != 'omit_claim':
            if token != 0 or lease_time >= 1:
                return 'stale-lease'
            if work_version != 1:
                return 'version-conflict'
        if fault != 'omit_state':
            if self.expected_state == 0 and present:
                return 'already-exists'
            if self.expected_state != 0:
                if not present:
                    return 'not-found'
                if hwm != self.expected_state:
                    return 'version-conflict'
        return 'committed'

    def expected_outcome(self, cut, lease_time):
        # Independent ordered specification cases. Do not call the transition
        # planner or accept its fault parameter: lease validity precedes work
        # version, which precedes state presence/version, including tombstones.
        if cut[3] != 0:
            return 'stale-lease'
        if lease_time == 1:
            return 'stale-lease'
        if cut[2] != 1:
            return 'version-conflict'
        if self.expected_state == 0:
            return 'already-exists' if cut[4] else 'committed'
        if not cut[4]:
            return 'not-found'
        return 'committed' if cut[5] == self.expected_state else 'version-conflict'

    def transitions(self, s):
        if s.environmental_steps < 2:
            changes = []
            if s.clock == 0:
                changes.append(('ExactExpiry', dict(clock=1)))
            if s.epoch == 1:
                changes.append(('Fence', dict(epoch=2)))
            if s.token == 0:
                changes.append(('WrongToken', dict(token=1, head=s.head + 1)))
            if s.work_version < 3:
                changes.append(('AdvanceWorkVersion', dict(work_version=s.work_version + 1, head=s.head + 1)))
            if s.state_present and s.state_hwm < 3:
                changes.append(('DeleteState', dict(state_present=False, state_hwm=s.state_hwm + 1, head=s.head + 1)))
                changes.append(('AdvanceStateVersion', dict(state_hwm=s.state_hwm + 1, head=s.head + 1)))
            if not s.state_present and s.state_hwm < 3:
                changes.append(('RecreateState', dict(state_present=True, state_hwm=s.state_hwm + 1, head=s.head + 1)))
            for label, fields in changes:
                yield label, replace(s, environmental_steps=s.environmental_steps + 1, **fields)
        if s.phase == 'idle' and s.invocation < 2:
            for body in ([0] if s.invocation == 0 else [0, 1]):
                yield 'InvokeEqual' if body == 0 else 'InvokeChanged', replace(
                    s, phase='resolve' if s.receipt else 'read', body=body,
                    cut=(), lease_time=-1, planned='')
        elif s.phase == 'read':
            yield 'ReadCut', replace(s, phase='sample', cut=self.metadata(s))
        elif s.phase == 'sample':
            yield 'SampleFreshTlease', replace(s, phase='publish', lease_time=s.clock,
                                               planned=self.decision(s.cut, s.clock, self.fault))
        elif s.phase == 'publish':
            if s.cut[:2] != (s.epoch, s.head) and self.fault != 'stale_cut':
                yield 'DiscardStaleCut', replace(s, phase='read', cut=(), planned='', lease_time=-1)
            else:
                accepted = s.planned == 'committed'
                mutate = accepted and self.mode == 'mixed'
                leak = (not accepted and self.fault == 'partial_rejection'
                        and s.cut[2:4] == (1, 0) and s.lease_time < 1)
                versions = ((s.business_version + 1, 1) if mutate else ())
                receipt = (s.body, s.planned, versions)
                fields = dict(head=s.head + 1, phase='resolve', receipt=receipt)
                if mutate:
                    fields.update(business_version=s.business_version + 1,
                                  business_value=s.business_value + 1,
                                  new_rows=s.new_rows + 1, sequence=s.sequence + 1)
                elif leak:
                    fields.update(business_version=s.business_version + 1,
                                  business_value=s.business_value + 1)
                if self.fault == 'check_version' and accepted:
                    fields['work_version'] = s.work_version + 1
                if self.fault == 'check_sequence' and accepted:
                    fields['sequence'] = s.sequence + 1
                if self.fault == 'missing_receipt' and accepted and self.mode == 'checks-only':
                    fields['receipt'] = ()
                yield 'PublishReceipt', replace(s, **fields)
        elif s.phase == 'resolve' and s.receipt:
            result = s.receipt[1] if s.body == s.receipt[0] else 'request-conflict'
            if self.fault == 'reevaluate_replay' and s.body == s.receipt[0]:
                result = self.decision(self.metadata(s), s.clock)
            yield 'ResolveOriginal', replace(s, invocation=s.invocation + 1, phase='idle',
                                             observations=s.observations + ((s.body, result),))

    def violation(self, before, action, after):
        if action == 'PublishReceipt':
            if before.cut[:2] != (before.epoch, before.head):
                return 'C-CUT'
            expected = self.expected_outcome(before.cut, before.lease_time)
            if before.planned != expected:
                return 'C-PREDICATE'
            if not after.receipt or after.receipt[:2] != (before.body, expected):
                return 'C-RECEIPT'
            if (after.work_version, after.token, after.state_present, after.state_hwm) != (
                    before.work_version, before.token, before.state_present, before.state_hwm):
                return 'C-NO-MUTATION'
            effects = (after.business_version - before.business_version,
                       after.business_value - before.business_value,
                       after.new_rows - before.new_rows, after.sequence - before.sequence)
            wanted = (1, 1, 1, 1) if expected == 'committed' and self.mode == 'mixed' else (0, 0, 0, 0)
            if effects != wanted:
                return 'C-ATOMIC' if expected != 'committed' else 'C-NO-MUTATION'
            if after.head != before.head + 1:
                return 'C-RECEIPT'
            wanted_versions = (before.business_version + 1, 1) if wanted[0] else ()
            if after.receipt[2] != wanted_versions:
                return 'C-NO-MUTATION'
        if before.receipt and after.receipt != before.receipt:
            return 'C-REPLAY'
        if action == 'ResolveOriginal':
            expected = before.receipt[1] if before.body == before.receipt[0] else 'request-conflict'
            if after.observations[-1] != (before.body, expected):
                return 'C-REPLAY'
        if after.state_hwm < before.state_hwm or (action == 'RecreateState' and after.state_hwm <= before.state_hwm):
            return 'C-PREDICATE'
        return None
