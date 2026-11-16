"""Independent executable specifications; never import production queue code."""
from dataclasses import dataclass, replace


class Model:
    obligations = ()
    fair_labels = frozenset()
    assumptions = []

    def suffix(self, state, action):
        return True

    def frontier(self, state):
        return False


@dataclass(frozen=True)
class RecoveryState:
    epoch: int = 0
    cp_epoch: int = 0
    owner: int = -1
    pending: bool = False
    fenced: bool = False
    sessions: tuple = (0, 0)
    jobs: tuple = ((0, 0), (0, 0))  # phase, cached fence
    log: tuple = ()  # command ID, fence
    published: int = 0  # independent durable-publication ghost history
    ack: int = 0
    applied: tuple = ()
    hint: int = 0
    up: bool = True
    uploaded: int = 0


class Recovery(Model):
    name = 'recovery-ownership'
    bounds = {'owners': 2, 'commands': 2, 'storage_epoch': 2,
              'cp_epoch': 2, 'hint_period': 2, 'frontier': 'close input; apply remains enabled'}
    properties = ['F-ACK', 'F-PREFIX', 'F-FENCE', 'F-EVENTUAL']
    assumptions = ['atomic conditional authoritative publication; uploaded bytes alone are not committed',
                   'liveness suffix has no further crashes; available nonpoisoned store',
                   'weak fairness of Apply; per published command obligation']
    initial = RecoveryState()
    fair_labels = frozenset({'Apply'})

    def __init__(self, fault=None):
        self.fault = fault
        self.obligations = tuple((f'command-{i}',
                                  lambda s, i=i: bool(s.published & (1 << i)) and i not in s.applied)
                                 for i in range(2))

    def suffix(self, state, action):
        return state.up and action not in ('Crash', 'Reopen')

    def frontier(self, s):
        return s.epoch == 2 or all(phase >= 2 for phase, _ in s.jobs)

    def transitions(self, s):
        if not s.up:
            applied = tuple(req for req, _ in s.log[:s.hint]) if self.fault == 'hint_authority' else ()
            log = s.log[:s.hint] if self.fault == 'hint_authority' else s.log
            yield 'Reopen', replace(s, up=True, log=log, applied=applied)
            return
        if s.owner >= 0 and not s.pending:
            yield 'ExpireOwner', replace(s, owner=-1)
        if s.owner == -1 and s.epoch < 2:
            for owner in range(2):
                yield f'Acquire:{owner}', replace(s, owner=owner, cp_epoch=s.cp_epoch + 1,
                                                  pending=True, fenced=False)
        if s.pending and not s.fenced:
            yield 'Fence', replace(s, epoch=s.epoch + 1, fenced=True)
        if s.pending and s.fenced:
            sessions = list(s.sessions)
            sessions[s.owner] = s.epoch
            yield 'Confirm', replace(s, pending=False, sessions=tuple(sessions))
        if s.owner >= 0 and not s.pending:
            yield 'Reaffirm', s  # same-owner reacquire must not self-fence
        for req, (phase, credential) in enumerate(s.jobs):
            jobs = list(s.jobs)
            if phase == 0:
                for owner, cached in enumerate(s.sessions):
                    if cached and s.owner == owner and not s.pending:
                        jobs[req] = (1, cached)
                        yield f'Submit:{req}', replace(s, jobs=tuple(jobs))
            elif phase == 1:
                if not s.uploaded & (1 << req):
                    yield f'UploadSegment:{req}', replace(s, uploaded=s.uploaded | (1 << req))
                yield f'CancelCaller:{req}', s  # dispatcher, not caller, owns the job
                if s.uploaded & (1 << req) and (credential == s.epoch or self.fault == 'stale_owner'):
                    jobs[req] = (2, credential)
                    log = s.log + ((req, credential),)
                    hint = len(log) if len(log) == 1 else s.hint
                    yield f'Publish:{req}', replace(s, jobs=tuple(jobs), log=log,
                                                    published=s.published | (1 << req), hint=hint)
                else:
                    jobs[req] = (0, 0)
                    yield f'RejectFence:{req}', replace(s, jobs=tuple(jobs))
                if self.fault == 'early_ack':
                    yield f'Ack:{req}', replace(s, ack=s.ack | (1 << req))
            elif phase == 2:
                jobs[req] = (3, credential)
                yield f'Ack:{req}', replace(s, jobs=tuple(jobs), ack=s.ack | (1 << req))
                yield f'LostResponse:{req}', s  # retry resolves the durable receipt
            elif phase == 3:
                yield f'Replay:{req}', s
        if len(s.applied) < len(s.log):
            applied = s.applied if self.fault == 'stalled_apply' else s.applied + (s.log[len(s.applied)][0],)
            yield 'Apply', replace(s, applied=applied)
        jobs = tuple((0, 0) if phase == 1 else (phase, epoch) for phase, epoch in s.jobs)
        yield 'Crash', replace(s, up=False, owner=-1, cp_epoch=0, pending=False,
                              fenced=False, sessions=(0, 0), jobs=jobs, applied=())

    def violation(self, before, action, s):
        committed = sum(1 << req for req, _ in s.log)
        if s.ack & ~committed:
            return 'F-ACK'
        if committed != s.published or s.applied != tuple(req for req, _ in s.log[:len(s.applied)]):
            return 'F-PREFIX'
        if action.startswith('Publish:') and s.log[-1][1] != before.epoch:
            return 'F-FENCE'
        if s.epoch < before.epoch or (action == 'Reaffirm' and s.epoch != before.epoch):
            return 'F-FENCE'
        if action.startswith('Submit:') and before.pending:
            return 'F-FENCE'
        return None


@dataclass(frozen=True)
class LifecycleState:
    now: int = 0
    token: int = 0
    expiry: int = 0
    completed: bool = False
    # receipt fingerprints and dedup item key IDs: 2 namespace scopes × 2 request IDs
    receipts: tuple = (-1, -1, -1, -1)
    items: tuple = ((), ())
    key_deadline: tuple = (0, 0)
    receipt_deadline: tuple = (0, 0, 0, 0)
    claim_history: tuple = ()  # request-bound lease generations retained after replacement
    claim_receipt: tuple = (0, 0)  # token, initial expiry (renewal resolves current expiry)
    unauthorized: bool = False
    duplicated: bool = False


class Lifecycle(Model):
    name = 'leases-receipts-item-keys'
    bounds = {'time': 3, 'lease_generations': 2, 'scopes': 2, 'requests_per_scope': 2,
              'logical_item_keys_per_scope': 1, 'receipt_retention': 2, 'key_retention': 3,
              'frontier': 'time/generation safety only; no liveness claim'}
    properties = ['F-LEASE', 'F-REPLAY', 'F-KEY']
    assumptions = ['two disjoint tenant/queue namespaces; operation fingerprints modeled as two values',
                   'single item lease; item-key conflicts normalized to accepted/duplicate',
                   'claim receipt active replay uses renewed current lease; expiry permits request-expired',
                   'cohort-key/member-count convergence is an independent production obligation']
    initial = LifecycleState()

    def __init__(self, fault=None, scopes=1, requests=2):
        self.fault, self.scopes, self.requests = fault, scopes, requests
        self.initial = LifecycleState(receipts=(-1,) * (scopes * requests),
            receipt_deadline=(0,) * (scopes * requests), items=((),) * scopes,
            key_deadline=(0,) * scopes)
        self.bounds = dict(self.bounds, scopes=scopes, requests_per_scope=requests)
        if scopes > 1:
            self.name = 'namespace-receipts'

    def frontier(self, s):
        return s.now == 3 or s.token == 2

    def transitions(self, s):
        if s.now < 3:
            yield 'Tick', replace(s, now=s.now + 1)
        if not s.completed and (not s.token or s.expiry <= s.now) and s.token < 2:
            new = s.token + 1
            yield 'Claim', replace(s, token=new, expiry=s.now + 1, claim_receipt=(new, s.now + 1), claim_history=s.claim_history + (new,))
        if s.token:
            for offered in range(1, 3):
                valid = offered == s.token and s.now < s.expiry and not s.completed
                if valid:
                    yield f'Finalize:{offered}', replace(s, completed=True)
                    if s.expiry < 4:
                        yield f'Renew:{offered}', replace(s, expiry=s.expiry + 1)
                elif self.fault == 'stale_lease' and offered != s.token and not s.completed:
                    yield f'Finalize:{offered}', replace(s, completed=True, unauthorized=True)
                else:
                    yield f'RejectToken:{offered}', s
            if s.now < s.expiry and not s.completed:
                yield 'ReplayClaim:active', s
            else:
                yield 'ReplayClaim:request-expired', s
        for original_token in s.claim_history:
            if original_token != s.token:
                disposition = 'active' if self.fault == 'old_receipt_rebind' else 'request-expired'
                yield f'ReplayOldClaim:{original_token}:{disposition}', s
        for scope in range(self.scopes):
            for req in range(self.requests):
                pos = scope * self.requests + req
                lookup = req if self.fault == 'scope_alias' else pos
                for body in range(2):
                    old = s.receipts[lookup]
                    retained = old >= 0 and s.now < s.receipt_deadline[lookup]
                    if retained:
                        if old != body:
                            yield f'Conflict:{scope}:{req}:{body}', s
                            continue
                        if self.fault != 'duplicate_request':
                            yield f'ReplayPush:{scope}:{req}:{body}', s
                            continue
                    receipts, deadlines = list(s.receipts), list(s.receipt_deadline)
                    receipts[pos], deadlines[pos] = body, s.now + 2
                    items, key_deadline = list(s.items), list(s.key_deadline)
                    key_retained = bool(items[scope]) and s.now < key_deadline[scope]
                    duplicated = s.duplicated
                    if not key_retained or self.fault in ('duplicate_key', 'duplicate_request'):
                        items[scope] = items[scope] + (len(items[scope]) + 1,)
                        if key_retained:
                            duplicated = True
                        key_deadline[scope] = s.now + 3
                    yield f'Push:{scope}:{req}:{body}', replace(s, receipts=tuple(receipts),
                          receipt_deadline=tuple(deadlines), items=tuple(items),
                          key_deadline=tuple(key_deadline), duplicated=duplicated)

    def violation(self, before, action, s):
        if action.startswith(('Finalize:', 'Renew:')):
            offered = int(action.split(':')[1])
            if offered != before.token or before.now >= before.expiry or before.completed:
                return 'F-LEASE'
        if action.startswith('Push:'):
            _, scope, req, body = action.split(':')
            pos = int(scope) * self.requests + int(req)
            if before.receipts[pos] >= 0 and before.now < before.receipt_deadline[pos]:
                if before.receipts[pos] == int(body) and s.items != before.items:
                    return 'F-REPLAY'
                if before.receipts[pos] != int(body):
                    return 'F-REPLAY'
        if action.startswith(('ReplayPush:', 'Conflict:')):
            kind, scope, req, body = action.split(':')
            pos = int(scope) * self.requests + int(req)
            retained = before.receipts[pos] >= 0 and before.now < before.receipt_deadline[pos]
            if not retained or ((kind == 'ReplayPush') != (before.receipts[pos] == int(body))):
                return 'F-REPLAY'
        for scope in range(self.scopes):
            if (before.items[scope] and before.now < before.key_deadline[scope]
                    and s.items[scope] != before.items[scope]):
                return 'F-KEY'
        if action.startswith('ReplayOldClaim:'):
            _, offered, disposition = action.split(':')
            valid = int(offered) == before.token and before.now < before.expiry and not before.completed
            if (disposition == 'active') != valid:
                return 'F-REPLAY'
        if action == 'ReplayClaim:active' and not (s.claim_receipt[0] == s.token and s.now < s.expiry):
            return 'F-REPLAY'
        return None


@dataclass(frozen=True)
class BatchState:
    phases: tuple = (0, 0)  # queued, planned, published, responded
    selection: tuple = ((), ())
    order: tuple = ()
    log: tuple = ()
    applied: int = 0
    completed: tuple = ()
    outcomes: tuple = ((), ())
    tokens: tuple = (0, 0)
    changed: bool = False


class Batching(Model):
    properties = ['F-BATCH', 'F-PREFIX', 'F-LEASE']
    assumptions = ['reserved/logged-unapplied selections excluded from later selection',
                   'two requests with one invalid addressed entry each; valid entries best effort',
                   'cohort fixture selects a whole indivisible unit; no partial cohort success',
                   'serial witness uses explicit applied cursor, not read-after-ack visibility']
    initial = BatchState()

    def __init__(self, fault=None, cohort=False):
        self.fault, self.cohort = fault, cohort
        self.name = 'batching-cohort' if cohort else 'batching-items'
        self.units = ((0, 1), (2, 3)) if cohort else ((0,), (1,))
        self.bounds = {'requests': 2, 'claim_units': 2, 'items': 4 if cohort else 2,
                       'selection_mutations': 1, 'frontier': 'all requests planned; apply/drain still enabled'}

    def frontier(self, s):
        return all(s.phases)

    def available(self, s):
        reserved = set() if self.fault == 'overlap' else {i for selection in s.selection for i in selection}
        return [unit for unit in self.units if not any(i in reserved or i in s.completed for i in unit)]

    def transitions(self, s):
        for req, phase in enumerate(s.phases):
            phases = list(s.phases)
            if phase == 0:
                available = self.available(s)
                selected = available[-1 if s.changed else 0] if available else ()
                if self.fault == 'split_cohort' and selected:
                    selected = selected[:1]
                selection, outcomes, tokens = list(s.selection), list(s.outcomes), list(s.tokens)
                selection[req] = selected
                outcomes[req] = tuple((i, 'leased') for i in selected) + ((99, 'rejected'),)
                tokens[req] = req + 1 if self.fault != 'shared_token' else 1
                phases[req] = 1
                yield f'Plan:{req}', replace(s, phases=tuple(phases), selection=tuple(selection),
                      outcomes=tuple(outcomes), tokens=tuple(tokens), order=s.order + (req,))
            elif phase == 1:
                # Publication order follows the serial reservation order.
                if s.order[len(s.log)] == req:
                    phases[req] = 2
                    yield f'Publish:{req}', replace(s, phases=tuple(phases), log=s.log + (req,))
            elif phase == 2:
                phases[req] = 3
                yield f'Respond:{req}', replace(s, phases=tuple(phases))
                yield f'LoseResponse:{req}', s
            else:
                yield f'Replay:{req}', s
        if s.applied < len(s.log):
            yield 'Apply', replace(s, applied=s.applied + 1)
        # Selection-changing mutation is serialized with planning reservations.
        if not s.changed:
            yield 'ChangePriority', replace(s, changed=True)
        # Completing applied leases releases rows; this fixture uses no new claims
        # after completion to keep its serial witness focused on first claim packs.
        if s.applied and not s.completed and all(s.phases):
            yield 'CompleteApplied', replace(s, completed=s.selection[s.log[0]])
        visible = tuple(i for req in s.log[:s.applied] for i in s.selection[req])
        yield 'Poll:empty' if not visible else 'Poll:leased', s

    def violation(self, before, action, s):
        if len(s.selection[0]) + len(s.selection[1]) != len(set(s.selection[0] + s.selection[1])):
            return 'F-BATCH'
        if all(s.phases) and s.tokens[0] == s.tokens[1]:
            return 'F-BATCH'
        if self.cohort and any(sel and sel not in self.units for sel in s.selection):
            return 'F-BATCH'
        if action.startswith('Plan:'):
            req = int(action.split(':')[1])
            # Independent sequential reference: derive reserved units directly from
            # prior selections, not from the optimized available() implementation.
            used = set(before.selection[0] + before.selection[1] + before.completed)
            reference = [u for u in self.units if not any(i in used for i in u)]
            selected = reference[-1 if before.changed else 0] if reference else ()
            expected = tuple((i, 'leased') for i in selected) + ((99, 'rejected'),)
            if s.selection[req] != selected or s.outcomes[req] != expected:
                return 'F-BATCH'
        if s.applied > len(s.log):
            return 'F-PREFIX'
        return None


@dataclass(frozen=True)
class AdmissionState:
    phases: tuple = (0, 0)  # absent, prepared/waiting, dispatched, CAS-unknown, resolved
    charged: tuple = (0, 0)
    cancelled: tuple = (False, False)
    closed: bool = False


class Admission(Model):
    name = 'admission-cancellation-ambiguity'
    bounds = {'jobs': 2, 'tenants': 2, 'queues': 2, 'global_bytes': 4,
              'tenant_bytes': 2, 'record_bytes': 1, 'seal_frame_bytes': 1,
              'running_slots': 1, 'waiting_slots': 2, 'frontier': 'shutdown closes input; owned jobs drain'}
    properties = ['F-BUDGET']
    assumptions = ['peak reservation = retained record + simultaneous seal-frame capacity',
                   'publication ambiguity retains charge until authoritative resolution',
                   'caller cancellation before submission frees preparation; after dispatch it does not']
    initial = AdmissionState()

    def __init__(self, fault=None):
        self.fault = fault

    def frontier(self, s):
        return s.closed or all(s.phases)

    def transitions(self, s):
        if not s.closed:
            yield 'CloseAdmission', replace(s, closed=True)
        for job, phase in enumerate(s.phases):
            phases, charge, cancelled = list(s.phases), list(s.charged), list(s.cancelled)
            if phase == 0 and not s.closed and sum(s.charged) + 2 <= 4:
                phases[job], charge[job] = 1, 2
                yield f'Prepare:{job}', replace(s, phases=tuple(phases), charged=tuple(charge))
            elif phase == 1:
                if (not s.closed or self.fault == 'dispatch_after_close') and (
                        not any(p in (2, 3) for p in s.phases) or self.fault == 'slot_overflow'):
                    phases[job] = 2
                    yield f'Dispatch:{job}', replace(s, phases=tuple(phases))
                phases[job], charge[job], cancelled[job] = 4, 0, True
                yield f'CancelPrepared:{job}', replace(s, phases=tuple(phases), charged=tuple(charge),
                                                       cancelled=tuple(cancelled))
            elif phase in (2, 3):
                if not s.cancelled[job]:
                    cancelled[job] = True
                    if self.fault == 'early_release':
                        charge[job] = 0
                    yield f'CancelOwned:{job}', replace(s, cancelled=tuple(cancelled), charged=tuple(charge))
                if phase == 2:
                    phases[job] = 3
                    yield f'CAS:effect-then-error:{job}', replace(s, phases=tuple(phases))
                else:
                    phases[job], charge[job] = 4, 0
                    yield f'ResolveAuthority:{job}', replace(s, phases=tuple(phases), charged=tuple(charge))

    def violation(self, before, action, s):
        if action.startswith('Dispatch:') and before.closed:
            return 'F-BUDGET'
        if any(c != (2 if p in (1, 2, 3) else 0) for p, c in zip(s.phases, s.charged)):
            return 'F-BUDGET'
        if sum(s.charged) > 4 or any(c > 2 for c in s.charged):
            return 'F-BUDGET'
        if sum(p in (2, 3) for p in s.phases) > 1:
            return 'F-BUDGET'
        return None


@dataclass(frozen=True)
class AgeState:
    now: int = 0
    age: int = 0
    # pending or leased; gate/not_before affect eligibility independently of owner
    leased_until: int = 0
    gate: bool = True
    owner: bool = True
    expected_age: int = 0
    gate_changes: int = 0
    owner_changes: int = 0


class EligibleAge(Model):
    name = 'eligible-age'
    bounds = {'time': 4, 'not_before': 1, 'gate_changes': 2, 'owner_changes': 2,
              'lease_duration': 1, 'frontier': 'time safety only; integral accrued on half-open ticks'}
    properties = ['F-DEADLINE:age-accounting']
    assumptions = ['logical eligibility independent of owner, projection lag and caller filter',
                   'ordinary item; recurrence age origin remains independent production obligation']
    initial = AgeState()

    def __init__(self, fault=None):
        self.fault = fault

    def eligible(self, s):
        return s.now >= 1 and s.gate and (s.now >= s.leased_until or self.fault == 'leased_counts_age')

    def frontier(self, s):
        return s.now == 4

    def transitions(self, s):
        if s.now < 4:
            increment = int(self.eligible(s))
            # Oracle independently integrates the interval [now, now+1): it
            # does not call the model implementation's eligibility predicate.
            reference_increment = int(s.gate and s.now >= 1 and not s.now < s.leased_until)
            age = s.age + increment
            if self.fault == 'owner_stops_age' and not s.owner:
                age = s.age
            if self.fault == 'expiry_resets_age' and s.leased_until and s.now + 1 == s.leased_until:
                age = 0
            yield 'Tick', replace(s, now=s.now + 1, age=age, expected_age=s.expected_age + reference_increment)
        if s.gate_changes < 2:
            yield 'ToggleGate', replace(s, gate=not s.gate, gate_changes=s.gate_changes + 1)
        if s.owner_changes < 2:
            yield 'ToggleOwner', replace(s, owner=not s.owner, owner_changes=s.owner_changes + 1)
        if self.eligible(s) and s.owner and s.now < 4:
            yield 'Claim', replace(s, leased_until=s.now + 1)
        yield 'PollFilteredEmpty', s
        yield 'ProjectionLag', s

    def violation(self, before, action, s):
        return 'F-DEADLINE:age-accounting' if s.age != s.expected_age else None


@dataclass(frozen=True)
class ServiceState:
    tick: int = 0
    done: tuple = ()
    ages: tuple = (0, 0)


class Deadline(Model):
    name = 'bounded-service-envelope'
    bounds = {'items': 2, 'initial_age': 0, 'new_arrivals': 0, 'service_slots_per_tick': 1,
              'progress_bound_ticks': 2, 'outage_ticks': 0, 'frontier': 'all items claimed'}
    properties = ['F-DEADLINE:fixture']
    assumptions = ['two initial eligible items; FIFO round robin; no arrivals/outage/group delay',
                   'tick has one mandatory service slot before age advancement',
                   'does not qualify production FR-12; service/load assumptions must be substantiated']
    initial = ServiceState()

    def __init__(self, fault=None):
        self.fault = fault

    def frontier(self, s):
        return len(s.done) == 2

    def transitions(self, s):
        if len(s.done) < 2 and s.tick < 3:
            done = s.done if self.fault == 'no_service' else s.done + (len(s.done),)
            ages = tuple(a + (i not in done) for i, a in enumerate(s.ages))
            yield 'ServiceTick', replace(s, tick=s.tick + 1, done=done, ages=ages)

    def violation(self, before, action, s):
        return 'F-DEADLINE:fixture' if any(a > 2 for a in s.ages) else None


@dataclass(frozen=True)
class AuthorityState:
    condition: str = 'unchecked'
    projection: tuple = (0, 1)
    serving: bool = False
    checked: bool = False


class RecoveryAuthority(Model):
    name = 'recovery-authority-loss'
    bounds = {'commands_in_projection': 2, 'authority_cases': ['valid', 'missing', 'corrupt'],
              'frontier': 'validated or fail-closed disposition'}
    properties = ['F-PREFIX:fail-closed']
    assumptions = ['physical loss/corruption is outside durable-publication assumption',
                   'remaining projection evidence must not be destructively discarded on failed validation']
    initial = AuthorityState()

    def __init__(self, fault=None):
        self.fault = fault

    def frontier(self, s):
        return s.checked

    def transitions(self, s):
        if s.condition == 'unchecked':
            for condition in self.bounds['authority_cases']:
                yield f'Inspect:{condition}', replace(s, condition=condition)
        elif not s.checked:
            if s.condition == 'valid':
                yield 'VerifyAndResume', replace(s, checked=True, serving=True)
            elif self.fault == 'destructive_rebuild':
                yield 'RebuildEmpty', replace(s, checked=True, serving=True, projection=())
            else:
                yield 'FailClosed', replace(s, checked=True)

    def violation(self, before, action, s):
        if s.condition in ('missing', 'corrupt') and (s.serving or s.projection != (0, 1)):
            return 'F-PREFIX:fail-closed'
        return None


def models():
    return [Recovery(), RecoveryAuthority(), Lifecycle(), Lifecycle(scopes=2, requests=1), Batching(), Batching(cohort=True), Admission(), EligibleAge(), Deadline()]


def negative_controls():
    return [(RecoveryAuthority('destructive_rebuild'), 'F-PREFIX:fail-closed'),
            (Recovery('early_ack'), 'F-ACK'), (Recovery('hint_authority'), 'F-PREFIX'),
            (Recovery('stale_owner'), 'F-FENCE'), (Recovery('stalled_apply'), 'F-EVENTUAL'),
            (Lifecycle('stale_lease'), 'F-LEASE'), (Lifecycle('duplicate_request'), 'F-REPLAY'),
            (Lifecycle('duplicate_key'), 'F-KEY'), (Lifecycle('scope_alias', scopes=2, requests=1), 'F-REPLAY'),
            (Lifecycle('old_receipt_rebind'), 'F-REPLAY'),
            (Batching('overlap'), 'F-BATCH'), (Batching('shared_token'), 'F-BATCH'),
            (Batching('split_cohort', cohort=True), 'F-BATCH'),
            (Admission('early_release'), 'F-BUDGET'),
            (Admission('dispatch_after_close'), 'F-BUDGET'), (Admission('slot_overflow'), 'F-BUDGET'),
            (EligibleAge('owner_stops_age'), 'F-DEADLINE:age-accounting'),
            (EligibleAge('expiry_resets_age'), 'F-DEADLINE:age-accounting'),
            (EligibleAge('leased_counts_age'), 'F-DEADLINE:age-accounting'),
            (Deadline('no_service'), 'F-DEADLINE:fixture')]
