"""Independent finite API-007 publication specification, not Rust refinement.

The immutable ghost history judges observations; implementation guards obtain
history only through an available, validated index. Work/state/new-work/cache
receipt components are distinct. Cache faults cover original receipt presence;
row/index contents, lease semantics and physical provider guarantees are excluded.
"""
from dataclasses import dataclass, replace
from models import Model


class ModelBoundError(RuntimeError):
    pass


@dataclass(frozen=True)
class Attempt:
    actor: int
    identity: int
    body: int
    epoch: int
    head: int
    disposition: str = 'unsubmitted'
    reply: str = 'unobserved'


@dataclass(frozen=True)
class Caller:
    phase: str = 'idle'
    body: int = -1
    attempt: int = -1
    witness_epoch: int = -1
    witness_head: int = -1
    witness_present: bool = False
    resolution_count: int = 0
    reprepared: bool = False
    result: str = ''
    original_body: int = -1
    replay_kind: str = ''
    replay_mask: int = 0


@dataclass(frozen=True)
class State:
    epoch: int = 1
    history: tuple = ()  # identity, canonical-body symbol, logical attempt
    attempts: tuple = ()
    callers: tuple = (Caller(), Caller())
    catalog: bool = True
    index: bool = True
    genesis_valid: bool = True
    remembered: bool = True
    cache_genesis: bool = True
    up: bool = True
    background_fence: bool = False
    applied: int = 0
    work: tuple = ()
    workflow: tuple = ()
    new_work: tuple = ()
    receipts: tuple = ()
    staged: bool = False
    expired: tuple = ()
    acknowledged: tuple = ()
    served: bool = False
    restored: bool = False


CONFIGURATIONS = ('original-resolution', 'background-publication', 'authority-reopen')
FAULTS = {
    'early_ack': ('original-resolution', 'F-ACK'),
    'stale_read': ('background-publication', 'F-ACK'),
    'unfenced_publish': ('original-resolution', 'F-FENCE'),
    'cache_absence_reprepare': ('original-resolution', 'F-REPLAY'),
    'sdk_retry_conflict': ('original-resolution', 'F-FENCE'),
    'partial_apply': ('authority-reopen', 'F-GROUP'),
    'rejected_effects': ('original-resolution', 'F-GROUP'),
    'expired_reuse': ('original-resolution', 'F-REPLAY'),
    'replacement_authority': ('authority-reopen', 'F-PREFIX'),
}


class Publication(Model):
    properties = ['F-ACK', 'F-PREFIX', 'F-FENCE', 'F-REPLAY', 'F-GROUP']
    assumptions = [
        'indexed checked frames and original genesis are authentic; hash collision resistance',
        'atomic durable conditional epoch/head comparison; stored frames survive process loss',
        'PostgreSQL transaction and native projection transaction are atomic physical primitives',
        'body0 is a fixed valid disjoint group; body1 a fixed state-CAS rejection with zero effects',
        'only original-receipt cache presence faults; other cached contents require separate integrity qualification',
        'authority-reopen covers unavailable index/genesis refusal, not catalog/process/cache evidence arbitration',
        'one mutation caller; independent concurrent same-identity callers are excluded',
        'finite safety exploration, without fairness, deadline or production capacity claims',
    ]

    def __init__(self, configuration, fault=None):
        if configuration not in CONFIGURATIONS:
            raise ValueError('unknown API-007 model configuration')
        if fault is not None and fault not in FAULTS:
            raise ValueError('unknown API-007 fault')
        if fault is not None and FAULTS[fault][0] != configuration:
            raise ValueError('fault does not belong to configuration')
        self.configuration = configuration
        self.fault = fault
        self.name = 'api007-' + configuration
        self.bounds = {'writers': 1 if configuration == 'authority-reopen' else 2,
                       'callers': 1, 'fixed_background_publishers': 1 if configuration=='background-publication' else 0,
                       'identities': 2 if configuration == 'background-publication' else 1,
                       'canonical_bodies': 2, 'work_keys':4,'workflow_keys':2, 'fresh_bodies': [0,1] if configuration=='original-resolution' else [0], 'epoch': 16, 'publication_slots_excluding_genesis': 2,
                       'attempt_slots': 3, 'resolutions_per_invocation': 3,
                       'repreparations_per_caller': 1,
                       'frontier': 'close new inputs; accepted storage/apply/resolution remains enabled'}
        self.properties = (['F-ACK','F-FENCE','F-REPLAY','F-GROUP'] if configuration=='original-resolution' else ['F-ACK','F-PREFIX','F-FENCE','F-GROUP'] if configuration=='background-publication' else ['F-ACK','F-PREFIX','F-GROUP'])
        self.initial = State()

    def identity(self, actor):
        return actor if self.configuration == 'background-publication' else 0

    @staticmethod
    def caller(s, actor, caller):
        values = list(s.callers)
        values[actor] = caller
        return replace(s, callers=tuple(values), served=False)

    @staticmethod
    def attempt(s, number, attempt):
        values = list(s.attempts)
        values[number] = attempt
        return replace(s, attempts=tuple(values), served=False)

    @staticmethod
    def indexed(s):
        return s.up and s.index and s.genesis_valid

    @staticmethod
    def expected(history):
        return tuple((identity, body) for identity, body, _ in history)

    @staticmethod
    def effects(history):
        return tuple((identity,body) for identity,body,_ in history if body==0)

    def known(self, s, identity):
        # This guard never consults the ghost when authority is unavailable.
        if not self.indexed(s):
            return None
        return next((body for key, body, _ in s.history if key == identity), None)

    def transitions(self, s):
        if not s.up:
            # Catalog or retained cache evidence survives this bounded reopen.
            yield 'Reopen', replace(s, up=True, remembered=s.catalog or s.cache_genesis, staged=False)
            return
        writers = 2 if self.configuration=='background-publication' else 1
        if self.configuration == 'original-resolution' and not s.background_fence and s.epoch < 4:
            yield 'BackgroundFence', replace(s, epoch=s.epoch + 1, background_fence=True, served=False)
        for actor in range(writers):
            c = s.callers[actor]
            identity = self.identity(actor)
            if actor==0 and c.phase == 'idle' and self.indexed(s) and s.applied==len(s.history) and len(s.attempts) < 2:
                for body in (range(2) if self.configuration=='original-resolution' else (0,)):
                    if s.epoch >= self.bounds['epoch'] - 3:
                        continue  # NEW inputs only; existing work is never disabled.
                    cached=next((value for key,value in s.receipts if key==identity),None)
                    if (cached is not None)!=(self.known(s,identity) is not None):
                        yield f'UnknownInitialCacheMismatch:{actor}:{body}',self.caller(s,actor,replace(c,phase='unknown',body=body,result='unknown'))
                        continue
                    if self.known(s,identity) is not None:
                        yield f'StartOriginalReplay:{actor}:{body}',self.caller(s,actor,replace(c,phase='resolve',body=body))
                        continue
                    number = len(s.attempts)
                    attempt = Attempt(actor, identity, body, s.epoch + 1, len(s.history))
                    target = replace(s, epoch=s.epoch + 1, attempts=s.attempts + (attempt,))
                    yield f'Prepare:{actor}:{body}', self.caller(target, actor, replace(c, phase='prepared', body=body, original_body=body, attempt=number))
            if self.configuration=='background-publication' and actor==1 and c.phase=='idle' and self.indexed(s) and len(s.attempts)<2:
                number=len(s.attempts)
                attempt=Attempt(actor,identity,0,s.epoch,len(s.history))
                yield 'PrepareGrantedEpoch:1',self.caller(replace(s,attempts=s.attempts+(attempt,)),actor,replace(c,phase='prepared',body=0,attempt=number))
            if c.phase == 'prepared':
                yield f'Dispatch:{actor}', self.caller(self.attempt(s, c.attempt, replace(s.attempts[c.attempt], disposition='pending')), actor, replace(c, phase='waiting'))
                if self.fault == 'early_ack':
                    yield f'EarlyAck:{actor}', replace(self.caller(s, actor, replace(c, phase='acked', result='success')), acknowledged=s.acknowledged + ((identity, c.body),))
            if c.phase == 'waiting':
                attempt = s.attempts[c.attempt]
                if actor==1 and self.configuration=='background-publication':
                    if attempt.disposition=='stored':
                        yield 'BackgroundStoredReply',self.caller(self.attempt(s,c.attempt,replace(attempt,reply='stored')),actor,replace(c,phase='background_done',result='native_stored'))
                    elif attempt.disposition=='conflict':
                        yield 'BackgroundConflictReply',self.caller(self.attempt(s,c.attempt,replace(attempt,reply='conflict')),actor,replace(c,phase='background_done',result='native_conflict'))
                    continue
                if attempt.disposition == 'stored':
                    yield f'ReceiveStored:{actor}', self.caller(self.attempt(s, c.attempt, replace(attempt, reply='stored')), actor, replace(c, phase='resolve'))
                    yield f'LoseStoredReply:{actor}', self.caller(self.attempt(s, c.attempt, replace(attempt, reply='unknown')), actor, replace(c, phase='resolve'))
                elif attempt.disposition == 'conflict':
                    yield f'ReceiveConflict:{actor}', self.caller(self.attempt(s, c.attempt, replace(attempt, reply='conflict')), actor, replace(c, phase='resolve'))
                else:
                    yield f'TimeoutBeforeStorage:{actor}', self.caller(self.attempt(s, c.attempt, replace(attempt, reply='unknown')), actor, replace(c, phase='resolve'))
            if c.phase == 'resolve':
                if not self.indexed(s):
                    yield f'UnknownAuthority:{actor}', self.caller(s, actor, replace(c, phase='unknown', result='unknown'))
                elif c.resolution_count >= 3:
                    yield f'ResolutionUnknown:{actor}', self.caller(s, actor, replace(c, phase='unknown', result='unknown'))
                else:
                    if s.epoch >= self.bounds['epoch']:
                        raise ModelBoundError('accepted resolution exceeds declared epoch domain')
                    yield f'AcquireWitness:{actor}', self.caller(replace(s, epoch=s.epoch + 1), actor, replace(c, phase='witness', witness_epoch=s.epoch + 1, witness_head=len(s.history), witness_present=self.known(s, identity) is not None, resolution_count=c.resolution_count + 1))
            if c.phase == 'witness' and self.indexed(s) and s.applied >= c.witness_head:
                if len(s.history) != c.witness_head:
                    yield f'DiscardStaleCut:{actor}', self.caller(s, actor, replace(c, phase='resolve'))
                else:
                    cached = next((body for key, body in s.receipts if key == identity), None)
                    present = c.witness_present
                    if (cached is not None) != present and not (self.fault == 'cache_absence_reprepare' and present and cached is None):
                        yield f'CacheHistoryUnknown:{actor}', self.caller(s, actor, replace(c, phase='unknown', result='unknown'))
                    elif cached is not None:
                        result = 'conflict' if cached != c.body else 'expired' if identity in s.expired else 'replayed' if c.replay_kind else 'success'
                        phase = 'acked' if result in ('success', 'replayed') else 'denied'
                        target = self.caller(s, actor, replace(c, phase=phase, result=result))
                        if result in ('success', 'replayed'):
                            target = replace(target, acknowledged=s.acknowledged + ((identity, c.body),))
                        yield f'Resolve:{actor}:{result}', target
                    elif len(s.attempts) < 3 and not c.reprepared:
                        old = s.attempts[c.attempt]
                        if (not present and c.witness_epoch > old.epoch) or self.fault == 'cache_absence_reprepare':
                            number = len(s.attempts)
                            new = Attempt(actor, identity, c.body, c.witness_epoch, len(s.history))
                            yield f'Reprepare:{actor}', self.caller(replace(s, attempts=s.attempts + (new,)), actor, replace(c, phase='prepared', attempt=number, reprepared=True))
                    else:
                        yield f'ContentionUnknown:{actor}', self.caller(s, actor, replace(c, phase='unknown', result='unknown'))
            if c.phase in ('acked', 'denied') and self.configuration=='original-resolution':
                for kind, bit in (('equal', 1), ('changed', 2), ('expired', 4)):
                    if c.replay_mask & bit or (kind == 'expired' and identity not in s.expired):
                        continue
                    body = 1 - c.original_body if kind == 'changed' else c.original_body
                    yield f'Replay{kind.title()}:{actor}', self.caller(s, actor, replace(
                        c, phase='resolve', body=body, result='', replay_kind=kind,
                        replay_mask=c.replay_mask | bit, resolution_count=0,
                        witness_epoch=-1, witness_head=-1, witness_present=False))
                if identity not in s.expired:
                    yield f'ExplicitRetentionCleanup:{actor}', replace(s, expired=s.expired + (identity,), served=False)
                elif self.fault == 'expired_reuse' and len(s.attempts) < 3:
                    new = Attempt(actor, identity, c.body, s.epoch, len(s.history))
                    yield f'ReuseExpired:{actor}', self.caller(replace(s, attempts=s.attempts + (new,)), actor, replace(c, phase='prepared', attempt=len(s.attempts)))
        for number, attempt in enumerate(s.attempts):
            if attempt.disposition == 'pending' and self.indexed(s):
                allowed = attempt.epoch == s.epoch and attempt.head == len(s.history)
                if allowed or self.fault == 'unfenced_publish':
                    if len(s.history) >= self.bounds['publication_slots_excluding_genesis']:
                        raise ModelBoundError('accepted storage exceeds declared publication domain')
                    target = self.attempt(s, number, replace(attempt, disposition='stored'))
                    yield f'CASStore:{number}', replace(target, history=s.history + ((attempt.identity, attempt.body, number),))
                else:
                    yield f'CASReject:{number}', self.attempt(s, number, replace(attempt, disposition='conflict'))
            if self.fault == 'sdk_retry_conflict' and attempt.disposition == 'stored' and attempt.reply == 'unknown':
                yield f'SDKRetryConflict:{number}', self.attempt(s, number, replace(attempt, reply='conflict'))
        if self.indexed(s) and s.applied < len(s.history):
            if not s.staged:
                yield 'BeginApply', replace(s, staged=True, served=False)
            else:
                end = s.applied + 1
                prefix = self.effects(s.history[:end])
                receipts = self.expected(s.history[:end])
                if self.fault == 'partial_apply':
                    yield 'CommitPartialApply', replace(s, applied=end, work=prefix, staged=False, served=False)
                else:
                    work=receipts if self.fault=='rejected_effects' else prefix
                    yield 'CommitApply', replace(s, applied=end, work=work, workflow=prefix, new_work=prefix, receipts=receipts, staged=False, served=False)
        expected = self.expected(s.history)
        if self.indexed(s) and s.applied == len(s.history) and s.receipts == expected:
            yield 'ServeFresh', replace(s, served=True)
        elif self.fault == 'stale_read' and s.acknowledged:
            yield 'ServeStale', replace(s, served=True)
        if self.configuration=='background-publication' and s.work:
            yield 'ResetProjection',replace(s,applied=0,work=(),workflow=(),new_work=(),receipts=(),staged=False,served=False)
        if self.configuration == 'original-resolution':
            if s.receipts:
                yield 'LoseCachedOriginal', replace(s, receipts=(), served=False)
            elif not s.history:
                yield 'PhantomCachedOriginal',replace(s,receipts=((0,0),),served=False)
        if self.configuration == 'authority-reopen':
            if s.staged and (s.catalog or s.cache_genesis):
                callers=tuple(replace(c,phase='resolve',witness_epoch=-1,witness_head=-1,witness_present=False) if c.phase in ('prepared','waiting','resolve','witness') else c for c in s.callers)
                attempts=tuple(replace(a,disposition='aborted',reply='unknown') if a.disposition in ('pending', 'unsubmitted') else a for a in s.attempts)
                yield 'CrashDuringApply', replace(s, up=False, callers=callers,attempts=attempts,staged=False, served=False)
            if s.index:
                yield 'LoseIndex', replace(s, index=False, served=False)
            if s.catalog:
                yield 'LoseCatalog', replace(s, catalog=False, served=False)
            if s.genesis_valid:
                yield 'CorruptGenesis', replace(s, genesis_valid=False, served=False)
            if not self.indexed(s) and self.fault == 'replacement_authority':
                yield 'ReplaceKnownAuthority', replace(s, index=True, genesis_valid=True, epoch=1, history=(), restored=True, served=False)
            # No cold all-witness-loss inference: restore authentic physical
            # authority without inventing a replacement or changing ghost H.
            if not s.index or not s.genesis_valid:
                yield 'RestoreAuthenticAuthority', replace(s, index=True, genesis_valid=True, served=False)

    def frontier(self, s):
        # Only admission of a new original input closes at these resource bounds.
        return (s.callers[0].phase == 'idle' and
                (len(s.attempts) >= 2 or s.epoch >= self.bounds['epoch'] - 3))

    def violation(self, before, action, s):
        actual = self.expected(s.history)
        if any(item not in actual for item in s.acknowledged):
            return 'F-ACK'
        if s.served and any(next((i+1 for i,frame in enumerate(s.history) if frame[:2]==item),999)>s.applied for item in s.acknowledged):
            return 'F-ACK'
        if s.restored or len(s.history) < len(before.history):
            return 'F-PREFIX'
        if action.startswith('CASStore:'):
            attempt = before.attempts[int(action.split(':')[1])]
            if attempt.epoch != before.epoch or attempt.head != len(before.history):
                return 'F-FENCE'
        if any(a.reply == 'conflict' and a.disposition == 'stored' for a in s.attempts):
            # Native refinement: a definite Conflict cannot follow a stored
            # subattempt of this logical CAS, even if upper layers reconcile.
            return 'F-FENCE'
        if action.startswith('Resolve:'):
            actor = int(action.split(':')[1])
            caller = s.callers[actor]
            identity = self.identity(actor)
            original = next((body for key, body, _ in before.history if key == identity), None)
            cached = next((body for key, body in before.receipts if key == identity), None)
            expected = ('unknown' if original is None or cached != original else
                        'conflict' if caller.body != original else
                        'expired' if identity in before.expired else
                        'replayed' if caller.replay_kind else 'success')
            if caller.result != expected:
                return 'F-REPLAY'
        identities = [identity for identity, _, _ in s.history]
        if len(identities) != len(set(identities)):
            return 'F-REPLAY'
        if action in ('CommitApply', 'CommitPartialApply'):
            prefix = self.effects(s.history[:s.applied])
            if any(component != prefix for component in (s.work, s.workflow, s.new_work)) or s.receipts!=self.expected(s.history[:s.applied]):
                return 'F-GROUP'
        if s.served and (s.work != self.effects(s.history) or s.workflow != self.effects(s.history) or s.new_work != self.effects(s.history) or s.receipts != actual):
            return 'F-PREFIX'
        return None
