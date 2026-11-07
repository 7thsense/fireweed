"""Finite retained-owner queue path specification; no production imports.

One queue, one owner handle, one foreign writer, two caller identities, one
ambiguous publish, one crash, one clean shutdown, one apply failure and one read.
Writes publish conditionally and apply the same record to the projection; reads
capture the head once and serve from the projection. Ghost fields judge the
protocol: `tainted` marks a projection file no clean shutdown vouched for,
`inflight` is a publish whose outcome the handle cannot know, and `read_floor`
is the durable length a read must cover. Rust refinement is not claimed.
"""
from dataclasses import dataclass, replace
from models import Model

IDS = ('a', 'b')
FOREIGN = 'f'
FAULTS = {'ignore_epoch': 'O-FENCE', 'reuse_epoch': 'O-FENCE',
          'plan_over_unresolved': 'O-UNRESOLVED', 'same_fence_resolve': 'O-UNRESOLVED',
          'retain_fence_across_crash': 'O-UNRESOLVED',
          'skip_identity_lookup': 'O-UNIQUE',
          'ack_ambiguous': 'O-ACK', 'skip_head': 'O-ACK', 'skip_catch_up': 'O-ACK',
          'trust_without_sidecar': 'O-TRUST', 'keep_sidecar_on_open': 'O-TRUST',
          'serve_poisoned': 'O-TRUST', 'plan_on_poisoned': 'O-TRUST',
          'rebuild_from_position': 'O-TRUST', 'shutdown_seals_poison': 'O-TRUST',
          'apply_before_publish': 'O-PREFIX'}
ACKS = ('ApplyAndAck', 'ReplayOriginal', 'ResolvePresent', 'AckAmbiguous')
SERVES = ('ServeFast', 'ServeLocked')


@dataclass(frozen=True)
class State:
    log: tuple = ()  # durable record identities, offset is the index
    fence: int = 0  # newest durable fence
    foreign_epoch: int = 0  # fence held by the foreign writer
    sidecar: bool = False  # durable clean-shutdown marker beside the projection
    inflight: tuple = ()  # identity, epoch, offset of a publish still travelling
    phase: str = 'closed'
    held: int = 0  # owner handle's retained fence; 0 means none
    projection: tuple = ()  # applied records; its length is the retained position
    poisoned: bool = False
    tainted: bool = False
    operation: str = ''
    plan: tuple = ()  # identity, epoch, expected offset
    unresolved: str = ''
    acknowledged: tuple = ()
    read_phase: str = ''
    read_head: int = 0  # the one head this read captured
    read_floor: int = 0  # ghost durable length at capture; earlier acks lie below it
    reads: int = 0
    foreign_fences: int = 0
    foreign_appends: int = 0
    ambiguous: int = 0
    crashes: int = 0
    shutdowns: int = 0
    apply_failures: int = 0


class Owner(Model):
    name = 'api007-native-queue-owner'
    initial = State()
    bounds = {'queues': 1, 'owner_handles': 1, 'caller_identities': len(IDS),
              'foreign_fences': 1, 'foreign_appends': 1, 'ambiguous_publishes': 1,
              'crashes': 1, 'clean_shutdowns': 1, 'apply_failures': 1, 'reads': 1,
              'frontier': 'no protocol or environment action remains enabled'}
    properties = ['O-ACK', 'O-FENCE', 'O-UNRESOLVED', 'O-TRUST', 'O-UNIQUE', 'O-PREFIX']
    assumptions = ['the log is immutable; conditional publication checks epoch and exact offset atomically',
        'fencing always yields a strictly newer durable epoch; a head read is a linearizable length',
        'the per-queue mutex serializes writes, catch-up and locked reads; the fast read path holds no mutex',
        'position, poison and projection contents are read atomically when a read serves',
        'a crash discards the handle and never restores a deleted sidecar; sidecar writes and deletes are durable',
        'records apply atomically, so catch-up at a trusted position needs no repair',
        'the offset condition is redundant while fences are exclusive; only the epoch is mutated here',
        'no fairness, deadline, memory, provider durability or multi-queue claim']

    def __init__(self, fault=None):
        if fault is not None and fault not in FAULTS:
            raise ValueError('unknown owner fault')
        self.fault = fault

    def frontier(self, state):
        return next(iter(self.transitions(state)), None) is None

    def accepted(self, state, epoch, offset):
        return (epoch == state.fence or self.fault == 'ignore_epoch') and offset == len(state.log)

    def transitions(self, s):
        yield from self.environment(s)
        if s.phase == 'closed':
            yield from self.opening(s)
            return
        if s.crashes < 1:
            yield 'Crash', self.crashed(s)
        yield from self.reading(s)
        if s.phase == 'idle':
            yield from self.idle(s)
        elif s.phase == 'write':
            yield from self.writing(s)
        elif s.phase == 'catch-up':
            yield 'CatchUp', replace(s, projection=s.log, phase='write')
        elif s.phase == 'publish':
            yield from self.publishing(s)
        elif s.phase == 'apply':
            yield 'ApplyAndAck', replace(s, projection=s.projection + (s.operation,),
                                         acknowledged=s.acknowledged + (s.operation,),
                                         phase='idle', operation='', plan=())
            if s.apply_failures < 1:
                yield 'ApplyFails', replace(s, poisoned=True, tainted=True, phase='rebuild',
                                            apply_failures=s.apply_failures + 1)
        elif s.phase == 'rebuild':
            yield from self.rebuilding(s)
        elif s.phase == 'rebuild-catch-up':
            yield 'RebuildCatchUp', replace(s, projection=s.log, poisoned=False, phase='write', plan=())
        elif s.phase == 'ambiguous':
            yield from self.ambiguous(s)
        elif s.phase == 'resolve-catch-up':
            yield 'ResolveCatchUp', replace(s, projection=s.log, phase='resolve-lookup')
        elif s.phase == 'resolve-lookup':
            yield from self.resolving(s)

    def environment(self, s):
        if s.foreign_fences < 1:
            yield 'ForeignFence', replace(s, fence=s.fence + 1, foreign_epoch=s.fence + 1,
                                          foreign_fences=s.foreign_fences + 1)
        if s.foreign_epoch and s.foreign_appends < 1 and self.accepted(s, s.foreign_epoch, len(s.log)):
            yield 'ForeignAppend', replace(s, log=s.log + (FOREIGN,), foreign_appends=s.foreign_appends + 1)
        if s.inflight:
            identity, epoch, offset = s.inflight
            if self.accepted(s, epoch, offset):
                yield 'InFlightLands', replace(s, log=s.log + (identity,), inflight=())
            yield 'InFlightExpires', replace(s, inflight=())

    def opening(self, s):
        volatile = dict(phase='idle', poisoned=False)
        if s.sidecar:
            yield 'OpenTrusted', replace(s, sidecar=self.fault == 'keep_sidecar_on_open', **volatile)
        elif self.fault == 'trust_without_sidecar':
            yield 'OpenTrusted', replace(s, **volatile)
        else:
            yield 'OpenEmpty', replace(s, projection=(), tainted=False, **volatile)

    def crashed(self, s):
        held = s.held if self.fault == 'retain_fence_across_crash' else 0
        return replace(s, phase='closed', held=held, poisoned=False, tainted=True, operation='',
                       plan=(), unresolved='', read_phase='', crashes=s.crashes + 1)

    def idle(self, s):
        if s.read_phase != 'locked':
            if len(s.acknowledged) < len(IDS):
                identity = IDS[len(s.acknowledged)]
                yield 'BeginWrite:' + identity, replace(s, phase='write', operation=identity)
            if s.read_phase == '' and s.shutdowns < 1:
                yield 'Shutdown', replace(s, phase='closed', held=0, sidecar=True,
                                          shutdowns=s.shutdowns + 1)

    def writing(self, s):
        if not s.held:
            yield 'AcquireFence', replace(s, fence=s.fence + (self.fault != 'reuse_epoch'),
                                          held=s.fence + (self.fault != 'reuse_epoch'), phase='catch-up')
        elif s.operation in s.projection and self.fault != 'skip_identity_lookup':
            yield 'ReplayOriginal', replace(s, acknowledged=s.acknowledged + (s.operation,),
                                            phase='idle', operation='', plan=())
        else:
            yield from self.planning(s)

    def planning(self, s):
        plan = (s.operation, s.held, len(s.projection))
        projection = s.projection
        if self.fault == 'apply_before_publish':
            projection += (s.operation,)
        yield 'Plan', replace(s, plan=plan, projection=projection, phase='publish')

    def publishing(self, s):
        identity, epoch, offset = s.plan
        accepted = self.accepted(s, epoch, offset)
        if accepted:
            yield 'PublishAccepted', replace(s, log=s.log + (identity,), phase='apply')
        else:
            yield 'PublishRejected', replace(s, held=0, plan=(), phase='write')
        if s.ambiguous < 1:
            ambiguous = replace(s, unresolved=identity, phase='ambiguous', ambiguous=s.ambiguous + 1)
            if accepted:
                yield 'PublishLostReply', replace(ambiguous, log=s.log + (identity,))
            yield 'PublishInFlight', replace(ambiguous, inflight=s.plan)

    def rebuilding(self, s):
        if self.fault == 'rebuild_from_position':
            yield 'PoisonRebuild', replace(s, phase='rebuild-catch-up')
        else:
            yield 'PoisonRebuild', replace(s, projection=(), tainted=False, phase='rebuild-catch-up')
        if self.fault == 'shutdown_seals_poison' and s.read_phase == '' and s.shutdowns < 1:
            yield 'Shutdown', replace(s, phase='closed', held=0, poisoned=False, operation='',
                                      plan=(), sidecar=True, shutdowns=s.shutdowns + 1)
        if self.fault == 'plan_on_poisoned':
            yield from self.planning(s)

    def ambiguous(self, s):
        if self.fault == 'plan_over_unresolved':
            yield from self.planning(s)
        if self.fault == 'ack_ambiguous':
            yield 'AckAmbiguous', replace(s, acknowledged=s.acknowledged + (s.operation,),
                                          unresolved='', phase='idle', operation='', plan=())
        newer = 0 if self.fault == 'same_fence_resolve' else 1
        yield 'ResolveFence', replace(s, fence=s.fence + newer, held=s.fence + newer if newer else s.held,
                                      plan=(), phase='resolve-catch-up')

    def resolving(self, s):
        if s.operation in s.projection and self.fault != 'skip_identity_lookup':
            yield 'ResolvePresent', replace(s, acknowledged=s.acknowledged + (s.operation,),
                                            unresolved='', phase='idle', operation='')
        else:
            yield 'ResolveAbsent', replace(s, unresolved='', phase='write')

    def reading(self, s):
        if s.read_phase == '' and s.reads < 1:
            head = len(s.projection) if self.fault == 'skip_head' else len(s.log)
            yield 'ReadHead', replace(s, read_phase='head', read_head=head, read_floor=len(s.log),
                                      reads=s.reads + 1)
        elif s.read_phase == 'head':
            covered = s.read_head <= len(s.projection)
            if covered and (not s.poisoned or self.fault == 'serve_poisoned'):
                yield 'ServeFast', replace(s, read_phase='', read_head=0, read_floor=0)
            elif s.phase == 'idle':
                yield 'ReadLock', replace(s, read_phase='locked')
        elif s.read_phase == 'locked':
            if s.read_head <= len(s.projection) or self.fault == 'skip_catch_up':
                yield 'ServeLocked', replace(s, read_phase='', read_head=0, read_floor=0)
            else:
                yield 'ReadCatchUp', replace(s, projection=s.log[:s.read_head])

    def violation(self, before, action, after):
        grew = len(after.log) > len(before.log)
        if grew:
            epoch = {'PublishAccepted': lambda: before.plan[1], 'PublishLostReply': lambda: before.plan[1],
                     'InFlightLands': lambda: before.inflight[1],
                     'ForeignAppend': lambda: before.foreign_epoch}[action]()
            if epoch != before.fence:
                return 'O-FENCE'
        if action == 'AcquireFence' and after.held <= before.fence:
            return 'O-FENCE'
        if len(set(after.log)) != len(after.log):
            return 'O-UNIQUE'
        if action == 'Plan':
            if before.tainted or before.poisoned:
                return 'O-TRUST'
            if before.unresolved:
                return 'O-UNRESOLVED'
            if before.inflight and before.inflight[1] == before.fence:
                return 'O-UNRESOLVED'
        if action in ACKS:
            identity = before.operation
            if identity not in after.log or identity not in after.projection:
                return 'O-ACK'
            if after.tainted or after.poisoned:
                return 'O-TRUST'
        if action in SERVES:
            if before.tainted or before.poisoned:
                return 'O-TRUST'
            if len(before.projection) < before.read_floor:
                return 'O-ACK'
        if not after.tainted and not after.poisoned and after.projection != after.log[:len(after.projection)]:
            return 'O-PREFIX'
        return None
