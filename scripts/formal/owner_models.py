"""Finite lease-owner queue path specification; no production imports.

Two handles can own one queue by a control-plane lease. An owner holds a local
deadline `d`, measured on its lease clock from before the latest lease request
was sent, and a projection that is the fold of the log prefix it rebuilt at entry.
Entry is the only way in: acquire, wait the skew allowance `S`, fence, rebuild,
confirm. Exit is the only failure path: a failed or unconfirmed publish, a passed
deadline, a rejected renewal and a crash all drop the projection. Time is real
time in ticks. A handle's wall clock, which stamps its lease requests, runs ahead
of real time by its offset; the lease clock is real time. Ghost fields judge the
protocol: `acked` holds acknowledged writes, a read records whether its view
missed one, `unresolved` marks a publish whose outcome the owner cannot know and
`trusted` marks a projection rebuilt in this tenure. Rust refinement is not claimed.
"""
from dataclasses import dataclass, replace
from models import Model

IDS = ('a',)
TTL = 2
SKEW = 1  # S: at least the largest wall-clock offset plus drift, here one tick
REPLY = 2  # a lease request is answered or lost within this many ticks
READS = 1
# Each scenario bounds one slice of the protocol so every graph stays small and complete.
SCENARIOS = {'base': dict(horizon=6, crashes=0, renewals=0, losses=0, writes=True),
             'crash': dict(horizon=5, crashes=1, renewals=0, losses=0, writes=True),
             'renew': dict(horizon=5, crashes=0, renewals=1, losses=0, writes=True),
             'lost': dict(horizon=6, crashes=0, renewals=1, losses=1, writes=False)}
OFFSETS = (0, 1)  # handle wall clock minus real time
FAULTS = {'serve_past_deadline': 'O-READ', 'deadline_from_after_send': 'O-READ',
          'new_owner_skips_skew': 'O-READ', 'ack_without_recheck': 'O-READ',
          'keep_projection_after_publish_failure': 'O-UNRESOLVED',
          'ignore_fence': 'O-FENCE', 'reuse_fence': 'O-FENCE',
          'retain_projection_across_crash': 'O-TRUST',
          'skip_identity_lookup': 'O-UNIQUE'}
FAULT_SCENARIO = {'retain_projection_across_crash': 'crash', 'deadline_from_after_send': 'renew'}


@dataclass(frozen=True)
class Handle:
    phase: str = 'out'  # out, requesting, skew, fencing, confirming, serving
    kind: str = ''  # lease request in flight: acquire or renew
    answer: str = ''  # '', sent, done: plane state of that request
    granted: bool = False
    sent: int = 0  # lease clock when the request was sent
    wait_until: int = 0
    candidate: int = 0  # deadline computed at the reply, adopted at confirm
    deadline: int = 0
    fence: int = 0  # writer epoch held
    proj: tuple = ()
    trusted: bool = False
    unresolved: bool = False
    read: str = ''  # take, eval, stale
    renewals: int = 0


@dataclass(frozen=True)
class State:
    now: int = 0
    log: tuple = ()  # (identity, epoch); the offset is the index
    fence: int = 0
    holder: int = -1
    expires: int = 0  # in the stamping handle's wall time
    owners: tuple = (Handle(), Handle())
    acked: tuple = ()
    crashes: int = 0
    reads: int = 0
    lost: int = 0


class Owner(Model):
    name = 'api007-native-queue-lease-owner'
    initial = State()
    bounds = {'queues': 1, 'handles': 2, 'caller_identities': len(IDS), 'ttl_ticks': TTL,
              'skew_allowance_ticks': SKEW, 'reply_window_ticks': REPLY, 'scenarios': SCENARIOS,
              'wall_clock_offsets': list(OFFSETS), 'reads': 1,
              'frontier': 'no protocol or environment action remains enabled'}
    properties = ['O-READ', 'O-FENCE', 'O-UNRESOLVED', 'O-TRUST', 'O-UNIQUE']
    assumptions = ['the log is immutable; conditional publication checks epoch and exact offset atomically',
        'fencing always yields a strictly newer durable epoch and the rebuild reads the whole log',
        'a handle stamps lease requests with its wall clock; the control plane compares stamps',
        'wall clocks differ by at most one tick; the lease clock is real time',
        'a lease request is answered, or its reply is lost, within the reply window',
        'a stall is any interleaving: the owner takes no step while time and others advance',
        'the offset condition is redundant while fences are exclusive; only the epoch is mutated here',
        'no fairness, memory, provider durability or multi-queue claim']

    def __init__(self, fault=None, scenario=None):
        if fault is not None and fault not in FAULTS:
            raise ValueError('unknown owner fault')
        self.fault = fault
        self.scenario = scenario or FAULT_SCENARIO.get(fault, 'base')
        self.limits = SCENARIOS[self.scenario]
        self.name = Owner.name + ('' if self.scenario == 'base' else '-' + self.scenario)

    def frontier(self, state):
        return next(iter(self.transitions(state)), None) is None

    def transitions(self, s):
        yield from self.clock(s)
        for index in range(len(s.owners)):
            yield from self.handle(s, index)

    def put(self, s, index, handle, **changes):
        owners = list(s.owners)
        owners[index] = handle
        return replace(s, owners=tuple(owners), **changes)

    def clock(self, s):
        # Entry steps run without stalling; only the reply and the serving
        # steps may be delayed, which is where a stall can matter.
        urgent = any(h.answer == 'sent' or h.phase in ('fencing', 'confirming')
                     or (h.phase == 'skew' and s.now >= h.wait_until) for h in s.owners)
        pending = any(h.answer and s.now + 1 - h.sent > REPLY for h in s.owners)
        if s.now < self.limits['horizon'] and not pending and not urgent:
            yield 'Tick', replace(s, now=s.now + 1)

    def handle(self, s, i):
        h = s.owners[i]
        wall = s.now + OFFSETS[i]
        tag = str(i)
        # Handle 0 enters first; handle 1, whose wall clock runs ahead, only after a lease exists.
        if h.phase == 'out' and (i == 0 or s.holder >= 0):
            yield 'Acquire' + tag, self.put(s, i, replace(h, phase='requesting', kind='acquire',
                                                          answer='sent', sent=s.now))
        if h.answer == 'sent':
            yield 'Process' + tag, self.processed(s, i, wall)
        if h.answer == 'done':
            yield from self.replied(s, i)
            if s.lost < self.limits['losses'] and h.kind == 'renew':
                yield 'LoseReply' + tag, self.lost_reply(s, i)
        if h.phase == 'skew' and s.now >= h.wait_until:
            yield 'SkewDone' + tag, self.put(s, i, replace(h, phase='fencing'))
        if h.phase == 'fencing':
            epoch = s.fence + (self.fault != 'reuse_fence')
            keep = self.fault == 'retain_projection_across_crash' and h.proj
            proj = h.proj if keep else tuple(identity for identity, _ in s.log)
            yield 'Fence' + tag, self.put(s, i, replace(h, phase='confirming', fence=epoch, proj=proj,
                                                        trusted=not keep), fence=epoch)
        if h.phase == 'confirming':
            live = s.holder == i and wall < s.expires and s.now < h.candidate
            if live:
                yield 'Confirm' + tag, self.put(s, i, replace(h, phase='serving', deadline=h.candidate))
            else:
                yield 'ConfirmFails' + tag, self.put(s, i, self.out(h))
        if h.phase == 'serving' and s.crashes < self.limits['crashes'] and i == 0:
            crashed = self.out(h, crash=True)
            yield 'Crash' + tag, self.put(s, i, crashed, crashes=s.crashes + 1)
        if h.phase == 'serving':
            yield from self.serving(s, i, wall)

    def out(self, h, crash=False):
        keep = crash and self.fault == 'retain_projection_across_crash'
        return Handle(proj=h.proj if keep else (), renewals=h.renewals)

    def processed(self, s, i, wall):
        h = s.owners[i]
        if h.kind == 'acquire':
            granted = s.holder in (-1, i) or wall >= s.expires
            if granted:
                return self.put(s, i, replace(h, answer='done', granted=True), holder=i, expires=wall + TTL)
        else:
            granted = s.holder == i and wall < s.expires
            if granted:
                return self.put(s, i, replace(h, answer='done', granted=True), expires=max(s.expires, wall + TTL))
        return self.put(s, i, replace(h, answer='done', granted=False))

    def replied(self, s, i):
        h = s.owners[i]
        tag = str(i)
        # The deadline is measured from before the request was sent.
        basis = s.now if self.fault == 'deadline_from_after_send' else h.sent
        if h.kind == 'acquire':
            if h.granted:
                wait = s.now if self.fault == 'new_owner_skips_skew' else s.now + SKEW
                yield 'Granted' + tag, self.put(s, i, replace(h, phase='skew', answer='', kind='', granted=False,
                                                              wait_until=wait, candidate=basis + TTL))
            else:
                yield 'Busy' + tag, self.put(s, i, self.out(h))
        elif h.granted:
            yield 'Renewed' + tag, self.put(s, i, replace(h, answer='', kind='', granted=False,
                                                          deadline=basis + TTL))
        else:
            yield 'Rejected' + tag, self.put(s, i, self.out(h))

    def lost_reply(self, s, i):
        h = s.owners[i]
        if h.kind == 'acquire':
            return self.put(s, i, self.out(h), lost=s.lost + 1)
        return self.put(s, i, replace(h, answer='', kind='', granted=False), lost=s.lost + 1)

    def serving(self, s, i, wall):
        h = s.owners[i]
        tag = str(i)
        expired = s.now >= h.deadline
        if expired and self.fault != 'serve_past_deadline':
            yield 'Exit' + tag, self.put(s, i, self.out(h))
        if h.unresolved:
            yield 'Exit' + tag, self.put(s, i, self.out(h))
            if self.fault != 'keep_projection_after_publish_failure':
                return
        if not h.kind and h.renewals < self.limits['renewals'] and i == 0:
            yield 'Renew' + tag, self.put(s, i, replace(h, kind='renew', answer='sent', sent=s.now,
                                                        renewals=h.renewals + 1))
        if self.limits['writes']:
            yield from self.writing(s, i, expired)
        yield from self.reading(s, i, expired)

    def writing(self, s, i, expired):
        """One request: plan against the projection, publish, apply and acknowledge."""
        h = s.owners[i]
        tag = str(i)
        for identity in IDS:
            if identity in s.acked:
                continue
            if identity in h.proj and self.fault != 'skip_identity_lookup':
                yield 'ReplayAck:' + identity + tag, self.put(s, i, h, acked=s.acked + (identity,))
                continue
            epoch, offset = h.fence, len(h.proj)
            accepted = (epoch == s.fence or self.fault == 'ignore_fence') and offset == len(s.log)
            if accepted:
                durable = s.log + ((identity, epoch),)
                yield ('Publish:' + identity + tag,
                       self.put(s, i, replace(h, proj=h.proj + (identity,)), log=durable,
                                acked=s.acked + (identity,)))
                yield ('PublishLostReply:' + identity + tag,
                       self.put(s, i, replace(h, unresolved=True), log=durable))
            else:
                yield 'PublishRejected:' + identity + tag, self.put(s, i, replace(h, unresolved=True))

    def reading(self, s, i, expired):
        h = s.owners[i]
        tag = str(i)
        if not h.read and s.reads < READS:
            # The pre-check variant looks at the deadline only when the batch is taken.
            if not (self.fault == 'ack_without_recheck' and expired):
                yield 'ReadTake' + tag, self.put(s, i, replace(h, read='take'))
        elif h.read == 'take':
            stale = not all(identity in h.proj for identity in s.acked)
            yield 'ReadEval' + tag, self.put(s, i, replace(h, read='stale' if stale else 'eval'))
        elif h.read in ('eval', 'stale'):
            if not expired or self.fault in ('ack_without_recheck', 'serve_past_deadline'):
                yield 'ReadAck' + tag, self.put(s, i, replace(h, read=''), reads=s.reads + 1)

    def violation(self, before, action, after):
        if len(after.log) > len(before.log) and after.log[-1][1] != before.fence:
            return 'O-FENCE'
        if action.startswith('Fence') and after.fence <= before.fence:
            return 'O-FENCE'
        if len({identity for identity, _ in after.log}) != len(after.log):
            return 'O-UNIQUE'
        index = int(action[-1]) if action[-1].isdigit() else -1
        if action.startswith(('Publish', 'ReplayAck')) and before.owners[index].unresolved:
            return 'O-UNRESOLVED'
        if action.startswith('Confirm') and not action.startswith('ConfirmFails') \
                and not after.owners[index].trusted:
            return 'O-TRUST'
        if action.startswith('ReadAck') and before.owners[index].read == 'stale':
            return 'O-READ'
        return None
