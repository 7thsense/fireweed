"""Independent bounded ownership/claim model for production trace replay.

No production queue types/functions are imported. Fingerprints are independently
computed from the fixture's explicitly frozen existing BatchClaim body format.
"""
from dataclasses import dataclass, replace
import hashlib

SCHEMA = 'fireweed-model-replay-v1'
BODY = '1|28000|ClaimCompatibility { group_key: None, same_group_key: false, metadata_equals: {}, group_batching: None, whole_cohort: false }'
FINGERPRINT = int.from_bytes(hashlib.sha256(BODY.encode()).digest()[:8], 'big')
BOUNDS = {'queues': 1, 'items': 2, 'requests': 2, 'owners': 2, 'issued_sessions': 3,
          'handoffs': 1, 'graceful_reopens': 1, 'logical_now_seconds': 2,
          'lease_expiry_seconds': 30, 'max_items_per_claim': 1}


@dataclass(frozen=True)
class State:
    owner: int = 0
    assignment: int = 1
    epoch: int = 1
    issued: tuple = ((0, 1, 1),)  # owner, assignment credential, storage credential
    history: tuple = ()  # request, selected item rank, publication epoch
    applied: int = 0
    lost: int = 0
    replayed: int = 0
    stale_tested: int = 0
    handoff: bool = False
    reopened: bool = False
    reaffirmed: bool = False


def claim_result(req, item):
    return {'status': 'claimed', 'request': req, 'item': item, 'token': req,
            'expiry': 30, 'version': 2, 'attempts': 1}


def record(req, item, epoch):
    return {'operation': 'claim', 'request': req, 'item': item, 'epoch': epoch,
            'token': req, 'worker': f'worker-{req}', 'expiry': 30,
            'fingerprint': FINGERPRINT}


def observation(s):
    applied = {item: req for req, item, _ in s.history[:s.applied]}
    rows = [{'item': i, 'state': 'leased' if i in applied else 'pending',
             'token': applied.get(i),
             'worker': f'worker-{applied[i]}' if i in applied else None,
             'expiry': 30 if i in applied else None,
             'version': 2 if i in applied else 1, 'attempts': 1 if i in applied else 0}
            for i in range(2)]
    receipts = [dict(record(req, item, epoch), epoch=None, retention_expiry=62) for req, item, epoch in s.history[:s.applied]]
    for receipt in receipts:
        del receipt['expiry']  # Receipt payload carries token/worker, not lease expiry.
    receipts.sort(key=lambda r: r['request'])
    return {'owner': s.owner, 'assignment_epoch': s.assignment, 'storage_epoch': s.epoch,
            'history': [record(*r) for r in s.history], 'rows': rows,
            'applied_receipts': receipts}


def actions(s):
    claimed = {req for req, _, _ in s.history}
    available = [i for i in range(2) if i not in {item for _, item, _ in s.history}]
    for req in range(2):
        if req not in claimed:
            yield f'Claim:{req}', replace(s, history=s.history + ((req, available[0], s.epoch),)), claim_result(req, available[0])
        else:
            if not s.lost & (1 << req) and not s.replayed & (1 << req):
                yield f'LoseResponse:{req}', replace(s, lost=s.lost | (1 << req)), {'status': 'response_lost', 'request': req}
            if s.applied == len(s.history) and not s.replayed & (1 << req):
                item = next(item for r, item, _ in s.history if r == req)
                yield f'Replay:{req}', replace(s, replayed=s.replayed | (1 << req)), claim_result(req, item)
    if not s.history:
        for order in ((0, 1), (1, 0)):
            records = tuple((req, item, s.epoch) for item, req in enumerate(order))
            by_request = sorted(records)
            yield 'ConcurrentClaims', replace(s, history=records), {
                'status': 'concurrent_claims', 'responses': [claim_result(req, item) for req, item, _ in by_request]}
    if not s.history and len(s.issued) > 1:
        yield 'ConcurrentMixedFence', replace(s, history=((0, 0, s.epoch),)), {
            'status': 'mixed_fence', 'responses': [claim_result(0, 0), {'status': 'epoch_fenced'}]}
    if s.applied < len(s.history):
        yield 'DrainApply', replace(s, applied=len(s.history)), {'status': 'applied'}
    if not s.handoff:
        issued = (1, s.assignment + 1, s.epoch + 1)
        yield 'Handoff', replace(s, owner=1, assignment=issued[1], epoch=issued[2],
                                issued=s.issued + (issued,), handoff=True), {'status': 'handed_off'}
    if not s.reopened:
        issued = (s.owner, 1, s.epoch + 1)
        yield 'Reopen', replace(s, assignment=1, epoch=issued[2], issued=s.issued + (issued,),
                               applied=len(s.history), reopened=True), {'status': 'reopened'}
    for session in range(len(s.issued) - 1):
        if not s.stale_tested & (1 << session):
            yield f'StaleClaim:{session}', replace(s, stale_tested=s.stale_tested | (1 << session)), {'status': 'epoch_fenced'}
    if not s.reaffirmed:
        yield 'Reaffirm', replace(s, reaffirmed=True), {'status': 'reaffirmed'}


def successors(s, action):
    return [(target, result) for label, target, result in actions(s) if label == action]


def validate_state(s):
    assert len(s.history) <= 2 and s.applied <= len(s.history)
    assert len({req for req, _, _ in s.history}) == len(s.history)
    assert len({item for _, item, _ in s.history}) == len(s.history)
    assert len(s.issued) <= 3 and s.epoch == len(s.issued)
    assert [e for _, _, e in s.issued] == list(range(1, s.epoch + 1))
    assert all(1 <= epoch <= s.epoch for _, _, epoch in s.history)
