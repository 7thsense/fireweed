---
ddx:
  id: discover-foqs-disaster-ready
  type: resource-summary
  links:
    - {kind: informs, to: prd}
---

# FOQS: Making a Distributed Priority Queue Disaster-Ready

## Source

- Publisher: Meta Engineering
- Published: 2022-01-18
- URL: <https://engineering.fb.com/2022/01/18/production-engineering/foqs-disaster-ready/>
- Accessed: 2026-07-22

## Summary

Meta describes FOQS's migration from region-isolated installations to a global service that follows
failed-over MySQL primaries and hides physical placement behind a routing service. The account is most
useful to fireweed as evidence about discovery-cache staleness, thundering-herd behavior, starvation, and
the operational cost of exposing placement decisions to clients.

## Relevant Findings

- Logical region preference is a routing hint, not a physical placement contract; routing continues when
  the preferred region is unavailable.
- Dequeue routing depends on an in-memory cache of queue nodes with ready items. Stale replenishment caused
  queueing delays and latency-objective violations.
- Directing requests to the top-ranked cached nodes caused a thundering herd at the top and starvation at
  the bottom, especially across regions.
- Random selection among already-prioritized candidates spread requests more evenly and reduced queueing
  delay without discarding priority as the first-stage filter.
- The migration used shadow load testing, incremental configuration rollout, rollback capability, and
  migration-health dashboards rather than a one-step cutover.

## HELIX Usage

This resource informs the router side of the [scheduler and router boundary](../01-frame/guides/scheduler-router-boundary.md).
fireweed has no discovery and no cross-queue claim, so a caller's router that picks queues from `stats`
inherits these failure modes. Use it when reviewing how many workers follow the same routing hint, how
stale a router's view of ready work may be, and whether a priority-preserving dispersion mechanism is
needed. It also applies to the cached queue-to-partition routing inside the implementation (TD-018)
before multi-region placement becomes product scope.

## Authority Boundary

The post does not establish that fireweed needs multi-region routing, client-visible region preferences, or
FOQS's MySQL topology. It supplies failure modes for advisory routing. Queue placement, ownership and fencing are
governed by ADR-008 and TD-003; the interface is governed by API-006.

## Review Checklist

- [x] Source URL and access date are present
- [x] Summary is concise and source-faithful
- [x] Findings are relevant to fireweed discovery
- [x] HELIX usage is specific
- [x] Boundary prevents over-applying the source
