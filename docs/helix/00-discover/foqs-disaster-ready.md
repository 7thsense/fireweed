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
useful to pqueue as evidence about discovery-cache staleness, thundering-herd behavior, starvation, and
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

This resource informs future design review of pqueue's `DiscoverActiveScopes` routing behavior and its
queue-owner discovery path. Use it when evaluating how many workers consume the same top-N discovery
result, how stale routing summaries are bounded, and whether a priority-preserving dispersion mechanism
is needed before multi-region placement becomes product scope.

## Authority Boundary

The post does not establish that pqueue needs multi-region routing, client-visible region preferences, or
FOQS's MySQL topology. It supplies failure modes for advisory discovery. Queue ownership and fencing remain
governed by pqueue's architecture and ADRs; API changes remain governed by API-001.

## Review Checklist

- [x] Source URL and access date are present
- [x] Summary is concise and source-faithful
- [x] Findings are relevant to pqueue discovery
- [x] HELIX usage is specific
- [x] Boundary prevents over-applying the source
