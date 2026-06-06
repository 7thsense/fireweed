---
ddx:
  id: product-vision
---

# Product Vision

## Mission Statement

pqueue provides the timestamp-prioritized durable queue backbone for the
Seventh Sense delivery engine. It accepts personalized delivery work, orders
execution by each recipient's scheduled send time rather than FIFO arrival, and
preserves clear execution state so email delivery can proceed reliably through
claims, retries, completion, and terminal failure.

## Positioning

For Seventh Sense delivery engine developers and operators who need optimized
email delivery to happen at each recipient's right time, pqueue is a
timestamp-prioritized work queue that makes scheduled delivery execution
deterministic and recoverable. Unlike a FIFO queue plus ad hoc scheduler logic,
pqueue treats delivery time as the primary ordering dimension while preserving
explicit write, in-process, retry, complete, and failed states.

## Vision

When pqueue succeeds, Seventh Sense has one dependable place to record pending
optimized-send work and one deterministic rule for what runs next: the eligible
item with the earliest scheduled timestamp. The delivery engine can honor
individual-level send-time decisions, layered slotting constraints, and
connector execution through a durable queue model instead of spreading timing,
claiming, retry, and recovery behavior across polling loops, slotting counters,
and connector-specific worker code.

**North Star**: Every runnable delivery item is durably executed according to
timestamp priority with no lost work, no duplicate claim, and an explicit final
state.

## User Experience

A scheduler computes a recipient-specific delivery time for a HubSpot, Marketo,
or future marketing automation send and writes that delivery item to pqueue.
The queue stores it durably and orders it against every other pending item by
scheduled timestamp. When a worker asks for work, pqueue returns the earliest
eligible item and marks it in process so another worker cannot claim the same
delivery. If the connector send succeeds, the item is marked complete. If the
send encounters a recoverable platform or network failure, the item is retried
according to queue policy. If retry policy is exhausted or the error is
unrecoverable, the item is marked failed with enough state for the delivery
engine to inspect and react.

## Target Market

| Attribute | Description |
|-----------|-------------|
| Who | Seventh Sense engineering teams responsible for optimized email delivery, connector execution, and delivery operations |
| Pain | Individualized send-time optimization creates large volumes of time-ordered work, but FIFO queues and ad hoc schedulers make ordering, retries, and recovery hard to reason about |
| Current Solution | FIFO queues, database polling loops, slot counters, or connector-specific scheduler logic layered onto general-purpose primitives |
| Why They Switch | Personalized delivery depends on timestamp intent and durable execution state, not arrival-order semantics with scattered recovery logic |

## Key Value Propositions

| Value Proposition | Customer Benefit |
|-------------------|------------------|
| Timestamp-prioritized ordering | Delivery work runs according to recipient-specific scheduled time rather than enqueue order |
| Durable execution lifecycle | Workers can claim, retry, complete, or fail connector delivery items without losing state across process boundaries |
| Delivery-engine backbone | Seventh Sense can centralize delivery ordering and recovery behavior instead of duplicating it across schedulers, slotting logic, and connector workers |

## Success Definition

| Metric | Target |
|--------|--------|
| Timestamp ordering correctness | 100% of queue conformance tests select the earliest eligible timestamp before later eligible work |
| Durable claim semantics | No accepted delivery item is lost or concurrently claimed when a worker exits during in-process execution in recovery tests |
| Lifecycle completeness | Accepted delivery work always reaches one explicit state: pending, in process, retrying, complete, or failed |

## Why Now

Seventh Sense's product promise depends on earning attention in the inbox by
initiating delivery when each person is most likely to engage. The local
slotting work already shows that delivery timing is becoming richer than a
single optimal timestamp: account time zones, day and time-of-day limits, and
hourly delivery constraints all shape when work should run. Building
timestamp-prioritized durable queue semantics now prevents the delivery engine
from accumulating FIFO assumptions, one-off retry handling, and recovery
conventions that would become expensive to replace as delivery volume and
operational expectations increase.

## Review Checklist

Use this checklist when reviewing a product vision artifact:

- [ ] Mission statement is specific — names the user, the problem, and the approach
- [ ] Positioning statement differentiates from the current alternative
- [ ] Vision describes a desired end state, not a feature list
- [ ] North star is a single measurable sentence
- [ ] User experience section describes a concrete scenario, not abstract benefits
- [ ] Target market identifies specific pain points and switching triggers
- [ ] Value propositions map to customer benefits, not internal capabilities
- [ ] Success metrics are measurable and time-bound
- [ ] Why Now section names a specific change, not a vague opportunity
- [ ] Business case details, competitor matrices, requirements, and technical choices are left to their own artifacts
- [ ] No implementation details (technology choices, architecture) — those belong in design
