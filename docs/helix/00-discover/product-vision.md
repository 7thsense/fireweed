---
ddx:
  id: product-vision
---

# Product Vision

## Mission Statement

pqueue is the timestamp-prioritized durable queue backbone for the Seventh
Sense delivery engine, ensuring personalized delivery work runs at the scheduled
time with explicit lifecycle state: pending, in process, retry, complete, or
failed.

## Positioning

For Seventh Sense delivery engineers who need optimized sends to happen at each
recipient's right time, pqueue is a durable priority queue ordered by timestamp,
not arrival order. Unlike FIFO queues with scheduler logic layered around them,
pqueue makes time the queue's primary ordering rule and keeps every item in an
explicit lifecycle state.

## Vision

When pqueue succeeds, Seventh Sense has one dependable place to record delivery
work; the rule for what runs next is always the eligible item with the earliest
scheduled timestamp.

**North Star**: Every runnable delivery item is durably executed according to
timestamp priority, with no lost work, no concurrent execution, and an explicit
final state.

## User Experience

Engineers use pqueue as one primitive: write timestamped delivery work, claim
the earliest eligible item, and record the outcome.

## Target Market

| Attribute | Description |
|-----------|-------------|
| Who | Seventh Sense delivery engineers and operators |
| Pain | FIFO queues do not model recipient-specific schedules directly |
| Current Solution | FIFO queues plus scheduler-specific polling and retry logic |
| Why They Switch | Personalized delivery needs schedule-aware ordering and durable lifecycle state in the queue itself |

## Key Value Propositions

| Value Proposition | Customer Benefit |
|-------------------|------------------|
| Timestamp-prioritized ordering | Delivery follows recipient-specific send time instead of enqueue order |
| Durable execution lifecycle | Delivery work remains recoverable across worker failures |
| Single queue backbone | Scheduling, execution, retry, and terminal state share one source of truth |

## Success Definition

| Criterion | Definition |
|--------|--------|
| Timestamp ordering correctness | The queue always selects the earliest eligible timestamp before later eligible work |
| Durable execution safety | No accepted item is lost or concurrently executed after worker failure |
| Lifecycle completeness | Every accepted item is observable in pending, in process, retry, complete, or failed |

## Why Now

The delivery engine is being defined around individualized send timing; making
time the queue's native ordering model now prevents FIFO assumptions from
hardening into the engine contract.
