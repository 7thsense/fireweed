---
ddx:
  id: name-and-positioning-analysis
  depends_on:
    - product-vision
  review:
    self_hash: 21867acc338876d20d3d1ad0e916f2615bcd39c673340d5034de5dbfa255c9e9
    deps:
      product-vision: d70aaff09b5d5f59211e5ef3ae9156ee30776e95bce7a70398978e83e39d39e8
    reviewed_at: "2026-07-23T00:50:56Z"
---

# Public Name and Positioning Analysis

- **Decision date**: 2026-07-23
- **Decision owner**: project maintainers
- **Decision state**: selected working public identity; legal clearance pending
- **Tracking bead**: `pqueue-1dd9158f`

## Product semantics

The project is not merely a priority queue. It owns the durable work lifecycle:
acceptance, configurable ordering, eligibility, grouping, claims and leases,
retries, idempotency, terminal state, and recovery across several storage
profiles. It is not a workflow engine and does not define dependency graphs.

The public descriptor is:

> A durable work-state engine for ordered, recoverable execution.

The name should support that broader contract without promising strict FIFO
behavior or reducing the product to a worker pool.

## Candidate scorecard

Scores use a five-point scale. Collision risk is a preliminary desk assessment;
a higher score means less observed risk.

| Candidate | Semantic fit | Distinctiveness | Identifier ergonomics | Collision risk | Total / 20 | Result |
|---|---:|---:|---:|---:|---:|---|
| **Fireweed Queue** | 5 | 4 | 5 | 4 | **18** | Selected |
| Queueyard | 5 | 4 | 5 | 5 | 19 | Historical identity |
| Taskcairn | 3 | 5 | 5 | 5 | 18 | Reserve fallback |
| Queuewright | 4 | 4 | 3 | 3 | 14 | Reject |
| Workspool | 4 | 4 | 5 | 1 | 14 | Reject |
| Ledgerail | 4 | 4 | 5 | 1 | 14 | Reject |
| Taskweir | 3 | 4 | 3 | 5 | 15 | Reject |

Fireweed Queue keeps the same durable-work semantics while shifting the public
identity to something shorter and less collision-prone for announcement and
packaging. Queueyard remains a historical identity only.

Taskcairn is more distinctive but describes durable markers rather than
ordering and dispatch. Queuewright is memorable but longer, easier to misspell,
and its `.com` was already registered when checked.

## Collision diligence

Three independent reviews assessed product fit, technical namespaces, and
confusion risk on 2026-07-22. Preliminary exact-name checks found:

- no exact `queueyard` package on crates.io, npm, or PyPI;
- no exact GitHub repository-name match and no `queueyard` GitHub user;
- no RDAP record for `queueyard.com`, `queueyard.dev`, or `queueyard.org`; and
- no obvious exact-name software product or company in bounded web searches.

Availability is a point-in-time observation, not a reservation. Generic
searches for the two words can surface physical yard and vehicle-queue
management. Public use should consistently retain the closed form Queueyard.

Workspool was the initial favorite, but it is one letter from
[WorkPool](https://www.workpool.co/), an established task and workflow product.
It can also be parsed as “WorksPool” when read or spoken. Oil-field “work spool”
usage, the generic work-pool programming pattern, and an adjacent
[Spooling.ai](https://spooling.ai/) brand add search noise. These risks outweigh
its otherwise clean package namespaces.

Ledgerail was rejected after a current exact-name search found an active SaaS
using the name for business-document and AI-disclosure workflows. A technically
available package identifier is insufficient when the product name is already
in use.

## Decision

Adopt **Fireweed Queue** as the working public name, subject to formal
clearance and namespace reservation before the repository is announced or made
public.

Use the descriptor “A durable work-state engine for ordered, recoverable
execution” on first reference. Do not position Fireweed Queue as FIFO, a
generic message broker, a worker pool, or a workflow-DAG engine.

Keep **Taskcairn** as the reserve fallback if clearance discovers a material
Fireweed Queue conflict. Do not use Workspool as the public name.

Queueyard and `pqueue` remain historical references only and are not the
current public name.

## Identifier map

| Surface | Selected value |
|---|---|
| Display name | `Fireweed Queue` |
| Short name | `Fireweed` |
| Repository/package stem | `fireweed-queue` |
| CLI command | `fireweed` |
| Rust crate stem | `fireweed` |
| Environment prefix | `FIREWEED_` |
| Informal abbreviation | `FWQ` |

This map is a naming input, not authorization for a mechanical global replace.
ADR-018 must classify public, compatibility, protocol, and persisted identifiers
before implementation begins.

## Trademark caveat

This analysis is preliminary naming diligence only and is not trademark
clearance or legal advice. Before public announcement, a qualified reviewer
must search the relevant software and hosted-service classes and jurisdictions,
assess confusing similarity, and approve use. Maintainers must then reserve the
chosen domains, package names, source-hosting organization, and social handles
together. If that gate fails, use the recorded fallback and repeat the same
review.
