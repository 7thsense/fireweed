---
ddx:
  id: api-fireweed-kernel-contract
  status: superseded
---

# API-006: Kernel redesign — superseded

Superseded on 2026-10-10 by the project owner's instruction to retain a simple
batch queue interface with atomic claim/commit groups and a PostgreSQL baseline.

The unmerged eight-operation kernel redesign is not the implementation target.
Use [API-007](API-007-queue-claim-commit-contract.md) and
[ADR-026](../adr/ADR-026-postgres-baseline-queue-contract.md).

Its original design and measured spikes remain historical evidence on
[archive/api006-design-20261010](https://github.com/7thsense/fireweed/tree/archive/api006-design-20261010)
at commit `1b65a5b731ddea052c1c5b77d9916732c09c8aae`. This supersession does not delete that evidence
or approve its storage proposals. Existing implementation contracts stay available
until the new binding passes the mandatory PostgreSQL and S3/Turso tests.
