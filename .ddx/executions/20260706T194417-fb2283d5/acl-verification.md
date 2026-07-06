# Verification

- Focused test run: `cargo test -p pqueue-server --test fjord_surface`
- Result: 8 passed, 0 failed
- Coverage:
  - `TestKafkaTenantAclRejectsCrossTenantRead`
  - `TestKafkaTenantAclRejectsCrossQueueRead`
  - Embedded fjord broker startup and change-log smoke tests

