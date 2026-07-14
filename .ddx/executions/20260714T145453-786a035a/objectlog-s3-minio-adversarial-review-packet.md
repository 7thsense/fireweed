# Objectlog S3/MinIO Adversarial Review Packet

Bead: `pqueue-eb0ea6ef`
Bundle: `.ddx/executions/20260714T145453-786a035a`
Dependency preserved: `pqueue-4157c36f`

## Purpose

This packet is a durable, self-contained review request for a fresh-eyes Codex adversarial review of the final implemented objectlog protocol. The reviewer should challenge:

- S3 semantics assumptions
- MinIO semantics assumptions
- conditional-write / CAS assumptions
- the boundary between local protocol hardening and provider-certification work

Do not rely on chat context. Use only the artifacts named here.

## Governing References

- TD-004: `docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md`
- ADR-003: `docs/helix/02-design/adr/ADR-003-rust-workspace-and-toolchain-policy.md`
- Dependency to preserve in the evidence trail: `pqueue-4157c36f`

## Required TD-004 Anchors

The review must explicitly inspect these source anchors:

- `docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md:188`
- `docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md:218`
- `docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md:730`

## Protocol Claims To Challenge

- Manifest commit is the CAS and fencing enforcement point for the objectlog head.
- The manifest path requires a documented conditional-write primitive; a store without usable conditional writes must not silently fall back to plain appends.
- Live provider-specific S3 hardening is a deployment-certification boundary, not a claim that local evidence already certifies AWS S3 behavior.
- MinIO compatibility must be challenged separately from AWS S3 assumptions.

## Review Question

Does the final objectlog protocol remain linearizable and safely fenced against stale writers when the manifest is the CAS boundary, the object store is only conditionally writable where documented, and provider-specific certification remains out of scope?

## Review Instructions For The Critic

You are a critic, not a validator. Find implementation rework risks, contradictions, missing constraints, ambiguous interfaces, hidden assumptions, and places where two competent implementers would make different choices. Do not implement the protocol or rewrite this packet unless explicitly asked for a separate execution step. Do not balance criticism with praise.

Focus on:

- stale-writer interleavings
- manifest-tail versus control-plane epoch fencing
- idempotent retry behavior after failed conditional writes
- MinIO versus AWS S3 conditional-write behavior
- whether any evidence overclaims provider-specific certification

If you find issues, classify each finding as:

- blocking
- non-blocking
- duplicate
- out of scope

Include file:line evidence for every finding that is not out of scope.

## Evidence Surface To Review

- `crates/pqueue-objectlog/tests/object_log_commit_recovery_tests.rs`
- `crates/pqueue-server/src/object_log_sqlite.rs`
- `crates/pqueue-server/src/lib.rs`

## Local Gate Context

These gates were attempted in this worktree:

- `go test ./...`
  - classification: `not-applicable`
  - output: `FAIL	./... [setup failed]`
  - output: `pattern ./...: directory prefix . does not contain main module or its selected dependencies`
  - interpretation: no Go module/packages are present in this workspace

- `lefthook run pre-commit`
  - classification: `operator_required`
  - output: `No config files with names ["lefthook" ".lefthook" ".config/lefthook"] have been found in "/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-eb0ea6ef-20260714T145453-786a035a"`

## Review Deliverable

Persist the reviewer transcript or result in this bundle path so later reviewers can inspect the exact prompt, scope, anchors, findings, and conclusion without needing live chat context.
