---
ddx:
  id: build-simple-queue-selective-field-materialization
  depends_on:
    - build-simple-queue-administrative-group-planner
    - build-simple-queue-indexed-attributes
  status: accepted
---

# Selective field authority and resolved edits

Metadata-only controls must not hydrate or republish unrelated payload equality
attributes. The shared internal queue_fields module separates read authority from
write intent without adding a consumer mutation operation. The full-image planner
remains a differential oracle until physical selective materialization is wired.

CheckedFields certifies presence through supplied values and absence through
Complete coverage or Selected exact names/complete prefixes. Missing outside
coverage is OutcomeUnknown, not absent. A covered parent prefix certifies its
children; empty prefixes are invalid. Prefix enumeration is complete under the
initial queue fence. Stored values/types still require physical decode validation.

ResolvedFields is Preserve, Replace(full bag), or Edit(exact Set/Remove names).
Prefix removals become explicit frozen names before publication; explicit edits
win over removal. No-op differences normalize to Preserve. Applying/composing
edits preserves unrelated fields and adds edited-name absence coverage; full
Replace produces complete coverage. One shared edit application now also serves
the existing commit planner, with its previous semantics unchanged.

Affected index closure includes every declared composite index, including
nonunique query indexes. All components of a touched index must be known; an
unloaded component cannot become a missing value. Untouched indexes remain
preserved. Exact IndexEffect tuples include old/new physical ordered keys even for sparse
unique tuples. Separate eligibility flags govern unique ownership releases and
claims. The resolver is existing-record-only: field absence is not record absence;
enclosing full create/delete generation owns index-row creation and deletion. Unique
FieldEffects releases only actual changed tuples, not every owner whose version
changed. Initial loaded owner/nonowner probes are mandatory, contradictory owner
releases fail closed, final sibling collisions reject and exact ordinary swaps
remain valid. Metadata-only A still owns x, so B→x conflicts.

Packed field composition is ordered: Edit→Preserve keeps the edit; Edit→Edit
composes names; Replace→Edit edits the replacement; Edit→Replace discards prior
edits. Record deletion/reinsertion is an enclosing command operation and must
preserve version highwater. The low-level field algebra is not a replay fence.
apply_command separately requires the expected predecessor record/version and
the exact prior highwater (including absent records) and an advancing output
version; zero existing/predecessor versions are invalid. Absent records require
full Replace. Partial patches cannot
create records or reset tombstones. This is an internal replay-invariant check,
not rerunning user CAS, selection or clocks. The enclosing projector must validate
schema/canonical command contents and atomically apply metadata, fields, receipts,
highwater and cursor. Packed versions may advance by more than one.

Pure tests compare ordered composition with a full-image oracle, prove an edit
under128 encoded bytes preserves a megabyte unrelated payload and long NUL/UTF-8
name without loading it into the selected view, and cover certified absence,
frozen removals/additions, nonunique composite closure, replay predecessor/ABA,
unchanged unique ownership, swaps, fresh-tuple collision and missing probes.
This is logical delta-size evidence, not physical performance qualification.

No selective delta is activated in QueueStore, PostgreSQL persistence or S3/Turso
by this slice. StoredWork and combine_deltas still use the existing complete-field
representation. Integrate the checked view/materialization into those paths next,
including exact tuple releases and index-preserving SQL writes, before streaming
accumulation and a versioned canonical durable envelope. This internal serde shape
is not a published command version. All new/retained/scratch buffers still require
admission before allocation; semantic publication TooLarge needs exact complete
envelope/result measurement rather than these provisional field components.

## Exact ownership integration before selective activation

The complete-field production planner now derives exact initial-to-final unique
releases and claims for work and state creates, changes and deletes. It probes
both sets, validates releases against exact loaded owners, validates claims
against initial ownership or an exact release, and merges only accepted effects.
Metadata/version-only changes preserve existing ownership. Administrative per-row
checks still use immutable initial owners; combined outer effects use the same
validator. Rejected groups preserve speculative ownership and loaded probes.

Insert vectors still load authority in one batch, now including old tuples of
addressed existing records as well as all prospective new tuples. This covers
shrinking upserts without per-item SQL probes. Ordered groups see ownership from
prior accepted groups, including deletions that free a tuple.

This removes a version-based ownership inference before selective activation;
it does not establish a previously exploitable metadata-only uniqueness defect
in the complete-field path. Complete field authority remains mandatory. Before
partial reads activate, PostgreSQL physical-owner reconciliation must use checked
tuple authority or an explicit release/claim overlay rather than treating an
unloaded component as absent.
