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
by this slice. StoredWork now carries checked read coverage, and combine_deltas composes exact
field commands in request order. Physical selective reads and index-preserving SQL
writes remain for the next activation before streaming accumulation and a
versioned canonical durable envelope. This internal serde shape
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

## Engine selective views and explicit writes

StoredWork replaces its all-or-nothing field flag with FieldCoverage. CommitDelta
stores one explicit ResolvedFields command for every changed live work row;
missing, orphan or duplicate mappings fail closed. The same production planner
accepts complete or selected views, freezes actual differences, checks every
component of affected query/unique indexes, and preserves untouched owners.
Create/replacement commands carry full bags; delete commands have no live row.
State Put remains a full image. Ordinary patch/delete lease and version rules,
receipt identities and public request fingerprints are unchanged.

Speculative application uses the command against the prior checked view. Packed
commands compose in order; a later metadata Preserve cannot erase an earlier
field edit, and delete/reinsert discards prior commands and requires Replace.
Command/view disagreement fails closed before source mutation. Materialization
knowledge is excluded from administrative business-change detection, so expanding
a selected-name certificate does not advance a no-op's version.

Differential tests run the same planner with complete versus selected field bags,
including a megabyte unrelated attribute, unique rekeys/sparse transitions,
metadata following edits, malformed commands, missing affected components and
packed deletion/reinsertion. This establishes planner semantics only. PostgreSQL
still loads complete fields and rejects partial views before its existing full
regeneration SQL. The pure broad administrative oracle also remains complete-only.
No new durable S3/Turso codec is activated; admission and canonical publication
qualification still belong to later integration slices.

## Guarded physical reader staging

The shared required_work_fields helper computes exact-name closure for an ordinary
work envelope: explicit patch names and every component of touched composite
indexes; old unique components for delete, replacement and insert/upsert readsets.
A replacement regenerates query indexes from its new full bag and therefore does
not load old nonunique components. Invalid unique schema authority fails closed.

PostgreSQL uses one field loader implementation with an optional exact-name
selection. The production load_group wrapper always passes None and continues
complete reads. Only the isolated test invokes selection; the existing production
persistence guard still rejects partial views before any writes. Loaded-value
sizes, encoded sort keys, full names and headers are measured using the same SQL
selection before allocating retained field bags. State reads remain complete.

An isolated PostgreSQL regression retains a 256 KiB reservation, proves a complete
read of a record with a megabyte equality attribute cannot fit, then loads only
its gate field. Unloaded target/payload values remain unknown. Planning its gate
change succeeds, persistence rejects the partial view, and the stored version
stays unchanged. This is guarded reader evidence, not physical selective-write or
production performance qualification. Automatic approval review rejected combined
read activation and guard removal; activation remains pending a concrete reviewed
writer and the required approval.

Reader review also requires reservation for requested-name certificates even when
SQL returns no matching field, multiplied by live work-row count, plus temporary
name/hash/ordinal buffers before their allocation. Loaded retains that guard.
Selected ordinal derivation is restricted to the supplied record IDs, so a state
collection's ordinal cannot widen a work read. The size expression counts payload
bytes even when an ordinal is corrupt; the decoder still rejects corrupt schema
references. The isolated regression covers mixed collection ordinals, two long
absent-name certificates, and rollback-only corrupt payload sizing.

## Guarded physical writer staging

One PostgreSQL applicator shares the existing batch encoders and insert SQL.
Production persistence still requires complete views and hardwires Complete mode.
Selected mode exists only in test builds, with no runtime configuration switch.
It consumes exact field commands: Preserve writes no field/index rows; Edit deletes
exact record/field or record/full-name pairs and only affected record/index pairs,
then encodes Set values and resolved physical after keys; Replace/Delete regenerate
or remove full field/index rows. Index edits use the initial physical snapshot and
final composed command, including sparse physical query keys. State Put remains
full and opaque work bodies retain their existing Preserve behavior.

Authority loads now assert fresh distinct tuples. Accepted releases and claims
require loaded probes, retained throughout the fenced operation; speculative
ownership changes occur only on accepted groups. Thus an unprobed tuple cannot
have changed earlier, and its exact physical owner is authoritative. The former
full-map reconciliation is removed. Unloaded unique components cannot masquerade
as absence when another row probes the owner.

Isolated test-only application uses the normal shared planning and output charges.
Two rows edit different declared/additional fields and indexes; untouched fields,
full-name additional rows, indexes and megabyte opaque bodies retain their ctids.
A no-op preserves field/index identities and version. Tests repeat with all discovery
hashes forced to collide. Ordered cases cover rejected rekey isolation, atomic
owner deletion/new insertion, create then rekey, tuple reuse, and creation of a
sparse query-index row. Reader/certificate tests retain their separate 256 KiB
reservation; writer planning uses an admitted 4 MiB budget. Transactions roll back;
these are applicator checks, not acknowledged-loss or production activation tests.

Production selected reads/writes and guard removal remain pending a concrete
implementation review and the required activation approval. No S3/Turso command
codec, administrative wire activation or exceptional performance qualification is
claimed by this staging slice.
