# Iteration 54: bounded addressed administrative preparation

Base: e3510fd6. The addressed driver validates a distinct exact-key positional
source, including absent/tombstoned records. Entry ordinals and emitted packet
ordinals are separate. Every input must match its request ordinal, key, fenced
sequence, identity/highwater, and reservation; successful EOF must cover every
position exactly once. Later duplicate positions are Invalid even when missing;
present duplicates retain original before metadata rather than proposed state.
Missing work is never claimed or inferred from an incomplete source.

The driver reuses the same per-target proposal/resolver, evidence packets,
awaited scratch sink, two-pass finalizer and resolved SQL application as broad
controls. It continues validation after overlap or provisional conflict.
Retained result keys/before values are charged before copying and retained in
the final summary independently of one-row hydration. Result containers and
seen identities are input-bounded. Complete logical result limits remain pending.

Native point lookups and the broad cursor share one hydration/probe helper;
key/action/prefix clones are precharged independently of selector SQL preparation.
The actual PostgreSQL fixture covers duplicate/missing outcomes and original
before values and a real inactive tombstone, plus disjoint state/next-work application and missing-key overlap
that rejects before the ordinary clock. Pure regressions cover exact EOF,
reordered/wrong/extra inputs, tombstones, foreign job/identity/sequence, retained
large before values, capacity failure, late corruption after overlap, suspended
sink backpressure and owner cleanup.

This remains a private prerequisite. Public administration, complete canonical
publication/result/receipt limits, durable TooLarge, preview, S3/Turso parity,
Snorri integration and settled comparable benchmarks remain required.
