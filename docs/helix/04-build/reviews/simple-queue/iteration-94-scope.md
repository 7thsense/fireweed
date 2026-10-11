# Iteration 94: shared native snapshot authority

Checkpoint and current-claim replay now use one private native snapshot reader.
It validates admitted same-operation checksummed genesis, original semantic
schema, full exact address, raw definition and secret, fixed-width progress and
nonzero/nonregressing historical epoch. SQL keeps corrupt stored definition and
secret out of the materialized result. The reader returns an admitted definition
and one native progress snapshot, without exporting a transaction handle.

Replay still compares recovered offset, insertion sequence and historical epoch
exactly inside its Deferred transaction. Checkpoint remains one native SQL
point-in-time snapshot, with no claim of authoritative log coverage. Existing
sixteen physical tests and strict local all-target Clippy must pass unchanged.

This removes duplicate authority checks before native filtered selection and
other queue readers are added; public backend activation and qualification remain
outstanding.
