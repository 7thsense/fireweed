# Iteration 68: shared durable payload append state machine

The existing command append wrapper supplies encoding and record count to one
shared append state machine. Encoding remains inside the epoch-checked queue
metadata permit; that permit drops before the sole durable produce. Existing
pre-position timeout, sequencer fencing, post-position ambiguity, diagnostics,
and sparse high-water publication remain shared. Nonpositive counts fail before
produce. Legacy command encoding is unchanged.

Physical local regression checks a wrong epoch never encodes, zero count does
not append, opaque payloads retain exact record counts and positions, and cold
bounded replay reaches actual EOF despite a sparse persisted high-water hint.
Existing concurrent append, ownership takeover, timeout and source guards run.

No public raw append API, owner completion or QueueStore activation is claimed.
Those must retain admission through physical completion and handle ambiguous
post-position outcomes before native queue factories are activated.
