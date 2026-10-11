# Iteration 56: request-bound FWQP2 frame preparation

Base: d1c27b74. Administrative fingerprints use an explicit frozen six-field
projection: the ordinary five fields followed by a direct administration value
(intent then max_result_bytes), with no Option::Some marker. Only this path uses
fireweed-api007-v4; ordinary/claim/insert fingerprints retain v3 and commit storage
domain remains 2. A literal payload and SHA golden pin the layout.

FWQP1 writer and strict reader/goldens remain unchanged. The separate shared
version-dispatch reader carries frame format through full validation and every
restartable receipt iterator. FP4 is domain-2/FWQP2-only; administrative binary
success tag 4 and TooLarge reason 8 are rejected for v1/FP3/inserts. Binary tags
0/1 remain legacy Committed/Rejected and differ from Postcard ordinals 2/3.
Single FP4 rejection frames have no commands or logical sequence advance.

Receipt binding checks ordinary producer-order identities, addressed positions/
duplicates/original before bits/requested projections, and broad clause IDs/order.
The admitted builder retains an input-bounded identity map plus one last key;
there is no broad row map. It binds ordinary commands to exact canonical delta
bytes using the shared writer and SHA, and addressed commands to accepted
result identities/versions/deletion shape. Broad Changed and Deleted counts must each match their command effect.
All ordinary work identities, including no-ops with no command, reject
administrative command overlap. Duplicate/order/count/byte/EOF failures poison the builder. Every
copy checks its end against the declared admitted size before allocation/copy.
Complete frame validation precedes exposure, and the final frame owns its job
reservation through cleanup. Data/fallback frame and Postcard receipt sizes are
separate per-logical-group measurements.

This codec does not establish fenced source/spool integrity, uniqueness/ownership
or storage ACK authority. Logical overflow execution and bounded addressed result
retention after overflow remain required before public activation, followed by
identical preview/receipt tests, S3/Turso, Snorri cutover and settled benchmarks.
