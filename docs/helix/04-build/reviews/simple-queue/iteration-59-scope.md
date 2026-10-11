# Iteration 59: bounded disposable command capture

Base: c6d3ccb7. One validated canonical work-fragment writer now supports both
count-only measurement and encoding. Administrative scratch accepts a raw command
byte allowance and counts every accepted command. After permanent overflow it
continues validating/counting all commands and writing all uniqueness evidence,
while retaining no more command payloads. Packet identity/order/EOF checks remain
mandatory; sealing reconciles accepted command count with Changed+Deleted totals.

The owned blocking sink exposes the same bounded constructor and cleanup path.
Sealed scratch exposes total logical command count/bytes and overflow. Predecision
validation exhausts every retained command and the complete evidence streams;
public command iteration fails closed on overflow, so no partial stream can be
applied. Scratch IO, corruption, quota and admission failures remain Unknown.

Tests cover exact raw command N/N-1, 5001 selected rows including a no-op,
continued sibling-conflict discovery, late evidence truncation, and an 8 MiB body
update group under a 1 MiB scratch quota with full EOF and cleanup.

This is a raw command-capture allowance, not a full FWQP2 publication decision.
The containing group must derive it from its full header/receipt lower bound,
verify the context, then count actual ordinary commands and complete receipt after
business checks. Native integration, durable TooLarge receipts, public activation,
S3/Turso, Snorri and settled benchmarks remain outstanding.
