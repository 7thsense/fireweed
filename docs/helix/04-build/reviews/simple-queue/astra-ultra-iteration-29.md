# Astra Ultra review — iteration 29

Model: gpt-6-astra, Ultra. Base: cfdd102c.

APPROVE one concrete fireweed::queue::Queue consumer facade and native PostgreSQL
construction. Private Arc<dyn QueueStore> preserves backend encapsulation; a
single macro directly delegates all nine mandatory methods and the internal port
without new tasks, admission, clocks or cancellation behavior. Public DTOs, Bytes,
Clock/SystemClock/UtcTimestamp, ConfigSecret and the future alias are nameable
through fireweed alone. Redacted PostgreSQL settings and optional feature gating
are consistent; clone shutdown affects shared admission. No backend getter,
injection or unsupported capability surface is introduced.

The explicitly registered public-only fixture exercises every shared semantic
scenario entry point, exact retained receipt equality across reopen, strengthened
acknowledged state/work replay, and Busy after shutdown through another clone.
Verification: expanded public native fixture passes; no-default-feature build
passes (three existing legacy dead-code warnings), native-postgres library build
passes, default S3/Turso feature build passes with stock Turso0.8.2, strict native
facade library/public-test Clippy passes, formatting and independent diff hygiene
pass. Cargo lock adds only the existing conformance dev dependency.

This does not restore retired PostgreSQL axes on the legacy facade. S3/Turso
construction, broad administration, actual consumer migration and matched release
performance qualification remain explicitly outstanding.
