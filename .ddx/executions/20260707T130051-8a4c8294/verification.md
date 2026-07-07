# Verification

Bead: `pqueue-13b60e5b`

Validated:
- `cargo test -p pqueue-server TestCommandKindWireSerializationIsStable -- --nocapture`
- `cargo test -p pqueue-server TestChangeRecordSinkConfigSelectsKafkaProducerPath -- --nocapture`
- `cargo test -p pqueue-server TestFjordBootstrapConfigWiresEmbeddedSurface -- --nocapture`
- `go test ./...`

Result:
- All commands completed successfully.
- The root Go harness finished with `ok github.com/telepathdata/7thsense-pqueue 270.000s`.
