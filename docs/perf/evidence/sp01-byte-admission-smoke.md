# SP-01 Byte-Admission Performance Smoke

Date: 2026-07-18. Command:

`cargo test -p fireweed-objectlog byte_admission_serialization_microbenchmark --lib -- --ignored --nocapture`

The local debug-build smoke encoded 100 commands for 2,000 iterations. The pre-SP-01 single-serialization
shape took 2.939 s; serialization plus exact peak-charge arithmetic and record-vector ownership took 2.942 s
(1.001 ratio). This is a developer smoke, not TP-002 release evidence: allocator noise and debug codegen make
the absolute values unsuitable for a performance claim. It demonstrates that charge calculation did not add
an obvious serialization-scale penalty and provides a reproducible guard while the release E3 matrix supplies
the required throughput and p99 comparison.
