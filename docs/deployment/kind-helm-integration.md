# kind Helm integration harness

`scripts/ci/kind-helm-test.sh` installs the pqueue Helm chart into a disposable
`kind` cluster and exercises the RESP runtime.

Use storage axes:

```sh
bash scripts/ci/kind-helm-test.sh --log-backend objectlog --projection-backend inmemory
bash scripts/ci/kind-helm-test.sh --log-backend objectlog --projection-backend sqlite
bash scripts/ci/kind-helm-test.sh --log-backend objectlog --projection-backend hybrid
bash scripts/ci/kind-helm-test.sh --log-backend objectlog --projection-backend hybrid-async
```

The release smoke path covers all four object-log projection modes above. The
`sqlite` projection persists its relational state on the chart's storage volume;
`hybrid` persists the same SQLite projection and rebuilds its hot in-memory
serving image after restart. `hybrid-async` serves from hot memory while
checkpointing to SQLite asynchronously; its live lane verifies that a write is
recoverable from the durable object log across a rollout restart even if the
checkpoint lags. Other chart axis combinations are documented in the
deployment-readiness matrix.

The harness builds `pqueue:ci`, creates a kind cluster, loads the image, installs
the chart with the matching CI values, waits for rollout, checks RESP `PING`,
`XADD`, and `XREADGROUP`, restarts the Deployment, and verifies the queue is
readable after restart. Thus the `objectlog/hybrid` and
`objectlog/hybrid-async` lanes prove functional PING, write, rollout restart,
and readback through their real runtime compositions rather than only rendering
Helm values. These smoke checks do not establish throughput or latency.
