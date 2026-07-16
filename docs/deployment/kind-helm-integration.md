# kind Helm integration harness

`scripts/ci/kind-helm-test.sh` installs the pqueue Helm chart into a disposable
`kind` cluster and exercises the RESP runtime.

Use storage axes:

```sh
bash scripts/ci/kind-helm-test.sh --log-backend objectlog --projection-backend inmemory
bash scripts/ci/kind-helm-test.sh --log-backend objectlog --projection-backend sqlite
bash scripts/ci/kind-helm-test.sh --log-backend objectlog --projection-backend hybrid
```

The release smoke path covers all three object-log projection modes above. The
`sqlite` projection persists its relational state on the chart's storage volume;
`hybrid` persists the same SQLite projection and rebuilds its hot in-memory
serving image after restart. Other chart axis combinations are documented in the
deployment-readiness matrix.

The harness builds `pqueue:ci`, creates a kind cluster, loads the image, installs
the chart with the matching CI values, waits for rollout, checks RESP `PING`,
`XADD`, and `XREADGROUP`, restarts the Deployment, and verifies the queue is
readable after restart. Thus the `objectlog/hybrid` lane proves PING, write,
rollout restart, and readback through the real runtime composition rather than
only rendering its Helm values.
