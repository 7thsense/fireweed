# kind Helm integration harness

`scripts/ci/kind-helm-test.sh` installs the Fireweed Queue Helm chart into a disposable
`kind` cluster and exercises the RESP runtime.

Use public storage axes:

```sh
bash scripts/ci/kind-helm-test.sh --log-backend filesystem --projection-backend memory
bash scripts/ci/kind-helm-test.sh --log-backend filesystem --projection-backend sqlite
bash scripts/ci/kind-helm-test.sh --log-backend filesystem --projection-backend turso
bash scripts/ci/kind-helm-test.sh --log-backend postgres --projection-backend memory
bash scripts/ci/kind-helm-test.sh --log-backend postgres --projection-backend sqlite
bash scripts/ci/kind-helm-test.sh --log-backend postgres --projection-backend turso
bash scripts/ci/kind-helm-test.sh --log-backend postgres --projection-backend postgres
```

The live kind smoke set is the chart-installable deployable set above
(filesystem × {memory,sqlite,turso} and postgres × {memory,sqlite,turso,postgres}).
That set equals the harness allow-list and the documented runtime cells in this
file. The full 20-cell matrix is statically proven by `scripts/ci/helm-gate.sh`;
process-local Class B cells (memory log) rely on T0–T3 and do not claim multi-node
kind coverage.

The `sqlite` and `turso` projections persist relational state on the chart's
storage volume; `memory` is ephemeral and rebuilds from a durable log after
restart when the log axis is durable. Chart defaults select Turso.

The harness builds `fireweed-service:ci`, creates a kind cluster, loads the image, installs
the chart with the matching CI values, waits for rollout, checks RESP `PING`,
`XADD`, and `XREADGROUP`, restarts the Deployment for durable log axes, and
verifies the queue is readable after restart. These smoke checks do not
establish throughput or latency.
