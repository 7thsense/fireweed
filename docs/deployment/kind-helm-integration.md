# kind Helm integration harness

`scripts/ci/kind-helm-test.sh` installs the Fireweed Queue Helm chart into a disposable
`kind` cluster and exercises the RESP runtime.

Use public storage axes:

```sh
bash scripts/ci/kind-helm-test.sh --log-backend filesystem --projection-backend memory
bash scripts/ci/kind-helm-test.sh --log-backend filesystem --projection-backend sqlite
bash scripts/ci/kind-helm-test.sh --log-backend postgres --projection-backend memory
bash scripts/ci/kind-helm-test.sh --log-backend postgres --projection-backend sqlite
bash scripts/ci/kind-helm-test.sh --log-backend postgres --projection-backend postgres
```

The release smoke path covers durable filesystem object-log projections and the
postgres log axis cells above. The `sqlite` projection persists its relational
state on the chart's storage volume; `memory` is ephemeral and rebuilds from a
durable log after restart when the log axis is durable. Other chart axis
combinations are documented in the deployment-readiness matrix.

The harness builds `fireweed-service:ci`, creates a kind cluster, loads the image, installs
the chart with the matching CI values, waits for rollout, checks RESP `PING`,
`XADD`, and `XREADGROUP`, restarts the Deployment for durable log axes, and
verifies the queue is readable after restart. These smoke checks do not
establish throughput or latency.
