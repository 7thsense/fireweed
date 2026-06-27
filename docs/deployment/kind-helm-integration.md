# kind Helm integration harness

`scripts/ci/kind-helm-test.sh` installs the pqueue Helm chart into a disposable
`kind` cluster and exercises the RESP runtime.

Use storage axes, not a backend profile:

```sh
bash scripts/ci/kind-helm-test.sh --log-backend objectlog --projection-backend inmemory
```

This is the only runtime combination currently wired by `pqueue-server` for the
release smoke path. Other chart axis combinations render statically, but the
server exits loudly until their composition roots are implemented.

The harness builds `pqueue:ci`, creates a kind cluster, loads the image, installs
the chart with the matching CI values, waits for rollout, checks RESP `PING`,
`XADD`, and `XREADGROUP`, restarts the Deployment, and verifies the queue is
readable after restart.
