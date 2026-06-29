# Helm static validation gate

`scripts/ci/helm-gate.sh` validates the pqueue Helm chart without creating a
cluster.

The chart storage contract is expressed as axes:

- `storage.log.backend`: `objectlog` or `postgres`
- `storage.projection.backend`: `inmemory`, `sqlite`, or `postgres`

The gate runs `helm lint --strict`, renders checked-in CI values for selected
axis combinations, asserts the rendered environment variables, and validates the
manifests with `kubeconform`.

```sh
bash scripts/ci/helm-gate.sh
```

Runtime smoke testing is separate:

```sh
bash scripts/ci/kind-helm-test.sh --log-backend objectlog --projection-backend inmemory
```
