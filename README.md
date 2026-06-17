# pqueue

## Documentation

- [Production deployment readiness](docs/helix/04-build/DEPLOYMENT-READINESS.md)
  defines the Helm, kind, `postgres_native`,
  `object_log_sqlite_projection`, MinIO, and S3/object-log release-readiness
  contract.
- [Container image and runtime config contract](docs/deployment/container-runtime-contract.md)
  defines the `pqueue-service` image entrypoint, environment/config keys, health
  endpoint/port, and backend-profile settings consumed by Helm.

## Container Image

Build and smoke-check the service image:

```sh
docker build -t pqueue:dev .
docker run --rm pqueue:dev --help
```

See the
[container runtime config contract](docs/deployment/container-runtime-contract.md)
for the full environment, health-probe, and backend-profile contract.
