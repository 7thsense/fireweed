# pqueue Helm Chart

This chart deploys the `pqueue-service` HTTP runtime. Backend selection is
controlled by `backend.profile`.

## postgres_native

Production operators using `backend.profile=postgres_native` must provide a
reachable PostgreSQL database and a Kubernetes Secret containing the connection
URL.

Required values:

```yaml
backend:
  profile: postgres_native
  shardCount:
    min: 1
    max: 1
  postgres:
    existingSecret: pqueue-postgres
    databaseUrlKey: database-url
```

The referenced Secret key must contain a URL accepted by `tokio-postgres`, for
example:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: pqueue-postgres
type: Opaque
stringData:
  database-url: postgres://pqueue:pqueue@postgres.example:5432/pqueue
```

The chart exposes that key as `PQUEUE_POSTGRES_DATABASE_URL`. For the
`postgres_native` profile, `/readyz` opens a PostgreSQL connection and runs
`SELECT 1`; Kubernetes will not mark the pqueue Deployment ready until the
database accepts that query.

## object_log_sqlite_projection

Production operators using `backend.profile=object_log_sqlite_projection` must
provide the same PostgreSQL Secret for the control plane, plus S3-compatible
object storage settings and a Kubernetes Secret containing only object-store
credentials.

Required values:

```yaml
backend:
  profile: object_log_sqlite_projection
  postgres:
    existingSecret: pqueue-postgres
    databaseUrlKey: database-url
  objectLog:
    endpoint: http://minio:9000
    bucket: pqueue-object-log
    region: us-east-1
    segmentMaxCommands: 1024
    existingSecret: pqueue-object-log
    accessKeyIdKey: access-key-id
    secretAccessKeyKey: secret-access-key
  sqliteProjection:
    mountPath: /var/lib/pqueue/projection
persistence:
  enabled: true
```

The chart renders endpoint, bucket, region, segment count, and SQLite projection
path into the ConfigMap. `PQUEUE_OBJECT_LOG_ACCESS_KEY_ID`,
`PQUEUE_OBJECT_LOG_SECRET_ACCESS_KEY`, and `PQUEUE_POSTGRES_DATABASE_URL` are
always sourced from Kubernetes Secret keys, so access keys, secret keys, and
local CI fixture credentials are not chart defaults.

When persistence is enabled, the chart creates or references a PVC and mounts it
at `backend.sqliteProjection.mountPath`. When persistence is disabled, the same
mount path is backed by `emptyDir`.
