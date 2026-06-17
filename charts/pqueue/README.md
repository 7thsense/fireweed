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
