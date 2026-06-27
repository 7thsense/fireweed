# pqueue Container Image and Runtime Configuration Contract

The release image entrypoint is `pqueue-service`, the RESP server built from
`crates/pqueue-server`.

## Environment

| Key | Required | Default | Meaning |
|-----|----------|---------|---------|
| `PQUEUE_LISTEN_ADDR` | no | `0.0.0.0:8080` | RESP listen address. |
| `PQUEUE_LOG_BACKEND` | no | `objectlog` | Log backend axis: `objectlog`, `postgres`, `sqlite`, or `memory`. |
| `PQUEUE_PROJECTION_BACKEND` | no | `inmemory` | Projection backend axis: `inmemory`, `sqlite`, or `postgres`. |
| `PQUEUE_OBJECT_LOG_ROOT` | when log is `objectlog` | `/var/lib/pqueue/object-log` | Local object-log root. |
| `PQUEUE_SQLITE_LOG_PATH` | when log is `sqlite` | `/var/lib/pqueue/pqueue-log.db` | Local SQLite log path. |
| `PQUEUE_BOOTSTRAP_QUEUES` | no | `t1:q1` | Comma-separated `tenant:queue` bootstrap list. |
| `PQUEUE_RECLAIM_INTERVAL_MS` | no | `1000` | Reclaim tick interval. |

The current server composition root wires `memory/inmemory`, `sqlite/inmemory`,
and `objectlog/inmemory`. Other combinations fail at startup with an explicit
unsupported-storage message.

The Helm chart exposes storage axes as:

- `storage.log.backend`
- `storage.projection.backend`

Kubernetes probes are TCP probes against the RESP port.
