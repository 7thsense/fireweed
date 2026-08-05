# P6p — Snorri access, runner, and integration branch provisioning

| Field | Value |
| --- | --- |
| Plan key | `P6p` |
| Bead | `fireweed-6ae3d8ed` |
| Capability ID | `SNORRI-RUNNER-PROVISIONING` |
| Machine-readable attestation | [`p6p-runner-attestation.json`](./p6p-runner-attestation.json) |
| Provisioned at (UTC) | See `selected_at` in the attestation JSON |

This record is **external provisioning only**. It does not execute P6s Snorri
S3 durability acceptance. Downstream beads (`fireweed-2886078a` / P6s,
`fireweed-4915c30f` / P7x) consume this runner + pin description.

**Garage / `eldir` are not accepted as implicit provisioning.** The live S3
endpoint is the P1s-selected MinIO CAS endpoint (bead `fireweed-f5fa7380`).

---

## 1. Snorri repository access

| Item | Observation |
| --- | --- |
| Repository | `https://github.com/telepathdata/snorri.git` (private) |
| Org | `telepathdata` |
| Worker GitHub identity | `easel` |
| Permission | `ADMIN` (`pull` + `push` + maintain) |
| Local checkout | `/home/erik/Projects/snorri` |
| `git fetch origin main` | succeeds |

Named owner grant is satisfied by admin membership on the private repository;
the local clone already has `origin` wired to the GitHub remote.

---

## 2. Maintained branch and test commands

| Item | Value |
| --- | --- |
| Maintained integration branch | `main` (GitHub default branch) |
| Observed HEAD at provisioning | `a0afd18eaface2772e0080fc8f768a7768bc7d44` |

### Offline / feature-matrix commands (Snorri checkout)

```bash
cd /home/erik/Projects/snorri
cargo test --workspace --all-targets

cargo test -p snorri-fireweed --no-default-features --features memory
cargo test -p snorri-fireweed --no-default-features --features sqlite
cargo test -p snorri-fireweed --no-default-features --features postgres
cargo test -p snorri-fireweed --no-default-features --features objectlog,sqlite
cargo test -p snorri-fireweed --no-default-features --features objectlog,postgres
cargo test -p snorri-fireweed --all-features
```

TP-004 also documents the corresponding `cargo check` matrix against a sibling
Fireweed path/rev pin.

### Live S3 × objectlog × (sqlite projection + postgres control) on current main

Historical harness still present on Snorri `main` (env names retain `GARAGE`
legacy prefix; provider must be P1s-attested MinIO, not Garage):

```bash
# After scripts/ci/snorri-runner-preflight.sh exports mapped env vars:
cd /home/erik/Projects/snorri
bash scripts/test-garage-live-mutations.sh
# equivalent cargo filter:
# cargo test -p snorri-fireweed --no-default-features --features objectlog,sqlite,postgres \
#   tests::live_mutations_are_authoritative_through_garage_sqlite_facade -- --exact --nocapture
```

TP-004 target env names for provider-neutral live rows (P6s should migrate
harnesses to these):

- `SNORRI_S3_TEST_ENDPOINT`
- `SNORRI_S3_TEST_BUCKET`
- `SNORRI_S3_TEST_REGION`
- `SNORRI_S3_TEST_ACCESS_KEY`
- `SNORRI_S3_TEST_SECRET_KEY`
- `SNORRI_FIREWEED_POSTGRES_URL`

Postgres-only live facade tests use `SNORRI_FIREWEED_POSTGRES_URL` alone.

---

## 3. How Fireweed revisions are pinned in Snorri

Primary mechanism (Snorri workspace root `Cargo.toml`):

```toml
# [workspace.dependencies]
fireweed = {
  git = "https://github.com/telepathdata/fireweed.git",
  rev = "7552f62a002e3c82ca0046ebee543c585c9808c8",
  version = "0.29.2",
  default-features = false
}
```

| Mechanism | Used by default? | Notes |
| --- | --- | --- |
| Git rev pin | **Yes** | Exact SHA in `Cargo.toml` + `Cargo.lock` `source = git+...?rev=...#...` |
| crates.io | No | Not the active pin |
| Path pin | Optional for pre-release integration | TP-004 allows `path = "../fireweed/crates/fireweed"` (or a worktree path) while landing Snorri commits against an unreleased Fireweed tip; restore git rev/tag before release acceptance |

`crates/snorri-fireweed` depends on `fireweed = { workspace = true }` and
forwards feature flags (`memory`, `sqlite`, `postgres`, `objectlog`).

At provisioning time:

- Snorri pin rev `7552f62a…` is an ancestor of Fireweed `main` tip `09053231…`
  (includes MinIO S3 P1s + P3v lineage).
- P6s/P7x are expected to **re-pin** (git rev or temporary path) to the Fireweed
  revision under test, then record both SHAs in their run-owned attestations.

Identity proof before accepting Snorri results:

```bash
cd /home/erik/Projects/snorri
rg -n 'name = "fireweed"' -A6 Cargo.lock | head -20
cargo tree -p snorri-fireweed -i fireweed
```

---

## 4. Named provider-neutral Snorri runner

| Item | Value |
| --- | --- |
| Runner identity | `fireweed-p6p-snorri@sindri` |
| Host | `sindri` (Linux WSL2 x86_64, Ubuntu 24.04) |
| Topology | Host-local Docker services on loopback; no Garage/eldir dependency |
| Network | Reachability to P1s MinIO on `127.0.0.1:<ephemeral>` and Postgres on `127.0.0.1:55432` |
| Toolchain | `rustc`/`cargo` 1.97.1 (Homebrew), Docker 29.x, `psql` client |

Resource / network capabilities used by this runner:

- Docker container lifecycle for P1s MinIO and local Postgres
- Loopback TCP to qualified S3 endpoint and Postgres control plane
- Host-managed secret files under `/tmp/fireweed-s3-secrets/` (mode `0600`)
- Optional Tailscale interface present on host; **not required** for loopback services

### P1s live supported-S3 endpoint (not Garage)

Consumed from host path **outside** the repository (default
`/tmp/fireweed-s3-secrets/`), produced by
`bash scripts/ci/s3-qualification-endpoint.sh provision`:

| Non-secret field | Value at provisioning |
| --- | --- |
| Provider | `minio` (P1s selected; Garage rejected) |
| Endpoint | `http://127.0.0.1:40735` |
| Bucket | `fireweed-qual-4e47448fea` |
| Region | `us-east-1` |
| TLS | plaintext-loopback |
| Native create-only / conditional update | attested `true` / `true` |
| Attestation file | `/tmp/fireweed-s3-secrets/s3-native-cas-capability-attestation.json` |
| Credentials file | `/tmp/fireweed-s3-secrets/credentials.env` |

Reachability proof (secrets never printed):

```bash
bash scripts/ci/snorri-runner-preflight.sh
# Authenticates HEAD bucket + PUT/DELETE probe under fireweed-p6p-reachability/
# and SELECT 1 against the isolated Postgres database.
```

Observed during P6p:

- TCP connect to endpoint: ok
- Unauthenticated GET: HTTP 403 (service up, auth required)
- Authenticated `HEAD` bucket: 200
- Authenticated PUT + DELETE probe object: 200 / 204

### Isolated live PostgreSQL control plane

| Item | Value |
| --- | --- |
| Container | `fireweed-postgres-1` (`postgres:16`) |
| Host / port | `127.0.0.1:55432` |
| Database | `fireweed_snorri_p6p` (dedicated; created for this runner) |
| User | `fireweed` |
| URL template (no password in git) | `postgres://fireweed:<host-managed-password>@127.0.0.1:55432/fireweed_snorri_p6p` |
| Snorri env var | `SNORRI_FIREWEED_POSTGRES_URL` |

Password material stays in Docker/`POSTGRES_PASSWORD` / operator shell env only.
Do not commit connection strings that embed passwords.

---

## 5. Secret isolation

| Policy | Detail |
| --- | --- |
| Secret directory | `/tmp/fireweed-s3-secrets/` (override with `FIREWEED_S3_SECRET_DIR`) |
| Never commit | credential values, signed URLs with keys, `.env.garage-e3` |
| Forbidden in-repo paths | repo-root `credentials.env`, `scripts/ci/credentials.env`, `.env.garage-e3` |
| Attestation records | paths and non-secret endpoint/bucket/region/provider only |
| Mapping helper | `scripts/ci/snorri-runner-preflight.sh` sources P1s env and optionally exports Snorri legacy live harness names |

P1s → current Snorri live harness env map (legacy names):

| P1s / Fireweed secret | Snorri live harness (main) |
| --- | --- |
| `FIREWEED_S3_TEST_ENDPOINT` | `SNORRI_GARAGE_S3_ENDPOINT` |
| `FIREWEED_S3_TEST_BUCKET` | `SNORRI_GARAGE_S3_BUCKET` |
| `FIREWEED_S3_TEST_REGION` | `SNORRI_GARAGE_S3_REGION` |
| `FIREWEED_S3_TEST_ACCESS_KEY` | `SNORRI_GARAGE_S3_ACCESS_KEY` |
| `FIREWEED_S3_TEST_SECRET_KEY` | `SNORRI_GARAGE_S3_SECRET_KEY` |
| (Postgres URL) | `SNORRI_FIREWEED_POSTGRES_URL` |

Also set `SNORRI_GARAGE_TEST=1` and a unique
`SNORRI_GARAGE_NAMESPACE_ROOT=snorri-garage-live-<token>` for the historical
test gate (script does this).

---

## 6. Cleanup

| Resource | Cleanup |
| --- | --- |
| P6p S3 probe keys | Preflight deletes its own `fireweed-p6p-reachability/*` objects |
| Snorri live test prefix | `scripts/test-garage-live-mutations.sh` EXIT trap empties encoded prefix |
| P1s MinIO container | `bash scripts/ci/s3-qualification-endpoint.sh teardown` when retiring the qualification endpoint (shared with other S3 beads—coordinate) |
| Isolated Postgres DB | `DROP DATABASE IF EXISTS fireweed_snorri_p6p;` on the local instance when no longer needed |
| Postgres container | stop/rm only if no other Fireweed work depends on `fireweed-postgres-1` |
| Host secrets | `rm -rf /tmp/fireweed-s3-secrets` (operator decision; never was in git) |

---

## 7. Governing references

- Storage closure brief: `docs/helix/04-build/storage-matrix-completion-brief.md` (P6p)
- Snorri acceptance plan: `docs/helix/03-test/test-plans/TP-004-fireweed-facade-and-snorri-acceptance.md`
- P1s S3 qualification: `scripts/ci/s3-qualification-endpoint.sh`, `scripts/ci/s3-matrix-job-requirements.md`
- Authority manifest (consume only): `docs/helix/04-build/storage-authority-manifest.json`
- Garage non-selection: `docs/operator/object-log-authority-compatibility.md`

---

## 8. Result

| Check | Status |
| --- | --- |
| Snorri repo access for worker | provisioned |
| Maintained branch + test commands recorded | yes (`main`) |
| Fireweed pin mechanism recorded | git rev pin (+ path-pin option) |
| Provider-neutral runner with P1s S3 reachability | yes (`fireweed-p6p-snorri@sindri`) |
| Isolated Postgres control plane | yes (`fireweed_snorri_p6p`) |
| Secret isolation | host `/tmp/fireweed-s3-secrets/` only |
| Garage/eldir used as implicit provisioning | **no** |
| Storage closure blocked on external acceptance | **no** |

Preflight command for operators and P6s:

```bash
bash scripts/ci/snorri-runner-preflight.sh
# optional: export mapped env into current shell
eval "$(bash scripts/ci/snorri-runner-preflight.sh --export-env)"
```
