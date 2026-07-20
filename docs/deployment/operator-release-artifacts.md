# Operator Release Artifacts

This is the operator-facing location for obtaining and verifying pqueue release
artifacts. Replace `OWNER`, `REPO`, and `v0.9.0` with the release repository and
tag you are installing from.

The current release workflow publishes:

| Artifact | Coordinate |
|----------|------------|
| Container image | `ghcr.io/OWNER/pqueue-service:<version>` and `ghcr.io/OWNER/pqueue-service:sha-<commit>` |
| Container image digest evidence | GitHub Release asset `pqueue-service-image.txt` |
| Helm chart package | GitHub Release asset `pqueue-<version>.tgz` |
| Helm chart evidence | GitHub Release asset `pqueue-helm-chart.txt` |
| Binary archives | GitHub Release assets `pqueue-<version>-<target-triple>.tar.gz` |
| Deployment proof | GitHub Release assets `deployment-proof.json` and `deployment-proof.md` |
| Checksums | GitHub Release asset `SHA256SUMS` |

For example, release tag `v0.9.0` uses version `0.9.0`, so the chart package is
`pqueue-0.9.0.tgz` and binary archives are named like
`pqueue-0.9.0-x86_64-linux.tar.gz`. The `v0.9.0` workflow publishes the Helm
chart as a GitHub Release package asset; it does not publish an OCI chart.

## Download

With the GitHub CLI:

```sh
OWNER=<github-owner>
REPO=pqueue
TAG=v0.9.0
VERSION="${TAG#v}"
DIST_DIR="release-${TAG}"

mkdir -p "$DIST_DIR"
gh release download "$TAG" \
  --repo "${OWNER}/${REPO}" \
  --pattern "pqueue-${VERSION}-*.tar.gz" \
  --pattern "pqueue-${VERSION}.tgz" \
  --pattern "pqueue-service-image.txt" \
  --pattern "pqueue-helm-chart.txt" \
  --pattern "deployment-proof.json" \
  --pattern "deployment-proof.md" \
  --pattern "SHA256SUMS" \
  --dir "$DIST_DIR"
```

Without `gh`, download the same assets from:

```text
https://github.com/OWNER/REPO/releases/tag/v0.9.0
```

## Verify Checksums

Run checksum verification before extracting binary archives or installing the
chart package:

```sh
if command -v sha256sum >/dev/null 2>&1; then
  (cd "$DIST_DIR" && sha256sum -c SHA256SUMS)
else
  (cd "$DIST_DIR" && shasum -a 256 -c SHA256SUMS)
fi
```

`SHA256SUMS` covers the release files, including binary archives, the Helm chart
package, image and chart evidence, and both deployment-proof files.

Operators with a source checkout can also verify a downloaded distribution with
the repository helper from the repository root:

```sh
bash scripts/release/verify-release-artifacts.sh \
  --version "$VERSION" \
  --dist "$DIST_DIR"
```

## Verify Container Image Digest

The release workflow writes the pushed image digest to
`pqueue-service-image.txt`. Verify the tag still resolves to that digest before
deployment:

```sh
IMAGE_OWNER="$(printf '%s' "$OWNER" | tr '[:upper:]' '[:lower:]')"
IMAGE="ghcr.io/${IMAGE_OWNER}/pqueue-service"
DIGEST="$(awk -F= '$1 == "digest" { print $2 }' "${DIST_DIR}/pqueue-service-image.txt")"
REMOTE_DIGEST="$(docker buildx imagetools inspect "${IMAGE}:${VERSION}" | awk '/Digest:/ { print $2; exit }')"

test "$REMOTE_DIGEST" = "$DIGEST"
docker pull "${IMAGE}@${DIGEST}"
```

Deploy by digest where possible:

```text
ghcr.io/<lowercase-owner>/pqueue-service@sha256:<digest>
```

`pqueue-service-image.txt` also records `version_coordinate`, `sha_coordinate`,
and `digest_coordinate` for audit trails.

## Deployment Release Proof

The release workflow publishes its release-note-ready deployment proof as
`deployment-proof.json` and `deployment-proof.md`. Source checkouts can generate
the same file shape after the release artifacts are present:

```sh
bash scripts/ci/deployment-release-gate.sh
```

The gate writes:

- `target/deployment-release-gate/deployment-proof.json`
- `target/deployment-release-gate/deployment-proof.md`
- `target/deployment-release-gate/release-dist/`

The JSON proof records the commit SHA, Helm chart version and package path,
container image tag/digest when supplied by `PQUEUE_IMAGE_TAG`,
`PQUEUE_IMAGE_DIGEST`, `PQUEUE_IMAGE_COORDINATE`, or
`PQUEUE_IMAGE_EVIDENCE_FILE`, every command and exit status, the storage
combination matrix, local Docker/kind skips, and supporting artifact paths. If image
coordinates are unavailable in a local non-release run, the image fields are
recorded as `unavailable` and the gate can still pass non-cluster checks.

Release notes should cite the JSON `release_notes` block or the Markdown summary
for the exact command list, chart package/version, storage matrix, and
supporting artifact paths. A local Docker/kind skip is scoped to the disposable
kind storage matrix only and is not CI matrix proof.

## Storage Boundary

The chart exposes separate storage axes:

- log backend: `objectlog` or `postgres`
- projection backend: `inmemory`, `sqlite`, `turso`, `hybrid`, `hybrid-async`, or
  `postgres`

`objectlog/hybrid-strict` is deliberately absent from that chart enum. It is an
experimental runtime path selectable only through environment or direct
configuration, not a chart-selectable or production-supported profile. The
static Helm gate names and verifies the exact schema rejection, so runtime
wiring cannot be mistaken for public deployment support.

The current live-kind matrix covers `objectlog` plus `inmemory`, `sqlite`,
`hybrid`, or `hybrid-async`, and `postgres` plus `inmemory`, `sqlite`, or
`postgres`. Other rendered combinations remain outside the live deployment
claim until their matrix entries and evidence land.
