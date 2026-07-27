#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
repo="$REPO_ROOT" out="" version="" revision="" builder="local-dry-run"
while (($#)); do
  case "$1" in
    --repo) repo="$2"; shift 2 ;; --out) out="$2"; shift 2 ;;
    --version) version="$2"; shift 2 ;; --revision) revision="$2"; shift 2 ;;
    --builder) builder="$2"; shift 2 ;; *) echo "unknown argument: $1" >&2; exit 64 ;;
  esac
done
[[ -n "$out" && -n "$version" && -n "$revision" && -n "$builder" ]] || {
  echo "usage: $0 --out <dir> --version <semver> --revision <sha> [--repo <dir>] [--builder <id>]" >&2; exit 64;
}
repo="$(realpath "$repo")"
revision="$(git -C "$repo" rev-parse "${revision}^{commit}")"
[[ "$revision" =~ ^[0-9a-f]{40}$ ]] || { echo "revision must resolve to a full commit" >&2; exit 1; }
[[ "$revision" == "$(git -C "$repo" rev-parse HEAD)" ]] || { echo "revision must equal checked-out HEAD" >&2; exit 1; }
[[ ! -e "$out" ]] || { echo "output already exists: $out" >&2; exit 1; }
mkdir -p "$out"
archive="fireweed-${version}-source.tar.gz"
sbom="fireweed-${version}-source.spdx.json"
provenance="fireweed-${version}-source-provenance.json"
metadata="$(mktemp "${TMPDIR:-/tmp}/fireweed-source-metadata.XXXXXX")"
trap 'rm -f "$metadata"' EXIT
git -C "$repo" archive --format=tar --prefix="fireweed-${version}/" "$revision" | gzip -n -9 >"$out/$archive"
cargo metadata --manifest-path "$repo/Cargo.toml" --locked --no-deps --format-version 1 >"$metadata"
python3 - "$metadata" "$out/$sbom" "$version" "$revision" <<'PY'
import datetime, json, subprocess, sys
metadata_path, output, version, revision = sys.argv[1:]
metadata = json.load(open(metadata_path, encoding="utf-8"))
epoch = int(subprocess.check_output(["git", "-C", metadata["workspace_root"], "show", "-s", "--format=%ct", revision], text=True).strip())
created = datetime.datetime.fromtimestamp(epoch, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
packages, relationships = [], []
for index, package in enumerate(sorted(metadata["packages"], key=lambda p: p["name"]), 1):
    spdx_id = f"SPDXRef-Package-{index}"
    packages.append({"SPDXID": spdx_id, "name": package["name"], "versionInfo": package["version"], "downloadLocation": "NOASSERTION", "filesAnalyzed": False, "licenseConcluded": "NOASSERTION", "licenseDeclared": package.get("license") or "NOASSERTION", "copyrightText": "NOASSERTION"})
    relationships.append({"spdxElementId": "SPDXRef-DOCUMENT", "relationshipType": "DESCRIBES", "relatedSpdxElement": spdx_id})
document = {"spdxVersion": "SPDX-2.3", "dataLicense": "CC0-1.0", "SPDXID": "SPDXRef-DOCUMENT", "name": f"fireweed-{version}-source", "documentNamespace": f"https://github.com/telepathdata/fireweed/releases/{revision}/sbom", "creationInfo": {"created": created, "creators": ["Tool: fireweed-source-preview-builder"]}, "packages": packages, "relationships": relationships}
with open(output, "w", encoding="utf-8") as handle: json.dump(document, handle, indent=2, sort_keys=True); handle.write("\n")
PY
archive_sha="$(sha256sum "$out/$archive" | awk '{print $1}')"
sbom_sha="$(sha256sum "$out/$sbom" | awk '{print $1}')"
commit_epoch="$(git -C "$repo" show -s --format=%ct "$revision")"
python3 - "$out/$provenance" "$revision" "$builder" "$version" "$commit_epoch" "$archive" "$archive_sha" "$sbom" "$sbom_sha" <<'PY'
import datetime, json, sys
output, revision, builder, version, epoch, archive, archive_sha, sbom, sbom_sha = sys.argv[1:]
generated = datetime.datetime.fromtimestamp(int(epoch), datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
document = {"schema": "fireweed.source_preview_provenance.v1", "source": {"repository": "https://github.com/telepathdata/fireweed", "commit": revision}, "builder": {"id": builder}, "invocation": {"mode": "dry-run", "version": version}, "generated_at": generated, "subjects": [{"name": archive, "digest": {"sha256": archive_sha}}, {"name": sbom, "digest": {"sha256": sbom_sha}}], "claims": {"slsa_level": None, "signed": False}}
with open(output, "w", encoding="utf-8") as handle: json.dump(document, handle, indent=2, sort_keys=True); handle.write("\n")
PY
bash "$SCRIPT_DIR/write-checksums.sh" "$out"
bash "$SCRIPT_DIR/verify-source-preview-artifacts.sh" --dist "$out" --version "$version" --revision "$revision"
echo "source preview dry-run artifacts: $out"
