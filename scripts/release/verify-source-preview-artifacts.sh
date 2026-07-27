#!/usr/bin/env bash
set -euo pipefail
dist="" version="" revision=""
while (($#)); do
  case "$1" in --dist) dist="$2"; shift 2 ;; --version) version="$2"; shift 2 ;; --revision) revision="$2"; shift 2 ;; *) echo "unknown argument: $1" >&2; exit 64 ;; esac
done
[[ -d "$dist" && -n "$version" && "$revision" =~ ^[0-9a-f]{40}$ ]] || { echo "usage: $0 --dist <dir> --version <semver> --revision <full-sha>" >&2; exit 64; }
archive="fireweed-${version}-source.tar.gz"; sbom="fireweed-${version}-source.spdx.json"; provenance="fireweed-${version}-source-provenance.json"
for file in "$archive" "$sbom" "$provenance" SHA256SUMS; do [[ -s "$dist/$file" ]] || { echo "missing source preview artifact: $file" >&2; exit 1; }; done
actual="$(find "$dist" -maxdepth 1 -type f -printf '%f\n' | sort)"
expected="$(printf '%s\n' SHA256SUMS "$archive" "$provenance" "$sbom" | sort)"
[[ "$actual" == "$expected" ]] || { echo "unexpected source preview artifact set" >&2; exit 1; }
(cd "$dist" && sha256sum -c SHA256SUMS >/dev/null)
tar -tzf "$dist/$archive" | awk -v prefix="fireweed-${version}/" 'index($0,prefix)!=1 || $0 ~ /(^|\/)\.\.($|\/)/ {exit 1}' || { echo "source archive contains an unsafe or unexpected path" >&2; exit 1; }
python3 - "$dist" "$version" "$revision" "$archive" "$sbom" "$provenance" <<'PY'
import hashlib, json, pathlib, sys
dist, version, revision, archive, sbom_name, provenance_name = sys.argv[1:]; root = pathlib.Path(dist)
sbom=json.loads((root/sbom_name).read_text()); provenance=json.loads((root/provenance_name).read_text())
assert sbom.get("spdxVersion")=="SPDX-2.3" and sbom.get("packages") and revision in sbom.get("documentNamespace","")
assert provenance.get("schema")=="fireweed.source_preview_provenance.v1"
assert provenance.get("source",{}).get("commit")==revision and provenance.get("builder",{}).get("id")
assert provenance.get("invocation")=={"mode":"dry-run","version":version}
assert provenance.get("claims")=={"signed":False,"slsa_level":None}
subjects={item["name"]:item["digest"]["sha256"] for item in provenance.get("subjects",[])}
for name in (archive,sbom_name): assert subjects.get(name)==hashlib.sha256((root/name).read_bytes()).hexdigest()
PY
echo "source preview artifact set verified: $dist"
