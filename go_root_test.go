package pqueue_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestGoCompatibilityModules(t *testing.T) {
	cmd := exec.Command("go", "test", "./...")
	cmd.Dir = "crates/pqueue-kafka/tests/compat/producer_oracle"
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("producer oracle go tests failed: %v\n%s", err, out)
	}
}

func TestReleaseWorkflowPublishesContainerDigest(t *testing.T) {
	workflow := readFile(t, ".github/workflows/release.yml")

	required := []string{
		"packages: write",
		"docker/setup-buildx-action@v3",
		"docker/login-action@v3",
		"registry: ghcr.io",
		"username: ${{ github.actor }}",
		"password: ${{ github.token }}",
		"docker/build-push-action@v6",
		"context: ./pqueue",
		"file: ./pqueue/Dockerfile",
		"push: true",
		"ghcr.io/${owner}/pqueue-service",
		"version_tag=${image}:${{ steps.release.outputs.version }}",
		"sha_tag=${image}:sha-${GITHUB_SHA}",
		"steps.container-build.outputs.digest",
		"target/release-dist/pqueue-service-image.txt",
		"scripts/release/write-container-image-evidence.sh",
	}
	for _, needle := range required {
		if !strings.Contains(workflow, needle) {
			t.Fatalf("release workflow missing %q", needle)
		}
	}
	if strings.Contains(workflow, ":latest") {
		t.Fatalf("release workflow must publish immutable version/SHA tags, not latest")
	}

	evidence := readFile(t, "scripts/release/write-container-image-evidence.sh")
	for _, needle := range []string{
		"artifact=pqueue-service-container-image",
		"digest_coordinate=${IMAGE}@${DIGEST}",
		"version_coordinate=${VERSION_TAG}",
		"sha_coordinate=${SHA_TAG}",
		"source_commit=${COMMIT}",
		"dockerfile=Dockerfile",
	} {
		if !strings.Contains(evidence, needle) {
			t.Fatalf("container evidence helper missing %q", needle)
		}
	}
}

func TestContainerPublishingEvidenceChecksummed(t *testing.T) {
	dist := filepath.Join(t.TempDir(), "release-dist")
	if err := os.MkdirAll(dist, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dist, "pqueue-0.1.0-linux.tar.gz"), []byte("binary archive"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dist, "pqueue-service-image.txt"), []byte("digest=sha256:abc123"), 0o644); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command("bash", "scripts/release/write-checksums.sh", dist)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("write-checksums failed: %v\n%s", err, out)
	}

	sums := readFile(t, filepath.Join(dist, "SHA256SUMS"))
	for _, artifact := range []string{"pqueue-0.1.0-linux.tar.gz", "pqueue-service-image.txt"} {
		if !strings.Contains(sums, artifact) {
			t.Fatalf("SHA256SUMS missing %s:\n%s", artifact, sums)
		}
	}

	packageScript := readFile(t, "scripts/release/package-binaries.sh")
	if !strings.Contains(packageScript, "bash scripts/release/write-checksums.sh \"$DIST_DIR\"") {
		t.Fatalf("package-binaries.sh must keep writing binary artifact checksums through the shared helper")
	}

	workflow := readFile(t, ".github/workflows/release.yml")
	evidenceIndex := strings.Index(workflow, "scripts/release/write-container-image-evidence.sh")
	checksumIndex := strings.Index(workflow, "scripts/release/write-checksums.sh target/release-dist")
	publishIndex := strings.Index(workflow, "name: Publish release assets")
	if evidenceIndex == -1 || checksumIndex == -1 || publishIndex == -1 {
		t.Fatalf("release workflow missing evidence, checksum, or publish step")
	}
	if !(evidenceIndex < checksumIndex && checksumIndex < publishIndex) {
		t.Fatalf("release workflow must write image evidence, refresh checksums, then publish assets")
	}
}

func TestReleaseGateOrderingPreserved(t *testing.T) {
	workflow := readFile(t, ".github/workflows/release.yml")
	gate := "bash scripts/ci/release-gate.sh --require-tp002-evidence E0,E1,E2,E3"
	publish := "name: Publish release assets"

	gateIndex := strings.Index(workflow, gate)
	publishIndex := strings.Index(workflow, publish)
	if gateIndex == -1 {
		t.Fatalf("release workflow missing release gate command")
	}
	if publishIndex == -1 {
		t.Fatalf("release workflow missing publish step")
	}
	if gateIndex > publishIndex {
		t.Fatalf("release gate must run before artifact publishing")
	}
	for _, source := range []string{
		"--tp002-e0e1-source pqueue-7e2b3132",
		"--tp002-e2-source pqueue-9afd88cc,pqueue-76d92a33",
		"--tp002-e3-source pqueue-b1abd895,pqueue-472a09d4",
	} {
		if !strings.Contains(workflow, source) {
			t.Fatalf("release gate source changed or missing: %s", source)
		}
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(content)
}
