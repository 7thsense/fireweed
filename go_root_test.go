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
		"scripts/release/verify-release-artifacts.sh",
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

func TestReleaseChecksumAggregation(t *testing.T) {
	dist := completeReleaseDistFixture(t)

	cmd := exec.Command("bash", "scripts/release/write-checksums.sh", dist)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("write-checksums failed: %v\n%s", err, out)
	}

	sums := readFile(t, filepath.Join(dist, "SHA256SUMS"))
	for _, artifact := range []string{
		"pqueue-0.2.0-x86_64-linux.tar.gz",
		"pqueue-0.2.0.tgz",
		"pqueue-helm-chart.txt",
		"pqueue-service-image.txt",
	} {
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
	verifyIndex := strings.Index(workflow, "name: Verify release artifact set")
	publishIndex := strings.Index(workflow, "name: Publish release assets")
	if evidenceIndex == -1 || checksumIndex == -1 || verifyIndex == -1 || publishIndex == -1 {
		t.Fatalf("release workflow missing evidence, checksum, verification, or publish step")
	}
	if !(evidenceIndex < checksumIndex && checksumIndex < verifyIndex && verifyIndex < publishIndex) {
		t.Fatalf("release workflow must write image evidence, refresh checksums, verify artifacts, then publish assets")
	}
}

func TestReleaseArtifactSetVerification(t *testing.T) {
	dist := completeReleaseDistFixture(t)
	writeReleaseChecksums(t, dist)

	verify := func(t *testing.T, releaseDist string, wantPass bool) {
		t.Helper()
		cmd := exec.Command("bash", "scripts/release/verify-release-artifacts.sh", "--version", "0.2.0", "--dist", releaseDist)
		out, err := cmd.CombinedOutput()
		if wantPass && err != nil {
			t.Fatalf("verify-release-artifacts failed: %v\n%s", err, out)
		}
		if !wantPass && err == nil {
			t.Fatalf("verify-release-artifacts unexpectedly passed:\n%s", out)
		}
	}

	verify(t, dist, true)

	for _, missing := range []string{
		"pqueue-0.2.0-x86_64-linux.tar.gz",
		"pqueue-0.2.0.tgz",
		"pqueue-helm-chart.txt",
		"pqueue-service-image.txt",
		"SHA256SUMS",
	} {
		t.Run("missing_"+missing, func(t *testing.T) {
			dist := completeReleaseDistFixture(t)
			writeReleaseChecksums(t, dist)
			if err := os.Remove(filepath.Join(dist, missing)); err != nil {
				t.Fatal(err)
			}
			verify(t, dist, false)
		})
	}

	t.Run("missing_binary_checksum_entry", func(t *testing.T) {
		dist := completeReleaseDistFixture(t)
		writeReleaseChecksums(t, dist)
		sums := readFile(t, filepath.Join(dist, "SHA256SUMS"))
		filtered := make([]string, 0)
		for _, line := range strings.Split(sums, "\n") {
			if strings.Contains(line, "pqueue-0.2.0-x86_64-linux.tar.gz") || line == "" {
				continue
			}
			filtered = append(filtered, line)
		}
		if err := os.WriteFile(filepath.Join(dist, "SHA256SUMS"), []byte(strings.Join(filtered, "\n")+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		verify(t, dist, false)
	})
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

func TestDeploymentReleaseGateRunsNonClusterChecks(t *testing.T) {
	result := runDeploymentReleaseGateWithStubs(t, stubOptions{dockerInfoSucceeds: false})
	if result.err != nil {
		t.Fatalf("deployment release gate failed: %v\n%s", result.err, result.output)
	}

	output := result.output
	assertOutputOrder(t, output,
		"+++ bash scripts/ci/release-gate.sh",
		"+++ bash scripts/ci/helm-gate.sh",
		"+++ bash scripts/release/package-helm-chart.sh",
		"+++ validate docs/microsite",
		"SKIPPED kind backend matrix",
	)

	for _, want := range []string{
		"bash\tscripts/ci/release-gate.sh",
		"bash\tscripts/ci/helm-gate.sh",
		"bash\tscripts/release/package-helm-chart.sh",
	} {
		if !strings.Contains(result.log, want) {
			t.Fatalf("deployment gate did not run %q; log:\n%s\noutput:\n%s", want, result.log, output)
		}
	}
	if strings.Contains(result.log, "scripts/ci/kind-helm-test.sh") {
		t.Fatalf("kind matrix must not run when Docker is unusable; log:\n%s", result.log)
	}
}

func TestDeploymentReleaseGateLocalKindSkipsAreDocumented(t *testing.T) {
	result := runDeploymentReleaseGateWithStubs(t, stubOptions{dockerInfoSucceeds: false})
	if result.err != nil {
		t.Fatalf("deployment release gate failed: %v\n%s", result.err, result.output)
	}

	for _, want := range []string{
		"SKIPPED kind backend matrix",
		"skip scope: kind backend matrix only (postgres_native object_log_sqlite_projection)",
		"missing local capability:",
		"docker daemon not usable: docker info failed",
		"non-cluster deployment release checks passed before this kind-only skip",
	} {
		if !strings.Contains(result.output, want) {
			t.Fatalf("deployment gate skip output missing %q:\n%s", want, result.output)
		}
	}
}

func TestDeploymentReleaseGateRunsBothBackendsWhenToolsExist(t *testing.T) {
	result := runDeploymentReleaseGateWithStubs(t, stubOptions{dockerInfoSucceeds: true})
	if result.err != nil {
		t.Fatalf("deployment release gate failed: %v\n%s", result.err, result.output)
	}

	assertOutputOrder(t, result.output,
		"+++ bash scripts/ci/kind-helm-test.sh --backend postgres_native",
		"+++ bash scripts/ci/kind-helm-test.sh --backend object_log_sqlite_projection",
		"=== deployment release gate PASSED ===",
	)
	for _, want := range []string{
		"bash\tscripts/ci/kind-helm-test.sh\t--backend\tpostgres_native",
		"bash\tscripts/ci/kind-helm-test.sh\t--backend\tobject_log_sqlite_projection",
	} {
		if !strings.Contains(result.log, want) {
			t.Fatalf("deployment gate did not run %q; log:\n%s\noutput:\n%s", want, result.log, result.output)
		}
	}
	if strings.Contains(result.output, "SKIPPED kind backend matrix") {
		t.Fatalf("kind matrix must not skip when all local tools are stubbed usable:\n%s", result.output)
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

func completeReleaseDistFixture(t *testing.T) string {
	t.Helper()
	dist := filepath.Join(t.TempDir(), "release-dist")
	if err := os.MkdirAll(dist, 0o755); err != nil {
		t.Fatal(err)
	}
	writeFixtureFile(t, dist, "pqueue-0.2.0-x86_64-linux.tar.gz", "binary archive")
	writeFixtureFile(t, dist, "pqueue-0.2.0.tgz", "helm chart archive")
	writeFixtureFile(t, dist, "pqueue-helm-chart.txt", strings.Join([]string{
		"artifact=pqueue-helm-chart",
		"chart=pqueue",
		"version=0.2.0",
		"package=pqueue-0.2.0.tgz",
		"package_sha256=unused",
	}, "\n"))
	writeFixtureFile(t, dist, "pqueue-service-image.txt", strings.Join([]string{
		"artifact=pqueue-service-container-image",
		"digest=sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	}, "\n"))
	return dist
}

func writeFixtureFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func writeReleaseChecksums(t *testing.T, dist string) {
	t.Helper()
	cmd := exec.Command("bash", "scripts/release/write-checksums.sh", dist)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("write-checksums failed: %v\n%s", err, out)
	}
}

type stubOptions struct {
	dockerInfoSucceeds bool
}

type gateResult struct {
	output string
	log    string
	err    error
}

func runDeploymentReleaseGateWithStubs(t *testing.T, opts stubOptions) gateResult {
	t.Helper()
	realBash, err := exec.LookPath("bash")
	if err != nil {
		t.Fatal(err)
	}
	realPython, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}

	bin := t.TempDir()
	logPath := filepath.Join(t.TempDir(), "commands.log")
	writeExecutable(t, bin, "bash", "#!/bin/sh\n"+
		"printf 'bash' >> \"$PQUEUE_GATE_TEST_LOG\"\n"+
		"for arg in \"$@\"; do printf '\\t%s' \"$arg\" >> \"$PQUEUE_GATE_TEST_LOG\"; done\n"+
		"printf '\\n' >> \"$PQUEUE_GATE_TEST_LOG\"\n"+
		"case \"$1\" in\n"+
		"  scripts/ci/release-gate.sh|scripts/ci/helm-gate.sh|scripts/release/package-helm-chart.sh|scripts/ci/kind-helm-test.sh) exit 0 ;;\n"+
		"  *) exec "+realBash+" \"$@\" ;;\n"+
		"esac\n")
	writeExecutable(t, bin, "python3", "#!/bin/sh\nexec "+realPython+" \"$@\"\n")
	writeExecutable(t, bin, "helm", "#!/bin/sh\nexit 0\n")
	writeExecutable(t, bin, "kind", "#!/bin/sh\nexit 0\n")
	writeExecutable(t, bin, "kubectl", "#!/bin/sh\nexit 0\n")
	dockerStatus := "1"
	if opts.dockerInfoSucceeds {
		dockerStatus = "0"
	}
	writeExecutable(t, bin, "docker", "#!/bin/sh\n"+
		"if [ \"$1\" = info ]; then exit "+dockerStatus+"; fi\n"+
		"exit 0\n")

	cmd := exec.Command(realBash, "scripts/ci/deployment-release-gate.sh")
	cmd.Env = append(os.Environ(),
		"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"PQUEUE_GATE_TEST_LOG="+logPath,
	)
	out, runErr := cmd.CombinedOutput()
	log := ""
	if content, err := os.ReadFile(logPath); err == nil {
		log = string(content)
	}
	return gateResult{output: string(out), log: log, err: runErr}
}

func writeExecutable(t *testing.T, dir, name, content string) {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatal(err)
	}
}

func assertOutputOrder(t *testing.T, output string, needles ...string) {
	t.Helper()
	previous := -1
	for _, needle := range needles {
		current := strings.Index(output, needle)
		if current == -1 {
			t.Fatalf("output missing %q:\n%s", needle, output)
		}
		if current <= previous {
			t.Fatalf("output order violation at %q:\n%s", needle, output)
		}
		previous = current
	}
}
