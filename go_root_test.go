package pqueue_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestGoCompatibilityModules(t *testing.T) {
	if _, err := os.Stat("crates/pqueue-kafka/tests/compat/producer_oracle"); os.IsNotExist(err) {
		t.Skip("skipping optional producer_oracle compatibility module: crates/pqueue-kafka/tests/compat/producer_oracle not present")
	} else if err != nil {
		t.Fatalf("could not inspect producer oracle go module: %v", err)
	}

	cmd := exec.Command("go", "test", "./...")
	cmd.Dir = "crates/pqueue-kafka/tests/compat/producer_oracle"
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("producer oracle go tests failed: %v\n%s", err, out)
	}
}

// NOTE (2026-07-06 review): two tests were removed here
// (TestKafkaIdempotencyKeyIsStableAcrossReemit, TestKafkaOffsetAdvanceDoesNotChangeDedupKey).
// They read the closing bead's own .ddx/executions prompt/manifest and asserted the PROMPT
// contained the CL-3 requirement strings — verifying the task description, not the code.
// The real epoch-stable dedup-key behavior is tested in
// crates/pqueue-engine (emission_cursor_failover_keeps_stable_dedup_key); the Kafka-surface
// CL-3 verification belongs to the embedded-fjord binding work (pqueue-a8a9e7e7 children).

func TestTerminalReapWaitsForEmissionCursor(t *testing.T) {
	runCargoTest(t, "-p", "pqueue-projection", "reap_waits_for_emission")
}

func TestTerminalReapAllowsOptOutAfterRetentionOnly(t *testing.T) {
	runCargoTest(t, "-p", "pqueue-projection", "reap_ignores_emission_when_disabled")
}

func TestTD008ConformanceSuiteGreen(t *testing.T) {
	runCargoTest(t, "-p", "pqueue-projection", "reap_")
}

func TestChangeRecordSinkDefaultsDisabledUntilEndpointIsSet(t *testing.T) {
	runCargoTest(t, "-p", "pqueue-server", "--lib", "TestChangeRecordSinkDefaultsDisabledUntilEndpointIsSet")
}

func TestChangeRecordSinkRejectsInvalidEndpointAndKeepsDisabled(t *testing.T) {
	runCargoTest(t, "-p", "pqueue-server", "--lib", "TestChangeRecordSinkRejectsInvalidEndpointAndKeepsDisabled")
}

func TestEmitChangeRecordTickSkipsOptedOutQueues(t *testing.T) {
	runCargoTest(t, "-p", "pqueue-server", "--lib", "TestEmitChangeRecordTickSkipsOptedOutQueues")
}

func TestEmitChangeRecordTickDoesNotAdvanceCursorForOptOut(t *testing.T) {
	runCargoTest(t, "-p", "pqueue-server", "--lib", "TestEmitChangeRecordTickDoesNotAdvanceCursorForOptOut")
}

func TestTD008EvidenceBundleRecorded(t *testing.T) {
	runCargoTestWithEnv(t, map[string]string{
		"PQUEUE_LEDGER_DIR": "docs/perf/evidence",
	}, "-p", "pqueue-release", "--test", "td008_evidence", "td008_evidence_bundle_recorded")
	path := filepath.Join("docs", "perf", "evidence", "td008-terminal-reap-frontier.jsonl")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected evidence bundle at %s: %v", path, err)
	}
	content := readFile(t, path)
	for _, needle := range []string{
		"td008_terminal_reap_frontier",
		"docs/perf/evidence/td008-terminal-reap-frontier.jsonl",
		"TestTD008EvidenceBundleRecorded",
	} {
		if !strings.Contains(content, needle) {
			t.Fatalf("evidence bundle missing %q:\n%s", needle, content)
		}
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
		"context: ./pqueue/target/release-image",
		"file: ./pqueue/target/release-image/Dockerfile",
		"push: true",
		"ghcr.io/${owner}/pqueue-service",
		"version_tag=${image}:${{ steps.release.outputs.version }}",
		"sha_tag=${image}:sha-${GITHUB_SHA}",
		"steps.container-build.outputs.digest",
		"target/release-dist/pqueue-service-image.txt",
		"cp Dockerfile.prebuilt target/release-image/Dockerfile",
		"--dockerfile Dockerfile.prebuilt",
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
		"dockerfile=${DOCKERFILE}",
	} {
		if !strings.Contains(evidence, needle) {
			t.Fatalf("container evidence helper missing %q", needle)
		}
	}
}

func runCargoTest(t *testing.T, args ...string) {
	runCargoTestWithEnv(t, nil, args...)
}

func runCargoTestWithEnv(t *testing.T, env map[string]string, args ...string) {
	t.Helper()
	cmd := exec.Command("cargo", append([]string{"test"}, args...)...)
	cmd.Env = append(os.Environ(), "CARGO_TERM_COLOR=never")
	for key, value := range env {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("cargo test %v failed: %v\n%s", args, err, out)
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

func TestActionsDeploymentMatrixProfiles(t *testing.T) {
	workflow := readFile(t, ".github/workflows/ci.yml")
	match := regexp.MustCompile(`(?s)matrix:[ \t]*\n[ \t]+include:[ \t]*\n(.*?)\n    steps:`).FindStringSubmatch(workflow)
	if match == nil {
		t.Fatalf("ci workflow must define the exact deployment storage matrix")
	}
	storageMatches := regexp.MustCompile(`(?m)^\s+- storage: ([a-z0-9-]+)\s*\n\s+log_backend: ([a-z0-9]+)\s*\n\s+projection_backend: ([a-z0-9]+)\s*$`).FindAllStringSubmatch(match[1], -1)
	combinations := make([]string, 0, len(storageMatches))
	for _, storage := range storageMatches {
		combinations = append(combinations, storage[1]+":"+storage[2]+":"+storage[3])
	}
	want := []string{"objectlog-inmemory:objectlog:inmemory"}
	if len(combinations) != len(want) {
		t.Fatalf("deployment matrix must contain exactly %v, got %v", want, combinations)
	}
	for i := range want {
		if combinations[i] != want[i] {
			t.Fatalf("deployment matrix must contain exactly %v in order, got %v", want, combinations)
		}
	}
}

func TestActionsHelmStaticValidationIncluded(t *testing.T) {
	workflow := readFile(t, ".github/workflows/ci.yml")
	assertWorkflowOrder(t, workflow,
		"name: Helm static validation gate",
		"bash scripts/ci/helm-gate.sh",
		"name: kind Helm integration proof (${{ matrix.storage }})",
	)
}

func TestActionsKindMatrixIsNotSkipped(t *testing.T) {
	workflow := readFile(t, ".github/workflows/ci.yml")
	for _, want := range []string{
		"runs-on: ubuntu-latest",
		"version: v1.31.0",
		"curl -fsSLo kind https://kind.sigs.k8s.io/dl/v0.25.0/kind-linux-amd64",
		"KIND_NODE_IMAGE: kindest/node:v1.31.0",
		"bash scripts/ci/kind-helm-test.sh --log-backend ${{ matrix.log_backend }} --projection-backend ${{ matrix.projection_backend }}",
	} {
		if !strings.Contains(workflow, want) {
			t.Fatalf("ci deployment workflow missing %q", want)
		}
	}
	for _, forbidden := range []string{
		"SKIPPED kind backend matrix",
		"skipped_local_environment",
		"--dry-run",
	} {
		if strings.Contains(workflow, forbidden) {
			t.Fatalf("ci deployment matrix must not accept local skip/dry-run proof via %q", forbidden)
		}
	}
}

func TestActionsReleaseGateComposition(t *testing.T) {
	workflow := readFile(t, ".github/workflows/release.yml")
	assertWorkflowOrder(t, workflow,
		"bash scripts/ci/release-gate.sh --require-tp002-evidence E0,E1,E2,E3",
		"name: Deployment release gate",
		"bash scripts/ci/deployment-release-gate.sh",
		"name: Resolve release tag",
	)
	for _, want := range []string{
		"version: v3.16.3",
		"version: v1.31.0",
		"curl -fsSLo kind https://kind.sigs.k8s.io/dl/v0.25.0/kind-linux-amd64",
		"KIND_NODE_IMAGE: kindest/node:v1.31.0",
	} {
		if !strings.Contains(workflow, want) {
			t.Fatalf("release deployment gate workflow missing %q", want)
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
		"SKIPPED kind storage matrix",
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
		"SKIPPED kind storage matrix",
		"skip scope: kind storage matrix only (objectlog:inmemory)",
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
		"+++ bash scripts/ci/kind-helm-test.sh --log-backend objectlog --projection-backend inmemory",
		"=== deployment release gate PASSED ===",
	)
	for _, want := range []string{
		"bash\tscripts/ci/kind-helm-test.sh\t--log-backend\tobjectlog\t--projection-backend\tinmemory",
	} {
		if !strings.Contains(result.log, want) {
			t.Fatalf("deployment gate did not run %q; log:\n%s\noutput:\n%s", want, result.log, result.output)
		}
	}
	if strings.Contains(result.output, "SKIPPED kind storage matrix") {
		t.Fatalf("kind matrix must not skip when all local tools are stubbed usable:\n%s", result.output)
	}
}

func TestDeploymentProofLedgerSchema(t *testing.T) {
	result := runDeploymentReleaseGateWithStubs(t, stubOptions{
		dockerInfoSucceeds: false,
		env: map[string]string{
			"PQUEUE_IMAGE_EVIDENCE_FILE": filepath.Join(t.TempDir(), "missing-image-evidence.txt"),
		},
	})
	if result.err != nil {
		t.Fatalf("deployment release gate failed: %v\n%s", result.err, result.output)
	}

	proof := result.proof
	assertStringField(t, proof, "schema", "pqueue.deployment_proof.v1")
	assertStringField(t, proof, "status", "passed_with_local_environment_skip")
	if stringField(t, proof, "commit_sha") == "" {
		t.Fatalf("proof missing commit SHA: %#v", proof)
	}

	chart := objectField(t, proof, "chart")
	if stringField(t, chart, "version") == "" || stringField(t, chart, "version") == "unavailable" {
		t.Fatalf("proof missing chart version: %#v", chart)
	}

	commands := arrayField(t, proof, "commands")
	for _, want := range []string{
		"bash scripts/ci/release-gate.sh",
		"bash scripts/ci/helm-gate.sh",
		"bash scripts/release/package-helm-chart.sh --version",
		"validate docs/microsite",
	} {
		if !proofContainsCommand(proof, want, 0) {
			t.Fatalf("proof missing successful command containing %q:\n%s", want, mustRead(t, result.proofPath))
		}
	}
	if len(commands) < 4 {
		t.Fatalf("proof command list too short: %#v", commands)
	}

	storageCombinations := arrayField(t, proof, "storage_combinations")
	if len(storageCombinations) != 1 {
		t.Fatalf("proof should record the storage combination, got %#v", storageCombinations)
	}
	for _, entry := range storageCombinations {
		combination := entry.(map[string]any)
		assertStringField(t, combination, "combination", "objectlog:inmemory")
		assertStringField(t, combination, "status", "skipped_local_environment")
	}

	localSkip := objectField(t, proof, "local_environment_skip")
	assertStringField(t, localSkip, "scope", "kind storage matrix only")
	if boolField(t, localSkip, "ci_matrix_proof") {
		t.Fatalf("local Docker/kind skip must not be marked as CI matrix proof: %#v", localSkip)
	}
	if len(arrayField(t, localSkip, "reasons")) == 0 {
		t.Fatalf("proof missing local skip reason: %#v", localSkip)
	}
}

func TestDeploymentProofImageEvidenceOptional(t *testing.T) {
	withImage := runDeploymentReleaseGateWithStubs(t, stubOptions{
		dockerInfoSucceeds: true,
		env: map[string]string{
			"PQUEUE_IMAGE_TAG":        "ghcr.io/example/pqueue-service:0.2.0",
			"PQUEUE_IMAGE_DIGEST":     "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			"PQUEUE_IMAGE_COORDINATE": "ghcr.io/example/pqueue-service@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		},
	})
	if withImage.err != nil {
		t.Fatalf("deployment release gate with image env failed: %v\n%s", withImage.err, withImage.output)
	}
	image := objectField(t, withImage.proof, "image")
	assertStringField(t, image, "tag", "ghcr.io/example/pqueue-service:0.2.0")
	assertStringField(t, image, "digest", "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	assertStringField(t, image, "coordinate", "ghcr.io/example/pqueue-service@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")

	withoutImage := runDeploymentReleaseGateWithStubs(t, stubOptions{
		dockerInfoSucceeds: false,
		env: map[string]string{
			"PQUEUE_IMAGE_EVIDENCE_FILE": filepath.Join(t.TempDir(), "missing-image-evidence.txt"),
		},
	})
	if withoutImage.err != nil {
		t.Fatalf("deployment release gate without image env failed: %v\n%s", withoutImage.err, withoutImage.output)
	}
	image = objectField(t, withoutImage.proof, "image")
	assertStringField(t, image, "tag", "unavailable")
	assertStringField(t, image, "digest", "unavailable")
	if stringField(t, image, "unavailable_reason") == "" {
		t.Fatalf("missing image unavailable reason: %#v", image)
	}
}

func TestDeploymentProofReleaseNotesReady(t *testing.T) {
	result := runDeploymentReleaseGateWithStubs(t, stubOptions{dockerInfoSucceeds: true})
	if result.err != nil {
		t.Fatalf("deployment release gate failed: %v\n%s", result.err, result.output)
	}

	notes := objectField(t, result.proof, "release_notes")
	for _, field := range []string{"command_list", "storage_matrix", "artifact_paths"} {
		if len(arrayField(t, notes, field)) == 0 {
			t.Fatalf("release notes proof missing %s:\n%s", field, mustRead(t, result.proofPath))
		}
	}
	if !proofContainsCommand(result.proof, "bash scripts/ci/kind-helm-test.sh --log-backend objectlog --projection-backend inmemory", 0) {
		t.Fatalf("proof missing objectlog/inmemory kind command:\n%s", mustRead(t, result.proofPath))
	}
	if !stringSliceContains(arrayField(t, notes, "storage_matrix"), "objectlog:inmemory:tested") {
		t.Fatalf("release notes matrix missing objectlog:inmemory tested: %#v", notes)
	}
}

func TestDeploymentProofDoesNotMaskFailures(t *testing.T) {
	for _, failCommand := range []string{
		"scripts/ci/release-gate.sh",
		"scripts/ci/helm-gate.sh",
		"scripts/release/package-helm-chart.sh",
		"scripts/ci/kind-helm-test.sh",
	} {
		t.Run(failCommand, func(t *testing.T) {
			result := runDeploymentReleaseGateWithStubs(t, stubOptions{
				dockerInfoSucceeds: true,
				failCommand:        failCommand,
			})
			if result.err == nil {
				t.Fatalf("deployment release gate unexpectedly passed with failing %s\n%s", failCommand, result.output)
			}
			assertStringField(t, result.proof, "status", "failed")
			if !proofContainsCommand(result.proof, failCommand, 23) {
				t.Fatalf("proof missing failed command %s:\n%s", failCommand, mustRead(t, result.proofPath))
			}
		})
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
	failCommand        string
	env                map[string]string
}

type gateResult struct {
	output    string
	log       string
	err       error
	proofPath string
	proof     map[string]any
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
	proofDir := filepath.Join("target", "deployment-release-gate", "go-test-"+strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()))
	if err := os.RemoveAll(proofDir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(proofDir) })

	failCase := ""
	if opts.failCommand != "" {
		failCase = "  " + opts.failCommand + ") exit 23 ;;\n"
	}
	writeExecutable(t, bin, "bash", "#!/bin/sh\n"+
		"printf 'bash' >> \"$PQUEUE_GATE_TEST_LOG\"\n"+
		"for arg in \"$@\"; do printf '\\t%s' \"$arg\" >> \"$PQUEUE_GATE_TEST_LOG\"; done\n"+
		"printf '\\n' >> \"$PQUEUE_GATE_TEST_LOG\"\n"+
		"case \"$1\" in\n"+
		failCase+
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
		"PQUEUE_DEPLOYMENT_PROOF_DIR="+proofDir,
	)
	for key, value := range opts.env {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	out, runErr := cmd.CombinedOutput()
	log := ""
	if content, err := os.ReadFile(logPath); err == nil {
		log = string(content)
	}
	proofPath := filepath.Join(proofDir, "deployment-proof.json")
	proof := map[string]any{}
	if content, err := os.ReadFile(proofPath); err == nil {
		if err := json.Unmarshal(content, &proof); err != nil {
			t.Fatalf("deployment proof is not valid JSON: %v\n%s", err, content)
		}
	} else {
		t.Fatalf("deployment proof was not written at %s\nerr: %v\noutput:\n%s", proofPath, err, out)
	}
	return gateResult{output: string(out), log: log, err: runErr, proofPath: proofPath, proof: proof}
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
	assertWorkflowOrder(t, output, needles...)
}

func assertWorkflowOrder(t *testing.T, output string, needles ...string) {
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

func mustRead(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(content)
}

func objectField(t *testing.T, object map[string]any, key string) map[string]any {
	t.Helper()
	value, ok := object[key].(map[string]any)
	if !ok {
		t.Fatalf("field %s is not an object: %#v", key, object[key])
	}
	return value
}

func arrayField(t *testing.T, object map[string]any, key string) []any {
	t.Helper()
	value, ok := object[key].([]any)
	if !ok {
		t.Fatalf("field %s is not an array: %#v", key, object[key])
	}
	return value
}

func stringField(t *testing.T, object map[string]any, key string) string {
	t.Helper()
	value, ok := object[key].(string)
	if !ok {
		t.Fatalf("field %s is not a string: %#v", key, object[key])
	}
	return value
}

func boolField(t *testing.T, object map[string]any, key string) bool {
	t.Helper()
	value, ok := object[key].(bool)
	if !ok {
		t.Fatalf("field %s is not a bool: %#v", key, object[key])
	}
	return value
}

func assertStringField(t *testing.T, object map[string]any, key, want string) {
	t.Helper()
	if got := stringField(t, object, key); got != want {
		t.Fatalf("field %s = %q, want %q", key, got, want)
	}
}

func proofContainsCommand(proof map[string]any, needle string, exitStatus int) bool {
	for _, entry := range proof["commands"].([]any) {
		command := entry.(map[string]any)
		display := command["display"].(string)
		status := int(command["exit_status"].(float64))
		if strings.Contains(display, needle) && status == exitStatus {
			return true
		}
	}
	return false
}

func stringSliceContains(values []any, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
