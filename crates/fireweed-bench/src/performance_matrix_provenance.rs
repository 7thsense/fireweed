use std::process::Command;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Provenance {
    pub remote_ref: String,
    pub remote_commit: String,
    pub remote_url: String,
    pub os_kernel: String,
    pub architecture: String,
    pub cpu_model: String,
    pub logical_cpu_count: usize,
    pub total_memory_kib: u64,
    pub rustc_version: String,
    pub cargo_version: String,
    pub filesystem: String,
    pub free_space: String,
    pub load_average: String,
    pub cpu_governor: String,
    pub turbo_state: String,
    pub virtualization: String,
    pub host_fingerprint_sha256: String,
    pub conformance_output_sha256: Option<String>,
    pub benchmark_test_output_sha256: Option<String>,
}

fn output(program: &str, args: &[&str]) -> String {
    Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|result| result.status.success())
        .map(|result| String::from_utf8_lossy(&result.stdout).trim().to_owned())
        .unwrap_or_else(|| "unknown".into())
}

fn sanitized_remote(remote: &str) -> String {
    let without_query = remote.split('?').next().unwrap_or(remote);
    if let Some((scheme, rest)) = without_query.split_once("://") {
        let host_path = rest
            .rsplit_once('@')
            .map(|(_, value)| value)
            .unwrap_or(rest);
        format!("{scheme}://{host_path}")
    } else {
        without_query.to_owned()
    }
}

fn cpu_model() -> String {
    std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|text| {
            text.lines()
                .find_map(|line| line.strip_prefix("model name\t: "))
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "unknown".into())
}

fn total_memory_kib() -> u64 {
    std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|text| {
            text.lines()
                .find_map(|line| line.strip_prefix("MemTotal:"))
                .and_then(|value| value.split_whitespace().next())
                .and_then(|value| value.parse().ok())
        })
        .unwrap_or(0)
}

pub fn collect() -> Provenance {
    let os_kernel = output("uname", &["-srv"]);
    let architecture = output("uname", &["-m"]);
    let cpu_model = cpu_model();
    let logical_cpu_count = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(0);
    let total_memory_kib = total_memory_kib();
    let filesystem = output("stat", &["-f", "-c", "%T:%s", "."]);
    let free_space = output("df", &["-Pk", "."]);
    let load_average = std::fs::read_to_string("/proc/loadavg")
        .unwrap_or_else(|_| "unknown".into())
        .trim()
        .to_owned();
    let cpu_governor =
        std::fs::read_to_string("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
            .unwrap_or_else(|_| "unknown".into())
            .trim()
            .to_owned();
    let turbo_state = std::fs::read_to_string("/sys/devices/system/cpu/intel_pstate/no_turbo")
        .unwrap_or_else(|_| "unknown".into())
        .trim()
        .to_owned();
    let virtualization = output("systemd-detect-virt", &[]);
    let rustc_version = output("rustc", &["--version", "--verbose"]);
    let cargo_version = output("cargo", &["--version", "--verbose"]);
    let remote_url = sanitized_remote(&output("git", &["remote", "get-url", "origin"]));
    let remote_ref = "refs/remotes/origin/main".to_owned();
    let remote_commit = output("git", &["rev-parse", &remote_ref]);
    let host_material = format!(
        "{os_kernel}|{architecture}|{cpu_model}|{logical_cpu_count}|{total_memory_kib}|{filesystem}"
    );
    let host_fingerprint_sha256 = Sha256::digest(host_material.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Provenance {
        remote_ref,
        remote_commit,
        remote_url,
        os_kernel,
        architecture,
        cpu_model,
        logical_cpu_count,
        total_memory_kib,
        rustc_version,
        cargo_version,
        filesystem,
        free_space,
        load_average,
        cpu_governor,
        turbo_state,
        virtualization,
        host_fingerprint_sha256,
        conformance_output_sha256: std::env::var("FIREWEED_PERF_CONFORMANCE_SHA256").ok(),
        benchmark_test_output_sha256: std::env::var("FIREWEED_PERF_BENCH_TEST_SHA256").ok(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_credentials_and_query_are_removed() {
        assert_eq!(
            sanitized_remote("https://user:password@example.test/repo?token=x"),
            "https://example.test/repo"
        );
    }
}
