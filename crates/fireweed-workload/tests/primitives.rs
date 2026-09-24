use std::process::Command;

#[test]
fn component_cli_labels_payloads_and_verifies_both_storage_modes() {
    // The memory store is retired (3a8d8270); both payload modes run on the public s3 × turso store.
    {
        for varied in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let mut command = Command::new(env!("CARGO_BIN_EXE_fireweed-workload"));
            command
                .args([
                    "--profile",
                    "primitives",
                    "--items",
                    "224",
                    "--batch",
                    "31",
                    "--shards",
                    "2",
                    "--deadline-seconds",
                    "120",
                    "--root",
                ])
                .arg(root.path());
            if varied {
                command.arg("--primitive-varied-payload");
            }
            let output = command.output().unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(
                report["payload_workload"],
                if varied {
                    "campaign_varied"
                } else {
                    "repeated_padding"
                }
            );
            let initial = report["initial_payload_bytes"].as_u64().unwrap();
            let replacement = report["payload_replacement_bytes"].as_u64().unwrap();
            if varied {
                assert!((224 * 900..224 * 1024).contains(&initial));
                assert!(replacement > initial);
            } else {
                assert_eq!(initial, 224 * 1024);
                assert_eq!(replacement, initial);
            }
            for phase in report["aggregate_phases"].as_array().unwrap() {
                assert_eq!(phase["records"], 224);
            }
        }
    }
}

#[test]
fn varied_payload_option_requires_the_component_profile() {
    let output = Command::new(env!("CARGO_BIN_EXE_fireweed-workload"))
        .args(["--profile", "campaign", "--primitive-varied-payload"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("requires --profile primitives"));
}
