use std::fs;
use std::process::Command;

#[test]
fn missing_database_configuration_returns_json_exit_three() {
    let working_directory = std::env::temp_dir().join(format!(
        "invitations-preflight-cli-{}",
        ulid::Ulid::new().to_string().to_lowercase()
    ));
    fs::create_dir(&working_directory).expect("create isolated preflight working directory");

    let output = Command::new(env!("CARGO_BIN_EXE_invitations"))
        .arg("--preflight")
        .current_dir(&working_directory)
        .env_clear()
        .output()
        .expect("run invitations --preflight");

    fs::remove_dir(&working_directory).expect("remove isolated preflight working directory");

    assert_eq!(
        output.status.code(),
        Some(3),
        "unexpected stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let payload: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("preflight error output is valid JSON");
    assert_eq!(
        payload,
        serde_json::json!({
            "ok": false,
            "error": {
                "code": "invalid_invitations_database_configuration",
                "message": "DATABASE_URL or INVITATIONS_DATABASE_URL must be set",
            }
        })
    );
}

#[test]
fn reconcile_apply_rejects_missing_settlement_mode_before_database_access() {
    let output = Command::new(env!("CARGO_BIN_EXE_invitations"))
        .args(["reconcile", "apply", "--expected-sha256", &"a".repeat(64)])
        .env_clear()
        .output()
        .expect("run invitations reconcile apply");

    assert_eq!(output.status.code(), Some(2));
    let payload: serde_json::Value =
        serde_json::from_slice(&output.stderr).expect("reconcile error output is valid JSON");
    assert_eq!(payload["error"]["code"], "configuration_error");
    assert!(payload["error"]["message"]
        .as_str()
        .unwrap()
        .contains("SETTLEMENT_MODE=read_only exactly"));
}
