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
