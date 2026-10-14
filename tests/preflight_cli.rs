use std::fs;
use std::process::Command;

use sqlx::PgPool;
use sqlx_core::migrate::Migrator;

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

#[test]
fn migrate_only_requires_read_only_mode_before_database_access() {
    let output = Command::new(env!("CARGO_BIN_EXE_invitations"))
        .arg("migrate-only")
        .env_clear()
        .output()
        .expect("run invitations migrate-only");
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr)
        .contains("migrate-only requires SETTLEMENT_MODE=read_only"));
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
async fn preflight_then_migrate_only_upgrades_version_seven_without_starting_server() {
    use std::borrow::Cow;

    let url = std::env::var("TEST_DATABASE_URL").expect("isolated PostgreSQL URL");
    let admin = PgPool::connect(&url).await.unwrap();
    let suffix = ulid::Ulid::new().to_string().to_lowercase();
    let schema = format!("migrate_cli_{suffix}");
    let external_schema = format!("migrate_cli_external_{suffix}");
    sqlx::raw_sql(&format!(
        "CREATE SCHEMA {schema}; CREATE SCHEMA {external_schema}"
    ))
    .execute(&admin)
    .await
    .unwrap();
    let join = if url.contains('?') { '&' } else { '?' };
    let database_url = format!("{url}{join}options=-csearch_path%3D{schema}");
    let newapi_database_url = format!("{url}{join}options=-csearch_path%3D{external_schema}");
    let pool = PgPool::connect(&database_url).await.unwrap();
    let external = PgPool::connect(&newapi_database_url).await.unwrap();
    sqlx::query("CREATE TABLE options(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
        .execute(&external)
        .await
        .unwrap();

    let all = Migrator::new(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/migrations"
    )))
    .await
    .unwrap();
    let seven = Migrator {
        migrations: Cow::Owned(
            all.iter()
                .filter(|migration| migration.version < 202610140001)
                .cloned()
                .collect(),
        ),
        ..Migrator::DEFAULT
    };
    seven.run(&pool).await.unwrap();
    sqlx::query(
        "UPDATE system_config SET value='true', updated_at=1 WHERE key='rebate_to_balance_enabled'",
    )
    .execute(&pool)
    .await
    .unwrap();

    let run = |command: &str| {
        Command::new(env!("CARGO_BIN_EXE_invitations"))
            .arg(command)
            .env("DATABASE_URL", &database_url)
            .env("NEWAPI_DATABASE_URL", &newapi_database_url)
            .env("SETTLEMENT_MODE", "read_only")
            .env("LISTEN_ADDR", "invalid-listener-address")
            .output()
            .unwrap()
    };
    let preflight = run("--preflight");
    assert!(preflight.status.success(), "{:?}", preflight);
    let report: serde_json::Value = serde_json::from_slice(&preflight.stdout).unwrap();
    assert_eq!(report["safeToMigrate"], true);
    let versions: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(versions.len(), 7, "preflight must not write migration 8");

    for _ in 0..2 {
        let migration = run("migrate-only");
        assert!(migration.status.success(), "{:?}", migration);
        let result: serde_json::Value = serde_json::from_slice(&migration.stdout).unwrap();
        assert_eq!(
            result,
            serde_json::json!({"ok":true,"schemaVersion":202610140001_i64})
        );
    }
    let versions: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(versions.len(), 8);
    assert_eq!(versions[7], 202610140001);
    let cutover: String = sqlx::query_scalar(
        "SELECT value FROM system_config WHERE key='order_rebate_seven_day_cutover_at'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(cutover.parse::<i64>().unwrap() > 0);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT value FROM system_config WHERE key='rebate_to_balance_enabled'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        "true"
    );

    pool.close().await;
    external.close().await;
    sqlx::raw_sql(&format!(
        "DROP SCHEMA {schema} CASCADE; DROP SCHEMA {external_schema} CASCADE"
    ))
    .execute(&admin)
    .await
    .unwrap();
    admin.close().await;
}
