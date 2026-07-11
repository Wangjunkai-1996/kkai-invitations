use std::env;

use crate::rebate_types::DEFAULT_QUOTA_UNITS_PER_YUAN;

pub const DEFAULT_AUTO_SYNC_INTERVAL_SECONDS: u64 = 60 * 60;
pub const DEFAULT_AUTO_SYNC_INITIAL_DELAY_SECONDS: u64 = 10;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SettlementMode {
    Disabled,
    ReadOnly,
    Enabled,
}

impl SettlementMode {
    pub const fn is_enabled(self) -> bool {
        matches!(self, Self::Enabled)
    }
}

#[derive(Clone)]
pub struct Config {
    pub listen_addr: String,
    pub base_path: String,
    pub database_url: String,
    pub newapi_database_url: String,
    pub session_secret: String,
    pub sync_secret: Option<String>,
    pub auto_sync_interval_seconds: u64,
    pub auto_sync_initial_delay_seconds: u64,
    pub settlement_mode: SettlementMode,
    pub quota_per_unit: i64,
}

impl Config {
    pub fn from_env() -> std::result::Result<Self, String> {
        Ok(Self {
            listen_addr: env::var("LISTEN_ADDR").unwrap_or_else(|_| "127.0.0.1:6212".to_string()),
            base_path: env::var("BASE_PATH").unwrap_or_else(|_| "".to_string()),
            database_url: database_url_env(),
            newapi_database_url: env::var("NEWAPI_DATABASE_URL")
                .expect("NEWAPI_DATABASE_URL must be set"),
            session_secret: env::var("SESSION_SECRET").expect("SESSION_SECRET must be set"),
            sync_secret: env::var("SYNC_SECRET")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            auto_sync_interval_seconds: optional_u64_env(
                "AUTO_SYNC_INTERVAL_SECONDS",
                DEFAULT_AUTO_SYNC_INTERVAL_SECONDS,
            ),
            auto_sync_initial_delay_seconds: optional_u64_env(
                "AUTO_SYNC_INITIAL_DELAY_SECONDS",
                DEFAULT_AUTO_SYNC_INITIAL_DELAY_SECONDS,
            ),
            settlement_mode: settlement_mode_env()?,
            quota_per_unit: quota_per_unit_env("QUOTA_PER_UNIT", DEFAULT_QUOTA_UNITS_PER_YUAN),
        })
    }
}

fn database_url_env() -> String {
    let database_url = env::var("DATABASE_URL").ok();
    let legacy_database_url = env::var("INVITATIONS_DATABASE_URL").ok();
    database_url_env_value(database_url.as_deref(), legacy_database_url.as_deref())
        .expect("DATABASE_URL or INVITATIONS_DATABASE_URL must be set")
}

fn database_url_env_value(
    database_url: Option<&str>,
    legacy_database_url: Option<&str>,
) -> Option<String> {
    database_url
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .or_else(|| {
            legacy_database_url
                .map(str::trim)
                .filter(|value| !value.is_empty())
        })
        .map(ToOwned::to_owned)
}

fn settlement_mode_env() -> std::result::Result<SettlementMode, String> {
    let settlement_mode = env::var("SETTLEMENT_MODE").ok();
    let legacy_payout_enabled = env::var("PAYOUT_ENABLED").ok();
    settlement_mode_env_value(settlement_mode.as_deref(), legacy_payout_enabled.as_deref())
}

fn settlement_mode_env_value(
    settlement_mode: Option<&str>,
    legacy_payout_enabled: Option<&str>,
) -> std::result::Result<SettlementMode, String> {
    if legacy_payout_enabled
        .map(str::trim)
        .is_some_and(|value| !value.is_empty())
    {
        tracing::warn!(
            "PAYOUT_ENABLED is deprecated and ignored; configure SETTLEMENT_MODE explicitly"
        );
    }

    match settlement_mode.map(str::trim) {
        Some("") | None => Ok(SettlementMode::Disabled),
        Some("disabled") => Ok(SettlementMode::Disabled),
        Some("read_only") => Ok(SettlementMode::ReadOnly),
        Some("enabled") => Ok(SettlementMode::Enabled),
        Some(value) => Err(format!(
            "SETTLEMENT_MODE must be one of disabled, read_only, or enabled; got {value:?}"
        )),
    }
}

fn quota_per_unit_env(name: &str, default_value: i64) -> i64 {
    let value = match env::var(name) {
        Ok(value) if !value.trim().is_empty() => parse_i64_env_value(name, &value),
        _ => default_value,
    };
    if value <= 0 || value % 100 != 0 {
        panic!("{name} must be positive and divisible by 100");
    }
    value
}

fn optional_u64_env(name: &str, default_value: u64) -> u64 {
    optional_u64_env_value(name, env::var(name).ok().as_deref(), default_value)
}

fn optional_u64_env_value(name: &str, value: Option<&str>, default_value: u64) -> u64 {
    match value {
        Some(value) if value.trim().is_empty() => default_value,
        Some(value) => parse_u64_env_value(name, value),
        None => default_value,
    }
}

fn parse_u64_env_value(name: &str, value: &str) -> u64 {
    value
        .trim()
        .parse::<u64>()
        .unwrap_or_else(|_| panic!("{name} must be a non-negative integer"))
}

fn parse_i64_env_value(name: &str, value: &str) -> i64 {
    value
        .trim()
        .parse::<i64>()
        .unwrap_or_else(|_| panic!("{name} must be an integer"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settlement_mode_defaults_to_disabled_without_new_setting() {
        assert_eq!(
            settlement_mode_env_value(None, Some("true")).unwrap(),
            SettlementMode::Disabled
        );
        assert_eq!(
            settlement_mode_env_value(Some(""), Some("true")).unwrap(),
            SettlementMode::Disabled
        );
    }

    #[test]
    fn settlement_mode_accepts_only_explicit_supported_values() {
        assert_eq!(
            settlement_mode_env_value(Some("disabled"), None).unwrap(),
            SettlementMode::Disabled
        );
        assert_eq!(
            settlement_mode_env_value(Some("read_only"), None).unwrap(),
            SettlementMode::ReadOnly
        );
        assert_eq!(
            settlement_mode_env_value(Some("enabled"), None).unwrap(),
            SettlementMode::Enabled
        );
        assert!(settlement_mode_env_value(Some("true"), None).is_err());
        assert!(settlement_mode_env_value(Some("write"), None).is_err());
    }

    #[test]
    fn legacy_payout_enabled_never_enables_settlement_writes() {
        assert_eq!(
            settlement_mode_env_value(None, Some("true")).unwrap(),
            SettlementMode::Disabled
        );
        assert_eq!(
            settlement_mode_env_value(Some("read_only"), Some("true")).unwrap(),
            SettlementMode::ReadOnly
        );
    }

    #[test]
    fn parses_u64_env_values_with_whitespace() {
        assert_eq!(parse_u64_env_value("TEST_SECONDS", " 3600 "), 3600);
        assert_eq!(parse_u64_env_value("TEST_SECONDS", "0"), 0);
    }

    #[test]
    fn optional_u64_env_values_use_default_for_empty_or_missing_values() {
        assert_eq!(optional_u64_env_value("TEST_SECONDS", None, 3600), 3600);
        assert_eq!(optional_u64_env_value("TEST_SECONDS", Some(""), 3600), 3600);
        assert_eq!(
            optional_u64_env_value("TEST_SECONDS", Some("  "), 3600),
            3600
        );
    }

    #[test]
    #[should_panic(expected = "TEST_SECONDS must be a non-negative integer")]
    fn invalid_u64_env_values_fail_fast() {
        parse_u64_env_value("TEST_SECONDS", "abc");
    }

    #[test]
    fn parses_i64_env_values_with_whitespace() {
        assert_eq!(parse_i64_env_value("TEST_QUOTA", " 500000 "), 500_000);
    }

    #[test]
    fn database_url_prefers_primary_env_name() {
        assert_eq!(
            database_url_env_value(
                Some("postgres://primary/invitations"),
                Some("postgres://legacy/invitations")
            ),
            Some("postgres://primary/invitations".to_string())
        );
    }

    #[test]
    fn database_url_falls_back_to_legacy_env_name() {
        assert_eq!(
            database_url_env_value(Some(" "), Some("postgres://legacy/invitations")),
            Some("postgres://legacy/invitations".to_string())
        );
    }
}
