use std::env;

use crate::rebate_types::DEFAULT_QUOTA_UNITS_PER_YUAN;

pub const DEFAULT_AUTO_SYNC_INTERVAL_SECONDS: u64 = 60 * 60;
pub const DEFAULT_AUTO_SYNC_INITIAL_DELAY_SECONDS: u64 = 10;

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
    pub payout_enabled: bool,
    pub quota_per_unit: i64,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
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
            payout_enabled: optional_bool_env("PAYOUT_ENABLED", false),
            quota_per_unit: quota_per_unit_env("QUOTA_PER_UNIT", DEFAULT_QUOTA_UNITS_PER_YUAN),
        }
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

fn optional_bool_env(name: &str, default_value: bool) -> bool {
    optional_bool_env_value(name, env::var(name).ok().as_deref(), default_value)
}

fn optional_bool_env_value(name: &str, value: Option<&str>, default_value: bool) -> bool {
    match value.map(str::trim) {
        Some("") | None => default_value,
        Some("true" | "1" | "yes" | "on") => true,
        Some("false" | "0" | "no" | "off") => false,
        Some(_) => panic!("{name} must be a boolean"),
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
    fn parses_bool_env_values() {
        assert!(optional_bool_env_value("TEST_BOOL", Some("true"), false));
        assert!(optional_bool_env_value("TEST_BOOL", Some("1"), false));
        assert!(!optional_bool_env_value("TEST_BOOL", Some("false"), true));
        assert_eq!(optional_bool_env_value("TEST_BOOL", Some(""), true), true);
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
