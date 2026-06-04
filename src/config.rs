use std::env;

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
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            listen_addr: env::var("LISTEN_ADDR").unwrap_or_else(|_| "127.0.0.1:6212".to_string()),
            base_path: env::var("BASE_PATH").unwrap_or_else(|_| "".to_string()),
            database_url: env::var("DATABASE_URL").expect("DATABASE_URL must be set"),
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
        }
    }
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
}
