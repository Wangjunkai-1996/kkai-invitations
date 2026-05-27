use std::env;

#[derive(Clone)]
pub struct Config {
    pub listen_addr: String,
    pub base_path: String,
    pub database_url: String,
    pub newapi_database_url: String,
    pub session_secret: String,
    pub sync_secret: Option<String>,
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
        }
    }
}
