use sqlx::PgPool;

use crate::error::{AppError, Result};

pub const USER_INVITATION_REBATE_ENABLED_KEY: &str = "user_invitation_rebate_enabled";
pub const ORDER_REBATE_ENABLED_KEY: &str = "order_rebate_enabled";
pub const INVITATION_SIGNUP_REWARD_ENABLED_KEY: &str = "invitation_signup_reward_enabled";
pub const INVITATION_SIGNUP_REWARD_AMOUNT_KEY: &str = "invitation_signup_reward_amount";
pub const INVITATION_SIGNUP_INVITER_REWARD_AMOUNT_KEY: &str =
    "invitation_signup_inviter_reward_amount";
pub const INVITATION_SIGNUP_INVITEE_REWARD_AMOUNT_KEY: &str =
    "invitation_signup_invitee_reward_amount";
pub const INVITATION_SIGNUP_REWARD_REVIEW_REQUIRED_KEY: &str =
    "invitation_signup_reward_review_required";
pub const INVITATION_SIGNUP_INVITER_REWARD_REQUIRES_PAID_ORDER_KEY: &str =
    "invitation_signup_inviter_reward_requires_paid_order";
pub const INVITATION_SIGNUP_INVITEE_REWARD_REQUIRES_PAID_ORDER_KEY: &str =
    "invitation_signup_invitee_reward_requires_paid_order";
pub const REBATE_TO_BALANCE_ENABLED_KEY: &str = "rebate_to_balance_enabled";

pub async fn get_config_i64(pool: &PgPool, key: &str) -> Result<i64> {
    let value: String = sqlx::query_scalar("SELECT value FROM system_config WHERE key = $1")
        .bind(key)
        .fetch_one(pool)
        .await?;

    parse_config_i64(&value)
}

pub async fn get_config_i64_or_fallback(
    pool: &PgPool,
    key: &str,
    fallback_key: &str,
) -> Result<i64> {
    let value: Option<String> =
        sqlx::query_scalar("SELECT value FROM system_config WHERE key = $1")
            .bind(key)
            .fetch_optional(pool)
            .await?;

    match value {
        Some(value) => parse_config_i64(&value),
        None => get_config_i64(pool, fallback_key).await,
    }
}

fn parse_config_i64(value: &str) -> Result<i64> {
    value
        .parse::<i64>()
        .map_err(|_| AppError::Internal("配置值格式错误".to_string()))
}

pub async fn get_config_bool(pool: &PgPool, key: &str) -> Result<bool> {
    let value: String = sqlx::query_scalar("SELECT value FROM system_config WHERE key = $1")
        .bind(key)
        .fetch_one(pool)
        .await?;

    parse_config_bool(&value)
}

pub fn parse_config_bool(value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        _ => Err(AppError::Internal("配置值格式错误".to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_boolean_config_values() {
        assert!(parse_config_bool("true").unwrap());
        assert!(parse_config_bool("1").unwrap());
        assert!(parse_config_bool("yes").unwrap());
        assert!(parse_config_bool("on").unwrap());
        assert!(!parse_config_bool("false").unwrap());
        assert!(!parse_config_bool("0").unwrap());
        assert!(!parse_config_bool("no").unwrap());
        assert!(!parse_config_bool("off").unwrap());
        assert!(parse_config_bool("invalid").is_err());
    }

    #[test]
    fn parses_integer_config_values() {
        assert_eq!(parse_config_i64("0").unwrap(), 0);
        assert_eq!(parse_config_i64("42").unwrap(), 42);
        assert!(parse_config_i64("invalid").is_err());
    }
}
