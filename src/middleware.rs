use axum::extract::FromRequestParts;
use axum::http::request::Parts;

use crate::error::AppError;
use crate::session::securecookie::SecureCookieDecoder;
use crate::session::SessionUser;
use crate::AppState;

impl FromRequestParts<AppState> for SessionUser {
    type Rejection = AppError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let api = state
            .config
            .balance_api
            .as_ref()
            .ok_or(AppError::Unauthorized)?;
        let status = console_http_client()
            .get(format!("{}/api/status", api.base_url.trim_end_matches('/')))
            .send()
            .await
            .map_err(|_| AppError::FundsMaintenance)?;
        if !status.status().is_success() {
            return Err(AppError::FundsMaintenance);
        }
        let status: serde_json::Value = status
            .json()
            .await
            .map_err(|_| AppError::FundsMaintenance)?;
        let mode = console_api_mode(&status).ok_or(AppError::FundsMaintenance)?;
        if !matches!(
            parts.method,
            axum::http::Method::GET | axum::http::Method::HEAD | axum::http::Method::OPTIONS
        ) {
            if let Some(epoch) = status
                .pointer("/data/credit_epoch")
                .and_then(|v| v.as_str())
                .filter(|v| v.starts_with("usd_credit_v1:"))
            {
                if parts
                    .headers
                    .get("X-KKAI-Credit-Epoch")
                    .and_then(|v| v.to_str().ok())
                    != Some(epoch)
                {
                    return Err(AppError::Conflict(
                        "余额币制已变化，请刷新页面后重试".to_string(),
                    ));
                }
            }
        }
        if mode == 2 {
            let authorization = parts
                .headers
                .get("authorization")
                .ok_or(AppError::Unauthorized)?;
            let response = console_http_client()
                .get(format!(
                    "{}/api/user/self",
                    api.base_url.trim_end_matches('/')
                ))
                .header("Authorization", authorization)
                .send()
                .await
                .map_err(|_| AppError::FundsMaintenance)?;
            if !response.status().is_success() {
                return Err(AppError::Unauthorized);
            }
            let response: serde_json::Value =
                response.json().await.map_err(|_| AppError::Unauthorized)?;
            return verified_console_session(response);
        }
        let cookie_header = parts
            .headers
            .get("cookie")
            .and_then(|v| v.to_str().ok())
            .ok_or(AppError::Unauthorized)?;

        let session_value = cookie_header
            .split(';')
            .map(|s| s.trim())
            .find_map(|s| {
                let (name, value) = s.split_once('=')?;
                if name.trim() == "session" {
                    Some(value.trim().to_string())
                } else {
                    None
                }
            })
            .ok_or(AppError::Unauthorized)?;

        let decoder = SecureCookieDecoder::new(state.config.session_secret.as_bytes(), "session");

        decoder
            .decode(&session_value)
            .map_err(|_| AppError::Unauthorized)
    }
}

// 管理员权限检查
pub struct AdminUser(pub SessionUser);

impl FromRequestParts<AppState> for AdminUser {
    type Rejection = AppError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let user = SessionUser::from_request_parts(parts, state).await?;

        if !user.is_admin() {
            return Err(AppError::Forbidden);
        }

        Ok(AdminUser(user))
    }
}

fn console_http_client() -> &'static reqwest::Client {
    static CLIENT: std::sync::LazyLock<reqwest::Client> = std::sync::LazyLock::new(|| {
        reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(2))
            .timeout(std::time::Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("console verification HTTP client")
    });
    &CLIENT
}

fn verified_console_session(response: serde_json::Value) -> Result<SessionUser, AppError> {
    if response.get("success").and_then(|v| v.as_bool()) != Some(true)
        || response
            .pointer("/data/auth_method")
            .and_then(|v| v.as_str())
            != Some("session")
    {
        return Err(AppError::Unauthorized);
    }
    let user: SessionUser =
        serde_json::from_value(response["data"].clone()).map_err(|_| AppError::Unauthorized)?;
    if user.id <= 0 || user.status != 1 {
        return Err(AppError::Unauthorized);
    }
    Ok(user)
}

fn console_api_mode(status: &serde_json::Value) -> Option<u8> {
    if status.get("success").and_then(|v| v.as_bool()) != Some(true) {
        return None;
    }
    let versions = status
        .pointer("/data/console_contract/api_contracts")?
        .as_array()?;
    if versions.iter().any(|v| v.as_u64() == Some(2)) {
        return Some(2);
    }
    if versions.iter().any(|v| v.as_u64() == Some(1)) {
        return Some(1);
    }
    None
}

#[cfg(test)]
mod credit_auth_tests {
    use super::*;
    #[test]
    fn api2_never_falls_back_to_legacy_cookie() {
        assert_eq!(
            console_api_mode(
                &serde_json::json!({"success":true,"data":{"console_contract":{"api_contracts":[1,2]}}})
            ),
            Some(2)
        );
        assert_eq!(
            console_api_mode(
                &serde_json::json!({"success":true,"data":{"console_contract":{"api_contracts":[1]}}})
            ),
            Some(1)
        );
        assert_eq!(
            console_api_mode(&serde_json::json!({"success":true,"data":{}})),
            None
        );
        assert_eq!(
            console_api_mode(
                &serde_json::json!({"success":false,"data":{"console_contract":{"api_contracts":[1]}}})
            ),
            None
        );
    }
    #[test]
    fn only_active_browser_sessions_can_authorize_invitations() {
        let mut response = serde_json::json!({"success":true,"data":{"auth_method":"session","id":7,"username":"admin","role":100,"status":1,"group":"default"}});
        assert!(verified_console_session(response.clone())
            .unwrap()
            .is_admin());
        response["data"]["auth_method"] = "access_token".into();
        assert!(verified_console_session(response.clone()).is_err());
        response["data"]["auth_method"] = "session".into();
        response["data"]["status"] = 2.into();
        assert!(verified_console_session(response.clone()).is_err());
        response["data"]["status"] = 1.into();
        response["success"] = false.into();
        assert!(verified_console_session(response).is_err());
        assert!(
            verified_console_session(serde_json::json!({"success":true,"data":{"role":100}}))
                .is_err()
        );
    }
}
