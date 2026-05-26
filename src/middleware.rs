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
