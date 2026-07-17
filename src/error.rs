use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

pub const FUNDS_MAINTENANCE_CODE: &str = "funds_maintenance";
pub const FUNDS_MAINTENANCE_MESSAGE: &str = "资金结算维护中，请稍后重试";
pub const LEGACY_COMPLETION_DISABLED_CODE: &str = "legacy_completion_disabled";
pub const LEGACY_COMPLETION_DISABLED_MESSAGE: &str =
    "旧版返利完成接口已停用，请使用余额发放与冲正接口";

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("Unauthorized")]
    Unauthorized,

    #[error("Forbidden")]
    Forbidden,

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Bad request: {0}")]
    BadRequest(String),

    #[error("Conflict: {0}")]
    Conflict(String),

    #[error("Unprocessable entity: {0}")]
    UnprocessableEntity(String),

    #[error("Locked: {0}")]
    Locked(String),

    #[error("Funds maintenance")]
    FundsMaintenance,

    #[error("Legacy completion disabled")]
    LegacyCompletionDisabled,

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Internal server error: {0}")]
    Internal(String),
}

impl AppError {
    fn response_parts(self) -> (StatusCode, String) {
        match self {
            AppError::Unauthorized => (StatusCode::UNAUTHORIZED, "未授权".to_string()),
            AppError::Forbidden => (StatusCode::FORBIDDEN, "无权限".to_string()),
            AppError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
            AppError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            AppError::Conflict(msg) => (StatusCode::CONFLICT, msg),
            AppError::UnprocessableEntity(msg) => (StatusCode::UNPROCESSABLE_ENTITY, msg),
            AppError::Locked(msg) => (StatusCode::LOCKED, msg),
            AppError::FundsMaintenance => {
                AppError::Locked(FUNDS_MAINTENANCE_MESSAGE.to_string()).response_parts()
            }
            AppError::LegacyCompletionDisabled => (
                StatusCode::GONE,
                LEGACY_COMPLETION_DISABLED_MESSAGE.to_string(),
            ),
            AppError::Database(e) => {
                tracing::error!("Database error: {}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, "数据库错误".to_string())
            }
            AppError::Internal(msg) => {
                tracing::error!("Internal error: {}", msg);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "服务器内部错误".to_string(),
                )
            }
        }
    }

    fn machine_code(&self) -> Option<&'static str> {
        match self {
            AppError::FundsMaintenance => Some(FUNDS_MAINTENANCE_CODE),
            AppError::LegacyCompletionDisabled => Some(LEGACY_COMPLETION_DISABLED_CODE),
            _ => None,
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let code = self.machine_code();
        let (status, message) = self.response_parts();

        let mut body = json!({ "success": false, "message": message });
        if let Some(code) = code {
            body["code"] = json!(code);
        }

        (status, Json(body)).into_response()
    }
}

pub type Result<T> = std::result::Result<T, AppError>;

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;

    use super::*;

    #[tokio::test]
    async fn ordinary_locked_error_does_not_expose_a_machine_code() {
        let response = AppError::Locked("账户暂时锁定".to_string()).into_response();

        assert_eq!(response.status(), StatusCode::LOCKED);
        let body = to_bytes(response.into_body(), 1024).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["success"], false);
        assert_eq!(json["message"], "账户暂时锁定");
        assert!(json.get("code").is_none());
    }
}
