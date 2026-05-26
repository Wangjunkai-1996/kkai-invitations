use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

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

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Internal server error: {0}")]
    Internal(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            AppError::Unauthorized => (StatusCode::UNAUTHORIZED, "未授权".to_string()),
            AppError::Forbidden => (StatusCode::FORBIDDEN, "无权限".to_string()),
            AppError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
            AppError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
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
        };

        (
            status,
            Json(json!({ "success": false, "message": message })),
        )
            .into_response()
    }
}

pub type Result<T> = std::result::Result<T, AppError>;
