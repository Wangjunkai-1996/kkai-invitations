use axum::Json;
use serde::Serialize;

#[derive(Serialize)]
pub struct ApiResponse<T>
where
    T: Serialize,
{
    pub success: bool,
    pub message: String,
    pub data: T,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PaginatedResponse<T>
where
    T: Serialize,
{
    pub items: Vec<T>,
    pub total: i64,
    pub page: i64,
    pub page_size: i64,
}

pub fn ok<T>(data: T) -> Json<ApiResponse<T>>
where
    T: Serialize,
{
    Json(ApiResponse {
        success: true,
        message: String::new(),
        data,
    })
}

pub fn message<T>(data: T, message: impl Into<String>) -> Json<ApiResponse<T>>
where
    T: Serialize,
{
    Json(ApiResponse {
        success: true,
        message: message.into(),
        data,
    })
}
