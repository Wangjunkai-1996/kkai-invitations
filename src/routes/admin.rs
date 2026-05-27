use axum::extract::{Path, Query, State};
use axum::Json;
use chrono::{TimeZone, Utc};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgRow;
use sqlx::Row;

use crate::error::{AppError, Result};
use crate::middleware::AdminUser;
use crate::response::{message, ok, ApiResponse, PaginatedResponse};
use crate::routes::user::{
    cents_to_quota, count_rebate_records, fetch_rebate_records, quota_to_cents, PaginationParams,
    RebateRecord,
};
use crate::AppState;

#[derive(Serialize, Deserialize)]
pub struct RebateRule {
    pub id: i64,
    pub user_group: String,
    pub rule_type: String,
    pub rebate_rate: String,
    pub effective_time: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Deserialize)]
pub struct RebateRuleRequest {
    pub user_group: String,
    pub rule_type: String,
    pub rebate_rate: serde_json::Value,
}

#[derive(Serialize)]
pub struct UserGroup {
    pub name: String,
    pub user_count: i64,
}

#[derive(Serialize, Deserialize)]
pub struct SystemConfig {
    pub min_withdrawal_amount: i64,
    pub withdrawal_frequency_days: i64,
}

#[derive(Deserialize)]
pub struct ApproveRequest {
    pub note: Option<String>,
    pub reason: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WithdrawalRequestDetail {
    pub id: i64,
    pub user_id: i64,
    #[serde(rename = "inviter_id")]
    pub inviter_id: i64,
    pub username: String,
    pub amount: i64,
    pub status: String,
    pub rebate_record_ids: Vec<i64>,
    pub created_at: String,
    pub updated_at: String,
    pub approved_at: Option<String>,
    pub completed_at: Option<String>,
    pub rejected_at: Option<String>,
    pub reject_reason: Option<String>,
}

#[derive(Serialize)]
pub struct RebateStats {
    pub total_rebate: i64,
    pub completed_rebate: i64,
    pub pending_rebate: i64,
    pub requested_rebate: i64,
    pub approved_rebate: i64,
    pub total_invitations: i64,
}

#[derive(Deserialize)]
pub struct WithdrawalListParams {
    pub status: Option<String>,
    pub page: Option<i64>,
    #[serde(alias = "pageSize")]
    pub page_size: Option<i64>,
}

impl WithdrawalListParams {
    fn page(&self) -> i64 {
        self.page.unwrap_or(1).max(1)
    }

    fn page_size(&self) -> i64 {
        self.page_size.unwrap_or(100).clamp(1, 1000)
    }

    fn offset(&self) -> i64 {
        (self.page() - 1) * self.page_size()
    }

    fn status_filter(&self) -> Option<&str> {
        match self.status.as_deref() {
            Some("pending" | "approved" | "rejected" | "completed") => self.status.as_deref(),
            _ => None,
        }
    }
}

// GET /api/admin/rebate-rules
pub async fn get_rebate_rules(
    State(state): State<AppState>,
    _admin: AdminUser,
) -> Result<Json<ApiResponse<Vec<RebateRule>>>> {
    let rows = sqlx::query(
        r#"
        SELECT id, user_group, rule_type, rebate_rate::TEXT as rebate_rate,
               effective_time, created_at, updated_at
        FROM rebate_rules
        ORDER BY user_group, rule_type
        "#,
    )
    .fetch_all(&state.pool)
    .await?;

    Ok(ok(rows
        .iter()
        .map(rebate_rule_from_row)
        .collect::<Result<Vec<_>>>()?))
}

// POST /api/admin/rebate-rules
pub async fn create_rebate_rule(
    State(state): State<AppState>,
    admin: AdminUser,
    Json(req): Json<RebateRuleRequest>,
) -> Result<Json<ApiResponse<RebateRule>>> {
    let rebate_rate = validate_rebate_rule_request(&req)?;
    let now = Utc::now().timestamp();

    let row = sqlx::query(
        r#"
        INSERT INTO rebate_rules (user_group, rule_type, rebate_rate, effective_time, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $4, $4)
        ON CONFLICT (user_group, rule_type)
        DO UPDATE SET rebate_rate = $3, effective_time = $4, updated_at = $4
        RETURNING id, user_group, rule_type, rebate_rate::TEXT as rebate_rate,
                  effective_time, created_at, updated_at
        "#,
    )
    .bind(req.user_group.trim())
    .bind(req.rule_type.trim())
    .bind(rebate_rate)
    .bind(now)
    .fetch_one(&state.pool)
    .await?;

    tracing::info!(
        admin_id = admin.0.id,
        user_group = req.user_group,
        rule_type = req.rule_type,
        "Admin saved rebate rule"
    );

    Ok(message(rebate_rule_from_row(&row)?, "返利规则已保存"))
}

// PUT /api/admin/rebate-rules/:id
pub async fn update_rebate_rule(
    State(state): State<AppState>,
    admin: AdminUser,
    Path(id): Path<i64>,
    Json(req): Json<RebateRuleRequest>,
) -> Result<Json<ApiResponse<RebateRule>>> {
    if id <= 0 {
        return Err(AppError::BadRequest("无效的规则 ID".to_string()));
    }
    let rebate_rate = validate_rebate_rule_request(&req)?;
    let now = Utc::now().timestamp();

    let row = sqlx::query(
        r#"
        UPDATE rebate_rules
        SET user_group = $1, rule_type = $2, rebate_rate = $3, effective_time = $4, updated_at = $4
        WHERE id = $5
        RETURNING id, user_group, rule_type, rebate_rate::TEXT as rebate_rate,
                  effective_time, created_at, updated_at
        "#,
    )
    .bind(req.user_group.trim())
    .bind(req.rule_type.trim())
    .bind(rebate_rate)
    .bind(now)
    .bind(id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or_else(|| AppError::NotFound("返利规则不存在".to_string()))?;

    tracing::info!(
        admin_id = admin.0.id,
        rule_id = id,
        "Admin updated rebate rule"
    );

    Ok(message(rebate_rule_from_row(&row)?, "返利规则已更新"))
}

// DELETE /api/admin/rebate-rules/:id
pub async fn delete_rebate_rule(
    State(state): State<AppState>,
    admin: AdminUser,
    Path(id): Path<i64>,
) -> Result<Json<ApiResponse<serde_json::Value>>> {
    if id <= 0 {
        return Err(AppError::BadRequest("无效的规则 ID".to_string()));
    }

    let rows = sqlx::query("DELETE FROM rebate_rules WHERE id = $1")
        .bind(id)
        .execute(&state.pool)
        .await?
        .rows_affected();

    if rows == 0 {
        return Err(AppError::NotFound("返利规则不存在".to_string()));
    }

    tracing::info!(
        admin_id = admin.0.id,
        rule_id = id,
        "Admin deleted rebate rule"
    );

    Ok(message(serde_json::json!({}), "返利规则已删除"))
}

// GET /api/admin/user-groups
pub async fn get_user_groups(
    State(state): State<AppState>,
    _admin: AdminUser,
) -> Result<Json<ApiResponse<Vec<UserGroup>>>> {
    let rows = sqlx::query(
        r#"
        SELECT "group" as name, COUNT(*) as user_count
        FROM users
        GROUP BY "group"
        ORDER BY "group"
        "#,
    )
    .fetch_all(&state.newapi_pool)
    .await?;

    Ok(ok(rows
        .iter()
        .map(|row| {
            Ok(UserGroup {
                name: row.try_get("name")?,
                user_count: row.try_get("user_count")?,
            })
        })
        .collect::<Result<Vec<_>>>()?))
}

// GET /api/admin/system-config
pub async fn get_system_config(
    State(state): State<AppState>,
    _admin: AdminUser,
) -> Result<Json<ApiResponse<SystemConfig>>> {
    let min_amount = get_config_value(&state.pool, "min_withdrawal_amount").await?;
    let frequency_days = get_config_value(&state.pool, "withdrawal_frequency_days").await?;

    Ok(ok(SystemConfig {
        min_withdrawal_amount: quota_to_cents(min_amount),
        withdrawal_frequency_days: frequency_days,
    }))
}

// POST/PUT /api/admin/system-config
pub async fn update_system_config(
    State(state): State<AppState>,
    admin: AdminUser,
    Json(config): Json<SystemConfig>,
) -> Result<Json<ApiResponse<SystemConfig>>> {
    if config.min_withdrawal_amount < 0 {
        return Err(AppError::BadRequest("最低提现金额不能为负数".to_string()));
    }
    if config.withdrawal_frequency_days < 1 {
        return Err(AppError::BadRequest("提现频率限制至少为 1 天".to_string()));
    }

    let now = Utc::now().timestamp();
    upsert_config_value(
        &state.pool,
        "min_withdrawal_amount",
        &cents_to_quota(config.min_withdrawal_amount).to_string(),
        "最低提现金额（人民币分，API 层；数据库存 new-api 额度单位）",
        now,
    )
    .await?;
    upsert_config_value(
        &state.pool,
        "withdrawal_frequency_days",
        &config.withdrawal_frequency_days.to_string(),
        "提现频率限制（天数）",
        now,
    )
    .await?;

    tracing::info!(admin_id = admin.0.id, "Admin updated system config");

    Ok(message(config, "系统配置已更新"))
}

// GET /api/admin/withdrawal-requests
pub async fn get_withdrawal_requests(
    State(state): State<AppState>,
    _admin: AdminUser,
    Query(params): Query<WithdrawalListParams>,
) -> Result<Json<ApiResponse<PaginatedResponse<WithdrawalRequestDetail>>>> {
    let total = count_withdrawal_requests(&state, params.status_filter()).await?;
    let withdrawal_rows =
        fetch_withdrawal_requests(&state, params.status_filter(), &params).await?;
    let user_map = fetch_user_map(&state, &withdrawal_rows).await?;

    let result = withdrawal_rows
        .iter()
        .map(|row| withdrawal_detail_from_row(row, &user_map))
        .collect::<Result<Vec<_>>>()?;

    Ok(ok(PaginatedResponse {
        items: result,
        total,
        page: params.page(),
        page_size: params.page_size(),
    }))
}

// POST /api/admin/withdrawal-requests/:id/approve
pub async fn approve_withdrawal(
    State(state): State<AppState>,
    admin: AdminUser,
    Path(id): Path<i64>,
    Json(req): Json<ApproveRequest>,
) -> Result<Json<ApiResponse<serde_json::Value>>> {
    validate_id(id)?;
    let now = Utc::now().timestamp();
    let mut tx = state.pool.begin().await?;

    let row = sqlx::query(
        "SELECT rebate_record_ids FROM withdrawal_requests WHERE id = $1 AND status = 'pending' FOR UPDATE",
    )
    .bind(id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| AppError::NotFound("申请不存在或状态不正确".to_string()))?;
    let rebate_record_ids: Vec<i64> = row.try_get("rebate_record_ids")?;

    sqlx::query(
        r#"
        UPDATE withdrawal_requests
        SET status = 'approved', admin_id = $1, admin_note = $2, approved_at = $3, updated_at = $3
        WHERE id = $4
        "#,
    )
    .bind(admin.0.id)
    .bind(&req.note)
    .bind(now)
    .bind(id)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        "UPDATE rebate_records SET status = 'approved', updated_at = $1 WHERE id = ANY($2) AND status = 'requested'",
    )
    .bind(now)
    .bind(&rebate_record_ids)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    tracing::info!(
        admin_id = admin.0.id,
        request_id = id,
        note = ?req.note,
        "Admin approved withdrawal request"
    );

    Ok(message(
        serde_json::json!({ "id": id }),
        "申请已通过，等待放款",
    ))
}

// POST /api/admin/withdrawal-requests/:id/reject
pub async fn reject_withdrawal(
    State(state): State<AppState>,
    admin: AdminUser,
    Path(id): Path<i64>,
    Json(req): Json<ApproveRequest>,
) -> Result<Json<ApiResponse<serde_json::Value>>> {
    validate_id(id)?;
    let now = Utc::now().timestamp();
    let note = req.reason.as_ref().or(req.note.as_ref()).cloned();
    if note.as_deref().unwrap_or("").trim().is_empty() {
        return Err(AppError::BadRequest("请填写拒绝原因".to_string()));
    }

    let mut tx = state.pool.begin().await?;
    let row = sqlx::query(
        "SELECT rebate_record_ids FROM withdrawal_requests WHERE id = $1 AND status = 'pending' FOR UPDATE",
    )
    .bind(id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| AppError::NotFound("申请不存在或状态不正确".to_string()))?;
    let rebate_record_ids: Vec<i64> = row.try_get("rebate_record_ids")?;

    sqlx::query(
        r#"
        UPDATE withdrawal_requests
        SET status = 'rejected', admin_id = $1, admin_note = $2, rejected_at = $3, updated_at = $3
        WHERE id = $4
        "#,
    )
    .bind(admin.0.id)
    .bind(&note)
    .bind(now)
    .bind(id)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        "UPDATE rebate_records SET status = 'pending', updated_at = $1 WHERE id = ANY($2) AND status = 'requested'",
    )
    .bind(now)
    .bind(&rebate_record_ids)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    tracing::info!(
        admin_id = admin.0.id,
        request_id = id,
        note = ?note,
        "Admin rejected withdrawal request"
    );

    Ok(message(serde_json::json!({ "id": id }), "申请已拒绝"))
}

// POST /api/admin/withdrawal-requests/:id/complete
pub async fn complete_withdrawal(
    State(state): State<AppState>,
    admin: AdminUser,
    Path(id): Path<i64>,
) -> Result<Json<ApiResponse<serde_json::Value>>> {
    validate_id(id)?;
    let now = Utc::now().timestamp();
    let mut tx = state.pool.begin().await?;

    let row = sqlx::query(
        "SELECT rebate_record_ids FROM withdrawal_requests WHERE id = $1 AND status = 'approved' FOR UPDATE",
    )
    .bind(id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| AppError::NotFound("申请不存在或状态不正确".to_string()))?;
    let rebate_record_ids: Vec<i64> = row.try_get("rebate_record_ids")?;

    sqlx::query(
        "UPDATE withdrawal_requests SET status = 'completed', completed_at = $1, updated_at = $1 WHERE id = $2",
    )
    .bind(now)
    .bind(id)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        "UPDATE rebate_records SET status = 'completed', updated_at = $1 WHERE id = ANY($2) AND status = 'approved'",
    )
    .bind(now)
    .bind(&rebate_record_ids)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    tracing::info!(
        admin_id = admin.0.id,
        request_id = id,
        "Admin marked withdrawal as completed"
    );

    Ok(message(serde_json::json!({ "id": id }), "放款已完成"))
}

// GET /api/admin/rebate-stats
pub async fn get_rebate_stats(
    State(state): State<AppState>,
    _admin: AdminUser,
) -> Result<Json<ApiResponse<RebateStats>>> {
    let row = sqlx::query(
        r#"
        SELECT
            COALESCE(SUM(rebate_amount), 0)::BIGINT as total,
            COALESCE(SUM(CASE WHEN status = 'completed' THEN rebate_amount ELSE 0 END), 0)::BIGINT as completed,
            COALESCE(SUM(CASE WHEN status = 'pending' THEN rebate_amount ELSE 0 END), 0)::BIGINT as pending,
            COALESCE(SUM(CASE WHEN status = 'requested' THEN rebate_amount ELSE 0 END), 0)::BIGINT as requested,
            COALESCE(SUM(CASE WHEN status = 'approved' THEN rebate_amount ELSE 0 END), 0)::BIGINT as approved
        FROM rebate_records
        "#,
    )
    .fetch_one(&state.pool)
    .await?;

    let total_invitations: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM users WHERE inviter_id IS NOT NULL AND inviter_id > 0",
    )
    .fetch_one(&state.newapi_pool)
    .await?;

    Ok(ok(RebateStats {
        total_rebate: quota_to_cents(row.try_get("total")?),
        completed_rebate: quota_to_cents(row.try_get("completed")?),
        pending_rebate: quota_to_cents(row.try_get("pending")?),
        requested_rebate: quota_to_cents(row.try_get("requested")?),
        approved_rebate: quota_to_cents(row.try_get("approved")?),
        total_invitations,
    }))
}

// GET /api/admin/user-rebates/:user_id
pub async fn get_user_rebates(
    State(state): State<AppState>,
    _admin: AdminUser,
    Path(user_id): Path<i64>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<ApiResponse<PaginatedResponse<RebateRecord>>>> {
    validate_id(user_id)?;

    let records = fetch_rebate_records(
        &state.pool,
        Some(user_id),
        None,
        params.page_size(),
        params.offset(),
    )
    .await?;
    let total = count_rebate_records(&state.pool, Some(user_id), None).await?;

    Ok(ok(PaginatedResponse {
        items: records,
        total,
        page: params.page(),
        page_size: params.page_size(),
    }))
}

fn rebate_rule_from_row(row: &PgRow) -> Result<RebateRule> {
    Ok(RebateRule {
        id: row.try_get("id")?,
        user_group: row.try_get("user_group")?,
        rule_type: row.try_get("rule_type")?,
        rebate_rate: row.try_get("rebate_rate")?,
        effective_time: row.try_get("effective_time")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn validate_rebate_rule_request(req: &RebateRuleRequest) -> Result<f64> {
    let user_group = req.user_group.trim();
    if user_group.is_empty() || user_group.len() > 64 {
        return Err(AppError::BadRequest(
            "用户组名称长度必须在 1-64 之间".to_string(),
        ));
    }
    let rule_type = req.rule_type.trim();
    if rule_type != "topup" && rule_type != "subscription" {
        return Err(AppError::BadRequest(
            "规则类型必须是 topup 或 subscription".to_string(),
        ));
    }

    let rate = match &req.rebate_rate {
        serde_json::Value::Number(value) => value.as_f64(),
        serde_json::Value::String(value) => value.parse::<f64>().ok(),
        _ => None,
    }
    .ok_or_else(|| AppError::BadRequest("返利比例格式错误".to_string()))?;

    if !(0.0..=1.0).contains(&rate) {
        return Err(AppError::BadRequest("返利比例必须在 0-1 之间".to_string()));
    }

    Ok(rate)
}

async fn get_config_value(pool: &sqlx::PgPool, key: &str) -> Result<i64> {
    let value: String = sqlx::query_scalar("SELECT value FROM system_config WHERE key = $1")
        .bind(key)
        .fetch_one(pool)
        .await?;

    value
        .parse::<i64>()
        .map_err(|_| AppError::Internal("配置值格式错误".to_string()))
}

async fn upsert_config_value(
    pool: &sqlx::PgPool,
    key: &str,
    value: &str,
    description: &str,
    now: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO system_config (key, value, description, updated_at)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (key)
        DO UPDATE SET value = $2, description = $3, updated_at = $4
        "#,
    )
    .bind(key)
    .bind(value)
    .bind(description)
    .bind(now)
    .execute(pool)
    .await?;

    Ok(())
}

async fn count_withdrawal_requests(state: &AppState, status: Option<&str>) -> Result<i64> {
    let total = if let Some(status) = status {
        sqlx::query_scalar("SELECT COUNT(*) FROM withdrawal_requests WHERE status = $1")
            .bind(status)
            .fetch_one(&state.pool)
            .await?
    } else {
        sqlx::query_scalar("SELECT COUNT(*) FROM withdrawal_requests")
            .fetch_one(&state.pool)
            .await?
    };
    Ok(total)
}

async fn fetch_withdrawal_requests(
    state: &AppState,
    status: Option<&str>,
    params: &WithdrawalListParams,
) -> Result<Vec<PgRow>> {
    let rows = if let Some(status) = status {
        sqlx::query(
            r#"
            SELECT id, user_id, rebate_record_ids, total_amount, status, created_at, updated_at,
                   approved_at, completed_at, rejected_at, admin_note
            FROM withdrawal_requests
            WHERE status = $1
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(status)
        .bind(params.page_size())
        .bind(params.offset())
        .fetch_all(&state.pool)
        .await?
    } else {
        sqlx::query(
            r#"
            SELECT id, user_id, rebate_record_ids, total_amount, status, created_at, updated_at,
                   approved_at, completed_at, rejected_at, admin_note
            FROM withdrawal_requests
            ORDER BY created_at DESC
            LIMIT $1 OFFSET $2
            "#,
        )
        .bind(params.page_size())
        .bind(params.offset())
        .fetch_all(&state.pool)
        .await?
    };
    Ok(rows)
}

async fn fetch_user_map(
    state: &AppState,
    withdrawal_rows: &[PgRow],
) -> Result<std::collections::HashMap<i64, String>> {
    let user_ids: Vec<i64> = withdrawal_rows
        .iter()
        .filter_map(|row| row.try_get::<i64, _>("user_id").ok())
        .collect();

    if user_ids.is_empty() {
        return Ok(std::collections::HashMap::new());
    }

    Ok(
        sqlx::query("SELECT id, username FROM users WHERE id = ANY($1)")
            .bind(&user_ids)
            .fetch_all(&state.newapi_pool)
            .await?
            .iter()
            .filter_map(|row| {
                let id = row.try_get::<i64, _>("id").ok()?;
                let username = row.try_get::<Option<String>, _>("username").ok()??;
                Some((id, username))
            })
            .collect(),
    )
}

fn withdrawal_detail_from_row(
    row: &PgRow,
    user_map: &std::collections::HashMap<i64, String>,
) -> Result<WithdrawalRequestDetail> {
    let user_id: i64 = row.try_get("user_id")?;

    Ok(WithdrawalRequestDetail {
        id: row.try_get("id")?,
        user_id,
        inviter_id: user_id,
        username: user_map.get(&user_id).cloned().unwrap_or_default(),
        amount: quota_to_cents(row.try_get("total_amount")?),
        status: row.try_get("status")?,
        rebate_record_ids: row.try_get("rebate_record_ids")?,
        created_at: timestamp_to_iso(row.try_get("created_at")?),
        updated_at: timestamp_to_iso(row.try_get("updated_at")?),
        approved_at: optional_timestamp_to_iso(row.try_get("approved_at")?),
        completed_at: optional_timestamp_to_iso(row.try_get("completed_at")?),
        rejected_at: optional_timestamp_to_iso(row.try_get("rejected_at")?),
        reject_reason: row.try_get("admin_note")?,
    })
}

fn validate_id(id: i64) -> Result<()> {
    if id <= 0 {
        return Err(AppError::BadRequest("无效的 ID".to_string()));
    }
    Ok(())
}

fn timestamp_to_iso(timestamp: i64) -> String {
    Utc.timestamp_opt(timestamp, 0)
        .single()
        .unwrap_or_else(Utc::now)
        .to_rfc3339()
}

fn optional_timestamp_to_iso(timestamp: Option<i64>) -> Option<String> {
    timestamp.map(timestamp_to_iso)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_string_rebate_rate() {
        let req = RebateRuleRequest {
            user_group: "default".to_string(),
            rule_type: "topup".to_string(),
            rebate_rate: serde_json::json!("0.05"),
        };

        assert_eq!(validate_rebate_rule_request(&req).unwrap(), 0.05);
    }

    #[test]
    fn rejects_percentage_rebate_rate() {
        let req = RebateRuleRequest {
            user_group: "default".to_string(),
            rule_type: "topup".to_string(),
            rebate_rate: serde_json::json!("5"),
        };

        assert!(validate_rebate_rule_request(&req).is_err());
    }
}
