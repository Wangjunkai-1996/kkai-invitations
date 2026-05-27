use axum::extract::{Path, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use sqlx::Row;

use crate::error::{AppError, Result};
use crate::middleware::AdminUser;
use crate::AppState;

#[derive(Serialize, Deserialize)]
pub struct RebateRule {
    pub id: Option<i64>,
    pub user_group: String,
    pub rule_type: String,
    pub rebate_rate: String,
    pub effective_time: i64,
}

#[derive(Deserialize)]
pub struct CreateRebateRuleRequest {
    pub user_group: String,
    pub rule_type: String,
    pub rebate_rate: f64,
}

#[derive(Serialize)]
pub struct UserGroup {
    pub name: String,
}

#[derive(Serialize, Deserialize)]
pub struct SystemConfig {
    pub min_withdrawal_amount: i64,
    pub withdrawal_frequency_days: i64,
}

#[derive(Deserialize)]
pub struct ApproveRequest {
    pub note: Option<String>,
}

#[derive(Serialize)]
pub struct WithdrawalRequestDetail {
    pub id: i64,
    pub user_id: i64,
    pub username: String,
    pub total_amount: i64,
    pub status: String,
    pub created_at: i64,
    pub approved_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub admin_note: Option<String>,
}

#[derive(Serialize)]
pub struct RebateStats {
    pub total_rebate: i64,
    pub completed_rebate: i64,
    pub pending_rebate: i64,
    pub requested_rebate: i64,
}

// GET /api/admin/rebate-rules
pub async fn get_rebate_rules(
    State(state): State<AppState>,
    _admin: AdminUser,
) -> Result<Json<Vec<RebateRule>>> {
    let rows = sqlx::query(
        r#"
        SELECT id, user_group, rule_type, rebate_rate::TEXT as rebate_rate, effective_time
        FROM rebate_rules
        ORDER BY user_group, rule_type
        "#,
    )
    .fetch_all(&state.pool)
    .await?;

    let rules = rows
        .iter()
        .map(|row| {
            Ok(RebateRule {
                id: row.try_get("id")?,
                user_group: row.try_get("user_group")?,
                rule_type: row.try_get("rule_type")?,
                rebate_rate: row.try_get("rebate_rate")?,
                effective_time: row.try_get("effective_time")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Json(rules))
}

// POST /api/admin/rebate-rules
pub async fn create_rebate_rule(
    State(state): State<AppState>,
    _admin: AdminUser,
    Json(req): Json<CreateRebateRuleRequest>,
) -> Result<Json<serde_json::Value>> {
    if req.rebate_rate < 0.0 || req.rebate_rate > 1.0 {
        return Err(AppError::BadRequest("返利比例必须在 0-1 之间".to_string()));
    }

    if req.rule_type != "topup" && req.rule_type != "subscription" {
        return Err(AppError::BadRequest(
            "规则类型必须是 topup 或 subscription".to_string(),
        ));
    }

    let now = chrono::Utc::now().timestamp();

    sqlx::query(
        r#"
        INSERT INTO rebate_rules (user_group, rule_type, rebate_rate, effective_time, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $4, $4)
        ON CONFLICT (user_group, rule_type)
        DO UPDATE SET rebate_rate = $3, effective_time = $4, updated_at = $4
        "#
    )
    .bind(&req.user_group)
    .bind(&req.rule_type)
    .bind(req.rebate_rate)
    .bind(now)
    .execute(&state.pool)
    .await?;

    Ok(Json(serde_json::json!({
        "success": true,
        "message": "返利规则已保存"
    })))
}

// GET /api/admin/user-groups
pub async fn get_user_groups(
    State(state): State<AppState>,
    _admin: AdminUser,
) -> Result<Json<Vec<UserGroup>>> {
    let rows = sqlx::query(r#"SELECT DISTINCT "group" as name FROM users ORDER BY "group""#)
        .fetch_all(&state.newapi_pool)
        .await?;

    let groups = rows
        .iter()
        .map(|row| {
            Ok(UserGroup {
                name: row.try_get("name")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Json(groups))
}

// GET /api/admin/system-config
pub async fn get_system_config(
    State(state): State<AppState>,
    _admin: AdminUser,
) -> Result<Json<SystemConfig>> {
    let min_amount = get_config_value(&state.pool, "min_withdrawal_amount").await?;
    let frequency_days = get_config_value(&state.pool, "withdrawal_frequency_days").await?;

    Ok(Json(SystemConfig {
        min_withdrawal_amount: min_amount,
        withdrawal_frequency_days: frequency_days,
    }))
}

// POST /api/admin/system-config
pub async fn update_system_config(
    State(state): State<AppState>,
    _admin: AdminUser,
    Json(config): Json<SystemConfig>,
) -> Result<Json<serde_json::Value>> {
    let now = chrono::Utc::now().timestamp();

    sqlx::query(
        "UPDATE system_config SET value = $1, updated_at = $2 WHERE key = 'min_withdrawal_amount'",
    )
    .bind(config.min_withdrawal_amount.to_string())
    .bind(now)
    .execute(&state.pool)
    .await?;

    sqlx::query("UPDATE system_config SET value = $1, updated_at = $2 WHERE key = 'withdrawal_frequency_days'")
        .bind(config.withdrawal_frequency_days.to_string())
        .bind(now)
        .execute(&state.pool)
        .await?;

    Ok(Json(serde_json::json!({
        "success": true,
        "message": "系统配置已更新"
    })))
}

// GET /api/admin/withdrawal-requests
pub async fn get_withdrawal_requests(
    State(state): State<AppState>,
    _admin: AdminUser,
) -> Result<Json<Vec<WithdrawalRequestDetail>>> {
    // 先从 invitations 数据库查询提现申请
    let withdrawal_rows = sqlx::query(
        r#"
        SELECT id, user_id, total_amount, status, created_at, approved_at, completed_at, admin_note
        FROM withdrawal_requests
        ORDER BY created_at DESC
        LIMIT 100
        "#,
    )
    .fetch_all(&state.pool)
    .await?;

    // 收集所有 user_id
    let user_ids: Vec<i64> = withdrawal_rows
        .iter()
        .filter_map(|row| row.try_get::<i64, _>("user_id").ok())
        .collect();

    // 从 new-api 数据库批量查询用户名
    let user_map: std::collections::HashMap<i64, String> = if !user_ids.is_empty() {
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
            .collect()
    } else {
        std::collections::HashMap::new()
    };

    // 组装结果
    let rows = withdrawal_rows;

    let result = rows
        .iter()
        .map(|row| {
            let user_id: i64 = row.try_get("user_id")?;
            let username = user_map.get(&user_id).cloned().unwrap_or_default();

            Ok(WithdrawalRequestDetail {
                id: row.try_get("id")?,
                user_id,
                username,
                total_amount: row.try_get("total_amount")?,
                status: row.try_get("status")?,
                created_at: row.try_get("created_at")?,
                approved_at: row.try_get("approved_at")?,
                completed_at: row.try_get("completed_at")?,
                admin_note: row.try_get("admin_note")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Json(result))
}

// POST /api/admin/withdrawal-requests/:id/approve
pub async fn approve_withdrawal(
    State(state): State<AppState>,
    admin: AdminUser,
    Path(id): Path<i64>,
    Json(req): Json<ApproveRequest>,
) -> Result<Json<serde_json::Value>> {
    let now = chrono::Utc::now().timestamp();

    let mut tx = state.pool.begin().await?;

    // 更新申请状态
    let rows = sqlx::query(
        r#"
        UPDATE withdrawal_requests
        SET status = 'approved', admin_id = $1, admin_note = $2, approved_at = $3, updated_at = $3
        WHERE id = $4 AND status = 'pending'
        "#,
    )
    .bind(admin.0.id)
    .bind(&req.note)
    .bind(now)
    .bind(id)
    .execute(&mut *tx)
    .await?
    .rows_affected();

    if rows == 0 {
        return Err(AppError::NotFound("申请不存在或状态不正确".to_string()));
    }

    tx.commit().await?;

    tracing::info!("管理员 {} 通过了提现申请 {}", admin.0.id, id);

    Ok(Json(serde_json::json!({
        "success": true,
        "message": "申请已通过，等待放款"
    })))
}

// POST /api/admin/withdrawal-requests/:id/reject
pub async fn reject_withdrawal(
    State(state): State<AppState>,
    admin: AdminUser,
    Path(id): Path<i64>,
    Json(req): Json<ApproveRequest>,
) -> Result<Json<serde_json::Value>> {
    let now = chrono::Utc::now().timestamp();

    let mut tx = state.pool.begin().await?;

    // 获取申请信息
    let row = sqlx::query(
        "SELECT rebate_record_ids FROM withdrawal_requests WHERE id = $1 AND status = 'pending'",
    )
    .bind(id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| AppError::NotFound("申请不存在或状态不正确".to_string()))?;

    let rebate_record_ids: Vec<i64> = row.try_get("rebate_record_ids")?;

    // 更新申请状态
    sqlx::query(
        r#"
        UPDATE withdrawal_requests
        SET status = 'rejected', admin_id = $1, admin_note = $2, approved_at = $3, updated_at = $3
        WHERE id = $4
        "#,
    )
    .bind(admin.0.id)
    .bind(&req.note)
    .bind(now)
    .bind(id)
    .execute(&mut *tx)
    .await?;

    // 恢复返利记录状态
    sqlx::query("UPDATE rebate_records SET status = 'pending', updated_at = $1 WHERE id = ANY($2)")
        .bind(now)
        .bind(&rebate_record_ids)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;

    tracing::info!("管理员 {} 拒绝了提现申请 {}", admin.0.id, id);

    Ok(Json(serde_json::json!({
        "success": true,
        "message": "申请已拒绝"
    })))
}

// POST /api/admin/withdrawal-requests/:id/complete
pub async fn complete_withdrawal(
    State(state): State<AppState>,
    _admin: AdminUser,
    Path(id): Path<i64>,
) -> Result<Json<serde_json::Value>> {
    let now = chrono::Utc::now().timestamp();

    let mut tx = state.pool.begin().await?;

    // 获取申请信息
    let row = sqlx::query(
        "SELECT rebate_record_ids FROM withdrawal_requests WHERE id = $1 AND status = 'approved'",
    )
    .bind(id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| AppError::NotFound("申请不存在或状态不正确".to_string()))?;

    let rebate_record_ids: Vec<i64> = row.try_get("rebate_record_ids")?;

    // 更新申请状态
    sqlx::query("UPDATE withdrawal_requests SET status = 'completed', completed_at = $1, updated_at = $1 WHERE id = $2")
        .bind(now)
        .bind(id)
        .execute(&mut *tx)
        .await?;

    // 更新返利记录状态
    sqlx::query(
        "UPDATE rebate_records SET status = 'completed', updated_at = $1 WHERE id = ANY($2)",
    )
    .bind(now)
    .bind(&rebate_record_ids)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    tracing::info!("管理员标记提现申请 {} 放款完成", id);

    Ok(Json(serde_json::json!({
        "success": true,
        "message": "放款已完成"
    })))
}

// GET /api/admin/rebate-stats
pub async fn get_rebate_stats(
    State(state): State<AppState>,
    _admin: AdminUser,
) -> Result<Json<RebateStats>> {
    let row = sqlx::query(
        r#"
        SELECT
            COALESCE(SUM(rebate_amount), 0) as total,
            COALESCE(SUM(CASE WHEN status = 'completed' THEN rebate_amount ELSE 0 END), 0) as completed,
            COALESCE(SUM(CASE WHEN status = 'pending' THEN rebate_amount ELSE 0 END), 0) as pending,
            COALESCE(SUM(CASE WHEN status IN ('requested', 'approved') THEN rebate_amount ELSE 0 END), 0) as requested
        FROM rebate_records
        "#
    )
    .fetch_one(&state.pool)
    .await?;

    Ok(Json(RebateStats {
        total_rebate: row.try_get("total")?,
        completed_rebate: row.try_get("completed")?,
        pending_rebate: row.try_get("pending")?,
        requested_rebate: row.try_get("requested")?,
    }))
}

// GET /api/admin/user-rebates/:user_id
pub async fn get_user_rebates(
    State(state): State<AppState>,
    _admin: AdminUser,
    Path(user_id): Path<i64>,
) -> Result<Json<Vec<crate::routes::user::RebateRecord>>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            ulid,
            order_type,
            order_amount::TEXT as order_amount,
            rebate_amount,
            rebate_rate::TEXT as rebate_rate,
            created_at,
            status
        FROM rebate_records
        WHERE inviter_id = $1
        ORDER BY created_at DESC
        "#,
    )
    .bind(user_id)
    .fetch_all(&state.pool)
    .await?;

    let records = rows
        .iter()
        .map(|row| {
            Ok(crate::routes::user::RebateRecord {
                id: row.try_get("id")?,
                ulid: row.try_get("ulid")?,
                order_type: row.try_get("order_type")?,
                order_amount: row.try_get("order_amount")?,
                rebate_amount: row.try_get("rebate_amount")?,
                rebate_rate: row.try_get("rebate_rate")?,
                created_at: row.try_get("created_at")?,
                status: row.try_get("status")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Json(records))
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
