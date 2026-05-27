use axum::extract::State;
use axum::Json;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};

use crate::error::{AppError, Result};
use crate::session::SessionUser;
use crate::AppState;

#[derive(Serialize)]
pub struct InvitationStats {
    pub aff_code: String,
    pub invitee_count: i64,
    pub total_rebate: i64,
    pub completed_rebate: i64,
    pub pending_rebate: i64,
    pub requested_rebate: i64,
}

#[derive(Serialize)]
pub struct RebateRecord {
    pub id: i64,
    pub ulid: Option<String>,
    pub order_type: String,
    pub order_amount: String,
    pub rebate_amount: i64,
    pub rebate_rate: String,
    pub created_at: i64,
    pub status: String,
}

#[derive(Deserialize)]
pub struct WithdrawalRequest {
    pub rebate_record_ids: Vec<i64>,
}

#[derive(Serialize)]
pub struct WithdrawalRequestResponse {
    pub id: i64,
    pub total_amount: i64,
    pub status: String,
    pub created_at: i64,
    pub approved_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub admin_note: Option<String>,
}

// GET /api/invitations/my-code
pub async fn my_code(
    State(state): State<AppState>,
    user: SessionUser,
) -> Result<Json<InvitationStats>> {
    // 从 new-api 数据库查询用户信息
    let user_info = sqlx::query("SELECT aff_code FROM users WHERE id = $1")
        .bind(user.id)
        .fetch_optional(&state.newapi_pool)
        .await?
        .ok_or_else(|| AppError::NotFound("用户不存在".to_string()))?;

    let aff_code: Option<String> = user_info.try_get("aff_code").ok();

    // 查询邀请人数
    let invitee_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users WHERE inviter_id = $1")
        .bind(user.id)
        .fetch_one(&state.newapi_pool)
        .await?;

    // 查询返利统计
    let stats = sqlx::query(
        r#"
        SELECT
            COALESCE(SUM(rebate_amount), 0) as total,
            COALESCE(SUM(CASE WHEN status = 'completed' THEN rebate_amount ELSE 0 END), 0) as completed,
            COALESCE(SUM(CASE WHEN status = 'pending' THEN rebate_amount ELSE 0 END), 0) as pending,
            COALESCE(SUM(CASE WHEN status IN ('requested', 'approved') THEN rebate_amount ELSE 0 END), 0) as requested
        FROM rebate_records
        WHERE inviter_id = $1
        "#
    )
    .bind(user.id)
    .fetch_one(&state.pool)
    .await?;

    let total: i64 = stats.try_get("total")?;
    let completed: i64 = stats.try_get("completed")?;
    let pending: i64 = stats.try_get("pending")?;
    let requested: i64 = stats.try_get("requested")?;

    Ok(Json(InvitationStats {
        aff_code: aff_code.unwrap_or_default(),
        invitee_count,
        total_rebate: total,
        completed_rebate: completed,
        pending_rebate: pending,
        requested_rebate: requested,
    }))
}

// GET /api/invitations/rebate-records
pub async fn rebate_records(
    State(state): State<AppState>,
    user: SessionUser,
) -> Result<Json<Vec<RebateRecord>>> {
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
        LIMIT 100
        "#,
    )
    .bind(user.id)
    .fetch_all(&state.pool)
    .await?;

    let records = rows
        .iter()
        .map(|row| {
            Ok(RebateRecord {
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

// GET /api/invitations/available-rebates
pub async fn available_rebates(
    State(state): State<AppState>,
    user: SessionUser,
) -> Result<Json<Vec<RebateRecord>>> {
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
        WHERE inviter_id = $1 AND status = 'pending'
        ORDER BY created_at DESC
        "#,
    )
    .bind(user.id)
    .fetch_all(&state.pool)
    .await?;

    let records = rows
        .iter()
        .map(|row| {
            Ok(RebateRecord {
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

// POST /api/invitations/request-withdrawal
pub async fn request_withdrawal(
    State(state): State<AppState>,
    user: SessionUser,
    Json(req): Json<WithdrawalRequest>,
) -> Result<Json<serde_json::Value>> {
    if req.rebate_record_ids.is_empty() {
        return Err(AppError::BadRequest("请选择要提现的返利记录".to_string()));
    }

    tracing::info!(
        "用户 {} 申请提现，返利记录数: {}",
        user.id,
        req.rebate_record_ids.len()
    );

    // 获取系统配置
    let min_amount = get_config_value(&state.pool, "min_withdrawal_amount").await?;
    let frequency_days = get_config_value(&state.pool, "withdrawal_frequency_days").await?;

    // 检查频率限制
    let now = chrono::Utc::now().timestamp();
    let last_request_time: Option<i64> = sqlx::query_scalar(
        "SELECT MAX(created_at) FROM withdrawal_requests WHERE user_id = $1 AND status != 'rejected'"
    )
    .bind(user.id)
    .fetch_optional(&state.pool)
    .await?
    .flatten();

    if let Some(last_time) = last_request_time {
        let days_since = (now - last_time) / 86400;
        if days_since < frequency_days {
            return Err(AppError::BadRequest(format!(
                "提现申请过于频繁，请等待 {} 天后再试",
                frequency_days - days_since
            )));
        }
    }

    // 查询返利记录并计算总金额
    let rows = sqlx::query(
        r#"
        SELECT id, rebate_amount, status
        FROM rebate_records
        WHERE id = ANY($1) AND inviter_id = $2
        "#,
    )
    .bind(&req.rebate_record_ids)
    .bind(user.id)
    .fetch_all(&state.pool)
    .await?;

    if rows.len() != req.rebate_record_ids.len() {
        return Err(AppError::BadRequest(
            "部分返利记录不存在或不属于您".to_string(),
        ));
    }

    // 检查状态
    for row in &rows {
        let status: String = row.try_get("status")?;
        if status != "pending" {
            return Err(AppError::BadRequest("部分返利记录状态不正确".to_string()));
        }
    }

    let total_amount: i64 = rows
        .iter()
        .map(|r| r.try_get::<i64, _>("rebate_amount"))
        .collect::<std::result::Result<Vec<_>, _>>()?
        .iter()
        .sum();

    // 检查最低金额
    if total_amount < min_amount {
        return Err(AppError::BadRequest(format!(
            "提现金额不足最低限额 {}",
            min_amount
        )));
    }

    // 开始事务
    let mut tx = state.pool.begin().await?;

    // 创建提现申请
    let request_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO withdrawal_requests (user_id, rebate_record_ids, total_amount, status, created_at, updated_at)
        VALUES ($1, $2, $3, 'pending', $4, $4)
        RETURNING id
        "#
    )
    .bind(user.id)
    .bind(&req.rebate_record_ids)
    .bind(total_amount)
    .bind(now)
    .fetch_one(&mut *tx)
    .await?;

    // 更新返利记录状态
    sqlx::query(
        "UPDATE rebate_records SET status = 'requested', updated_at = $1 WHERE id = ANY($2)",
    )
    .bind(now)
    .bind(&req.rebate_record_ids)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    tracing::info!(
        "用户 {} 提现申请已提交，申请 ID: {}, 金额: {}",
        user.id,
        request_id,
        total_amount
    );

    Ok(Json(serde_json::json!({
        "success": true,
        "message": "提现申请已提交",
        "request_id": request_id
    })))
}

// GET /api/invitations/my-requests
pub async fn my_requests(
    State(state): State<AppState>,
    user: SessionUser,
) -> Result<Json<Vec<WithdrawalRequestResponse>>> {
    let rows = sqlx::query(
        r#"
        SELECT id, total_amount, status, created_at, approved_at, completed_at, admin_note
        FROM withdrawal_requests
        WHERE user_id = $1
        ORDER BY created_at DESC
        "#,
    )
    .bind(user.id)
    .fetch_all(&state.pool)
    .await?;

    let requests = rows
        .iter()
        .map(|row| {
            Ok(WithdrawalRequestResponse {
                id: row.try_get("id")?,
                total_amount: row.try_get("total_amount")?,
                status: row.try_get("status")?,
                created_at: row.try_get("created_at")?,
                approved_at: row.try_get("approved_at")?,
                completed_at: row.try_get("completed_at")?,
                admin_note: row.try_get("admin_note")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Json(requests))
}

async fn get_config_value(pool: &PgPool, key: &str) -> Result<i64> {
    let value: String = sqlx::query_scalar("SELECT value FROM system_config WHERE key = $1")
        .bind(key)
        .fetch_one(pool)
        .await?;

    value
        .parse::<i64>()
        .map_err(|_| AppError::Internal("配置值格式错误".to_string()))
}
