use std::collections::HashSet;

use axum::extract::{Query, State};
use axum::Json;
use chrono::{TimeZone, Utc};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};

use crate::error::{AppError, Result};
use crate::response::{message, ok, ApiResponse, PaginatedResponse};
use crate::session::SessionUser;
use crate::AppState;

const QUOTA_UNITS_PER_YUAN: i64 = 500_000;
const CENTS_PER_YUAN: i64 = 100;
const DEFAULT_PAGE_SIZE: i64 = 10;
const MAX_PAGE_SIZE: i64 = 1000;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InvitationStats {
    pub invitation_code: String,
    pub invited_count: i64,
    pub total_rebate: i64,
    pub completed_rebate: i64,
    pub pending_withdrawal: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RebateRecord {
    pub id: i64,
    pub ulid: Option<String>,
    pub inviter_id: i64,
    pub order_type: String,
    pub order_amount: i64,
    pub rebate_amount: i64,
    pub rebate_ratio: f64,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Deserialize)]
pub struct WithdrawalRequest {
    pub amount: Option<i64>,
    #[serde(default, alias = "rebateRecordIds")]
    pub rebate_record_ids: Vec<i64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WithdrawalRequestResponse {
    pub id: i64,
    pub user_id: i64,
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
#[serde(rename_all = "camelCase")]
pub struct AvailableRebates {
    pub amount: i64,
    pub record_ids: Vec<i64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WithdrawalSubmitResponse {
    pub id: i64,
    pub request_id: i64,
    pub amount: i64,
    pub rebate_record_ids: Vec<i64>,
}

#[derive(Deserialize)]
pub struct PaginationParams {
    pub page: Option<i64>,
    #[serde(alias = "pageSize")]
    pub page_size: Option<i64>,
    pub status: Option<String>,
}

impl PaginationParams {
    pub fn page(&self) -> i64 {
        self.page.unwrap_or(1).max(1)
    }

    pub fn page_size(&self) -> i64 {
        self.page_size
            .unwrap_or(DEFAULT_PAGE_SIZE)
            .clamp(1, MAX_PAGE_SIZE)
    }

    pub fn offset(&self) -> i64 {
        (self.page() - 1) * self.page_size()
    }

    fn rebate_status_filter(&self) -> Option<&str> {
        match self.status.as_deref() {
            Some("pending" | "requested" | "approved" | "completed" | "rejected") => {
                self.status.as_deref()
            }
            _ => None,
        }
    }

    fn withdrawal_status_filter(&self) -> Option<&str> {
        match self.status.as_deref() {
            Some("pending" | "approved" | "completed" | "rejected") => self.status.as_deref(),
            _ => None,
        }
    }
}

// GET /api/invitations/my-code
pub async fn my_code(
    State(state): State<AppState>,
    user: SessionUser,
) -> Result<Json<ApiResponse<InvitationStats>>> {
    let user_info = sqlx::query("SELECT aff_code FROM users WHERE id = $1")
        .bind(user.id)
        .fetch_optional(&state.newapi_pool)
        .await?
        .ok_or_else(|| AppError::NotFound("用户不存在".to_string()))?;

    let aff_code: Option<String> = user_info.try_get("aff_code").ok();

    let invited_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users WHERE inviter_id = $1")
        .bind(user.id)
        .fetch_one(&state.newapi_pool)
        .await?;

    let stats = sqlx::query(
        r#"
        SELECT
            COALESCE(SUM(rebate_amount), 0) as total,
            COALESCE(SUM(CASE WHEN status = 'completed' THEN rebate_amount ELSE 0 END), 0) as completed,
            COALESCE(SUM(CASE WHEN status IN ('pending', 'requested', 'approved') THEN rebate_amount ELSE 0 END), 0) as pending_withdrawal
        FROM rebate_records
        WHERE inviter_id = $1
        "#,
    )
    .bind(user.id)
    .fetch_one(&state.pool)
    .await?;

    Ok(ok(InvitationStats {
        invitation_code: aff_code.unwrap_or_default(),
        invited_count,
        total_rebate: quota_to_cents(stats.try_get("total")?),
        completed_rebate: quota_to_cents(stats.try_get("completed")?),
        pending_withdrawal: quota_to_cents(stats.try_get("pending_withdrawal")?),
    }))
}

// GET /api/invitations/rebate-records
pub async fn rebate_records(
    State(state): State<AppState>,
    user: SessionUser,
    Query(params): Query<PaginationParams>,
) -> Result<Json<ApiResponse<PaginatedResponse<RebateRecord>>>> {
    let records = fetch_rebate_records(
        &state.pool,
        Some(user.id),
        params.rebate_status_filter(),
        params.page_size(),
        params.offset(),
    )
    .await?;

    let total =
        count_rebate_records(&state.pool, Some(user.id), params.rebate_status_filter()).await?;

    Ok(ok(PaginatedResponse {
        items: records,
        total,
        page: params.page(),
        page_size: params.page_size(),
    }))
}

// GET /api/invitations/available-rebates
pub async fn available_rebates(
    State(state): State<AppState>,
    user: SessionUser,
) -> Result<Json<ApiResponse<AvailableRebates>>> {
    let rows = sqlx::query(
        r#"
        SELECT id, rebate_amount
        FROM rebate_records
        WHERE inviter_id = $1 AND status = 'pending'
        ORDER BY created_at DESC
        "#,
    )
    .bind(user.id)
    .fetch_all(&state.pool)
    .await?;

    let mut amount = 0;
    let mut record_ids = Vec::with_capacity(rows.len());
    for row in rows {
        record_ids.push(row.try_get("id")?);
        amount += quota_to_cents(row.try_get("rebate_amount")?);
    }

    Ok(ok(AvailableRebates { amount, record_ids }))
}

// POST /api/invitations/request-withdrawal
pub async fn request_withdrawal(
    State(state): State<AppState>,
    user: SessionUser,
    Json(req): Json<WithdrawalRequest>,
) -> Result<Json<ApiResponse<WithdrawalSubmitResponse>>> {
    let rebate_record_ids = unique_ids(&req.rebate_record_ids)?;
    if rebate_record_ids.is_empty() {
        return Err(AppError::BadRequest("请选择要提现的返利记录".to_string()));
    }

    tracing::info!(
        user_id = user.id,
        rebate_record_count = rebate_record_ids.len(),
        "User requesting withdrawal"
    );

    let mut tx = state.pool.begin().await?;
    let min_amount = get_config_value_tx(&mut tx, "min_withdrawal_amount").await?;
    let frequency_days = get_config_value_tx(&mut tx, "withdrawal_frequency_days").await?;
    let now = Utc::now().timestamp();

    let last_request_time: Option<i64> = sqlx::query_scalar(
        "SELECT MAX(created_at) FROM withdrawal_requests WHERE user_id = $1 AND status != 'rejected'",
    )
    .bind(user.id)
    .fetch_optional(&mut *tx)
    .await?
    .flatten();

    if let Some(last_time) = last_request_time {
        let days_since = (now - last_time) / 86400;
        if days_since < frequency_days {
            tracing::warn!(
                user_id = user.id,
                days_since,
                frequency_days,
                "Withdrawal request rejected: too frequent"
            );
            return Err(AppError::BadRequest(format!(
                "提现申请过于频繁，请等待 {} 天后再试",
                frequency_days - days_since
            )));
        }
    }

    let rows = sqlx::query(
        r#"
        SELECT id, rebate_amount, status
        FROM rebate_records
        WHERE id = ANY($1) AND inviter_id = $2
        ORDER BY id
        FOR UPDATE
        "#,
    )
    .bind(&rebate_record_ids)
    .bind(user.id)
    .fetch_all(&mut *tx)
    .await?;

    if rows.len() != rebate_record_ids.len() {
        return Err(AppError::BadRequest(
            "部分返利记录不存在或不属于您".to_string(),
        ));
    }

    for row in &rows {
        let status: String = row.try_get("status")?;
        if status != "pending" {
            return Err(AppError::BadRequest("部分返利记录状态不正确".to_string()));
        }
    }

    let total_amount: i64 = rows
        .iter()
        .map(|row| row.try_get::<i64, _>("rebate_amount"))
        .collect::<std::result::Result<Vec<_>, _>>()?
        .iter()
        .sum();

    if total_amount < min_amount {
        return Err(AppError::BadRequest(format!(
            "提现金额不足最低限额 {}",
            quota_to_cents(min_amount)
        )));
    }

    if let Some(requested_cents) = req.amount {
        let computed_cents = quota_to_cents(total_amount);
        if requested_cents > 0 && requested_cents != computed_cents {
            tracing::warn!(
                user_id = user.id,
                requested_cents,
                computed_cents,
                "Ignoring client supplied withdrawal amount"
            );
        }
    }

    let request_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO withdrawal_requests (user_id, rebate_record_ids, total_amount, status, created_at, updated_at)
        VALUES ($1, $2, $3, 'pending', $4, $4)
        RETURNING id
        "#,
    )
    .bind(user.id)
    .bind(&rebate_record_ids)
    .bind(total_amount)
    .bind(now)
    .fetch_one(&mut *tx)
    .await?;

    let updated = sqlx::query(
        "UPDATE rebate_records SET status = 'requested', updated_at = $1 WHERE id = ANY($2) AND inviter_id = $3 AND status = 'pending'",
    )
    .bind(now)
    .bind(&rebate_record_ids)
    .bind(user.id)
    .execute(&mut *tx)
    .await?
    .rows_affected();

    if updated != rebate_record_ids.len() as u64 {
        return Err(AppError::BadRequest(
            "部分返利记录已被其他申请占用，请刷新后重试".to_string(),
        ));
    }

    tx.commit().await?;

    tracing::info!(
        user_id = user.id,
        request_id,
        total_amount,
        "Withdrawal request submitted"
    );

    Ok(message(
        WithdrawalSubmitResponse {
            id: request_id,
            request_id,
            amount: quota_to_cents(total_amount),
            rebate_record_ids,
        },
        "提现申请已提交",
    ))
}

// GET /api/invitations/my-requests
pub async fn my_requests(
    State(state): State<AppState>,
    user: SessionUser,
    Query(params): Query<PaginationParams>,
) -> Result<Json<ApiResponse<PaginatedResponse<WithdrawalRequestResponse>>>> {
    let status = params.withdrawal_status_filter();
    let total = count_user_withdrawal_requests(&state.pool, user.id, status).await?;
    let rows = fetch_user_withdrawal_requests(
        &state.pool,
        user.id,
        status,
        params.page_size(),
        params.offset(),
    )
    .await?;

    let requests = rows
        .iter()
        .map(withdrawal_response_from_row)
        .collect::<Result<Vec<_>>>()?;

    Ok(ok(PaginatedResponse {
        items: requests,
        total,
        page: params.page(),
        page_size: params.page_size(),
    }))
}

async fn count_user_withdrawal_requests(
    pool: &PgPool,
    user_id: i64,
    status: Option<&str>,
) -> Result<i64> {
    let total = if let Some(status) = status {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM withdrawal_requests WHERE user_id = $1 AND status = $2",
        )
        .bind(user_id)
        .bind(status)
        .fetch_one(pool)
        .await?
    } else {
        sqlx::query_scalar("SELECT COUNT(*) FROM withdrawal_requests WHERE user_id = $1")
            .bind(user_id)
            .fetch_one(pool)
            .await?
    };

    Ok(total)
}

async fn fetch_user_withdrawal_requests(
    pool: &PgPool,
    user_id: i64,
    status: Option<&str>,
    page_size: i64,
    offset: i64,
) -> Result<Vec<PgRow>> {
    let rows = if let Some(status) = status {
        sqlx::query(
            r#"
            SELECT id, user_id, rebate_record_ids, total_amount, status, created_at, updated_at,
                   approved_at, completed_at, rejected_at, admin_note
            FROM withdrawal_requests
            WHERE user_id = $1 AND status = $2
            ORDER BY created_at DESC
            LIMIT $3 OFFSET $4
            "#,
        )
        .bind(user_id)
        .bind(status)
        .bind(page_size)
        .bind(offset)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query(
            r#"
            SELECT id, user_id, rebate_record_ids, total_amount, status, created_at, updated_at,
                   approved_at, completed_at, rejected_at, admin_note
            FROM withdrawal_requests
            WHERE user_id = $1
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(user_id)
        .bind(page_size)
        .bind(offset)
        .fetch_all(pool)
        .await?
    };

    Ok(rows)
}

pub async fn fetch_rebate_records(
    pool: &PgPool,
    inviter_id: Option<i64>,
    status: Option<&str>,
    page_size: i64,
    offset: i64,
) -> Result<Vec<RebateRecord>> {
    let rows = match (inviter_id, status) {
        (Some(inviter_id), Some(status)) => {
            sqlx::query(
                r#"
                SELECT id, ulid, inviter_id, order_type, order_amount::TEXT as order_amount,
                       rebate_amount, rebate_rate::TEXT as rebate_rate, created_at, updated_at, status
                FROM rebate_records
                WHERE inviter_id = $1 AND status = $2
                ORDER BY created_at DESC
                LIMIT $3 OFFSET $4
                "#,
            )
                .bind(inviter_id)
                .bind(status)
                .bind(page_size)
                .bind(offset)
                .fetch_all(pool)
                .await?
        }
        (Some(inviter_id), None) => {
            sqlx::query(
                r#"
                SELECT id, ulid, inviter_id, order_type, order_amount::TEXT as order_amount,
                       rebate_amount, rebate_rate::TEXT as rebate_rate, created_at, updated_at, status
                FROM rebate_records
                WHERE inviter_id = $1
                ORDER BY created_at DESC
                LIMIT $2 OFFSET $3
                "#,
            )
                .bind(inviter_id)
                .bind(page_size)
                .bind(offset)
                .fetch_all(pool)
                .await?
        }
        (None, Some(status)) => {
            sqlx::query(
                r#"
                SELECT id, ulid, inviter_id, order_type, order_amount::TEXT as order_amount,
                       rebate_amount, rebate_rate::TEXT as rebate_rate, created_at, updated_at, status
                FROM rebate_records
                WHERE status = $1
                ORDER BY created_at DESC
                LIMIT $2 OFFSET $3
                "#,
            )
                .bind(status)
                .bind(page_size)
                .bind(offset)
                .fetch_all(pool)
                .await?
        }
        (None, None) => {
            sqlx::query(
                r#"
                SELECT id, ulid, inviter_id, order_type, order_amount::TEXT as order_amount,
                       rebate_amount, rebate_rate::TEXT as rebate_rate, created_at, updated_at, status
                FROM rebate_records
                ORDER BY created_at DESC
                LIMIT $1 OFFSET $2
                "#,
            )
                .bind(page_size)
                .bind(offset)
                .fetch_all(pool)
                .await?
        }
    };

    rows.iter().map(rebate_record_from_row).collect()
}

pub async fn count_rebate_records(
    pool: &PgPool,
    inviter_id: Option<i64>,
    status: Option<&str>,
) -> Result<i64> {
    let total = match (inviter_id, status) {
        (Some(inviter_id), Some(status)) => {
            sqlx::query_scalar(
                "SELECT COUNT(*) FROM rebate_records WHERE inviter_id = $1 AND status = $2",
            )
            .bind(inviter_id)
            .bind(status)
            .fetch_one(pool)
            .await?
        }
        (Some(inviter_id), None) => {
            sqlx::query_scalar("SELECT COUNT(*) FROM rebate_records WHERE inviter_id = $1")
                .bind(inviter_id)
                .fetch_one(pool)
                .await?
        }
        (None, Some(status)) => {
            sqlx::query_scalar("SELECT COUNT(*) FROM rebate_records WHERE status = $1")
                .bind(status)
                .fetch_one(pool)
                .await?
        }
        (None, None) => {
            sqlx::query_scalar("SELECT COUNT(*) FROM rebate_records")
                .fetch_one(pool)
                .await?
        }
    };

    Ok(total)
}

pub fn quota_to_cents(amount: i64) -> i64 {
    (amount * CENTS_PER_YUAN + QUOTA_UNITS_PER_YUAN / 2) / QUOTA_UNITS_PER_YUAN
}

pub fn cents_to_quota(amount: i64) -> i64 {
    (amount * QUOTA_UNITS_PER_YUAN + CENTS_PER_YUAN / 2) / CENTS_PER_YUAN
}

fn rebate_record_from_row(row: &PgRow) -> Result<RebateRecord> {
    Ok(RebateRecord {
        id: row.try_get("id")?,
        ulid: row.try_get("ulid")?,
        inviter_id: row.try_get("inviter_id")?,
        order_type: row.try_get("order_type")?,
        order_amount: yuan_text_to_cents(row.try_get("order_amount")?),
        rebate_amount: quota_to_cents(row.try_get("rebate_amount")?),
        rebate_ratio: text_to_f64(row.try_get("rebate_rate")?),
        created_at: timestamp_to_iso(row.try_get("created_at")?),
        updated_at: timestamp_to_iso(row.try_get("updated_at")?),
        status: row.try_get("status")?,
    })
}

fn withdrawal_response_from_row(row: &PgRow) -> Result<WithdrawalRequestResponse> {
    Ok(WithdrawalRequestResponse {
        id: row.try_get("id")?,
        user_id: row.try_get("user_id")?,
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

fn unique_ids(ids: &[i64]) -> Result<Vec<i64>> {
    if ids.iter().any(|id| *id <= 0) {
        return Err(AppError::BadRequest("返利记录 ID 无效".to_string()));
    }

    let unique: HashSet<i64> = ids.iter().copied().collect();
    if unique.len() != ids.len() {
        return Err(AppError::BadRequest("返利记录不能重复选择".to_string()));
    }

    let mut result = ids.to_vec();
    result.sort_unstable();
    Ok(result)
}

async fn get_config_value_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key: &str,
) -> Result<i64> {
    let value: String = sqlx::query_scalar("SELECT value FROM system_config WHERE key = $1")
        .bind(key)
        .fetch_one(&mut **tx)
        .await?;

    value
        .parse::<i64>()
        .map_err(|_| AppError::Internal("配置值格式错误".to_string()))
}

fn yuan_text_to_cents(value: String) -> i64 {
    (text_to_f64(value) * CENTS_PER_YUAN as f64).round() as i64
}

fn text_to_f64(value: String) -> f64 {
    value.parse::<f64>().unwrap_or_default()
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
    fn converts_quota_units_to_cents() {
        assert_eq!(quota_to_cents(500_000), 100);
        assert_eq!(quota_to_cents(2_500_000), 500);
        assert_eq!(cents_to_quota(100), 500_000);
        assert_eq!(cents_to_quota(500), 2_500_000);
    }

    #[test]
    fn accepts_camel_case_withdrawal_request_ids() {
        let req: WithdrawalRequest =
            serde_json::from_str(r#"{"amount":500,"rebateRecordIds":[2,1]}"#).unwrap();

        assert_eq!(req.amount, Some(500));
        assert_eq!(unique_ids(&req.rebate_record_ids).unwrap(), vec![1, 2]);
    }

    #[test]
    fn rejects_duplicate_withdrawal_request_ids() {
        let req: WithdrawalRequest =
            serde_json::from_str(r#"{"rebate_record_ids":[1,1]}"#).unwrap();

        assert!(unique_ids(&req.rebate_record_ids).is_err());
    }

    #[test]
    fn separates_rebate_and_withdrawal_status_filters() {
        let requested = PaginationParams {
            page: None,
            page_size: None,
            status: Some("requested".to_string()),
        };
        assert_eq!(requested.rebate_status_filter(), Some("requested"));
        assert_eq!(requested.withdrawal_status_filter(), None);

        let rejected = PaginationParams {
            page: None,
            page_size: None,
            status: Some("rejected".to_string()),
        };
        assert_eq!(rejected.rebate_status_filter(), Some("rejected"));
        assert_eq!(rejected.withdrawal_status_filter(), Some("rejected"));
    }
}
