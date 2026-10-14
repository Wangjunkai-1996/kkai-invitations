use std::collections::HashSet;

use axum::extract::{Query, State};
use axum::Json;
use chrono::{TimeZone, Utc};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};

use crate::error::{AppError, Result};
use crate::invitation_signup::unlock_paid_invitation_signup_rewards;
use crate::rebate_types::{
    is_invitation_signup_reward_order_type, is_order_rebate_order_type, CENTS_PER_YUAN,
    DEFAULT_QUOTA_UNITS_PER_YUAN, INVITATION_SIGNUP_INVITEE_ORDER_TYPE,
    INVITATION_SIGNUP_INVITER_ORDER_TYPE,
};
use crate::response::{message, ok, ApiResponse, PaginatedResponse};
use crate::session::SessionUser;
use crate::system_config::{
    get_config_bool, parse_config_bool, INVITATION_SIGNUP_REWARD_ENABLED_KEY,
    INVITATION_SIGNUP_REWARD_REVIEW_REQUIRED_KEY, ORDER_REBATE_ENABLED_KEY,
    REBATE_TO_BALANCE_ENABLED_KEY, USER_INVITATION_REBATE_ENABLED_KEY,
};
use crate::AppState;

const QUOTA_UNITS_PER_YUAN: i64 = DEFAULT_QUOTA_UNITS_PER_YUAN;
const DEFAULT_PAGE_SIZE: i64 = 10;
const MAX_PAGE_SIZE: i64 = 1000;

#[derive(Clone, Copy)]
pub struct RebateRecordSourceFilter {
    pub order_rebate_enabled: bool,
    pub invitation_signup_reward_enabled: bool,
}

impl RebateRecordSourceFilter {
    fn any_enabled(&self) -> bool {
        self.order_rebate_enabled || self.invitation_signup_reward_enabled
    }

    fn allows_order_type(&self, order_type: &str) -> bool {
        (self.order_rebate_enabled && is_order_rebate_order_type(order_type))
            || (self.invitation_signup_reward_enabled
                && is_invitation_signup_reward_order_type(order_type))
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InvitationStats {
    pub invitation_code: String,
    pub is_sales: bool,
    pub has_sales_account: bool,
    pub invited_count: i64,
    pub total_rebate: i64,
    pub completed_rebate: i64,
    pub pending_rebate: i64,
    pub confirming_rebate: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InvitationFeatureStatus {
    pub available: bool,
    pub user_invitation_rebate_enabled: bool,
    pub order_rebate_enabled: bool,
    pub invitation_signup_reward_enabled: bool,
    pub rebate_to_balance_enabled: bool,
    pub settlement_enabled: bool,
    pub credit_currency: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RebateRecord {
    pub credit_currency: &'static str,
    pub id: i64,
    pub ulid: Option<String>,
    pub inviter_id: i64,
    pub order_type: String,
    pub order_amount: i64,
    pub order_time: String,
    pub rebate_amount: i64,
    pub rebate_ratio: Option<f64>,
    pub status: String,
    pub display_status: String,
    pub issued_at: Option<String>,
    pub unlock_required: bool,
    pub unlocked_at: Option<String>,
    pub effective_at: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RebateRequest {
    pub amount: Option<i64>,
    #[serde(default)]
    pub rebate_record_ids: Vec<i64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RebateRequestResponse {
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
    pub request_kind: String,
    pub issued_at: Option<String>,
    pub issue_note: Option<String>,
    pub credit_currency: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AvailableRebates {
    pub credit_currency: &'static str,
    pub amount: i64,
    pub record_ids: Vec<i64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RebateRequestSubmitResponse {
    pub request_kind: String,
    pub credit_currency: &'static str,
    pub id: i64,
    pub request_id: i64,
    pub amount: i64,
    pub rebate_record_ids: Vec<i64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PaginationParams {
    pub page: Option<i64>,
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
        (self.page() - 1).saturating_mul(self.page_size())
    }

    fn rebate_status_filter(&self) -> Option<&str> {
        match self.status.as_deref() {
            Some(
                "initializing" | "pending" | "requested" | "approved" | "completed" | "rejected",
            ) => self.status.as_deref(),
            _ => None,
        }
    }

    fn rebate_request_status_filter(&self) -> Option<&str> {
        match self.status.as_deref() {
            Some("pending" | "approved" | "completed" | "rejected") => self.status.as_deref(),
            _ => None,
        }
    }
}

// GET /api/invitations/status
pub async fn invitation_feature_status(
    State(state): State<AppState>,
) -> Result<Json<ApiResponse<InvitationFeatureStatus>>> {
    let user_invitation_rebate_enabled =
        get_config_bool(&state.pool, USER_INVITATION_REBATE_ENABLED_KEY).await?;
    let order_rebate_enabled = get_config_bool(&state.pool, ORDER_REBATE_ENABLED_KEY).await?;
    let invitation_signup_reward_enabled =
        get_config_bool(&state.pool, INVITATION_SIGNUP_REWARD_ENABLED_KEY).await?;
    let rebate_to_balance_enabled =
        get_config_bool(&state.pool, REBATE_TO_BALANCE_ENABLED_KEY).await?;

    Ok(ok(InvitationFeatureStatus {
        available: true,
        user_invitation_rebate_enabled,
        order_rebate_enabled,
        invitation_signup_reward_enabled,
        rebate_to_balance_enabled,
        settlement_enabled: state.config.settlement_mode.is_enabled(),
        credit_currency: crate::credit_currency::current_epoch(&state.pool).await?,
    }))
}

// GET /api/invitations/my-code
pub async fn my_code(
    State(state): State<AppState>,
    user: SessionUser,
) -> Result<Json<ApiResponse<InvitationStats>>> {
    let sales_profile = ensure_invitation_access(&state.pool, user.id).await?;
    let source_filter = get_rebate_record_source_filter(&state.pool).await?;

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
            COALESCE(SUM(rebate_amount), 0)::BIGINT as total,
            COALESCE(SUM(CASE WHEN status = 'completed' OR issued.request_id IS NOT NULL THEN rebate_amount ELSE 0 END), 0)::BIGINT as completed,
            COALESCE(SUM(CASE WHEN status IN ('pending', 'requested', 'approved') AND issued.request_id IS NULL THEN rebate_amount ELSE 0 END), 0)::BIGINT as pending_rebate,
            COALESCE(SUM(CASE WHEN status = 'initializing' THEN rebate_amount ELSE 0 END), 0)::BIGINT as confirming_rebate
        FROM rebate_records
        LEFT JOIN rebate_record_request_ownership owner ON owner.rebate_record_id=rebate_records.id AND owner.ownership_state='active'
        LEFT JOIN sales_disbursements issued ON issued.request_id=owner.request_id
        WHERE inviter_id = $1 AND status <> 'closed'
          AND (
              ($2::BOOLEAN = TRUE AND order_type IN ('topup', 'subscription', 'redemption'))
              OR ($3::BOOLEAN = TRUE AND order_type IN ($4, $5))
          )
        "#,
    )
    .bind(user.id)
    .bind(source_filter.order_rebate_enabled)
    .bind(source_filter.invitation_signup_reward_enabled)
    .bind(INVITATION_SIGNUP_INVITER_ORDER_TYPE)
    .bind(INVITATION_SIGNUP_INVITEE_ORDER_TYPE)
    .fetch_one(&state.pool)
    .await?;

    Ok(ok(InvitationStats {
        invitation_code: aff_code.unwrap_or_default(),
        is_sales: sales_profile.active,
        has_sales_account: sales_profile.registered,
        invited_count,
        total_rebate: quota_to_cents(stats.try_get("total")?),
        completed_rebate: quota_to_cents(stats.try_get("completed")?),
        pending_rebate: quota_to_cents(stats.try_get("pending_rebate")?),
        confirming_rebate: quota_to_cents(stats.try_get("confirming_rebate")?),
    }))
}

// GET /api/invitations/rebate-records
pub async fn rebate_records(
    State(state): State<AppState>,
    user: SessionUser,
    Query(params): Query<PaginationParams>,
) -> Result<Json<ApiResponse<PaginatedResponse<RebateRecord>>>> {
    ensure_invitation_access(&state.pool, user.id).await?;
    let source_filter = get_rebate_record_source_filter(&state.pool).await?;
    unlock_paid_invitation_signup_rewards(&state.pool, &state.newapi_pool, Utc::now().timestamp())
        .await?;

    let records = fetch_rebate_records(
        &state.pool,
        Some(user.id),
        params.rebate_status_filter(),
        false,
        Some(source_filter),
        params.page_size(),
        params.offset(),
    )
    .await?;

    let total = count_rebate_records(
        &state.pool,
        Some(user.id),
        params.rebate_status_filter(),
        false,
        Some(source_filter),
    )
    .await?;

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
    let sales_profile = ensure_invitation_access(&state.pool, user.id).await?;
    ensure_claim_access(&state.pool, &sales_profile).await?;
    let source_filter = get_rebate_record_source_filter(&state.pool).await?;
    let now = Utc::now().timestamp();

    if !source_filter.any_enabled() {
        return Ok(ok(AvailableRebates {
            amount: 0,
            record_ids: Vec::new(),
            credit_currency: crate::credit_currency::current_epoch(&state.pool).await?,
        }));
    }

    unlock_paid_invitation_signup_rewards(&state.pool, &state.newapi_pool, now).await?;

    let rows = sqlx::query(
        r#"
        SELECT id, rebate_amount
        FROM rebate_records
        WHERE inviter_id = $1
          AND status = 'pending'
          AND rebate_effective_at <= $2
          AND rebate_amount > 0
          AND (
              rebate_rate IS NOT NULL
              OR order_type IN ($3, $4)
          )
          AND (
              ($5::BOOLEAN = TRUE AND order_type IN ('topup', 'subscription', 'redemption'))
              OR ($6::BOOLEAN = TRUE AND order_type IN ($3, $4))
          )
          AND (
              unlock_required = FALSE
              OR unlocked_at IS NOT NULL
          )
        ORDER BY created_at DESC
        "#,
    )
    .bind(user.id)
    .bind(now)
    .bind(INVITATION_SIGNUP_INVITER_ORDER_TYPE)
    .bind(INVITATION_SIGNUP_INVITEE_ORDER_TYPE)
    .bind(source_filter.order_rebate_enabled)
    .bind(source_filter.invitation_signup_reward_enabled)
    .fetch_all(&state.pool)
    .await?;

    let mut amount: i64 = 0;
    let mut record_ids = Vec::with_capacity(rows.len());
    for row in rows {
        record_ids.push(row.try_get("id")?);
        amount = amount
            .checked_add(row.try_get::<i64, _>("rebate_amount")?)
            .ok_or_else(|| AppError::Internal("返利汇总金额超出范围".into()))?;
    }

    Ok(ok(AvailableRebates {
        amount: quota_to_cents(amount),
        record_ids,
        credit_currency: crate::credit_currency::current_epoch(&state.pool).await?,
    }))
}

// POST /api/invitations/rebate-requests
pub async fn request_rebate(
    State(state): State<AppState>,
    user: SessionUser,
    Json(req): Json<RebateRequest>,
) -> Result<Json<ApiResponse<RebateRequestSubmitResponse>>> {
    let sales_profile = ensure_invitation_access(&state.pool, user.id).await?;
    ensure_claim_access(&state.pool, &sales_profile).await?;
    let source_filter = get_rebate_record_source_filter(&state.pool).await?;
    let now = Utc::now().timestamp();
    unlock_paid_invitation_signup_rewards(&state.pool, &state.newapi_pool, now).await?;

    let rebate_record_ids = unique_ids(&req.rebate_record_ids)?;
    if rebate_record_ids.is_empty() {
        return Err(AppError::BadRequest("请选择要返利的记录".to_string()));
    }

    tracing::info!(
        user_id = user.id,
        rebate_record_count = rebate_record_ids.len(),
        "User requesting rebate to balance"
    );

    let mut tx = state.pool.begin().await?;
    let sales_active: Option<bool> =
        sqlx::query_scalar("SELECT active FROM sales_accounts WHERE user_id=$1 FOR UPDATE")
            .bind(user.id)
            .fetch_optional(&mut *tx)
            .await?;
    if sales_active == Some(false) {
        return Err(AppError::Forbidden);
    }
    let is_sales = sales_active == Some(true);
    let request_kind = if is_sales { "sales" } else { "ordinary" };
    let min_amount = get_config_value_tx(&mut tx, "min_rebate_request_amount").await?;
    let frequency_days = get_config_value_tx(&mut tx, "rebate_request_frequency_days").await?;

    let rows = sqlx::query(
        r#"
        SELECT id, rebate_amount, rebate_rate::TEXT AS rebate_rate, rebate_effective_at, status,
               order_type, unlock_required, unlocked_at
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

    let mut all_invitation_signup_rewards = true;
    for row in &rows {
        let status: String = row.try_get("status")?;
        if status != "pending" {
            return Err(AppError::BadRequest("部分返利记录状态不正确".to_string()));
        }
        let order_type: String = row.try_get("order_type")?;
        if !source_filter.allows_order_type(&order_type) {
            return Err(AppError::BadRequest("该返利功能已关闭".to_string()));
        }
        let is_invitation_signup_reward = is_invitation_signup_reward_order_type(&order_type);
        all_invitation_signup_rewards =
            all_invitation_signup_rewards && is_invitation_signup_reward;
        let unlock_required: bool = row.try_get("unlock_required")?;
        let unlocked_at: Option<i64> = row.try_get("unlocked_at")?;
        let rebate_rate: Option<String> = row.try_get("rebate_rate")?;
        let rebate_amount: i64 = row.try_get("rebate_amount")?;
        let rebate_effective_at: i64 = row.try_get("rebate_effective_at")?;
        if (unlock_required && unlocked_at.is_none())
            || (!is_invitation_signup_reward && rebate_rate.is_none())
            || rebate_amount <= 0
            || rebate_effective_at > now
        {
            return Err(AppError::BadRequest(
                "部分返利记录尚未达到可返利条件".to_string(),
            ));
        }
    }

    let total_amount = rows.iter().try_fold(0_i64, |total, row| -> Result<i64> {
        total
            .checked_add(row.try_get::<i64, _>("rebate_amount")?)
            .ok_or_else(|| AppError::Internal("返利汇总金额超出范围".into()))
    })?;

    if should_enforce_rebate_request_frequency(frequency_days, all_invitation_signup_rewards) {
        let last_request_time: Option<i64> = sqlx::query_scalar(
            "SELECT MAX(created_at) FROM rebate_requests WHERE user_id = $1 AND status != 'rejected'",
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
                    "Rebate request rejected: too frequent"
                );
                return Err(AppError::BadRequest(format!(
                    "返利申请过于频繁，请等待 {} 天后再试",
                    frequency_days - days_since
                )));
            }
        }
    }

    if !all_invitation_signup_rewards && total_amount < min_amount {
        return Err(AppError::BadRequest(format!(
            "返利金额不足最低申请金额 {}",
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
                "Ignoring client supplied rebate amount"
            );
        }
    }

    let invitation_signup_reward_review_required =
        get_config_bool_value_tx(&mut tx, INVITATION_SIGNUP_REWARD_REVIEW_REQUIRED_KEY).await?;
    let auto_approved =
        !is_sales && all_invitation_signup_rewards && !invitation_signup_reward_review_required;
    let request_status = if auto_approved { "approved" } else { "pending" };
    let record_status = if auto_approved {
        "approved"
    } else {
        "requested"
    };
    let approved_at = if auto_approved { Some(now) } else { None };

    let request_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO rebate_requests (user_id, rebate_record_ids, total_amount, status, created_at, updated_at, approved_at, request_kind)
        VALUES ($1, $2, $3, $4, $5, $5, $6, $7)
        RETURNING id
        "#,
    )
    .bind(user.id)
    .bind(&rebate_record_ids)
    .bind(total_amount)
    .bind(request_status)
    .bind(now)
    .bind(approved_at)
    .bind(request_kind)
    .fetch_one(&mut *tx)
    .await?;

    let updated = sqlx::query(
        "UPDATE rebate_records SET status = $1, updated_at = $2 WHERE id = ANY($3) AND inviter_id = $4 AND status = 'pending'",
    )
    .bind(record_status)
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
        auto_approved,
        "Rebate request submitted"
    );

    let response_message = if auto_approved {
        "返利申请已提交，等待管理员手动打款"
    } else {
        "返利申请已提交"
    };

    Ok(message(
        RebateRequestSubmitResponse {
            request_kind: request_kind.into(),
            credit_currency: crate::credit_currency::current_epoch(&state.pool).await?,
            id: request_id,
            request_id,
            amount: quota_to_cents(total_amount),
            rebate_record_ids,
        },
        response_message,
    ))
}

// GET /api/invitations/rebate-requests
pub async fn my_rebate_requests(
    State(state): State<AppState>,
    user: SessionUser,
    Query(params): Query<PaginationParams>,
) -> Result<Json<ApiResponse<PaginatedResponse<RebateRequestResponse>>>> {
    let sales_profile = ensure_invitation_access(&state.pool, user.id).await?;
    if !sales_profile.registered {
        ensure_rebate_to_balance_enabled(&state.pool).await?;
    }

    let status = params.rebate_request_status_filter();
    let total = count_user_rebate_requests(&state.pool, user.id, status).await?;
    let rows = fetch_user_rebate_requests(
        &state.pool,
        user.id,
        status,
        params.page_size(),
        params.offset(),
    )
    .await?;

    let credit_currency = crate::credit_currency::current_epoch(&state.pool).await?;
    let requests = rows
        .iter()
        .map(|row| rebate_request_response_from_row(row, credit_currency))
        .collect::<Result<Vec<_>>>()?;

    Ok(ok(PaginatedResponse {
        items: requests,
        total,
        page: params.page(),
        page_size: params.page_size(),
    }))
}

async fn count_user_rebate_requests(
    pool: &PgPool,
    user_id: i64,
    status: Option<&str>,
) -> Result<i64> {
    let total = if let Some(status) = status {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM rebate_requests WHERE user_id = $1 AND (CASE WHEN EXISTS(SELECT 1 FROM sales_disbursements WHERE request_id=rebate_requests.id) THEN 'completed' ELSE status END) = $2",
        )
        .bind(user_id)
        .bind(status)
        .fetch_one(pool)
        .await?
    } else {
        sqlx::query_scalar("SELECT COUNT(*) FROM rebate_requests WHERE user_id = $1")
            .bind(user_id)
            .fetch_one(pool)
            .await?
    };

    Ok(total)
}

async fn fetch_user_rebate_requests(
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
                   approved_at, completed_at, rejected_at, admin_note, request_kind,
                   (SELECT confirmed_at FROM sales_disbursements WHERE request_id=rebate_requests.id) AS issued_at,
                   (SELECT note FROM sales_disbursements WHERE request_id=rebate_requests.id) AS issue_note
            FROM rebate_requests
            WHERE user_id = $1 AND (CASE WHEN EXISTS(SELECT 1 FROM sales_disbursements WHERE request_id=rebate_requests.id) THEN 'completed' ELSE status END) = $2
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
                   approved_at, completed_at, rejected_at, admin_note, request_kind,
                   (SELECT confirmed_at FROM sales_disbursements WHERE request_id=rebate_requests.id) AS issued_at,
                   (SELECT note FROM sales_disbursements WHERE request_id=rebate_requests.id) AS issue_note
            FROM rebate_requests
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
    hide_initializing: bool,
    source_filter: Option<RebateRecordSourceFilter>,
    page_size: i64,
    offset: i64,
) -> Result<Vec<RebateRecord>> {
    let source_filter_enabled = source_filter.is_some();
    let order_rebate_enabled = source_filter
        .map(|filter| filter.order_rebate_enabled)
        .unwrap_or(false);
    let invitation_signup_reward_enabled = source_filter
        .map(|filter| filter.invitation_signup_reward_enabled)
        .unwrap_or(false);

    let rows = sqlx::query(
        r#"
        SELECT id, ulid, inviter_id, order_type, order_amount::TEXT as order_amount,
               order_time, rebate_amount, rebate_rate::TEXT as rebate_rate, rebate_effective_at,
               unlock_required, unlocked_at, created_at, updated_at, status,
               (SELECT issued.confirmed_at FROM sales_disbursements issued
                JOIN rebate_record_request_ownership owner ON owner.request_id=issued.request_id
                WHERE owner.rebate_record_id=rebate_records.id AND owner.ownership_state='active') AS issued_at
        FROM rebate_records
        WHERE ($1::BIGINT IS NULL OR inviter_id = $1)
          AND ($2::TEXT IS NULL OR (CASE WHEN EXISTS(
              SELECT 1 FROM sales_disbursements issued JOIN rebate_record_request_ownership owner ON owner.request_id=issued.request_id
              WHERE owner.rebate_record_id=rebate_records.id AND owner.ownership_state='active'
          ) THEN 'completed' ELSE status END) = $2)
          AND status <> 'closed'
          AND ($3::BOOLEAN = FALSE OR status <> 'initializing')
          AND (
              $4::BOOLEAN = FALSE
              OR ($5::BOOLEAN = TRUE AND order_type IN ('topup', 'subscription', 'redemption'))
              OR ($6::BOOLEAN = TRUE AND order_type IN ($7, $8))
          )
        ORDER BY created_at DESC
        LIMIT $9 OFFSET $10
        "#,
    )
    .bind(inviter_id)
    .bind(status)
    .bind(hide_initializing)
    .bind(source_filter_enabled)
    .bind(order_rebate_enabled)
    .bind(invitation_signup_reward_enabled)
    .bind(INVITATION_SIGNUP_INVITER_ORDER_TYPE)
    .bind(INVITATION_SIGNUP_INVITEE_ORDER_TYPE)
    .bind(page_size)
    .bind(offset)
    .fetch_all(pool)
    .await?;

    let now = Utc::now().timestamp();
    let credit_currency = crate::credit_currency::current_epoch(pool).await?;
    rows.iter()
        .map(|row| rebate_record_from_row(row, now, credit_currency))
        .collect()
}

pub async fn count_rebate_records(
    pool: &PgPool,
    inviter_id: Option<i64>,
    status: Option<&str>,
    hide_initializing: bool,
    source_filter: Option<RebateRecordSourceFilter>,
) -> Result<i64> {
    let source_filter_enabled = source_filter.is_some();
    let order_rebate_enabled = source_filter
        .map(|filter| filter.order_rebate_enabled)
        .unwrap_or(false);
    let invitation_signup_reward_enabled = source_filter
        .map(|filter| filter.invitation_signup_reward_enabled)
        .unwrap_or(false);

    let total = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM rebate_records
        WHERE ($1::BIGINT IS NULL OR inviter_id = $1)
          AND ($2::TEXT IS NULL OR (CASE WHEN EXISTS(
              SELECT 1 FROM sales_disbursements issued JOIN rebate_record_request_ownership owner ON owner.request_id=issued.request_id
              WHERE owner.rebate_record_id=rebate_records.id AND owner.ownership_state='active'
          ) THEN 'completed' ELSE status END) = $2)
          AND status <> 'closed'
          AND ($3::BOOLEAN = FALSE OR status <> 'initializing')
          AND (
              $4::BOOLEAN = FALSE
              OR ($5::BOOLEAN = TRUE AND order_type IN ('topup', 'subscription', 'redemption'))
              OR ($6::BOOLEAN = TRUE AND order_type IN ($7, $8))
          )
        "#,
    )
    .bind(inviter_id)
    .bind(status)
    .bind(hide_initializing)
    .bind(source_filter_enabled)
    .bind(order_rebate_enabled)
    .bind(invitation_signup_reward_enabled)
    .bind(INVITATION_SIGNUP_INVITER_ORDER_TYPE)
    .bind(INVITATION_SIGNUP_INVITEE_ORDER_TYPE)
    .fetch_one(pool)
    .await?;

    Ok(total)
}

pub async fn fetch_user_related_rebate_records(
    pool: &PgPool,
    user_id: i64,
    page_size: i64,
    offset: i64,
) -> Result<Vec<RebateRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT id, ulid, inviter_id, order_type, order_amount::TEXT as order_amount,
               order_time, rebate_amount, rebate_rate::TEXT as rebate_rate, rebate_effective_at,
               unlock_required, unlocked_at, created_at, updated_at, status,
               (SELECT issued.confirmed_at FROM sales_disbursements issued
                JOIN rebate_record_request_ownership owner ON owner.request_id=issued.request_id
                WHERE owner.rebate_record_id=rebate_records.id AND owner.ownership_state='active') AS issued_at
        FROM rebate_records
        WHERE inviter_id = $1 OR invitee_id = $1
        ORDER BY created_at DESC
        LIMIT $2 OFFSET $3
        "#,
    )
    .bind(user_id)
    .bind(page_size)
    .bind(offset)
    .fetch_all(pool)
    .await?;

    let now = Utc::now().timestamp();
    let credit_currency = crate::credit_currency::current_epoch(pool).await?;
    rows.iter()
        .map(|row| rebate_record_from_row(row, now, credit_currency))
        .collect()
}

pub async fn count_user_related_rebate_records(pool: &PgPool, user_id: i64) -> Result<i64> {
    sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM rebate_records
        WHERE inviter_id = $1 OR invitee_id = $1
        "#,
    )
    .bind(user_id)
    .fetch_one(pool)
    .await
    .map_err(AppError::from)
}

pub fn quota_to_cents(amount: i64) -> i64 {
    // Dividing by 5,000 after multiplication keeps every i64 input within i64.
    ((i128::from(amount) * i128::from(CENTS_PER_YUAN) + i128::from(QUOTA_UNITS_PER_YUAN / 2))
        / i128::from(QUOTA_UNITS_PER_YUAN)) as i64
}

pub fn cents_to_quota(amount: i64) -> Result<i64> {
    if amount < 0 {
        return Err(AppError::BadRequest("金额不能为负数".into()));
    }
    let quota = (i128::from(amount) * i128::from(QUOTA_UNITS_PER_YUAN)
        + i128::from(CENTS_PER_YUAN / 2))
        / i128::from(CENTS_PER_YUAN);
    i64::try_from(quota).map_err(|_| AppError::BadRequest("金额超出支持范围".into()))
}

fn rebate_record_from_row(
    row: &PgRow,
    now: i64,
    credit_currency: &'static str,
) -> Result<RebateRecord> {
    let rebate_rate: Option<String> = row.try_get("rebate_rate")?;
    let rebate_effective_at: i64 = row.try_get("rebate_effective_at")?;
    let status: String = row.try_get("status")?;
    let order_type: String = row.try_get("order_type")?;
    let unlock_required: bool = row.try_get("unlock_required")?;
    let unlocked_at: Option<i64> = row.try_get("unlocked_at")?;
    let rule_missing =
        rebate_rate.is_none() && !is_invitation_signup_reward_order_type(&order_type);
    let waiting_unlock = unlock_required && unlocked_at.is_none();
    let issued_at: Option<i64> = row.try_get("issued_at")?;
    let display_status = if issued_at.is_some() {
        "paid"
    } else {
        derive_rebate_display_status(
            &status,
            now,
            rebate_effective_at,
            rule_missing,
            waiting_unlock,
        )
    }
    .to_string();

    Ok(RebateRecord {
        credit_currency,
        id: row.try_get("id")?,
        ulid: row.try_get("ulid")?,
        inviter_id: row.try_get("inviter_id")?,
        order_type,
        order_amount: yuan_text_to_cents(row.try_get("order_amount")?),
        order_time: timestamp_to_iso(row.try_get("order_time")?),
        rebate_amount: quota_to_cents(row.try_get("rebate_amount")?),
        rebate_ratio: rebate_rate.map(text_to_f64),
        status,
        display_status,
        issued_at: optional_timestamp_to_iso(issued_at),
        unlock_required,
        unlocked_at: optional_timestamp_to_iso(unlocked_at),
        effective_at: timestamp_to_iso(rebate_effective_at),
        created_at: timestamp_to_iso(row.try_get("created_at")?),
        updated_at: timestamp_to_iso(row.try_get("updated_at")?),
    })
}

fn derive_rebate_display_status(
    status: &str,
    now: i64,
    effective_at: i64,
    rule_missing: bool,
    waiting_unlock: bool,
) -> &'static str {
    match status {
        "initializing" => "confirming",
        "requested" => "requested",
        "approved" => "approved",
        "completed" => "paid",
        "pending" | "rejected" => {
            if waiting_unlock {
                "waiting_unlock"
            } else if rule_missing || now < effective_at {
                "estimated"
            } else {
                "claimable"
            }
        }
        _ => "estimated",
    }
}

fn rebate_request_response_from_row(
    row: &PgRow,
    credit_currency: &'static str,
) -> Result<RebateRequestResponse> {
    let status: String = row.try_get("status")?;
    let admin_note: Option<String> = row.try_get("admin_note")?;
    Ok(RebateRequestResponse {
        credit_currency,
        id: row.try_get("id")?,
        user_id: row.try_get("user_id")?,
        amount: quota_to_cents(row.try_get("total_amount")?),
        status: status.clone(),
        rebate_record_ids: row.try_get("rebate_record_ids")?,
        created_at: timestamp_to_iso(row.try_get("created_at")?),
        updated_at: timestamp_to_iso(row.try_get("updated_at")?),
        approved_at: optional_timestamp_to_iso(row.try_get("approved_at")?),
        completed_at: optional_timestamp_to_iso(row.try_get("completed_at")?),
        rejected_at: optional_timestamp_to_iso(row.try_get("rejected_at")?),
        request_kind: row.try_get("request_kind")?,
        issued_at: optional_timestamp_to_iso(row.try_get("issued_at")?),
        issue_note: row.try_get("issue_note")?,
        reject_reason: if status == "rejected" {
            admin_note
        } else {
            None
        },
    })
}

fn should_enforce_rebate_request_frequency(
    frequency_days: i64,
    all_invitation_signup_rewards: bool,
) -> bool {
    !all_invitation_signup_rewards && frequency_days > 0
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

async fn ensure_invitation_access(
    pool: &PgPool,
    user_id: i64,
) -> Result<crate::sales::SalesProfile> {
    let profile = crate::sales::profile(pool, user_id).await?;
    if !profile.registered {
        ensure_user_invitation_rebate_enabled(pool).await?;
    }
    Ok(profile)
}

async fn ensure_claim_access(pool: &PgPool, profile: &crate::sales::SalesProfile) -> Result<()> {
    if profile.registered {
        if !profile.active {
            return Err(AppError::Forbidden);
        }
        return Ok(());
    }
    ensure_rebate_to_balance_enabled(pool).await
}

async fn ensure_user_invitation_rebate_enabled(pool: &PgPool) -> Result<()> {
    if get_config_bool(pool, USER_INVITATION_REBATE_ENABLED_KEY).await? {
        return Ok(());
    }

    Err(AppError::Forbidden)
}

async fn ensure_rebate_to_balance_enabled(pool: &PgPool) -> Result<()> {
    if get_config_bool(pool, REBATE_TO_BALANCE_ENABLED_KEY).await? {
        return Ok(());
    }

    Err(AppError::Forbidden)
}

async fn get_rebate_record_source_filter(pool: &PgPool) -> Result<RebateRecordSourceFilter> {
    Ok(RebateRecordSourceFilter {
        order_rebate_enabled: get_config_bool(pool, ORDER_REBATE_ENABLED_KEY).await?,
        invitation_signup_reward_enabled: get_config_bool(
            pool,
            INVITATION_SIGNUP_REWARD_ENABLED_KEY,
        )
        .await?,
    })
}

async fn get_config_bool_value_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key: &str,
) -> Result<bool> {
    let value: String = sqlx::query_scalar("SELECT value FROM system_config WHERE key = $1")
        .bind(key)
        .fetch_one(&mut **tx)
        .await?;

    parse_config_bool(&value)
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
        assert_eq!(cents_to_quota(100).unwrap(), 500_000);
        assert_eq!(cents_to_quota(500).unwrap(), 2_500_000);
        assert_eq!(quota_to_cents(i64::MAX), 1_844_674_407_370_955);
        assert!(cents_to_quota(-1).is_err());
        assert!(cents_to_quota(i64::MAX).is_err());
    }

    #[test]
    fn accepts_camel_case_rebate_request_ids() {
        let req: RebateRequest =
            serde_json::from_str(r#"{"amount":500,"rebateRecordIds":[2,1]}"#).unwrap();

        assert_eq!(req.amount, Some(500));
        assert_eq!(unique_ids(&req.rebate_record_ids).unwrap(), vec![1, 2]);
    }

    #[test]
    fn rejects_duplicate_rebate_request_ids() {
        let req: RebateRequest = serde_json::from_str(r#"{"rebateRecordIds":[1,1]}"#).unwrap();

        assert!(unique_ids(&req.rebate_record_ids).is_err());
    }

    #[test]
    fn separates_rebate_and_rebate_request_status_filters() {
        let initializing = PaginationParams {
            page: None,
            page_size: None,
            status: Some("initializing".to_string()),
        };
        assert_eq!(initializing.rebate_status_filter(), Some("initializing"));
        assert_eq!(initializing.rebate_request_status_filter(), None);

        let requested = PaginationParams {
            page: None,
            page_size: None,
            status: Some("requested".to_string()),
        };
        assert_eq!(requested.rebate_status_filter(), Some("requested"));
        assert_eq!(requested.rebate_request_status_filter(), None);

        let rejected = PaginationParams {
            page: None,
            page_size: None,
            status: Some("rejected".to_string()),
        };
        assert_eq!(rejected.rebate_status_filter(), Some("rejected"));
        assert_eq!(rejected.rebate_request_status_filter(), Some("rejected"));
    }

    #[test]
    fn rebate_request_frequency_zero_disables_limit() {
        assert!(!should_enforce_rebate_request_frequency(0, false));
        assert!(!should_enforce_rebate_request_frequency(-1, false));
        assert!(!should_enforce_rebate_request_frequency(7, true));
        assert!(should_enforce_rebate_request_frequency(7, false));
    }

    #[test]
    fn derives_user_facing_rebate_statuses() {
        assert_eq!(
            derive_rebate_display_status("initializing", 1_000, 2_000, false, false),
            "confirming"
        );
        assert_eq!(
            derive_rebate_display_status("pending", 1_000, 2_000, false, false),
            "estimated"
        );
        assert_eq!(
            derive_rebate_display_status("pending", 2_000, 1_000, false, false),
            "claimable"
        );
        assert_eq!(
            derive_rebate_display_status("pending", 2_000, 1_000, true, false),
            "estimated"
        );
        assert_eq!(
            derive_rebate_display_status("requested", 2_000, 1_000, false, false),
            "requested"
        );
        assert_eq!(
            derive_rebate_display_status("approved", 2_000, 1_000, false, false),
            "approved"
        );
        assert_eq!(
            derive_rebate_display_status("completed", 1_000, 2_000, false, true),
            "paid"
        );
        assert_eq!(
            derive_rebate_display_status("pending", 2_000, 1_000, false, true),
            "waiting_unlock"
        );
    }

    #[test]
    fn serializes_confirming_rebate_in_invitation_stats() {
        let stats = InvitationStats {
            invitation_code: "invite".to_string(),
            is_sales: false,
            has_sales_account: false,
            invited_count: 2,
            total_rebate: 1_500,
            completed_rebate: 500,
            pending_rebate: 500,
            confirming_rebate: 500,
        };

        let value = serde_json::to_value(stats).unwrap();
        assert_eq!(value["confirmingRebate"], 500);
    }

    #[test]
    fn rebate_source_filter_separates_order_and_signup_rewards() {
        let order_only = RebateRecordSourceFilter {
            order_rebate_enabled: true,
            invitation_signup_reward_enabled: false,
        };
        assert!(order_only.any_enabled());
        assert!(order_only.allows_order_type("topup"));
        assert!(order_only.allows_order_type("subscription"));
        assert!(!order_only.allows_order_type(INVITATION_SIGNUP_INVITER_ORDER_TYPE));

        let signup_only = RebateRecordSourceFilter {
            order_rebate_enabled: false,
            invitation_signup_reward_enabled: true,
        };
        assert!(signup_only.any_enabled());
        assert!(!signup_only.allows_order_type("topup"));
        assert!(signup_only.allows_order_type(INVITATION_SIGNUP_INVITER_ORDER_TYPE));
        assert!(signup_only.allows_order_type(INVITATION_SIGNUP_INVITEE_ORDER_TYPE));

        let disabled = RebateRecordSourceFilter {
            order_rebate_enabled: false,
            invitation_signup_reward_enabled: false,
        };
        assert!(!disabled.any_enabled());
        assert!(!disabled.allows_order_type("topup"));
        assert!(!disabled.allows_order_type(INVITATION_SIGNUP_INVITEE_ORDER_TYPE));
    }
}
