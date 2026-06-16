use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::Json;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Postgres, Row, Transaction};
use thiserror::Error;

use crate::error::{AppError, Result};
use crate::middleware::AdminUser;
use crate::response::{ok, ApiResponse};
use crate::AppState;

const MAX_REQUEST_ID_LEN: usize = 128;

#[derive(Debug, Error)]
enum PayoutError {
    #[error("rebate record not found")]
    NotFound,
    #[error("rebate record is not approved")]
    NotApproved,
    #[error("completed rebate record has no payout ledger")]
    HistoricalComplete,
    #[error("quota conversion failed: {0}")]
    InvalidQuota(String),
    #[error("user quota is insufficient for reversal")]
    InsufficientQuota,
    #[error("payout is not completed")]
    PayoutNotCompleted,
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

type PayoutResult<T> = std::result::Result<T, PayoutError>;

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PayoutActionParams {
    pub dry_run: Option<String>,
}

impl PayoutActionParams {
    fn is_dry_run(&self) -> bool {
        matches!(
            self.dry_run.as_deref().map(str::trim),
            Some("1" | "true" | "yes" | "on")
        )
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PayResult {
    pub outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payout_id: Option<i64>,
    pub record_id: i64,
    pub request_id: String,
    pub amount_cents: i64,
    pub quota_delta: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub balance_after: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub newapi_log_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub newapi_mark_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    pub payout_enabled: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PayoutStatusResult {
    pub record_id: i64,
    pub record_status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payout_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payout_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub amount_cents: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quota_delta: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub newapi_log_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub newapi_mark_id: Option<i64>,
    pub can_pay: bool,
    pub can_reverse: bool,
    pub needs_review: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

struct RebateRecord {
    id: i64,
    request_id: Option<i64>,
    inviter_user_id: i64,
    invitee_user_id: i64,
    order_id: i64,
    order_type: String,
    rebate_quota: i64,
    status: String,
    amount_cents: i64,
}

#[derive(Clone)]
struct Payout {
    id: i64,
    rebate_record_id: i64,
    rebate_request_id: Option<i64>,
    inviter_user_id: i64,
    invitee_user_id: i64,
    order_id: i64,
    order_type: String,
    amount_cents: i64,
    quota_delta: i64,
    status: String,
    request_id: String,
    newapi_mark_id: Option<i64>,
    newapi_log_id: Option<i64>,
}

struct NewApiBalanceMark {
    id: i64,
    log_id: Option<i64>,
    balance: i64,
}

enum ReservePayoutResult {
    Reserved(Payout),
    AlreadyPaid(Payout),
}

enum ReserveReversalResult {
    Reserved { payout: Payout, reversal: Payout },
    AlreadyReversed(Payout),
}

// GET /api/admin/rebate-payouts/:id/status
pub async fn get_payout_status(
    State(state): State<AppState>,
    _admin: AdminUser,
    Path(record_id): Path<i64>,
) -> Result<Json<ApiResponse<PayoutStatusResult>>> {
    validate_record_id(record_id)?;
    let status = read_payout_status(&state, record_id).await?;
    Ok(ok(status))
}

// POST /api/admin/rebate-payouts/:id/pay
pub async fn pay_rebate_payout(
    State(state): State<AppState>,
    admin: AdminUser,
    Path(record_id): Path<i64>,
    Query(params): Query<PayoutActionParams>,
    headers: HeaderMap,
) -> Result<Json<ApiResponse<PayResult>>> {
    validate_record_id(record_id)?;
    require_payout_action_header(&headers, "pay")?;
    let request_id = request_id_from_headers(&headers)?;
    let now = Utc::now().timestamp();

    if !state.config.payout_enabled || params.is_dry_run() {
        return preview_pay(&state, record_id, request_id, &admin, now).await;
    }

    let payout = match reserve_payout(&state, record_id, &request_id, &admin, now).await {
        Ok(ReservePayoutResult::Reserved(payout)) => payout,
        Ok(ReservePayoutResult::AlreadyPaid(payout)) => {
            record_attempt(
                &state.pool,
                record_id,
                "pay",
                &request_id,
                &admin,
                "already_paid",
                200,
                "",
                now,
            )
            .await;
            return Ok(ok(pay_result_from_payout(
                "already_paid",
                &payout,
                None,
                state.config.payout_enabled,
            )));
        }
        Err(err) => {
            return fail_pay(&state.pool, record_id, &request_id, &admin, err, now).await;
        }
    };

    let mark = match apply_newapi_payout(&state.newapi_write_pool, &payout, now).await {
        Ok(mark) => mark,
        Err(err) => {
            mark_payout_failed(&state.pool, payout.id, &err.to_string(), now).await;
            record_attempt(
                &state.pool,
                record_id,
                "pay",
                &request_id,
                &admin,
                "failed",
                500,
                &err.to_string(),
                now,
            )
            .await;
            tracing::error!(
                record_id,
                payout_id = payout.id,
                error = %err,
                "New API payout apply failed"
            );
            return Err(AppError::Internal("余额发放失败，请稍后重试".to_string()));
        }
    };

    if let Err(err) = complete_payout(&state.pool, &payout, &mark, now).await {
        record_attempt(
            &state.pool,
            record_id,
            "pay",
            &request_id,
            &admin,
            "newapi_applied_completion_failed",
            500,
            &err.to_string(),
            now,
        )
        .await;
        tracing::error!(
            record_id,
            payout_id = payout.id,
            mark_id = mark.id,
            error = %err,
            "Payout completion failed after New API balance apply"
        );
        return Err(AppError::Internal(
            "余额已发放，但返利状态回写失败；请重试发放以补齐状态".to_string(),
        ));
    }

    record_attempt(
        &state.pool,
        record_id,
        "pay",
        &request_id,
        &admin,
        "paid",
        200,
        "",
        now,
    )
    .await;

    Ok(ok(pay_result_from_payout(
        "paid",
        &payout,
        Some(&mark),
        state.config.payout_enabled,
    )))
}

// POST /api/admin/rebate-payouts/:id/reverse
pub async fn reverse_rebate_payout(
    State(state): State<AppState>,
    admin: AdminUser,
    Path(record_id): Path<i64>,
    headers: HeaderMap,
) -> Result<Json<ApiResponse<PayResult>>> {
    validate_record_id(record_id)?;
    require_payout_action_header(&headers, "reverse")?;
    let request_id = request_id_from_headers(&headers)?;
    let now = Utc::now().timestamp();

    if !state.config.payout_enabled {
        record_attempt(
            &state.pool,
            record_id,
            "reverse",
            &request_id,
            &admin,
            "disabled",
            423,
            "payout is disabled",
            now,
        )
        .await;
        return Err(AppError::Locked("返利余额发放开关未开启".to_string()));
    }

    let (payout, reversal) =
        match reserve_reversal(&state, record_id, &request_id, &admin, now).await {
            Ok(ReserveReversalResult::Reserved { payout, reversal }) => (payout, reversal),
            Ok(ReserveReversalResult::AlreadyReversed(reversal)) => {
                record_attempt(
                    &state.pool,
                    record_id,
                    "reverse",
                    &request_id,
                    &admin,
                    "already_reversed",
                    200,
                    "",
                    now,
                )
                .await;
                return Ok(ok(pay_result_from_payout(
                    "already_reversed",
                    &reversal,
                    None,
                    state.config.payout_enabled,
                )));
            }
            Err(err) => {
                return fail_reverse(&state.pool, record_id, &request_id, &admin, err, now).await;
            }
        };

    let mark = match apply_newapi_reversal(&state.newapi_write_pool, &reversal, &payout, now).await
    {
        Ok(mark) => mark,
        Err(PayoutError::InsufficientQuota) => {
            mark_reversal_pending(
                &state.pool,
                payout.id,
                reversal.id,
                "user quota is insufficient for reversal",
                now,
            )
            .await;
            record_attempt(
                &state.pool,
                record_id,
                "reverse",
                &request_id,
                &admin,
                "reversal_pending",
                409,
                "user quota is insufficient for reversal",
                now,
            )
            .await;
            return Err(AppError::Conflict(
                "用户余额不足，冲正已标记为待复核".to_string(),
            ));
        }
        Err(err) => {
            record_attempt(
                &state.pool,
                record_id,
                "reverse",
                &request_id,
                &admin,
                "failed",
                500,
                &err.to_string(),
                now,
            )
            .await;
            tracing::error!(record_id, error = %err, "Payout reversal failed");
            return Err(AppError::Internal("返利冲正失败，请稍后重试".to_string()));
        }
    };

    if let Err(err) = complete_reversal(&state.pool, &payout, &reversal, &mark, now).await {
        record_attempt(
            &state.pool,
            record_id,
            "reverse",
            &request_id,
            &admin,
            "newapi_applied_completion_failed",
            500,
            &err.to_string(),
            now,
        )
        .await;
        tracing::error!(
            record_id,
            payout_id = payout.id,
            reversal_id = reversal.id,
            mark_id = mark.id,
            error = %err,
            "Payout reversal completion failed after New API balance apply"
        );
        return Err(AppError::Internal(
            "余额已冲正，但返利状态回写失败；请重试冲正以补齐状态".to_string(),
        ));
    }

    record_attempt(
        &state.pool,
        record_id,
        "reverse",
        &request_id,
        &admin,
        "reversed",
        200,
        "",
        now,
    )
    .await;

    Ok(ok(pay_result_from_payout(
        "reversed",
        &reversal,
        Some(&mark),
        state.config.payout_enabled,
    )))
}

async fn preview_pay(
    state: &AppState,
    record_id: i64,
    request_id: String,
    admin: &AdminUser,
    now: i64,
) -> Result<Json<ApiResponse<PayResult>>> {
    match preview_pay_record(state, record_id).await {
        Ok(record) => {
            record_attempt(
                &state.pool,
                record_id,
                "pay",
                &request_id,
                admin,
                "dry_run",
                200,
                "",
                now,
            )
            .await;
            Ok(ok(PayResult {
                outcome: "dry_run".to_string(),
                payout_id: None,
                record_id: record.id,
                request_id,
                amount_cents: record.amount_cents,
                quota_delta: record.rebate_quota,
                balance_after: None,
                newapi_log_id: None,
                newapi_mark_id: None,
                message: Some("payout is disabled".to_string()),
                payout_enabled: state.config.payout_enabled,
            }))
        }
        Err(err) => fail_pay(&state.pool, record_id, &request_id, admin, err, now).await,
    }
}

async fn preview_pay_record(state: &AppState, record_id: i64) -> PayoutResult<RebateRecord> {
    let mut tx = state.pool.begin().await?;
    let record = read_rebate_record(&mut tx, record_id, false, state.config.quota_per_unit).await?;
    if record.status == "completed" {
        match read_payout_by_record(&mut tx, record_id, false).await {
            Ok(_) => {}
            Err(PayoutError::NotFound) => return Err(PayoutError::HistoricalComplete),
            Err(err) => return Err(err),
        }
    }
    if record.status != "approved" {
        return Err(PayoutError::NotApproved);
    }
    tx.commit().await?;
    Ok(record)
}

async fn read_payout_status(state: &AppState, record_id: i64) -> Result<PayoutStatusResult> {
    let mut tx = state.pool.begin().await?;
    let record = read_rebate_record(&mut tx, record_id, false, state.config.quota_per_unit)
        .await
        .map_err(app_error_from_payout_error)?;

    let mut result = PayoutStatusResult {
        record_id: record.id,
        record_status: record.status.clone(),
        payout_status: None,
        payout_id: None,
        request_id: None,
        amount_cents: None,
        quota_delta: None,
        newapi_log_id: None,
        newapi_mark_id: None,
        can_pay: record.status == "approved",
        can_reverse: false,
        needs_review: false,
        message: None,
    };

    match read_payout_by_record(&mut tx, record_id, false).await {
        Ok(payout) => {
            result.payout_status = Some(payout.status.clone());
            result.payout_id = Some(payout.id);
            result.request_id = Some(payout.request_id);
            result.amount_cents = Some(payout.amount_cents);
            result.quota_delta = Some(payout.quota_delta);
            result.newapi_log_id = payout.newapi_log_id;
            result.newapi_mark_id = payout.newapi_mark_id;
            result.can_pay = false;
            result.can_reverse = matches!(payout.status.as_str(), "completed" | "reversal_pending");
            result.needs_review = matches!(payout.status.as_str(), "failed" | "reversal_pending");
            if payout.status == "reversal_pending" {
                result.message = Some("冲正待复核：用户余额不足或上次冲正未完成".to_string());
            }
        }
        Err(PayoutError::NotFound) => {
            if record.status == "completed" {
                result.needs_review = true;
                result.message = Some("历史已完成记录缺少余额发放流水，需要人工复核".to_string());
            }
        }
        Err(err) => return Err(app_error_from_payout_error(err)),
    }

    tx.commit().await?;
    Ok(result)
}

async fn reserve_payout(
    state: &AppState,
    record_id: i64,
    request_id: &str,
    admin: &AdminUser,
    now: i64,
) -> PayoutResult<ReservePayoutResult> {
    let mut tx = state.pool.begin().await?;
    let record = read_rebate_record(&mut tx, record_id, true, state.config.quota_per_unit).await?;

    match read_payout_by_record(&mut tx, record_id, false).await {
        Ok(existing) if matches!(existing.status.as_str(), "completed" | "reversed") => {
            tx.commit().await?;
            return Ok(ReservePayoutResult::AlreadyPaid(existing));
        }
        Ok(existing) => {
            tx.commit().await?;
            return Ok(ReservePayoutResult::Reserved(existing));
        }
        Err(PayoutError::NotFound) => {}
        Err(err) => return Err(err),
    }

    if record.status == "completed" {
        return Err(PayoutError::HistoricalComplete);
    }
    if record.status != "approved" {
        return Err(PayoutError::NotApproved);
    }

    let payout = insert_payout(&mut tx, &record, request_id, admin, now).await?;
    tx.commit().await?;
    Ok(ReservePayoutResult::Reserved(payout))
}

async fn reserve_reversal(
    state: &AppState,
    record_id: i64,
    request_id: &str,
    admin: &AdminUser,
    now: i64,
) -> PayoutResult<ReserveReversalResult> {
    let mut tx = state.pool.begin().await?;
    let payout = read_payout_by_record(&mut tx, record_id, true).await?;
    if payout.status == "reversed" {
        let reversal = read_reversal_by_original(&mut tx, payout.id, false)
            .await
            .unwrap_or_else(|_| payout.clone());
        tx.commit().await?;
        return Ok(ReserveReversalResult::AlreadyReversed(reversal));
    }
    if !matches!(payout.status.as_str(), "completed" | "reversal_pending") {
        return Err(PayoutError::PayoutNotCompleted);
    }

    match read_reversal_by_original(&mut tx, payout.id, false).await {
        Ok(reversal) if reversal.status == "completed" => {
            tx.commit().await?;
            return Ok(ReserveReversalResult::AlreadyReversed(reversal));
        }
        Ok(reversal) => {
            tx.commit().await?;
            return Ok(ReserveReversalResult::Reserved { payout, reversal });
        }
        Err(PayoutError::NotFound) => {}
        Err(err) => return Err(err),
    }

    let reversal = insert_reversal(&mut tx, &payout, request_id, admin, now).await?;
    tx.commit().await?;
    Ok(ReserveReversalResult::Reserved { payout, reversal })
}

async fn read_rebate_record(
    tx: &mut Transaction<'_, Postgres>,
    record_id: i64,
    lock: bool,
    quota_per_unit: i64,
) -> PayoutResult<RebateRecord> {
    let lock_clause = if lock { " FOR UPDATE" } else { "" };
    let query = format!(
        r#"
        SELECT record.id,
               (
                   SELECT request.id
                   FROM rebate_requests request
                   WHERE record.id = ANY(request.rebate_record_ids)
                   ORDER BY request.id DESC
                   LIMIT 1
               ) AS request_id,
               record.inviter_id,
               record.invitee_id,
               record.order_id,
               record.order_type,
               record.rebate_amount,
               record.status
        FROM rebate_records record
        WHERE record.id = $1{lock_clause}
        "#
    );
    let row = sqlx::query(&query)
        .bind(record_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(PayoutError::NotFound)?;

    let rebate_quota: i64 = row.try_get("rebate_amount")?;
    let amount_cents = quota_to_whole_cents(rebate_quota, quota_per_unit)?;

    Ok(RebateRecord {
        id: row.try_get("id")?,
        request_id: row.try_get("request_id")?,
        inviter_user_id: row.try_get("inviter_id")?,
        invitee_user_id: row.try_get("invitee_id")?,
        order_id: row.try_get("order_id")?,
        order_type: row.try_get("order_type")?,
        rebate_quota,
        status: row.try_get("status")?,
        amount_cents,
    })
}

async fn insert_payout(
    tx: &mut Transaction<'_, Postgres>,
    record: &RebateRecord,
    request_id: &str,
    admin: &AdminUser,
    now: i64,
) -> PayoutResult<Payout> {
    let row = sqlx::query(
        r#"
        INSERT INTO invitation_rebate_payouts(
            rebate_record_id, rebate_request_id, inviter_user_id, invitee_user_id,
            order_id, order_type, amount_cents, quota_delta, kind, status,
            request_id, admin_user_id, admin_username, created_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'payout', 'pending', $9, $10, $11, $12, $12)
        RETURNING id, rebate_record_id, rebate_request_id, inviter_user_id, invitee_user_id,
                  order_id, order_type, amount_cents, quota_delta, status, request_id,
                  newapi_mark_id, newapi_log_id
        "#,
    )
    .bind(record.id)
    .bind(record.request_id)
    .bind(record.inviter_user_id)
    .bind(record.invitee_user_id)
    .bind(record.order_id)
    .bind(&record.order_type)
    .bind(record.amount_cents)
    .bind(record.rebate_quota)
    .bind(request_id)
    .bind(admin.0.id)
    .bind(&admin.0.username)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;

    payout_from_row(&row)
}

async fn read_payout_by_record(
    tx: &mut Transaction<'_, Postgres>,
    record_id: i64,
    lock: bool,
) -> PayoutResult<Payout> {
    let lock_clause = if lock { " FOR UPDATE" } else { "" };
    let query = format!(
        r#"
        SELECT id, rebate_record_id, rebate_request_id, inviter_user_id, invitee_user_id,
               order_id, order_type, amount_cents, quota_delta, status, request_id,
               newapi_mark_id, newapi_log_id
        FROM invitation_rebate_payouts
        WHERE rebate_record_id = $1
          AND kind = 'payout'
        ORDER BY id DESC
        LIMIT 1
        {lock_clause}
        "#
    );
    let row = sqlx::query(&query)
        .bind(record_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(PayoutError::NotFound)?;

    payout_from_row(&row)
}

async fn insert_reversal(
    tx: &mut Transaction<'_, Postgres>,
    payout: &Payout,
    request_id: &str,
    admin: &AdminUser,
    now: i64,
) -> PayoutResult<Payout> {
    let row = sqlx::query(
        r#"
        INSERT INTO invitation_rebate_payouts(
            rebate_record_id, rebate_request_id, inviter_user_id, invitee_user_id,
            order_id, order_type, amount_cents, quota_delta, kind, status,
            request_id, admin_user_id, admin_username, original_payout_id, created_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'reversal', 'pending', $9, $10, $11, $12, $13, $13)
        ON CONFLICT DO NOTHING
        RETURNING id, rebate_record_id, rebate_request_id, inviter_user_id, invitee_user_id,
                  order_id, order_type, amount_cents, quota_delta, status, request_id,
                  newapi_mark_id, newapi_log_id
        "#,
    )
    .bind(payout.rebate_record_id)
    .bind(payout.rebate_request_id)
    .bind(payout.inviter_user_id)
    .bind(payout.invitee_user_id)
    .bind(payout.order_id)
    .bind(&payout.order_type)
    .bind(payout.amount_cents)
    .bind(-payout.quota_delta)
    .bind(request_id)
    .bind(admin.0.id)
    .bind(&admin.0.username)
    .bind(payout.id)
    .bind(now)
    .fetch_optional(&mut **tx)
    .await?;

    match row {
        Some(row) => payout_from_row(&row),
        None => read_reversal_by_original(tx, payout.id, false).await,
    }
}

async fn read_reversal_by_original(
    tx: &mut Transaction<'_, Postgres>,
    original_payout_id: i64,
    lock: bool,
) -> PayoutResult<Payout> {
    let lock_clause = if lock { " FOR UPDATE" } else { "" };
    let query = format!(
        r#"
        SELECT id, rebate_record_id, rebate_request_id, inviter_user_id, invitee_user_id,
               order_id, order_type, amount_cents, quota_delta, status, request_id,
               newapi_mark_id, newapi_log_id
        FROM invitation_rebate_payouts
        WHERE original_payout_id = $1
          AND kind = 'reversal'
        ORDER BY id DESC
        LIMIT 1
        {lock_clause}
        "#
    );
    let row = sqlx::query(&query)
        .bind(original_payout_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(PayoutError::NotFound)?;

    payout_from_row(&row)
}

fn payout_from_row(row: &PgRow) -> PayoutResult<Payout> {
    Ok(Payout {
        id: row.try_get("id")?,
        rebate_record_id: row.try_get("rebate_record_id")?,
        rebate_request_id: row.try_get("rebate_request_id")?,
        inviter_user_id: row.try_get("inviter_user_id")?,
        invitee_user_id: row.try_get("invitee_user_id")?,
        order_id: row.try_get("order_id")?,
        order_type: row.try_get("order_type")?,
        amount_cents: row.try_get("amount_cents")?,
        quota_delta: row.try_get("quota_delta")?,
        status: row.try_get("status")?,
        request_id: row.try_get("request_id")?,
        newapi_mark_id: row.try_get("newapi_mark_id")?,
        newapi_log_id: row.try_get("newapi_log_id")?,
    })
}

async fn apply_newapi_payout(
    newapi_pool: &PgPool,
    payout: &Payout,
    now: i64,
) -> PayoutResult<NewApiBalanceMark> {
    let mut tx = newapi_pool.begin().await?;
    let inserted_mark_id: Option<i64> = sqlx::query_scalar(
        r#"
        INSERT INTO kkai_invitation_rebate_balance_marks
            (kind, rebate_record_id, payout_id, user_id, quota_delta, amount_cents, request_id, created_at)
        VALUES ('payout', $1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT DO NOTHING
        RETURNING id
        "#,
    )
    .bind(payout.rebate_record_id)
    .bind(payout.id)
    .bind(payout.inviter_user_id)
    .bind(payout.quota_delta)
    .bind(payout.amount_cents)
    .bind(&payout.request_id)
    .bind(now)
    .fetch_optional(&mut *tx)
    .await?;

    let Some(mark_id) = inserted_mark_id else {
        let mark = read_newapi_mark(&mut tx, "payout", Some(payout.rebate_record_id), None).await?;
        tx.commit().await?;
        return Ok(mark);
    };

    let user = sqlx::query(
        r#"
        UPDATE users
        SET quota = COALESCE(quota, 0) + $1
        WHERE id = $2
          AND deleted_at IS NULL
        RETURNING quota, COALESCE(NULLIF(display_name, ''), NULLIF(username, ''), NULLIF(email, ''), '')
        "#,
    )
    .bind(payout.quota_delta)
    .bind(payout.inviter_user_id)
    .fetch_one(&mut *tx)
    .await?;
    let balance: i64 = user.try_get("quota")?;
    let username: String = user.try_get(1)?;

    let content = format!(
        "邀请返利发放 ¥{:.2}，返利记录ID {}",
        payout.amount_cents as f64 / 100.0,
        payout.rebate_record_id
    );
    let log_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO logs(user_id, created_at, type, content, username, quota, request_id, other)
        VALUES ($1, $2, 1, $3, $4, 0, LEFT($5::TEXT, 64), $6)
        RETURNING id
        "#,
    )
    .bind(payout.inviter_user_id)
    .bind(now)
    .bind(content)
    .bind(username)
    .bind(&payout.request_id)
    .bind(payout_log_other("payout", payout))
    .fetch_one(&mut *tx)
    .await?;

    sqlx::query("UPDATE kkai_invitation_rebate_balance_marks SET log_id = $2 WHERE id = $1")
        .bind(mark_id)
        .bind(log_id)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    Ok(NewApiBalanceMark {
        id: mark_id,
        log_id: Some(log_id),
        balance,
    })
}

async fn apply_newapi_reversal(
    newapi_pool: &PgPool,
    reversal: &Payout,
    payout: &Payout,
    now: i64,
) -> PayoutResult<NewApiBalanceMark> {
    let mut tx = newapi_pool.begin().await?;
    let inserted_mark_id: Option<i64> = sqlx::query_scalar(
        r#"
        INSERT INTO kkai_invitation_rebate_balance_marks
            (kind, rebate_record_id, payout_id, original_payout_id, user_id, quota_delta, amount_cents, request_id, created_at)
        VALUES ('reversal', $1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT DO NOTHING
        RETURNING id
        "#,
    )
    .bind(reversal.rebate_record_id)
    .bind(reversal.id)
    .bind(payout.id)
    .bind(reversal.inviter_user_id)
    .bind(reversal.quota_delta)
    .bind(reversal.amount_cents)
    .bind(&reversal.request_id)
    .bind(now)
    .fetch_optional(&mut *tx)
    .await?;

    let Some(mark_id) = inserted_mark_id else {
        let mark = read_newapi_mark(&mut tx, "reversal", None, Some(payout.id)).await?;
        tx.commit().await?;
        return Ok(mark);
    };

    let deduct_quota = -reversal.quota_delta;
    let user = sqlx::query(
        r#"
        UPDATE users
        SET quota = COALESCE(quota, 0) - $1
        WHERE id = $2
          AND deleted_at IS NULL
          AND COALESCE(quota, 0) >= $1
        RETURNING quota, COALESCE(NULLIF(display_name, ''), NULLIF(username, ''), NULLIF(email, ''), '')
        "#,
    )
    .bind(deduct_quota)
    .bind(reversal.inviter_user_id)
    .fetch_optional(&mut *tx)
    .await?;

    let Some(user) = user else {
        return Err(PayoutError::InsufficientQuota);
    };
    let balance: i64 = user.try_get("quota")?;
    let username: String = user.try_get(1)?;

    let content = format!(
        "邀请返利冲正 -¥{:.2}，返利记录ID {}",
        reversal.amount_cents as f64 / 100.0,
        reversal.rebate_record_id
    );
    let log_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO logs(user_id, created_at, type, content, username, quota, request_id, other)
        VALUES ($1, $2, 1, $3, $4, 0, LEFT($5::TEXT, 64), $6)
        RETURNING id
        "#,
    )
    .bind(reversal.inviter_user_id)
    .bind(now)
    .bind(content)
    .bind(username)
    .bind(&reversal.request_id)
    .bind(payout_log_other("reversal", reversal))
    .fetch_one(&mut *tx)
    .await?;

    sqlx::query("UPDATE kkai_invitation_rebate_balance_marks SET log_id = $2 WHERE id = $1")
        .bind(mark_id)
        .bind(log_id)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    Ok(NewApiBalanceMark {
        id: mark_id,
        log_id: Some(log_id),
        balance,
    })
}

async fn read_newapi_mark(
    tx: &mut Transaction<'_, Postgres>,
    kind: &str,
    record_id: Option<i64>,
    original_payout_id: Option<i64>,
) -> PayoutResult<NewApiBalanceMark> {
    let row = sqlx::query(
        r#"
        SELECT mark.id,
               mark.log_id,
               COALESCE(users.quota, 0) AS balance
        FROM kkai_invitation_rebate_balance_marks mark
        LEFT JOIN users ON users.id = mark.user_id
        WHERE mark.kind = $1
          AND (($2::BIGINT IS NOT NULL AND mark.rebate_record_id = $2)
               OR ($3::BIGINT IS NOT NULL AND mark.original_payout_id = $3))
        ORDER BY mark.id DESC
        LIMIT 1
        "#,
    )
    .bind(kind)
    .bind(record_id)
    .bind(original_payout_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(PayoutError::NotFound)?;

    Ok(NewApiBalanceMark {
        id: row.try_get("id")?,
        log_id: row.try_get("log_id")?,
        balance: row.try_get("balance")?,
    })
}

async fn complete_payout(
    pool: &PgPool,
    payout: &Payout,
    mark: &NewApiBalanceMark,
    now: i64,
) -> PayoutResult<()> {
    let mut tx = pool.begin().await?;
    sqlx::query(
        r#"
        UPDATE invitation_rebate_payouts
        SET status = 'completed',
            newapi_mark_id = $2,
            newapi_log_id = $3,
            updated_at = $4,
            completed_at = COALESCE(completed_at, $4),
            error_message = NULL
        WHERE id = $1
          AND kind = 'payout'
        "#,
    )
    .bind(payout.id)
    .bind(mark.id)
    .bind(mark.log_id)
    .bind(now)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        r#"
        UPDATE rebate_records
        SET status = 'completed',
            finalized_at = COALESCE(finalized_at, $2),
            updated_at = $2
        WHERE id = $1
        "#,
    )
    .bind(payout.rebate_record_id)
    .bind(now)
    .execute(&mut *tx)
    .await?;

    if let Some(request_id) = payout.rebate_request_id {
        sqlx::query(
            r#"
            UPDATE rebate_requests request
            SET status = 'completed',
                completed_at = COALESCE(completed_at, $2),
                updated_at = $2
            WHERE request.id = $1
              AND NOT EXISTS (
                  SELECT 1
                  FROM rebate_records record
                  WHERE record.id = ANY(request.rebate_record_ids)
                    AND record.status <> 'completed'
              )
            "#,
        )
        .bind(request_id)
        .bind(now)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(())
}

async fn complete_reversal(
    pool: &PgPool,
    payout: &Payout,
    reversal: &Payout,
    mark: &NewApiBalanceMark,
    now: i64,
) -> PayoutResult<()> {
    let mut tx = pool.begin().await?;
    sqlx::query(
        r#"
        UPDATE invitation_rebate_payouts
        SET status = 'reversed',
            reversed_at = COALESCE(reversed_at, $2),
            updated_at = $2,
            error_message = NULL
        WHERE id = $1
        "#,
    )
    .bind(payout.id)
    .bind(now)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        r#"
        UPDATE invitation_rebate_payouts
        SET status = 'completed',
            newapi_mark_id = $2,
            newapi_log_id = $3,
            completed_at = COALESCE(completed_at, $4),
            updated_at = $4,
            error_message = NULL
        WHERE id = $1
        "#,
    )
    .bind(reversal.id)
    .bind(mark.id)
    .bind(mark.log_id)
    .bind(now)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        r#"
        UPDATE rebate_records
        SET status = 'approved',
            finalized_at = NULL,
            updated_at = $2
        WHERE id = $1
        "#,
    )
    .bind(payout.rebate_record_id)
    .bind(now)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

async fn mark_payout_failed(pool: &PgPool, payout_id: i64, message: &str, now: i64) {
    let _ = sqlx::query(
        r#"
        UPDATE invitation_rebate_payouts
        SET status = CASE WHEN status = 'pending' THEN 'failed' ELSE status END,
            error_message = LEFT($2::TEXT, 1000),
            updated_at = $3
        WHERE id = $1
          AND status IN ('pending', 'failed')
        "#,
    )
    .bind(payout_id)
    .bind(message)
    .bind(now)
    .execute(pool)
    .await;
}

async fn mark_reversal_pending(
    pool: &PgPool,
    payout_id: i64,
    reversal_id: i64,
    message: &str,
    now: i64,
) {
    let _ = sqlx::query(
        r#"
        UPDATE invitation_rebate_payouts
        SET status = 'reversal_pending',
            error_message = LEFT($2::TEXT, 1000),
            updated_at = $3
        WHERE id = $1
        "#,
    )
    .bind(payout_id)
    .bind(message)
    .bind(now)
    .execute(pool)
    .await;

    let _ = sqlx::query(
        r#"
        UPDATE invitation_rebate_payouts
        SET status = 'failed',
            error_message = LEFT($2::TEXT, 1000),
            updated_at = $3
        WHERE id = $1
        "#,
    )
    .bind(reversal_id)
    .bind(message)
    .bind(now)
    .execute(pool)
    .await;
}

async fn record_attempt(
    pool: &PgPool,
    record_id: i64,
    action: &str,
    request_id: &str,
    admin: &AdminUser,
    status: &str,
    http_status: i32,
    message: &str,
    now: i64,
) {
    let _ = sqlx::query(
        r#"
        INSERT INTO invitation_rebate_payout_attempts(
            rebate_record_id, action, request_id, admin_user_id, admin_username,
            status, http_status, error_message, created_at, completed_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, LEFT($8::TEXT, 1000), $9, $9)
        "#,
    )
    .bind(record_id)
    .bind(action)
    .bind(request_id)
    .bind(admin.0.id)
    .bind(&admin.0.username)
    .bind(status)
    .bind(http_status)
    .bind(message)
    .bind(now)
    .execute(pool)
    .await;
}

async fn fail_pay(
    pool: &PgPool,
    record_id: i64,
    request_id: &str,
    admin: &AdminUser,
    err: PayoutError,
    now: i64,
) -> Result<Json<ApiResponse<PayResult>>> {
    let (status, http_status, app_error) = pay_error_response(err);
    record_attempt(
        pool,
        record_id,
        "pay",
        request_id,
        admin,
        status,
        http_status,
        app_error_message(&app_error),
        now,
    )
    .await;
    Err(app_error)
}

async fn fail_reverse(
    pool: &PgPool,
    record_id: i64,
    request_id: &str,
    admin: &AdminUser,
    err: PayoutError,
    now: i64,
) -> Result<Json<ApiResponse<PayResult>>> {
    let (status, http_status, app_error) = reverse_error_response(err);
    record_attempt(
        pool,
        record_id,
        "reverse",
        request_id,
        admin,
        status,
        http_status,
        app_error_message(&app_error),
        now,
    )
    .await;
    Err(app_error)
}

fn pay_error_response(err: PayoutError) -> (&'static str, i32, AppError) {
    match err {
        PayoutError::NotFound => (
            "record_not_found",
            404,
            AppError::NotFound("返利记录不存在".to_string()),
        ),
        PayoutError::NotApproved => (
            "not_approved",
            409,
            AppError::Conflict("返利记录未审核通过，不能发放".to_string()),
        ),
        PayoutError::HistoricalComplete => (
            "historical_completed_without_payout",
            409,
            AppError::Conflict("该历史已完成记录缺少余额发放流水，需要人工复核".to_string()),
        ),
        PayoutError::InvalidQuota(message) => (
            "invalid_quota",
            400,
            AppError::BadRequest(format!("返利金额无法按整分发放：{message}")),
        ),
        PayoutError::Database(err) => ("pay_failed", 500, AppError::Database(err)),
        other => (
            "pay_failed",
            500,
            AppError::Internal(format!("返利发放失败：{other}")),
        ),
    }
}

fn reverse_error_response(err: PayoutError) -> (&'static str, i32, AppError) {
    match err {
        PayoutError::NotFound => (
            "payout_not_found",
            404,
            AppError::NotFound("余额发放流水不存在".to_string()),
        ),
        PayoutError::PayoutNotCompleted => (
            "not_completed",
            409,
            AppError::Conflict("只有已完成的余额发放流水可以冲正".to_string()),
        ),
        PayoutError::InvalidQuota(message) => (
            "invalid_quota",
            400,
            AppError::BadRequest(format!("返利金额无法按整分冲正：{message}")),
        ),
        PayoutError::Database(err) => ("reverse_failed", 500, AppError::Database(err)),
        other => (
            "reverse_failed",
            500,
            AppError::Internal(format!("返利冲正失败：{other}")),
        ),
    }
}

fn app_error_from_payout_error(err: PayoutError) -> AppError {
    match err {
        PayoutError::NotFound => AppError::NotFound("返利记录不存在".to_string()),
        PayoutError::InvalidQuota(message) => {
            AppError::BadRequest(format!("返利金额无法按整分处理：{message}"))
        }
        PayoutError::Database(err) => AppError::Database(err),
        other => AppError::Internal(other.to_string()),
    }
}

fn app_error_message(error: &AppError) -> &str {
    match error {
        AppError::Unauthorized => "未授权",
        AppError::Forbidden => "无权限",
        AppError::NotFound(message)
        | AppError::BadRequest(message)
        | AppError::Conflict(message)
        | AppError::Locked(message)
        | AppError::Internal(message) => message,
        AppError::Database(_) => "数据库错误",
    }
}

fn pay_result_from_payout(
    outcome: &str,
    payout: &Payout,
    mark: Option<&NewApiBalanceMark>,
    payout_enabled: bool,
) -> PayResult {
    PayResult {
        outcome: outcome.to_string(),
        payout_id: Some(payout.id),
        record_id: payout.rebate_record_id,
        request_id: payout.request_id.clone(),
        amount_cents: payout.amount_cents,
        quota_delta: payout.quota_delta,
        balance_after: mark.map(|mark| mark.balance),
        newapi_log_id: mark.and_then(|mark| mark.log_id).or(payout.newapi_log_id),
        newapi_mark_id: mark.map(|mark| mark.id).or(payout.newapi_mark_id),
        message: None,
        payout_enabled,
    }
}

fn quota_to_whole_cents(quota_delta: i64, quota_per_unit: i64) -> PayoutResult<i64> {
    if quota_delta <= 0 {
        return Err(PayoutError::InvalidQuota(format!(
            "quota delta must be positive: {quota_delta}"
        )));
    }
    let cents_per_quota_unit = quota_per_unit / 100;
    if cents_per_quota_unit <= 0 {
        return Err(PayoutError::InvalidQuota(format!(
            "invalid quota unit: {quota_per_unit}"
        )));
    }
    if quota_delta % cents_per_quota_unit != 0 {
        return Err(PayoutError::InvalidQuota(format!(
            "quota delta {quota_delta} is not representable as whole cents"
        )));
    }
    Ok(quota_delta / cents_per_quota_unit)
}

fn validate_record_id(record_id: i64) -> Result<()> {
    if record_id <= 0 {
        return Err(AppError::BadRequest("无效的返利记录 ID".to_string()));
    }
    Ok(())
}

fn require_payout_action_header(headers: &HeaderMap, expected: &str) -> Result<()> {
    let actual = headers
        .get("x-kkai-payout-action")
        .and_then(|value| value.to_str().ok())
        .map(str::trim);

    if actual == Some(expected) {
        Ok(())
    } else {
        Err(AppError::Forbidden)
    }
}

fn request_id_from_headers(headers: &HeaderMap) -> Result<String> {
    for key in ["idempotency-key", "x-request-id"] {
        if let Some(value) = headers.get(key).and_then(|value| value.to_str().ok()) {
            let value = value.trim();
            if value.is_empty() {
                continue;
            }
            if value.len() > MAX_REQUEST_ID_LEN {
                return Err(AppError::BadRequest(
                    "幂等请求 ID 不能超过 128 个字符".to_string(),
                ));
            }
            return Ok(value.to_string());
        }
    }
    Ok(format!("kkai-{}", ulid::Ulid::new()))
}

fn payout_log_other(kind: &str, payout: &Payout) -> String {
    serde_json::json!({
        "kind": kind,
        "rebateRecordId": payout.rebate_record_id,
        "rebateRequestId": payout.rebate_request_id.unwrap_or_default(),
        "orderId": payout.order_id,
        "orderType": payout.order_type,
        "payoutId": payout.id,
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;

    #[test]
    fn quota_to_whole_cents_converts_exact_amounts() {
        assert_eq!(quota_to_whole_cents(2_500_000, 500_000).unwrap(), 500);
        assert_eq!(quota_to_whole_cents(5_000, 500_000).unwrap(), 1);
    }

    #[test]
    fn quota_to_whole_cents_rejects_fractional_cents() {
        assert!(quota_to_whole_cents(5_001, 500_000).is_err());
    }

    #[test]
    fn request_id_prefers_idempotency_key() {
        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", HeaderValue::from_static("fallback"));
        headers.insert("idempotency-key", HeaderValue::from_static("primary"));

        assert_eq!(request_id_from_headers(&headers).unwrap(), "primary");
    }

    #[test]
    fn payout_action_header_requires_expected_action() {
        let mut headers = HeaderMap::new();
        headers.insert("x-kkai-payout-action", HeaderValue::from_static("pay"));

        assert!(require_payout_action_header(&headers, "pay").is_ok());
        assert!(require_payout_action_header(&headers, "reverse").is_err());
    }
}
