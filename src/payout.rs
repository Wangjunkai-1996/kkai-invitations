use axum::extract::{Path, Query, Request, State};
use axum::http::{HeaderMap, Method};
use axum::middleware::Next;
use axum::response::Response;
use axum::Json;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Postgres, Row, Transaction};
use thiserror::Error;

use crate::balance_dispatcher::{
    enqueue_operation, original_credit_operation_ulid, EnqueueOperation, OperationKind,
};
use crate::config::SettlementMode;
use crate::error::{
    AppError, Result, FUNDS_MAINTENANCE_MESSAGE, LEGACY_COMPLETION_DISABLED_MESSAGE,
};
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
    #[error("sales requests cannot use balance payouts")]
    SalesRequest,
    #[error("completed rebate record has no payout ledger")]
    HistoricalComplete,
    #[error("quota conversion failed: {0}")]
    InvalidQuota(String),
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

pub struct PayRecordInput<'a> {
    pub record_id: i64,
    pub request_id: String,
    pub admin: &'a AdminUser,
    pub now: i64,
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
    sales_request: bool,
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

enum ReservePayoutResult {
    Reserved(Payout),
    AlreadyPaid(Payout),
}

enum ReserveReversalResult {
    Reserved { payout: Payout, reversal: Payout },
    AlreadyReversed(Payout),
}

struct PayoutAttempt<'a> {
    record_id: i64,
    action: &'a str,
    request_id: &'a str,
    admin: &'a AdminUser,
    status: &'a str,
    http_status: i32,
    message: &'a str,
    occurred_at: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SettlementWriteRoute {
    Current,
    LegacyCompletion,
}

pub async fn enforce_settlement_mode(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response> {
    match settlement_write_route(request.method(), request.uri().path()) {
        Some(SettlementWriteRoute::LegacyCompletion) => {
            return Err(AppError::LegacyCompletionDisabled);
        }
        Some(SettlementWriteRoute::Current) => {
            require_settlement_writes_enabled(state.config.settlement_mode)?;
        }
        None => {}
    }
    Ok(next.run(request).await)
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
    require_settlement_writes_enabled(state.config.settlement_mode)?;
    let request_id = request_id_from_headers(&headers)?;
    let now = Utc::now().timestamp();

    if params.is_dry_run() {
        return preview_pay(&state, record_id, request_id).await;
    }

    let result = pay_rebate_record(
        &state,
        PayRecordInput {
            record_id,
            request_id,
            admin: &admin,
            now,
        },
    )
    .await?;

    Ok(ok(result))
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
    require_settlement_writes_enabled(state.config.settlement_mode)?;
    let request_id = request_id_from_headers(&headers)?;
    let now = Utc::now().timestamp();

    let (_payout, reversal) =
        match reserve_reversal(&state, record_id, &request_id, &admin, now).await {
            Ok(ReserveReversalResult::Reserved { payout, reversal }) => (payout, reversal),
            Ok(ReserveReversalResult::AlreadyReversed(reversal)) => {
                record_attempt(
                    &state.pool,
                    PayoutAttempt {
                        record_id,
                        action: "reverse",
                        request_id: &request_id,
                        admin: &admin,
                        status: "already_reversed",
                        http_status: 200,
                        message: "",
                        occurred_at: now,
                    },
                )
                .await;
                return Ok(ok(pay_result_from_payout(
                    "already_reversed",
                    &reversal,
                    state.config.settlement_mode,
                )));
            }
            Err(err) => {
                return fail_reverse(&state.pool, record_id, &request_id, &admin, err, now).await;
            }
        };

    record_attempt(
        &state.pool,
        PayoutAttempt {
            record_id,
            action: "reverse",
            request_id: &request_id,
            admin: &admin,
            status: "pending",
            http_status: 202,
            message: "",
            occurred_at: now,
        },
    )
    .await;

    Ok(ok(pay_result_from_payout(
        "pending",
        &reversal,
        state.config.settlement_mode,
    )))
}

async fn preview_pay(
    state: &AppState,
    record_id: i64,
    request_id: String,
) -> Result<Json<ApiResponse<PayResult>>> {
    match preview_pay_record(state, record_id).await {
        Ok(record) => Ok(ok(PayResult {
            outcome: "dry_run".to_string(),
            payout_id: None,
            record_id: record.id,
            request_id,
            amount_cents: record.amount_cents,
            quota_delta: record.rebate_quota,
            balance_after: None,
            newapi_log_id: None,
            newapi_mark_id: None,
            message: Some("dry run; no balance or payout state was changed".to_string()),
            payout_enabled: state.config.settlement_mode.is_enabled(),
        })),
        Err(err) => Err(pay_error_response(err).2),
    }
}

async fn preview_pay_record(state: &AppState, record_id: i64) -> PayoutResult<RebateRecord> {
    let mut tx = state.pool.begin().await?;
    let record = read_rebate_record(&mut tx, record_id, false, state.config.quota_per_unit).await?;
    if record.sales_request {
        return Err(PayoutError::SalesRequest);
    }
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
    tx.rollback().await?;
    Ok(record)
}

pub async fn pay_rebate_record(state: &AppState, input: PayRecordInput<'_>) -> Result<PayResult> {
    require_settlement_writes_enabled(state.config.settlement_mode)?;

    let payout = match reserve_payout(
        state,
        input.record_id,
        &input.request_id,
        input.admin,
        input.now,
    )
    .await
    {
        Ok(ReservePayoutResult::Reserved(payout)) => payout,
        Ok(ReservePayoutResult::AlreadyPaid(payout)) => {
            record_attempt(
                &state.pool,
                PayoutAttempt {
                    record_id: input.record_id,
                    action: "pay",
                    request_id: &input.request_id,
                    admin: input.admin,
                    status: "already_paid",
                    http_status: 200,
                    message: "",
                    occurred_at: input.now,
                },
            )
            .await;
            return Ok(pay_result_from_payout(
                "already_paid",
                &payout,
                state.config.settlement_mode,
            ));
        }
        Err(err) => {
            return record_failed_pay_attempt(
                &state.pool,
                input.record_id,
                &input.request_id,
                input.admin,
                err,
                input.now,
            )
            .await;
        }
    };

    record_attempt(
        &state.pool,
        PayoutAttempt {
            record_id: input.record_id,
            action: "pay",
            request_id: &input.request_id,
            admin: input.admin,
            status: "pending",
            http_status: 202,
            message: "",
            occurred_at: input.now,
        },
    )
    .await;

    Ok(pay_result_from_payout(
        "pending",
        &payout,
        state.config.settlement_mode,
    ))
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
        can_pay: record.status == "approved" && !record.sales_request,
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
            let credit_operation_exists: bool = sqlx::query_scalar(
                r#"
                SELECT EXISTS(
                    SELECT 1 FROM balance_operations
                    WHERE rebate_record_id = $1 AND operation_kind = 'credit'
                )
                "#,
            )
            .bind(record_id)
            .fetch_one(&mut *tx)
            .await?;
            result.can_reverse = credit_operation_exists
                && matches!(payout.status.as_str(), "completed" | "reversal_pending");
            let operation_state: Option<String> = sqlx::query_scalar(
                r#"
                SELECT operation_state
                FROM balance_operations
                WHERE rebate_record_id = $1
                ORDER BY id DESC LIMIT 1
                "#,
            )
            .bind(record_id)
            .fetch_optional(&mut *tx)
            .await?;
            result.needs_review = matches!(payout.status.as_str(), "failed" | "reversal_pending")
                || matches!(operation_state.as_deref(), Some("blocked" | "needs_review"));
            if payout.status == "completed" && !credit_operation_exists {
                result.needs_review = true;
                result.message =
                    Some("历史发放缺少新余额操作账本，只能人工复核，不能自动冲正".to_string());
            }
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

    if record.sales_request {
        return Err(PayoutError::SalesRequest);
    }

    match read_payout_by_record(&mut tx, record_id, false).await {
        Ok(existing) if existing.status == "completed" => {
            tx.commit().await?;
            return Ok(ReservePayoutResult::AlreadyPaid(existing));
        }
        Ok(existing) if existing.status == "reversed" => {
            return Err(PayoutError::PayoutNotCompleted);
        }
        Ok(existing) => {
            enqueue_credit_for_payout(&mut tx, &existing, now).await?;
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
    enqueue_credit_for_payout(&mut tx, &payout, now).await?;
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
            enqueue_reversal_for_payout(&mut tx, &payout, &reversal, now).await?;
            tx.commit().await?;
            return Ok(ReserveReversalResult::Reserved { payout, reversal });
        }
        Err(PayoutError::NotFound) => {}
        Err(err) => return Err(err),
    }

    let reversal = insert_reversal(&mut tx, &payout, request_id, admin, now).await?;
    enqueue_reversal_for_payout(&mut tx, &payout, &reversal, now).await?;
    tx.commit().await?;
    Ok(ReserveReversalResult::Reserved { payout, reversal })
}

async fn enqueue_credit_for_payout(
    transaction: &mut Transaction<'_, Postgres>,
    payout: &Payout,
    now: i64,
) -> PayoutResult<()> {
    let operation = enqueue_operation(
        transaction,
        EnqueueOperation {
            kind: OperationKind::Credit,
            rebate_record_id: payout.rebate_record_id,
            rebate_request_id: payout.rebate_request_id,
            original_operation_ulid: None,
            user_id: payout.inviter_user_id,
            payout_id: payout.id,
            delta_quota: payout.quota_delta,
            now,
        },
    )
    .await?;
    tracing::debug!(
        operation_id = %operation.operation_ulid,
        operation_state = %operation.operation_state,
        "Payout balance operation queued"
    );
    Ok(())
}

async fn enqueue_reversal_for_payout(
    transaction: &mut Transaction<'_, Postgres>,
    payout: &Payout,
    reversal: &Payout,
    now: i64,
) -> PayoutResult<()> {
    let original_operation_ulid =
        original_credit_operation_ulid(transaction, payout.rebate_record_id)
            .await?
            .ok_or(PayoutError::HistoricalComplete)?;
    let operation = enqueue_operation(
        transaction,
        EnqueueOperation {
            kind: OperationKind::Reversal,
            rebate_record_id: payout.rebate_record_id,
            rebate_request_id: payout.rebate_request_id,
            original_operation_ulid: Some(original_operation_ulid),
            user_id: payout.inviter_user_id,
            payout_id: reversal.id,
            delta_quota: reversal.quota_delta,
            now,
        },
    )
    .await?;
    tracing::debug!(
        operation_id = %operation.operation_ulid,
        operation_state = %operation.operation_state,
        "Reversal balance operation queued"
    );
    Ok(())
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
                   SELECT owner.request_id
                   FROM rebate_record_request_ownership owner
                   WHERE owner.rebate_record_id = record.id AND owner.ownership_state = 'active'
               ) AS request_id,
               EXISTS(
                   SELECT 1 FROM rebate_record_request_ownership owner
                   JOIN rebate_requests request ON request.id=owner.request_id
                   WHERE owner.rebate_record_id=record.id AND owner.ownership_state='active'
                     AND request.request_kind='sales'
               ) AS sales_request,
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
        sales_request: row.try_get("sales_request")?,
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

async fn record_attempt(pool: &PgPool, attempt: PayoutAttempt<'_>) {
    let _ = sqlx::query(
        r#"
        INSERT INTO invitation_rebate_payout_attempts(
            rebate_record_id, action, request_id, admin_user_id, admin_username,
            status, http_status, error_message, created_at, completed_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, LEFT($8::TEXT, 1000), $9, $9)
        "#,
    )
    .bind(attempt.record_id)
    .bind(attempt.action)
    .bind(attempt.request_id)
    .bind(attempt.admin.0.id)
    .bind(&attempt.admin.0.username)
    .bind(attempt.status)
    .bind(attempt.http_status)
    .bind(attempt.message)
    .bind(attempt.occurred_at)
    .execute(pool)
    .await;
}

async fn record_failed_pay_attempt(
    pool: &PgPool,
    record_id: i64,
    request_id: &str,
    admin: &AdminUser,
    err: PayoutError,
    now: i64,
) -> Result<PayResult> {
    let (status, http_status, app_error) = pay_error_response(err);
    record_attempt(
        pool,
        PayoutAttempt {
            record_id,
            action: "pay",
            request_id,
            admin,
            status,
            http_status,
            message: app_error_message(&app_error),
            occurred_at: now,
        },
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
        PayoutAttempt {
            record_id,
            action: "reverse",
            request_id,
            admin,
            status,
            http_status,
            message: app_error_message(&app_error),
            occurred_at: now,
        },
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
        PayoutError::SalesRequest => (
            "sales_manual_settlement_required",
            409,
            AppError::Conflict("销售申请必须人工确认发放，不能转入站内余额".into()),
        ),
        PayoutError::NotApproved => (
            "not_approved",
            409,
            AppError::Conflict("返利记录未审核通过，不能发放".to_string()),
        ),
        PayoutError::HistoricalComplete => (
            "historical_settlement_requires_review",
            409,
            AppError::Conflict("该历史结算缺少新余额操作账本，只能人工复核".to_string()),
        ),
        PayoutError::InvalidQuota(message) => (
            "invalid_quota",
            400,
            AppError::BadRequest(format!("返利金额无法按整分发放：{message}")),
        ),
        PayoutError::PayoutNotCompleted => (
            "payout_not_payable",
            409,
            AppError::Conflict("该返利已进入冲正流程，不能再次发放".to_string()),
        ),
        PayoutError::Database(err) => ("pay_failed", 500, AppError::Database(err)),
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
        PayoutError::HistoricalComplete => (
            "historical_settlement_requires_review",
            409,
            AppError::Conflict("该历史结算缺少新余额操作账本，只能人工复核".to_string()),
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
        AppError::FundsMaintenance => FUNDS_MAINTENANCE_MESSAGE,
        AppError::LegacyCompletionDisabled => LEGACY_COMPLETION_DISABLED_MESSAGE,
        AppError::NotFound(message)
        | AppError::BadRequest(message)
        | AppError::Conflict(message)
        | AppError::UnprocessableEntity(message)
        | AppError::Locked(message)
        | AppError::Internal(message) => message,
        AppError::Database(_) => "数据库错误",
    }
}

fn pay_result_from_payout(
    outcome: &str,
    payout: &Payout,
    settlement_mode: SettlementMode,
) -> PayResult {
    PayResult {
        outcome: outcome.to_string(),
        payout_id: Some(payout.id),
        record_id: payout.rebate_record_id,
        request_id: payout.request_id.clone(),
        amount_cents: payout.amount_cents,
        quota_delta: payout.quota_delta,
        balance_after: None,
        newapi_log_id: payout.newapi_log_id,
        newapi_mark_id: payout.newapi_mark_id,
        message: None,
        payout_enabled: settlement_mode.is_enabled(),
    }
}

pub(crate) fn require_settlement_writes_enabled(mode: SettlementMode) -> Result<()> {
    if mode.is_enabled() {
        return Ok(());
    }
    Err(AppError::FundsMaintenance)
}

fn settlement_write_route(method: &Method, path: &str) -> Option<SettlementWriteRoute> {
    if method != Method::POST {
        return None;
    }

    let payout_action = path.contains("/api/admin/rebate-payouts/")
        && (path.ends_with("/pay") || path.ends_with("/reverse"));
    let request_payout = path.contains("/api/admin/rebate-requests/")
        && (path.ends_with("/approve-and-pay")
            || path.ends_with("/approve-and-pay-batch")
            || path.ends_with("/confirm-issued"));
    let legacy_completion = path.contains("/api/admin/rebate-requests/")
        && (path.ends_with("/complete") || path.ends_with("/undo-complete"));

    if legacy_completion {
        return Some(SettlementWriteRoute::LegacyCompletion);
    }
    if payout_action || request_payout {
        return Some(SettlementWriteRoute::Current);
    }
    None
}

fn quota_to_whole_cents(quota_delta: i64, quota_per_unit: i64) -> PayoutResult<i64> {
    if quota_delta <= 0 {
        return Err(PayoutError::InvalidQuota(format!(
            "quota delta must be positive: {quota_delta}"
        )));
    }
    if quota_per_unit <= 0 {
        return Err(PayoutError::InvalidQuota(format!(
            "invalid quota unit: {quota_per_unit}"
        )));
    }
    // Cents are an audit/display amount. The original quota is sent unchanged;
    // fractional cents must never prevent an exact wallet credit or reversal.
    let cents = (i128::from(quota_delta) * 100 + i128::from(quota_per_unit) / 2)
        / i128::from(quota_per_unit);
    i64::try_from(cents).map_err(|_| PayoutError::InvalidQuota("amount exceeds i64".to_string()))
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

pub fn request_id_from_headers(headers: &HeaderMap) -> Result<String> {
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use axum::body::{to_bytes, Body};
    use axum::extract::FromRequestParts;
    use axum::http::{request::Parts, HeaderValue, Request as HttpRequest, StatusCode};
    use axum::response::IntoResponse;
    use axum::routing::{get, post};
    use axum::{Extension, Router};
    use sqlx::postgres::PgPoolOptions;
    use tower::ServiceExt;

    use super::*;
    use crate::config::{Config, SettlementMode};

    const CURRENT_SETTLEMENT_PATHS: &[&str] = &[
        "/api/admin/rebate-payouts/1/pay",
        "/api/admin/rebate-payouts/1/reverse",
        "/api/admin/rebate-requests/1/approve-and-pay",
        "/api/admin/rebate-requests/1/confirm-issued",
        "/api/admin/rebate-requests/approve-and-pay-batch",
    ];
    const LEGACY_COMPLETION_PATHS: &[&str] = &[
        "/api/admin/rebate-requests/1/complete",
        "/api/admin/rebate-requests/1/undo-complete",
    ];

    #[derive(Default)]
    struct RouteExecutionProbe {
        extractor_calls: AtomicUsize,
        handler_calls: AtomicUsize,
    }

    struct CountedExtractor(Arc<RouteExecutionProbe>);

    impl FromRequestParts<AppState> for CountedExtractor {
        type Rejection = StatusCode;

        async fn from_request_parts(
            parts: &mut Parts,
            _state: &AppState,
        ) -> std::result::Result<Self, Self::Rejection> {
            let probe = parts
                .extensions
                .get::<Arc<RouteExecutionProbe>>()
                .cloned()
                .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
            probe.extractor_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Self(probe))
        }
    }

    async fn counted_handler(CountedExtractor(probe): CountedExtractor) -> StatusCode {
        probe.handler_calls.fetch_add(1, Ordering::SeqCst);
        StatusCode::NO_CONTENT
    }

    fn settlement_contract_router(mode: SettlementMode) -> (Router, Arc<RouteExecutionProbe>) {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgresql://invalid:invalid@127.0.0.1:1/unreachable")
            .unwrap();
        let state = AppState {
            config: Config {
                listen_addr: "127.0.0.1:0".to_string(),
                base_path: String::new(),
                database_url: "postgresql://invalid/invitations".to_string(),
                newapi_database_url: "postgresql://invalid/newapi".to_string(),
                session_secret: "test-only-session-secret".to_string(),
                rebate_event_ingest_secret: "0123456789abcdef0123456789abcdef".to_string(),
                sync_secret: None,
                auto_sync_interval_seconds: 0,
                auto_sync_initial_delay_seconds: 0,
                topup_reconciliation_interval_seconds: 0,
                topup_reconciliation_initial_delay_seconds: 0,
                topup_rebate_active_from_id: 1,
                settlement_mode: mode,
                quota_per_unit: 500_000,
                balance_api: None,
                balance_dispatch_interval_milliseconds: 1_000,
            },
            pool: pool.clone(),
            newapi_pool: pool.clone(),
            balance_client: None,
        };
        let probe = Arc::new(RouteExecutionProbe::default());
        let guard = axum::middleware::from_fn_with_state(state.clone(), enforce_settlement_mode);
        let app = Router::new()
            .route("/api/admin/rebate-payouts/{id}/pay", post(counted_handler))
            .route(
                "/api/admin/rebate-payouts/{id}/reverse",
                post(counted_handler),
            )
            .route(
                "/api/admin/rebate-payouts/{id}/status",
                get(counted_handler),
            )
            .route(
                "/api/admin/rebate-requests/{id}/approve-and-pay",
                post(counted_handler),
            )
            .route(
                "/api/admin/rebate-requests/{id}/confirm-issued",
                post(counted_handler),
            )
            .route(
                "/api/admin/rebate-requests/approve-and-pay-batch",
                post(counted_handler),
            )
            .route(
                "/api/admin/rebate-requests/{id}/complete",
                post(counted_handler),
            )
            .route(
                "/api/admin/rebate-requests/{id}/undo-complete",
                post(counted_handler),
            )
            .layer(guard)
            .layer(Extension(probe.clone()))
            .with_state(state);
        (app, probe)
    }

    async fn call_contract_route(app: &Router, method: Method, path: &str) -> Response {
        app.clone()
            .oneshot(
                HttpRequest::builder()
                    .method(method)
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    async fn assert_error_contract(
        response: Response,
        status: StatusCode,
        code: &str,
        message: &str,
    ) {
        assert_eq!(response.status(), status);
        let body = to_bytes(response.into_body(), 2048).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["success"], false);
        assert_eq!(json["code"], code);
        assert_eq!(json["message"], message);
    }

    #[tokio::test]
    async fn disabled_and_read_only_block_writes_before_extractors_or_handlers() {
        for mode in [SettlementMode::Disabled, SettlementMode::ReadOnly] {
            let (app, probe) = settlement_contract_router(mode);

            for path in CURRENT_SETTLEMENT_PATHS {
                let response = call_contract_route(&app, Method::POST, path).await;
                assert_error_contract(
                    response,
                    StatusCode::LOCKED,
                    "funds_maintenance",
                    "资金结算维护中，请稍后重试",
                )
                .await;
            }
            for path in LEGACY_COMPLETION_PATHS {
                let response = call_contract_route(&app, Method::POST, path).await;
                assert_error_contract(
                    response,
                    StatusCode::GONE,
                    "legacy_completion_disabled",
                    "旧版返利完成接口已停用，请使用余额发放与冲正接口",
                )
                .await;
            }

            assert_eq!(probe.extractor_calls.load(Ordering::SeqCst), 0);
            assert_eq!(probe.handler_calls.load(Ordering::SeqCst), 0);

            let response =
                call_contract_route(&app, Method::GET, "/api/admin/rebate-payouts/1/status").await;
            assert_eq!(response.status(), StatusCode::NO_CONTENT);
            assert_eq!(probe.extractor_calls.load(Ordering::SeqCst), 1);
            assert_eq!(probe.handler_calls.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn enabled_allows_current_writes_but_never_legacy_completion() {
        let (app, probe) = settlement_contract_router(SettlementMode::Enabled);

        for path in CURRENT_SETTLEMENT_PATHS {
            let response = call_contract_route(&app, Method::POST, path).await;
            assert_eq!(response.status(), StatusCode::NO_CONTENT);
        }
        let extractor_calls = probe.extractor_calls.load(Ordering::SeqCst);
        let handler_calls = probe.handler_calls.load(Ordering::SeqCst);
        assert_eq!(extractor_calls, CURRENT_SETTLEMENT_PATHS.len());
        assert_eq!(handler_calls, CURRENT_SETTLEMENT_PATHS.len());

        for path in LEGACY_COMPLETION_PATHS {
            let response = call_contract_route(&app, Method::POST, path).await;
            assert_error_contract(
                response,
                StatusCode::GONE,
                "legacy_completion_disabled",
                "旧版返利完成接口已停用，请使用余额发放与冲正接口",
            )
            .await;
        }
        assert_eq!(
            probe.extractor_calls.load(Ordering::SeqCst),
            extractor_calls
        );
        assert_eq!(probe.handler_calls.load(Ordering::SeqCst), handler_calls);
    }

    #[test]
    fn settlement_write_gate_fails_closed_with_stable_error_code() {
        for mode in [SettlementMode::Disabled, SettlementMode::ReadOnly] {
            assert!(matches!(
                require_settlement_writes_enabled(mode),
                Err(AppError::FundsMaintenance)
            ));
        }
    }

    #[test]
    fn settlement_write_gate_allows_enabled_mode() {
        assert!(require_settlement_writes_enabled(SettlementMode::Enabled).is_ok());
    }

    #[tokio::test]
    async fn settlement_write_gate_returns_locked_with_stable_error_code() {
        let response = require_settlement_writes_enabled(SettlementMode::Disabled)
            .unwrap_err()
            .into_response();

        assert_eq!(response.status(), 423);
        let body = to_bytes(response.into_body(), 1024).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["success"], false);
        assert_eq!(json["code"], "funds_maintenance");
        assert_eq!(json["message"], "资金结算维护中，请稍后重试");
    }

    #[test]
    fn classifies_current_and_legacy_writes_without_blocking_reads() {
        for path in [
            "/api/admin/rebate-payouts/1/pay",
            "/api/admin/rebate-payouts/1/reverse",
            "/api/admin/rebate-requests/1/approve-and-pay",
            "/api/admin/rebate-requests/1/confirm-issued",
            "/api/admin/rebate-requests/approve-and-pay-batch",
            "/invitations/api/admin/rebate-requests/1/approve-and-pay",
        ] {
            assert_eq!(
                settlement_write_route(&Method::POST, path),
                Some(SettlementWriteRoute::Current),
                "{path}"
            );
        }

        for path in LEGACY_COMPLETION_PATHS {
            assert_eq!(
                settlement_write_route(&Method::POST, path),
                Some(SettlementWriteRoute::LegacyCompletion),
                "{path}"
            );
        }

        for (method, path) in [
            (Method::GET, "/api/admin/rebate-payouts/1/status"),
            (Method::POST, "/api/admin/rebate-requests/1/approve"),
            (Method::POST, "/api/admin/tools/pay"),
        ] {
            assert_eq!(settlement_write_route(&method, path), None, "{path}");
        }
    }

    #[test]
    fn quota_to_whole_cents_converts_exact_amounts() {
        assert_eq!(quota_to_whole_cents(2_500_000, 500_000).unwrap(), 500);
        assert_eq!(quota_to_whole_cents(5_000, 500_000).unwrap(), 1);
    }

    #[test]
    fn quota_to_whole_cents_rounds_display_without_rejecting_fractional_credit() {
        assert_eq!(quota_to_whole_cents(5_001, 500_000).unwrap(), 1);
        assert_eq!(quota_to_whole_cents(375, 500_000).unwrap(), 0);
        assert_eq!(quota_to_whole_cents(7_500, 500_000).unwrap(), 2);
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
