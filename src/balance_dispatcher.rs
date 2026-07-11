use std::time::Duration;

use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Row, Transaction};
use tokio::time::{interval, MissedTickBehavior};

use crate::config::{BalanceApiConfig, BALANCE_LEASE_MILLISECONDS};
use crate::AppState;

const DISPATCH_BATCH_SIZE: usize = 25;
const LEASE_SECONDS: i64 = (BALANCE_LEASE_MILLISECONDS / 1_000) as i64;
const MAX_NEWAPI_DELTA: i64 = i32::MAX as i64;
const MAX_RETRY_DELAY_SECONDS: i64 = 15 * 60;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum BalanceReason {
    InvitationReward,
    InvitationRewardReversal,
}

#[derive(Debug, Serialize)]
struct BalanceAdjustmentRequest {
    operation_id: String,
    user_id: i64,
    delta: i64,
    reason: BalanceReason,
    metadata: BalanceMetadata,
}

#[derive(Debug, Serialize)]
struct BalanceMetadata {
    rebate_record_id: i64,
    payout_id: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    original_operation_id: Option<String>,
}

struct RequestIdentity<'a> {
    operation_ulid: &'a str,
    kind: OperationKind,
    rebate_record_id: i64,
    user_id: i64,
    payout_id: i64,
    original_operation_id: Option<&'a str>,
    delta_quota: i64,
}

#[derive(Debug, Deserialize)]
struct BalanceAdjustmentResponse {
    operation_id: String,
    user_id: i64,
    delta: i64,
    reason: BalanceReason,
    balance_before: i64,
    balance_after: i64,
    created_at: serde_json::Value,
    replayed: bool,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum BalanceResponseEnvelope {
    Direct(BalanceAdjustmentResponse),
    Wrapped { data: BalanceAdjustmentResponse },
}

impl BalanceResponseEnvelope {
    fn into_inner(self) -> BalanceAdjustmentResponse {
        match self {
            Self::Direct(response) | Self::Wrapped { data: response } => response,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationKind {
    Credit,
    Reversal,
}

impl OperationKind {
    fn database_value(self) -> &'static str {
        match self {
            Self::Credit => "credit",
            Self::Reversal => "reversal",
        }
    }

    fn reason(self) -> BalanceReason {
        match self {
            Self::Credit => BalanceReason::InvitationReward,
            Self::Reversal => BalanceReason::InvitationRewardReversal,
        }
    }
}

pub struct EnqueueOperation {
    pub kind: OperationKind,
    pub rebate_record_id: i64,
    pub rebate_request_id: Option<i64>,
    pub original_operation_ulid: Option<String>,
    pub user_id: i64,
    pub payout_id: i64,
    pub delta_quota: i64,
    pub now: i64,
}

#[derive(Debug)]
pub struct EnqueuedOperation {
    pub operation_ulid: String,
    pub operation_state: String,
}

struct ClaimedOperation {
    operation_id: i64,
    outbox_id: i64,
    operation_ulid: String,
    operation_kind: OperationKind,
    rebate_record_id: i64,
    user_id: i64,
    payout_id: i64,
    original_operation_ulid: Option<String>,
    delta_quota: i64,
    payload_sha256: String,
    attempt_count: i32,
    lease_token: String,
}

enum DispatchOutcome {
    Applied(BalanceAdjustmentResponse, u16),
    Retry {
        status: Option<u16>,
        code: &'static str,
    },
    Terminal {
        state: &'static str,
        status: Option<u16>,
        code: &'static str,
    },
}

pub fn build_http_client(config: &BalanceApiConfig) -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_millis(config.connect_timeout_milliseconds))
        .timeout(Duration::from_millis(config.request_timeout_milliseconds))
        .build()
}

pub async fn enqueue_operation(
    transaction: &mut Transaction<'_, Postgres>,
    input: EnqueueOperation,
) -> Result<EnqueuedOperation, sqlx::Error> {
    if input.delta_quota == 0 || input.delta_quota.unsigned_abs() > MAX_NEWAPI_DELTA as u64 {
        return Err(sqlx::Error::Protocol(
            "balance adjustment delta exceeds the NewAPI contract".to_string(),
        ));
    }
    if let Some(existing) = read_operation(transaction, input.kind, input.rebate_record_id).await? {
        if !matches!(
            existing.operation_state.as_str(),
            "needs_review" | "blocked"
        ) {
            return Ok(existing);
        }
        return requeue_terminal_operation(transaction, &input, existing).await;
    }

    let operation_ulid = ulid::Ulid::new().to_string();
    let original_operation_ulid = input.original_operation_ulid.clone();
    let request = request_for(RequestIdentity {
        operation_ulid: &operation_ulid,
        kind: input.kind,
        rebate_record_id: input.rebate_record_id,
        user_id: input.user_id,
        payout_id: input.payout_id,
        original_operation_id: original_operation_ulid.as_deref(),
        delta_quota: input.delta_quota,
    });
    let payload_sha256 = payload_sha256(&request);
    let business_operation_key = format!(
        "rebate-record:{}:{}",
        input.rebate_record_id,
        input.kind.database_value()
    );

    let inserted = sqlx::query(
        r#"
        INSERT INTO balance_operations(
            operation_ulid, business_operation_key, operation_kind,
            rebate_record_id, rebate_request_id, original_operation_ulid,
            user_id, delta_quota, idempotency_key, payload_sha256,
            operation_state, next_attempt_at, created_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $1, $9, 'queued', $10, $10, $10)
        ON CONFLICT DO NOTHING
        RETURNING id, operation_ulid::TEXT AS operation_ulid, operation_state
        "#,
    )
    .bind(&operation_ulid)
    .bind(business_operation_key)
    .bind(input.kind.database_value())
    .bind(input.rebate_record_id)
    .bind(input.rebate_request_id)
    .bind(original_operation_ulid)
    .bind(input.user_id)
    .bind(input.delta_quota)
    .bind(payload_sha256)
    .bind(input.now)
    .fetch_optional(&mut **transaction)
    .await?;

    let (operation_id, operation_ulid, operation_state): (i64, String, String) = match inserted {
        Some(row) => (
            row.try_get("id")?,
            row.try_get("operation_ulid")?,
            row.try_get("operation_state")?,
        ),
        None => {
            let existing = read_operation(transaction, input.kind, input.rebate_record_id)
                .await?
                .ok_or(sqlx::Error::RowNotFound)?;
            return Ok(existing);
        }
    };

    sqlx::query(
        r#"
        INSERT INTO balance_operation_outbox(
            operation_id, event_type, available_at, created_at
        ) VALUES ($1, 'dispatch_balance_adjustment', $2, $2)
        ON CONFLICT (operation_id, event_type) DO NOTHING
        "#,
    )
    .bind(operation_id)
    .bind(input.now)
    .execute(&mut **transaction)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO balance_operation_events(
            operation_id, event_type, state_to, occurred_at
        ) VALUES ($1, 'queued', 'queued', $2)
        "#,
    )
    .bind(operation_id)
    .bind(input.now)
    .execute(&mut **transaction)
    .await?;

    let settlement_state = match input.kind {
        OperationKind::Credit => "payout_pending",
        OperationKind::Reversal => "reversal_pending",
    };
    sqlx::query(
        r#"
        UPDATE rebate_record_settlements
        SET rebate_request_id = COALESCE(rebate_request_id, $2),
            settlement_state = $3,
            state_version = state_version + 1,
            last_operation_ulid = $4,
            updated_at = $5
        WHERE rebate_record_id = $1
        "#,
    )
    .bind(input.rebate_record_id)
    .bind(input.rebate_request_id)
    .bind(settlement_state)
    .bind(&operation_ulid)
    .bind(input.now)
    .execute(&mut **transaction)
    .await?;

    Ok(EnqueuedOperation {
        operation_ulid,
        operation_state,
    })
}

async fn requeue_terminal_operation(
    transaction: &mut Transaction<'_, Postgres>,
    input: &EnqueueOperation,
    mut operation: EnqueuedOperation,
) -> Result<EnqueuedOperation, sqlx::Error> {
    let terminal_state = operation.operation_state.clone();
    let operation_id: i64 = sqlx::query_scalar(
        r#"
        UPDATE balance_operations
        SET operation_state = 'queued', next_attempt_at = $2,
            lease_token = NULL, lease_expires_at = NULL,
            last_http_status = NULL, last_error_code = NULL, updated_at = $2
        WHERE operation_ulid = $1 AND operation_state IN ('needs_review', 'blocked')
        RETURNING id
        "#,
    )
    .bind(&operation.operation_ulid)
    .bind(input.now)
    .fetch_one(&mut **transaction)
    .await?;
    sqlx::query(
        r#"
        UPDATE balance_operation_outbox
        SET available_at = $2, dispatched_at = NULL,
            lease_token = NULL, lease_expires_at = NULL, last_error_code = NULL
        WHERE operation_id = $1 AND event_type = 'dispatch_balance_adjustment'
        "#,
    )
    .bind(operation_id)
    .bind(input.now)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        r#"
        UPDATE invitation_rebate_payouts
        SET status = 'pending', error_message = NULL, updated_at = $2
        WHERE id = $1 AND status IN ('failed', 'needs_review', 'reversal_pending')
        "#,
    )
    .bind(input.payout_id)
    .bind(input.now)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        r#"
        UPDATE rebate_record_settlements
        SET settlement_state = $2, state_version = state_version + 1,
            last_operation_ulid = $3, updated_at = $4
        WHERE rebate_record_id = $1
        "#,
    )
    .bind(input.rebate_record_id)
    .bind(match input.kind {
        OperationKind::Credit => "payout_pending",
        OperationKind::Reversal => "reversal_pending",
    })
    .bind(&operation.operation_ulid)
    .bind(input.now)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO balance_operation_events(
            operation_id, event_type, state_from, state_to, occurred_at
        ) VALUES ($1, 'manual_retry', $2, 'queued', $3)
        "#,
    )
    .bind(operation_id)
    .bind(terminal_state)
    .bind(input.now)
    .execute(&mut **transaction)
    .await?;
    operation.operation_state = "queued".to_string();
    Ok(operation)
}

pub async fn original_credit_operation_ulid(
    transaction: &mut Transaction<'_, Postgres>,
    rebate_record_id: i64,
) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar(
        r#"
        SELECT operation_ulid::TEXT
        FROM balance_operations
        WHERE rebate_record_id = $1 AND operation_kind = 'credit'
        "#,
    )
    .bind(rebate_record_id)
    .fetch_optional(&mut **transaction)
    .await
}

async fn read_operation(
    transaction: &mut Transaction<'_, Postgres>,
    kind: OperationKind,
    rebate_record_id: i64,
) -> Result<Option<EnqueuedOperation>, sqlx::Error> {
    let row = sqlx::query(
        r#"
        SELECT operation_ulid::TEXT AS operation_ulid, operation_state
        FROM balance_operations
        WHERE rebate_record_id = $1 AND operation_kind = $2
        "#,
    )
    .bind(rebate_record_id)
    .bind(kind.database_value())
    .fetch_optional(&mut **transaction)
    .await?;
    row.map(|row| {
        Ok(EnqueuedOperation {
            operation_ulid: row.try_get("operation_ulid")?,
            operation_state: row.try_get("operation_state")?,
        })
    })
    .transpose()
}

pub fn spawn_balance_dispatcher(state: AppState) {
    if !state.config.settlement_mode.is_enabled() {
        return;
    }
    let interval_milliseconds = state.config.balance_dispatch_interval_milliseconds;
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_millis(interval_milliseconds));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        tracing::info!(interval_milliseconds, "Balance dispatcher started");
        loop {
            ticker.tick().await;
            if let Err(error) = dispatch_available(&state, DISPATCH_BATCH_SIZE).await {
                tracing::error!(%error, "Balance dispatcher iteration failed");
            }
        }
    });
}

pub async fn dispatch_available(state: &AppState, limit: usize) -> Result<usize, sqlx::Error> {
    let Some(api_config) = state.config.balance_api.as_ref() else {
        tracing::error!("Balance API configuration missing while dispatcher is enabled");
        return Ok(0);
    };
    let Some(client) = state.balance_client.as_ref() else {
        tracing::error!("Balance API HTTP client missing while dispatcher is enabled");
        return Ok(0);
    };

    let mut dispatched = 0;
    for _ in 0..limit {
        let now = chrono::Utc::now().timestamp();
        let Some(claimed) = claim_next_operation(&state.pool, now).await? else {
            break;
        };
        let outcome = dispatch_claim(client, api_config, &claimed).await;
        persist_outcome(
            &state.pool,
            &claimed,
            outcome,
            chrono::Utc::now().timestamp(),
        )
        .await?;
        dispatched += 1;
    }
    Ok(dispatched)
}

#[cfg(test)]
pub async fn claim_operation_ids_for_test(
    pool: &PgPool,
    now: i64,
    count: usize,
) -> Result<Vec<String>, sqlx::Error> {
    let mut ids = Vec::new();
    for _ in 0..count {
        let Some(operation) = claim_next_operation(pool, now).await? else {
            break;
        };
        ids.push(operation.operation_ulid);
    }
    Ok(ids)
}

#[cfg(test)]
pub async fn persist_applied_for_test(
    pool: &PgPool,
    operation_ulid: &str,
    now: i64,
) -> Result<(), sqlx::Error> {
    let row = sqlx::query(
        r#"
        SELECT operation.id AS operation_id, outbox.id AS outbox_id,
               operation.operation_ulid::TEXT AS operation_ulid,
               operation.operation_kind, operation.rebate_record_id,
               operation.user_id, payout.id AS payout_id,
               operation.original_operation_ulid::TEXT AS original_operation_ulid,
               operation.delta_quota, operation.payload_sha256::TEXT AS payload_sha256,
               operation.attempt_count, operation.lease_token
        FROM balance_operations operation
        JOIN balance_operation_outbox outbox ON outbox.operation_id = operation.id
        JOIN invitation_rebate_payouts payout
          ON payout.rebate_record_id = operation.rebate_record_id
         AND payout.kind = CASE WHEN operation.operation_kind = 'credit' THEN 'payout' ELSE 'reversal' END
        WHERE operation.operation_ulid = $1
        "#,
    )
    .bind(operation_ulid)
    .fetch_one(pool)
    .await?;
    let operation_kind: String = row.try_get("operation_kind")?;
    let claimed = ClaimedOperation {
        operation_id: row.try_get("operation_id")?,
        outbox_id: row.try_get("outbox_id")?,
        operation_ulid: row.try_get("operation_ulid")?,
        operation_kind: if operation_kind == "credit" {
            OperationKind::Credit
        } else {
            OperationKind::Reversal
        },
        rebate_record_id: row.try_get("rebate_record_id")?,
        user_id: row.try_get("user_id")?,
        payout_id: row.try_get("payout_id")?,
        original_operation_ulid: row.try_get("original_operation_ulid")?,
        delta_quota: row.try_get("delta_quota")?,
        payload_sha256: row.try_get("payload_sha256")?,
        attempt_count: row.try_get("attempt_count")?,
        lease_token: row.try_get("lease_token")?,
    };
    let response = BalanceAdjustmentResponse {
        operation_id: operation_ulid.to_string(),
        user_id: claimed.user_id,
        delta: claimed.delta_quota,
        reason: claimed.operation_kind.reason(),
        balance_before: 10_000_000,
        balance_after: 10_000_000 + claimed.delta_quota,
        created_at: serde_json::json!(now),
        replayed: false,
    };
    persist_applied(pool, &claimed, response, 201, now).await
}

#[cfg(test)]
pub async fn mark_needs_review_for_test(
    pool: &PgPool,
    operation_ulid: &str,
    now: i64,
) -> Result<(), sqlx::Error> {
    let operation_id: i64 =
        sqlx::query_scalar("SELECT id FROM balance_operations WHERE operation_ulid = $1")
            .bind(operation_ulid)
            .fetch_one(pool)
            .await?;
    sqlx::query(
        r#"
        UPDATE balance_operations
        SET operation_state = 'needs_review', last_error_code = 'insufficient_balance',
            lease_token = NULL, lease_expires_at = NULL, updated_at = $2
        WHERE id = $1
        "#,
    )
    .bind(operation_id)
    .bind(now)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        UPDATE balance_operation_outbox
        SET dispatched_at = $2, lease_token = NULL, lease_expires_at = NULL
        WHERE operation_id = $1
        "#,
    )
    .bind(operation_id)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

async fn claim_next_operation(
    pool: &PgPool,
    now: i64,
) -> Result<Option<ClaimedOperation>, sqlx::Error> {
    let mut transaction = pool.begin().await?;
    let lease_token = ulid::Ulid::new().to_string();
    let row = sqlx::query(
        r#"
        SELECT outbox.id AS outbox_id,
               operation.id AS operation_id,
               operation.operation_ulid::TEXT AS operation_ulid,
               operation.operation_kind,
               operation.rebate_record_id,
               operation.user_id,
               payout.id AS payout_id,
               operation.original_operation_ulid::TEXT AS original_operation_ulid,
               operation.delta_quota,
               operation.payload_sha256::TEXT AS payload_sha256,
               operation.attempt_count
        FROM balance_operation_outbox outbox
        JOIN balance_operations operation ON operation.id = outbox.operation_id
        JOIN rebate_records record ON record.id = operation.rebate_record_id
        JOIN invitation_rebate_payouts payout
          ON payout.rebate_record_id = operation.rebate_record_id
         AND payout.kind = CASE WHEN operation.operation_kind = 'credit' THEN 'payout' ELSE 'reversal' END
        WHERE outbox.dispatched_at IS NULL
          AND outbox.available_at <= $1
          AND (outbox.lease_expires_at IS NULL OR outbox.lease_expires_at <= $1)
          AND operation.operation_state IN ('queued', 'retry_wait', 'dispatching', 'external_pending')
        ORDER BY outbox.available_at, outbox.id
        FOR UPDATE OF outbox SKIP LOCKED
        LIMIT 1
        "#,
    )
    .bind(now)
    .fetch_optional(&mut *transaction)
    .await?;

    let Some(row) = row else {
        transaction.commit().await?;
        return Ok(None);
    };
    let outbox_id: i64 = row.try_get("outbox_id")?;
    let operation_id: i64 = row.try_get("operation_id")?;
    let prior_attempt_count: i32 = row.try_get("attempt_count")?;
    let lease_expires_at = now + LEASE_SECONDS;

    sqlx::query(
        r#"
        UPDATE balance_operation_outbox
        SET lease_token = $2, lease_expires_at = $3, attempt_count = attempt_count + 1
        WHERE id = $1
        "#,
    )
    .bind(outbox_id)
    .bind(&lease_token)
    .bind(lease_expires_at)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        r#"
        UPDATE balance_operations
        SET operation_state = 'dispatching',
            attempt_count = attempt_count + 1,
            lease_token = $2,
            lease_expires_at = $3,
            updated_at = $4
        WHERE id = $1
        "#,
    )
    .bind(operation_id)
    .bind(&lease_token)
    .bind(lease_expires_at)
    .bind(now)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;

    let operation_kind: String = row.try_get("operation_kind")?;
    Ok(Some(ClaimedOperation {
        operation_id,
        outbox_id,
        operation_ulid: row.try_get("operation_ulid")?,
        operation_kind: if operation_kind == "credit" {
            OperationKind::Credit
        } else {
            OperationKind::Reversal
        },
        rebate_record_id: row.try_get("rebate_record_id")?,
        user_id: row.try_get("user_id")?,
        payout_id: row.try_get("payout_id")?,
        original_operation_ulid: row.try_get("original_operation_ulid")?,
        delta_quota: row.try_get("delta_quota")?,
        payload_sha256: row.try_get("payload_sha256")?,
        attempt_count: prior_attempt_count + 1,
        lease_token,
    }))
}

async fn dispatch_claim(
    client: &reqwest::Client,
    config: &BalanceApiConfig,
    claimed: &ClaimedOperation,
) -> DispatchOutcome {
    let request = request_for(RequestIdentity {
        operation_ulid: &claimed.operation_ulid,
        kind: claimed.operation_kind,
        rebate_record_id: claimed.rebate_record_id,
        user_id: claimed.user_id,
        payout_id: claimed.payout_id,
        original_operation_id: claimed.original_operation_ulid.as_deref(),
        delta_quota: claimed.delta_quota,
    });
    if payload_sha256(&request) != claimed.payload_sha256 {
        return DispatchOutcome::Terminal {
            state: "needs_review",
            status: None,
            code: "payload_hash_mismatch",
        };
    }

    let url = format!(
        "{}/api/internal/balance-adjustments",
        config.base_url.trim_end_matches('/')
    );
    let response = match client
        .post(url)
        .bearer_auth(&config.secret)
        .json(&request)
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!(operation_id = %claimed.operation_ulid, %error, "Balance API request failed");
            return DispatchOutcome::Retry {
                status: None,
                code: if error.is_timeout() {
                    "request_timeout"
                } else {
                    "network_error"
                },
            };
        }
    };

    let status = response.status();
    if status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_EARLY
        || status == StatusCode::TOO_MANY_REQUESTS
        || status == StatusCode::SERVICE_UNAVAILABLE
        || status.is_server_error()
    {
        return DispatchOutcome::Retry {
            status: Some(status.as_u16()),
            code: if status == StatusCode::REQUEST_TIMEOUT {
                "upstream_request_timeout"
            } else if status == StatusCode::TOO_EARLY {
                "upstream_too_early"
            } else if status == StatusCode::TOO_MANY_REQUESTS {
                "rate_limited"
            } else if status == StatusCode::SERVICE_UNAVAILABLE {
                "balance_api_unavailable"
            } else {
                "upstream_error"
            },
        };
    }
    if status == StatusCode::CONFLICT || status == StatusCode::UNPROCESSABLE_ENTITY {
        let code = response_error_code(response).await;
        return DispatchOutcome::Terminal {
            state: "needs_review",
            status: Some(status.as_u16()),
            code: classify_review_code(status, code.as_deref()),
        };
    }
    if status.is_client_error() {
        let code = response_error_code(response).await;
        if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN)
            || (status == StatusCode::NOT_FOUND && code.as_deref() != Some("user_not_found"))
        {
            return DispatchOutcome::Retry {
                status: Some(status.as_u16()),
                code: match status {
                    StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => "balance_api_auth_failed",
                    _ => "balance_api_route_not_found",
                },
            };
        }
        return DispatchOutcome::Terminal {
            state: "needs_review",
            status: Some(status.as_u16()),
            code: match code.as_deref() {
                Some("user_not_found") => "balance_user_not_found",
                Some("invalid_request") => "invalid_request",
                _ => "balance_api_rejected",
            },
        };
    }
    if !status.is_success() {
        return DispatchOutcome::Retry {
            status: Some(status.as_u16()),
            code: "unexpected_http_status",
        };
    }

    let response = match response.json::<BalanceResponseEnvelope>().await {
        Ok(response) => response.into_inner(),
        Err(error) => {
            tracing::error!(operation_id = %claimed.operation_ulid, %error, "Invalid Balance API success response");
            return DispatchOutcome::Retry {
                status: Some(status.as_u16()),
                code: "invalid_success_response",
            };
        }
    };
    if !response_matches_request(&response, &request) {
        return DispatchOutcome::Retry {
            status: Some(status.as_u16()),
            code: "success_response_mismatch",
        };
    }
    DispatchOutcome::Applied(response, status.as_u16())
}

async fn response_error_code(response: reqwest::Response) -> Option<String> {
    let body = response.json::<serde_json::Value>().await.ok()?;
    body.pointer("/error/code")
        .or_else(|| body.get("code"))
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
}

fn classify_review_code(status: StatusCode, upstream_code: Option<&str>) -> &'static str {
    match upstream_code {
        Some("idempotency_conflict") | Some("reversal_conflict") => "idempotency_conflict",
        Some("insufficient_balance") => "insufficient_balance",
        Some("balance_overflow") => "balance_overflow",
        Some("invalid_request") => "invalid_request",
        _ if status == StatusCode::CONFLICT => "idempotency_conflict",
        _ => "unprocessable_adjustment",
    }
}

fn response_matches_request(
    response: &BalanceAdjustmentResponse,
    request: &BalanceAdjustmentRequest,
) -> bool {
    response.operation_id == request.operation_id
        && response.user_id == request.user_id
        && response.delta == request.delta
        && response.reason == request.reason
        && response.balance_after == response.balance_before.saturating_add(response.delta)
        && !response.created_at.is_null()
}

fn external_created_at(value: &serde_json::Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

async fn persist_outcome(
    pool: &PgPool,
    claimed: &ClaimedOperation,
    outcome: DispatchOutcome,
    now: i64,
) -> Result<(), sqlx::Error> {
    match outcome {
        DispatchOutcome::Applied(response, status) => {
            persist_applied(pool, claimed, response, status, now).await
        }
        DispatchOutcome::Retry { status, code } => {
            persist_failure(pool, claimed, "retry_wait", status, code, now).await
        }
        DispatchOutcome::Terminal {
            state,
            status,
            code,
        } => persist_failure(pool, claimed, state, status, code, now).await,
    }
}

async fn persist_applied(
    pool: &PgPool,
    claimed: &ClaimedOperation,
    response: BalanceAdjustmentResponse,
    status: u16,
    now: i64,
) -> Result<(), sqlx::Error> {
    let mut transaction = pool.begin().await?;
    let current_state: Option<String> = sqlx::query_scalar(
        r#"
        SELECT operation_state FROM balance_operations
        WHERE id = $1 AND lease_token = $2
        FOR UPDATE
        "#,
    )
    .bind(claimed.operation_id)
    .bind(&claimed.lease_token)
    .fetch_optional(&mut *transaction)
    .await?;
    let Some(current_state) = current_state else {
        transaction.rollback().await?;
        return Ok(());
    };
    if current_state == "completed" {
        mark_outbox_dispatched(&mut transaction, claimed.outbox_id, now).await?;
        transaction.commit().await?;
        return Ok(());
    }

    sqlx::query(
        r#"
        UPDATE balance_operations
        SET operation_state = 'completed', next_attempt_at = NULL,
            lease_token = NULL, lease_expires_at = NULL,
            last_http_status = $2, last_error_code = NULL,
            balance_before = $4, balance_after = $5, external_created_at = $6,
            updated_at = $3, completed_at = COALESCE(completed_at, $3)
        WHERE id = $1
        "#,
    )
    .bind(claimed.operation_id)
    .bind(i32::from(status))
    .bind(now)
    .bind(response.balance_before)
    .bind(response.balance_after)
    .bind(external_created_at(&response.created_at))
    .execute(&mut *transaction)
    .await?;
    mark_outbox_dispatched(&mut transaction, claimed.outbox_id, now).await?;
    sqlx::query(
        r#"
        INSERT INTO balance_operation_events(
            operation_id, event_type, state_from, state_to, occurred_at
        ) VALUES ($1, 'api_applied', $2, 'completed', $3)
        "#,
    )
    .bind(claimed.operation_id)
    .bind(current_state)
    .bind(now)
    .execute(&mut *transaction)
    .await?;

    match claimed.operation_kind {
        OperationKind::Credit => complete_credit(&mut transaction, claimed, now).await?,
        OperationKind::Reversal => complete_reversal(&mut transaction, claimed, now).await?,
    }
    transaction.commit().await?;
    tracing::info!(
        operation_id = %claimed.operation_ulid,
        replayed = response.replayed,
        balance_after = response.balance_after,
        "Balance operation completed"
    );
    Ok(())
}

async fn mark_outbox_dispatched(
    transaction: &mut Transaction<'_, Postgres>,
    outbox_id: i64,
    now: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE balance_operation_outbox
        SET dispatched_at = COALESCE(dispatched_at, $2),
            lease_token = NULL, lease_expires_at = NULL
        WHERE id = $1
        "#,
    )
    .bind(outbox_id)
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn complete_credit(
    transaction: &mut Transaction<'_, Postgres>,
    operation: &ClaimedOperation,
    now: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE invitation_rebate_payouts
        SET status = 'completed', newapi_mark_id = NULL, newapi_log_id = NULL,
            error_message = NULL, completed_at = COALESCE(completed_at, $2), updated_at = $2
        WHERE id = $1 AND kind = 'payout'
        "#,
    )
    .bind(operation.payout_id)
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        r#"
        UPDATE rebate_records
        SET status = 'completed', finalized_at = COALESCE(finalized_at, $2), updated_at = $2
        WHERE id = $1
        "#,
    )
    .bind(operation.rebate_record_id)
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        r#"
        UPDATE rebate_record_settlements
        SET settlement_state = 'paid', state_version = state_version + 1,
            last_operation_ulid = $2, updated_at = $3
        WHERE rebate_record_id = $1
        "#,
    )
    .bind(operation.rebate_record_id)
    .bind(&operation.operation_ulid)
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    complete_request_if_ready(transaction, operation.rebate_record_id, now).await
}

async fn complete_request_if_ready(
    transaction: &mut Transaction<'_, Postgres>,
    rebate_record_id: i64,
    now: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE rebate_requests request
        SET status = 'completed', completed_at = COALESCE(completed_at, $2), updated_at = $2
        WHERE request.id = (
            SELECT rebate_request_id FROM invitation_rebate_payouts
            WHERE rebate_record_id = $1 AND kind = 'payout'
        )
          AND NOT EXISTS (
              SELECT 1 FROM rebate_records record
              WHERE record.id = ANY(request.rebate_record_ids) AND record.status <> 'completed'
          )
        "#,
    )
    .bind(rebate_record_id)
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn complete_reversal(
    transaction: &mut Transaction<'_, Postgres>,
    operation: &ClaimedOperation,
    now: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE invitation_rebate_payouts
        SET status = 'reversed', error_message = NULL,
            reversed_at = COALESCE(reversed_at, $2), updated_at = $2
        WHERE rebate_record_id = $1 AND kind = 'payout'
        "#,
    )
    .bind(operation.rebate_record_id)
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        r#"
        UPDATE invitation_rebate_payouts
        SET status = 'completed', newapi_mark_id = NULL, newapi_log_id = NULL,
            error_message = NULL, completed_at = COALESCE(completed_at, $2), updated_at = $2
        WHERE id = $1 AND kind = 'reversal'
        "#,
    )
    .bind(operation.payout_id)
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "UPDATE rebate_records SET status = 'approved', finalized_at = NULL, updated_at = $2 WHERE id = $1",
    )
    .bind(operation.rebate_record_id)
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        r#"
        UPDATE rebate_record_settlements
        SET settlement_state = 'reversed', state_version = state_version + 1,
            last_operation_ulid = $2, updated_at = $3
        WHERE rebate_record_id = $1
        "#,
    )
    .bind(operation.rebate_record_id)
    .bind(&operation.operation_ulid)
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        r#"
        UPDATE rebate_requests
        SET status = 'approved', completed_at = NULL, updated_at = $2
        WHERE id = (
            SELECT rebate_request_id FROM invitation_rebate_payouts
            WHERE rebate_record_id = $1 AND kind = 'payout'
        ) AND status = 'completed'
        "#,
    )
    .bind(operation.rebate_record_id)
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn persist_failure(
    pool: &PgPool,
    claimed: &ClaimedOperation,
    state: &'static str,
    status: Option<u16>,
    code: &'static str,
    now: i64,
) -> Result<(), sqlx::Error> {
    let mut transaction = pool.begin().await?;
    let current_state: Option<String> = sqlx::query_scalar(
        r#"
        SELECT operation_state FROM balance_operations
        WHERE id = $1 AND lease_token = $2
        FOR UPDATE
        "#,
    )
    .bind(claimed.operation_id)
    .bind(&claimed.lease_token)
    .fetch_optional(&mut *transaction)
    .await?;
    let Some(current_state) = current_state else {
        transaction.rollback().await?;
        return Ok(());
    };
    if current_state == "completed" {
        mark_outbox_dispatched(&mut transaction, claimed.outbox_id, now).await?;
        transaction.commit().await?;
        return Ok(());
    }

    let terminal = state != "retry_wait";
    let next_attempt_at = (!terminal).then(|| now + retry_delay_seconds(claimed.attempt_count));
    sqlx::query(
        r#"
        UPDATE balance_operations
        SET operation_state = $2, next_attempt_at = $3,
            lease_token = NULL, lease_expires_at = NULL,
            last_http_status = $4, last_error_code = $5, updated_at = $6
        WHERE id = $1
        "#,
    )
    .bind(claimed.operation_id)
    .bind(state)
    .bind(next_attempt_at)
    .bind(status.map(i32::from))
    .bind(code)
    .bind(now)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        r#"
        UPDATE balance_operation_outbox
        SET available_at = COALESCE($2, available_at),
            dispatched_at = CASE WHEN $3 THEN COALESCE(dispatched_at, $4) ELSE NULL END,
            lease_token = NULL, lease_expires_at = NULL
        WHERE id = $1
        "#,
    )
    .bind(claimed.outbox_id)
    .bind(next_attempt_at)
    .bind(terminal)
    .bind(now)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO balance_operation_events(
            operation_id, event_type, state_from, state_to, error_code, occurred_at
        ) VALUES ($1, 'dispatch_failed', $2, $3, $4, $5)
        "#,
    )
    .bind(claimed.operation_id)
    .bind(current_state)
    .bind(state)
    .bind(code)
    .bind(now)
    .execute(&mut *transaction)
    .await?;

    if terminal {
        mark_legacy_needs_review(&mut transaction, claimed, code, now).await?;
    }
    transaction.commit().await?;
    Ok(())
}

async fn mark_legacy_needs_review(
    transaction: &mut Transaction<'_, Postgres>,
    operation: &ClaimedOperation,
    code: &str,
    now: i64,
) -> Result<(), sqlx::Error> {
    match operation.operation_kind {
        OperationKind::Credit => {
            sqlx::query(
                r#"
                UPDATE invitation_rebate_payouts
                SET status = 'failed', error_message = $2, updated_at = $3
                WHERE id = $1 AND kind = 'payout' AND status <> 'completed'
                "#,
            )
            .bind(operation.payout_id)
            .bind(code)
            .bind(now)
            .execute(&mut **transaction)
            .await?;
        }
        OperationKind::Reversal => {
            sqlx::query(
                r#"
                UPDATE invitation_rebate_payouts
                SET status = 'reversal_pending', error_message = $2, updated_at = $3
                WHERE rebate_record_id = $1 AND kind = 'payout'
                "#,
            )
            .bind(operation.rebate_record_id)
            .bind(code)
            .bind(now)
            .execute(&mut **transaction)
            .await?;
            sqlx::query(
                r#"
                UPDATE invitation_rebate_payouts
                SET status = 'failed', error_message = $2, updated_at = $3
                WHERE id = $1 AND kind = 'reversal'
                "#,
            )
            .bind(operation.payout_id)
            .bind(code)
            .bind(now)
            .execute(&mut **transaction)
            .await?;
        }
    }
    sqlx::query(
        r#"
        UPDATE rebate_record_settlements
        SET settlement_state = 'needs_review', state_version = state_version + 1,
            last_operation_ulid = $2, updated_at = $3
        WHERE rebate_record_id = $1
        "#,
    )
    .bind(operation.rebate_record_id)
    .bind(&operation.operation_ulid)
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn request_for(identity: RequestIdentity<'_>) -> BalanceAdjustmentRequest {
    BalanceAdjustmentRequest {
        operation_id: identity.operation_ulid.to_string(),
        user_id: identity.user_id,
        delta: identity.delta_quota,
        reason: identity.kind.reason(),
        metadata: BalanceMetadata {
            rebate_record_id: identity.rebate_record_id,
            payout_id: identity.payout_id,
            original_operation_id: identity.original_operation_id.map(ToOwned::to_owned),
        },
    }
}

fn payload_sha256(request: &BalanceAdjustmentRequest) -> String {
    let bytes = serde_json::to_vec(request).expect("balance adjustment request is serializable");
    hex::encode(Sha256::digest(bytes))
}

fn retry_delay_seconds(attempt_count: i32) -> i64 {
    let exponent = u32::try_from(attempt_count.saturating_sub(1).min(8)).unwrap_or_default();
    (5_i64.saturating_mul(2_i64.saturating_pow(exponent))).min(MAX_RETRY_DELAY_SECONDS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_is_stable_and_uses_operation_ulid_as_business_idempotency_key() {
        let request = request_for(RequestIdentity {
            operation_ulid: "01J00000000000000000000001",
            kind: OperationKind::Credit,
            rebate_record_id: 42,
            user_id: 7,
            payout_id: 88,
            original_operation_id: None,
            delta_quota: 500_000,
        });
        assert_eq!(request.operation_id, "01J00000000000000000000001");
        assert_eq!(request.reason, BalanceReason::InvitationReward);
        assert_eq!(request.metadata.rebate_record_id, 42);
        assert_eq!(request.metadata.payout_id, 88);
        assert_eq!(payload_sha256(&request), payload_sha256(&request));
    }

    #[test]
    fn retry_backoff_is_exponential_and_capped() {
        assert_eq!(retry_delay_seconds(1), 5);
        assert_eq!(retry_delay_seconds(2), 10);
        assert_eq!(retry_delay_seconds(9), 900);
        assert_eq!(retry_delay_seconds(100), 900);
    }

    #[test]
    fn newapi_delta_contract_rejects_out_of_range_values_before_enqueue() {
        assert_eq!(MAX_NEWAPI_DELTA, i64::from(i32::MAX));
        assert!(i64::from(i32::MAX) <= MAX_NEWAPI_DELTA);
        assert!(i64::from(i32::MAX) + 1 > MAX_NEWAPI_DELTA);
    }

    #[test]
    fn classifies_conflicts_and_insufficient_reversals_for_review() {
        assert_eq!(
            classify_review_code(StatusCode::CONFLICT, Some("idempotency_conflict")),
            "idempotency_conflict"
        );
        assert_eq!(
            classify_review_code(
                StatusCode::UNPROCESSABLE_ENTITY,
                Some("insufficient_balance")
            ),
            "insufficient_balance"
        );
    }

    #[tokio::test]
    async fn dispatches_exact_newapi_contract_and_accepts_wrapped_success() {
        use axum::extract::State;
        use axum::http::{HeaderMap, StatusCode as AxumStatusCode};
        use axum::routing::post;
        use axum::{Json, Router};

        async fn endpoint(
            State(secret): State<String>,
            headers: HeaderMap,
            Json(body): Json<serde_json::Value>,
        ) -> (AxumStatusCode, Json<serde_json::Value>) {
            assert_eq!(
                headers.get("authorization").unwrap(),
                &format!("Bearer {secret}")
            );
            assert_eq!(body["operation_id"], "01J00000000000000000000001");
            assert_eq!(body["metadata"]["rebate_record_id"], 42);
            assert_eq!(body["metadata"]["payout_id"], 88);
            assert!(body["metadata"].get("original_operation_id").is_none());
            (
                AxumStatusCode::CREATED,
                Json(serde_json::json!({
                    "success": true,
                    "data": {
                        "adjustment_id": 1,
                        "operation_id": body["operation_id"],
                        "user_id": body["user_id"],
                        "delta": body["delta"],
                        "reason": body["reason"],
                        "balance_before": 1_000_000,
                        "balance_after": 1_500_000,
                        "created_at": 1_700_000_000,
                        "replayed": false
                    }
                })),
            )
        }

        let secret = "integration-secret".to_string();
        let app = Router::new()
            .route("/api/internal/balance-adjustments", post(endpoint))
            .with_state(secret.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let claimed = ClaimedOperation {
            operation_id: 1,
            outbox_id: 1,
            operation_ulid: "01J00000000000000000000001".to_string(),
            operation_kind: OperationKind::Credit,
            rebate_record_id: 42,
            user_id: 7,
            payout_id: 88,
            original_operation_ulid: None,
            delta_quota: 500_000,
            payload_sha256: payload_sha256(&request_for(RequestIdentity {
                operation_ulid: "01J00000000000000000000001",
                kind: OperationKind::Credit,
                rebate_record_id: 42,
                user_id: 7,
                payout_id: 88,
                original_operation_id: None,
                delta_quota: 500_000,
            })),
            attempt_count: 1,
            lease_token: "lease".to_string(),
        };
        let config = BalanceApiConfig {
            base_url: format!("http://{address}"),
            secret,
            connect_timeout_milliseconds: 1_000,
            request_timeout_milliseconds: 1_000,
        };
        let client = build_http_client(&config).unwrap();
        assert!(matches!(
            dispatch_claim(&client, &config, &claimed).await,
            DispatchOutcome::Applied(_, 201)
        ));
    }

    #[test]
    fn validates_success_response_financial_identity_and_balance_math() {
        let request = request_for(RequestIdentity {
            operation_ulid: "01J00000000000000000000001",
            kind: OperationKind::Reversal,
            rebate_record_id: 42,
            user_id: 7,
            payout_id: 89,
            original_operation_id: Some("01J00000000000000000000000"),
            delta_quota: -500_000,
        });
        let response = BalanceAdjustmentResponse {
            operation_id: request.operation_id.clone(),
            user_id: 7,
            delta: -500_000,
            reason: BalanceReason::InvitationRewardReversal,
            balance_before: 1_000_000,
            balance_after: 500_000,
            created_at: serde_json::json!(1_700_000_000),
            replayed: true,
        };
        assert!(response_matches_request(&response, &request));
    }
}
