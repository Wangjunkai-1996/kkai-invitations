use std::collections::HashMap;

use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::Json;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::pool::PoolConnection;
use sqlx::Postgres;
use sqlx::Row;

use crate::error::{AppError, Result};
use crate::invitation_signup::{
    fetch_paid_invitee_ids, insert_invitation_signup_reward_record,
    resolve_invitation_signup_reward_unlocked_at, unlock_paid_invitation_signup_rewards,
    upsert_invitation_registration, InsertInvitationSignupReward, UpsertInvitationRegistration,
};
use crate::rebate_lifecycle::{
    finalize_ready_initializing_records, finalize_ready_topup_event_records,
    INITIALIZATION_SECONDS, REBATE_EFFECTIVE_DELAY_SECONDS,
};
use crate::rebate_types::{
    INVITATION_SIGNUP_INVITEE_ORDER_TYPE, INVITATION_SIGNUP_INVITER_ORDER_TYPE,
    REDEMPTION_ORDER_TYPE, SUBSCRIPTION_ORDER_TYPE,
};
use crate::response::{message, ApiResponse};
use crate::system_config::{
    get_config_bool, get_config_i64_or_fallback, INVITATION_SIGNUP_INVITEE_REWARD_AMOUNT_KEY,
    INVITATION_SIGNUP_INVITEE_REWARD_REQUIRES_PAID_ORDER_KEY,
    INVITATION_SIGNUP_INVITER_REWARD_AMOUNT_KEY,
    INVITATION_SIGNUP_INVITER_REWARD_REQUIRES_PAID_ORDER_KEY, INVITATION_SIGNUP_REWARD_AMOUNT_KEY,
    INVITATION_SIGNUP_REWARD_ENABLED_KEY, ORDER_REBATE_ENABLED_KEY,
};
use crate::AppState;

const REBATE_SYNC_ADVISORY_LOCK_ID: i64 = 6_212_202_606_050;
const TOPUP_RECONCILIATION_SOURCE: &str = "newapi";
const TOPUP_RECONCILIATION_LOOKBACK_SECONDS: i64 = 24 * 60 * 60;
const TOPUP_EVENT_DELIVERY_GRACE_SECONDS: i64 = 60;
const TOPUP_EVENT_STALLED_SECONDS: i64 = 5 * 60;
const TOPUP_COMPLETED_TOPIC: &str = "kkai.billing.topup.completed";

#[derive(Clone, Copy, Eq, PartialEq)]
enum RebateSourceReceiptOutcome {
    Created,
    IgnoredNoInviter,
}

#[derive(Clone, Copy)]
struct RebateSourceReceiptAudit {
    outcome: RebateSourceReceiptOutcome,
    canonical_exists: bool,
    integrity_valid: bool,
}

// POST /api/system/sync-rebates
pub async fn sync_rebates(
    State(state): State<AppState>,
    Query(params): Query<SyncParams>,
    headers: HeaderMap,
) -> Result<Json<ApiResponse<SyncResponse>>> {
    authorize_sync(&state, &headers, params.secret.as_deref())?;

    let result = run_rebate_sync(&state).await?;
    let message_text = if result.skipped {
        "同步正在运行"
    } else {
        "同步完成"
    };

    Ok(message(result, message_text))
}

pub async fn run_rebate_sync(state: &AppState) -> Result<SyncResponse> {
    let Some(lock_conn) = acquire_rebate_sync_lock(state, "rebate sync").await? else {
        return Ok(SyncResponse::skipped());
    };

    release_rebate_sync_lock(lock_conn, run_rebate_sync_unlocked(state).await).await
}

pub struct TopUpReconciliationResult {
    pub skipped: bool,
    pub finalized: i64,
    pub created_cases: i64,
}

pub async fn run_topup_reconciliation(state: &AppState) -> Result<TopUpReconciliationResult> {
    let Some(lock_conn) = acquire_rebate_sync_lock(state, "top-up reconciliation").await? else {
        return Ok(TopUpReconciliationResult {
            skipped: true,
            finalized: 0,
            created_cases: 0,
        });
    };

    let result = async {
        let now = chrono::Utc::now().timestamp();
        let finalized = finalize_ready_topup_event_records(&state.pool, now).await?;
        let created_cases = sync_topup_orders(state, now).await?;
        Ok(TopUpReconciliationResult {
            skipped: false,
            finalized,
            created_cases,
        })
    }
    .await;
    release_rebate_sync_lock(lock_conn, result).await
}

async fn acquire_rebate_sync_lock(
    state: &AppState,
    operation: &str,
) -> Result<Option<PoolConnection<Postgres>>> {
    let mut lock_conn = state.pool.acquire().await?;
    let lock_acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(REBATE_SYNC_ADVISORY_LOCK_ID)
        .fetch_one(&mut *lock_conn)
        .await?;

    if !lock_acquired {
        tracing::info!(
            operation,
            "Operation skipped: another rebate sync is already running"
        );
        return Ok(None);
    }
    Ok(Some(lock_conn))
}

async fn release_rebate_sync_lock<T>(
    mut lock_conn: PoolConnection<Postgres>,
    result: Result<T>,
) -> Result<T> {
    let unlock_result: std::result::Result<bool, sqlx::Error> =
        sqlx::query_scalar("SELECT pg_advisory_unlock($1)")
            .bind(REBATE_SYNC_ADVISORY_LOCK_ID)
            .fetch_one(&mut *lock_conn)
            .await;

    match (result, unlock_result) {
        (Ok(value), Ok(true)) => Ok(value),
        (Ok(value), Ok(false)) => {
            tracing::warn!("Rebate sync advisory lock was not held while unlocking");
            Ok(value)
        }
        (Ok(_), Err(err)) => Err(AppError::from(err)),
        (Err(err), Ok(_)) => Err(err),
        (Err(sync_err), Err(unlock_err)) => {
            tracing::error!(
                error = %unlock_err,
                "Failed to release rebate sync advisory lock after sync error"
            );
            Err(sync_err)
        }
    }
}

async fn run_rebate_sync_unlocked(state: &AppState) -> Result<SyncResponse> {
    let now = chrono::Utc::now().timestamp();
    tracing::info!("Starting rebate sync");

    let finalized = finalize_ready_initializing_records(&state.pool, now).await?;
    tracing::info!(
        finalized_count = finalized,
        "Initializing rebate records finalized"
    );

    let invitation_registrations_synced = sync_invitation_registrations(state, now).await?;
    tracing::info!(
        synced_count = invitation_registrations_synced,
        "Invitation registrations synced"
    );

    let invitation_signup_synced = sync_invitation_signup_rewards(state, now).await?;
    tracing::info!(
        synced_count = invitation_signup_synced,
        "Invitation signup rewards synced"
    );

    let invitation_signup_unlocked = sync_invitation_signup_reward_unlocks(state, now).await?;
    tracing::info!(
        unlocked_count = invitation_signup_unlocked,
        "Invitation signup rewards unlocked"
    );

    let order_rebate_enabled = get_config_bool(&state.pool, ORDER_REBATE_ENABLED_KEY).await?;
    let (topup_synced, subscription_synced, redemption_synced) = if order_rebate_enabled {
        let topup_synced = sync_topup_orders(state, now).await?;
        tracing::info!(synced_count = topup_synced, "Topup orders synced");

        let subscription_synced = sync_subscription_orders(state, now).await?;
        tracing::info!(
            synced_count = subscription_synced,
            "Subscription orders synced"
        );

        let redemption_synced = sync_redemption_orders(state, now).await?;
        tracing::info!(synced_count = redemption_synced, "Redemption orders synced");

        (topup_synced, subscription_synced, redemption_synced)
    } else {
        tracing::info!("Order rebate sync skipped: disabled");
        (0, 0, 0)
    };

    Ok(SyncResponse {
        skipped: false,
        finalized,
        invitation_registrations_synced,
        invitation_signup_synced,
        invitation_signup_unlocked,
        topup_synced,
        subscription_synced,
        redemption_synced,
    })
}

#[derive(Deserialize)]
pub struct SyncParams {
    pub secret: Option<String>,
}

#[derive(Serialize)]
pub struct SyncResponse {
    pub skipped: bool,
    pub finalized: i64,
    pub invitation_registrations_synced: i64,
    pub invitation_signup_synced: i64,
    pub invitation_signup_unlocked: i64,
    pub topup_synced: i64,
    pub subscription_synced: i64,
    pub redemption_synced: i64,
}

impl SyncResponse {
    fn skipped() -> Self {
        Self {
            skipped: true,
            finalized: 0,
            invitation_registrations_synced: 0,
            invitation_signup_synced: 0,
            invitation_signup_unlocked: 0,
            topup_synced: 0,
            subscription_synced: 0,
            redemption_synced: 0,
        }
    }
}

async fn sync_invitation_registrations(state: &AppState, now: i64) -> Result<i64> {
    let last_sync_id = last_sync_id(state, "invitation_registration").await?;
    let users = sqlx::query(
        r#"
        SELECT
            id,
            inviter_id,
            COALESCE(NULLIF("group", ''), 'default') AS user_group,
            COALESCE(NULLIF(created_at, 0), $2) AS invited_at
        FROM users
        WHERE id > $1
          AND inviter_id IS NOT NULL
          AND inviter_id > 0
        ORDER BY id
        LIMIT 1000
        "#,
    )
    .bind(last_sync_id)
    .bind(now)
    .fetch_all(&state.newapi_pool)
    .await?;

    tracing::info!(
        last_sync_id,
        fetched_count = users.len(),
        "Invitation registration sync loaded invited users"
    );

    let mut synced = 0;
    let mut max_processed_id = last_sync_id;

    for user in users {
        let invitee_id: i64 = user.try_get("id")?;
        let inviter_id: i64 = user.try_get("inviter_id")?;
        let user_group: String = user.try_get("user_group")?;
        let invited_at: i64 = user.try_get("invited_at")?;

        upsert_invitation_registration(
            &state.pool,
            UpsertInvitationRegistration {
                inviter_id,
                invitee_id,
                user_group: &user_group,
                invited_at,
                now,
            },
        )
        .await?;

        synced += 1;
        max_processed_id = invitee_id;
    }

    update_sync_log_if_advanced(
        state,
        "invitation_registration",
        max_processed_id,
        last_sync_id,
        now,
    )
    .await?;
    tracing::info!(
        last_sync_id,
        max_processed_id,
        synced_count = synced,
        "Invitation registration sync processed"
    );

    Ok(synced)
}

async fn sync_invitation_signup_rewards(state: &AppState, now: i64) -> Result<i64> {
    if !get_config_bool(&state.pool, INVITATION_SIGNUP_REWARD_ENABLED_KEY).await? {
        tracing::info!("Invitation signup reward sync skipped: disabled");
        return Ok(0);
    }

    let inviter_reward_amount = get_config_i64_or_fallback(
        &state.pool,
        INVITATION_SIGNUP_INVITER_REWARD_AMOUNT_KEY,
        INVITATION_SIGNUP_REWARD_AMOUNT_KEY,
    )
    .await?;
    let invitee_reward_amount = get_config_i64_or_fallback(
        &state.pool,
        INVITATION_SIGNUP_INVITEE_REWARD_AMOUNT_KEY,
        INVITATION_SIGNUP_REWARD_AMOUNT_KEY,
    )
    .await?;
    if inviter_reward_amount <= 0 && invitee_reward_amount <= 0 {
        tracing::info!(
            inviter_reward_amount,
            invitee_reward_amount,
            "Invitation signup reward sync skipped: non-positive reward amounts"
        );
        return Ok(0);
    }

    let inviter_unlock_required = get_config_bool(
        &state.pool,
        INVITATION_SIGNUP_INVITER_REWARD_REQUIRES_PAID_ORDER_KEY,
    )
    .await?;
    let invitee_unlock_required = get_config_bool(
        &state.pool,
        INVITATION_SIGNUP_INVITEE_REWARD_REQUIRES_PAID_ORDER_KEY,
    )
    .await?;

    let last_sync_id = last_sync_id(state, "invite_signup").await?;
    let users = sqlx::query(
        r#"
        SELECT
            id,
            inviter_id,
            "group",
            COALESCE(NULLIF(created_at, 0), $2) AS invited_at
        FROM users
        WHERE id > $1
          AND inviter_id IS NOT NULL
          AND inviter_id > 0
        ORDER BY id
        LIMIT 1000
        "#,
    )
    .bind(last_sync_id)
    .bind(now)
    .fetch_all(&state.newapi_pool)
    .await?;

    tracing::info!(
        last_sync_id,
        fetched_count = users.len(),
        "Invitation signup reward sync loaded invited users"
    );

    let invitee_ids = users
        .iter()
        .map(|row| row.try_get::<i64, _>("id"))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let paid_invitee_ids = if inviter_unlock_required || invitee_unlock_required {
        fetch_paid_invitee_ids(&state.newapi_pool, &invitee_ids).await?
    } else {
        Default::default()
    };

    let mut synced = 0;
    let mut max_processed_id = last_sync_id;

    for user in users {
        let invitee_id: i64 = user.try_get("id")?;
        let inviter_id: i64 = user.try_get("inviter_id")?;
        let group: Option<String> = user.try_get("group")?;
        let invited_at: i64 = user.try_get("invited_at")?;
        let user_group = group.as_deref().unwrap_or("default");

        if inviter_reward_amount > 0 {
            synced += insert_invitation_signup_reward_record(
                &state.pool,
                InsertInvitationSignupReward {
                    order_type: INVITATION_SIGNUP_INVITER_ORDER_TYPE,
                    reward_owner_id: inviter_id,
                    invitee_id,
                    user_group,
                    reward_amount: inviter_reward_amount,
                    order_time: invited_at,
                    unlock_required: inviter_unlock_required,
                    unlocked_at: resolve_invitation_signup_reward_unlocked_at(
                        inviter_unlock_required,
                        paid_invitee_ids.contains(&invitee_id),
                        now,
                    ),
                    now,
                },
            )
            .await?
            .inserted_count();
        }

        if invitee_reward_amount > 0 {
            synced += insert_invitation_signup_reward_record(
                &state.pool,
                InsertInvitationSignupReward {
                    order_type: INVITATION_SIGNUP_INVITEE_ORDER_TYPE,
                    reward_owner_id: invitee_id,
                    invitee_id,
                    user_group,
                    reward_amount: invitee_reward_amount,
                    order_time: invited_at,
                    unlock_required: invitee_unlock_required,
                    unlocked_at: resolve_invitation_signup_reward_unlocked_at(
                        invitee_unlock_required,
                        paid_invitee_ids.contains(&invitee_id),
                        now,
                    ),
                    now,
                },
            )
            .await?
            .inserted_count();
        }

        max_processed_id = invitee_id;
    }

    update_sync_log_if_advanced(state, "invite_signup", max_processed_id, last_sync_id, now)
        .await?;
    tracing::info!(
        last_sync_id,
        max_processed_id,
        inserted_count = synced,
        "Invitation signup reward sync processed"
    );
    Ok(synced)
}

async fn sync_invitation_signup_reward_unlocks(state: &AppState, now: i64) -> Result<i64> {
    let updated =
        unlock_paid_invitation_signup_rewards(&state.pool, &state.newapi_pool, now).await? as i64;

    tracing::info!(
        unlocked_count = updated,
        "Invitation signup reward unlock sync processed"
    );

    Ok(updated)
}

async fn sync_topup_orders(state: &AppState, now: i64) -> Result<i64> {
    let cursor = topup_reconciliation_cursor(state).await?;
    let open_case_order_ids = load_open_topup_case_order_ids(state).await?;
    let lookback_start = if cursor.last_time == 0 {
        now.saturating_sub(TOPUP_RECONCILIATION_LOOKBACK_SECONDS)
    } else {
        cursor
            .last_time
            .saturating_sub(TOPUP_RECONCILIATION_LOOKBACK_SECONDS)
    };
    let orders = sqlx::query(
        r#"
        SELECT
            t.id,
            COALESCE(NULLIF(t.complete_time, 0), NULLIF(t.create_time, 0), 0) AS order_time,
            outbox.status AS outbox_status
        FROM top_ups t
        LEFT JOIN kkai_outbox outbox
          ON outbox.event_key = 'newapi:topup:' || t.id::TEXT
         AND outbox.topic = $2
        WHERE t.status = 'success'
          AND t.id >= $4
          AND (
              COALESCE(NULLIF(t.complete_time, 0), NULLIF(t.create_time, 0), 0) >= $1
              OR t.id = ANY($3)
          )
          AND NOT EXISTS (
              SELECT 1
              FROM subscription_orders s
              WHERE s.trade_no = t.trade_no
          )
        ORDER BY order_time, t.id
        "#,
    )
    .bind(lookback_start)
    .bind(TOPUP_COMPLETED_TOPIC)
    .bind(&open_case_order_ids)
    .bind(state.config.topup_rebate_active_from_id)
    .fetch_all(&state.newapi_pool)
    .await?;

    tracing::info!(
        cursor_time = cursor.last_time,
        cursor_id = cursor.last_id,
        lookback_start,
        fetched_count = orders.len(),
        "Topup reconciliation loaded successful new-api orders"
    );

    let event_keys = orders
        .iter()
        .map(|order| {
            order
                .try_get::<i64, _>("id")
                .map(|id| format!("newapi:topup:{id}"))
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let receipt_audits = load_rebate_source_receipt_audits(state, &event_keys).await?;
    let mut created_cases = 0;
    let mut max_processed = cursor;

    for order in &orders {
        let order_id: i64 = order.try_get("id")?;
        let order_time: i64 = order.try_get("order_time")?;
        let outbox_status: Option<String> = order.try_get("outbox_status")?;
        let event_key = format!("newapi:topup:{order_id}");
        let elapsed = now.saturating_sub(order_time);
        let receipt_audit = receipt_audits.get(&event_key).copied();
        let receipt_outcome = receipt_audit.map(|audit| audit.outcome);

        created_cases += reconcile_topup_issue(
            state,
            "topup_outbox_missing",
            &event_key,
            order_id,
            elapsed >= TOPUP_EVENT_DELIVERY_GRACE_SECONDS
                && receipt_outcome.is_none()
                && outbox_status.is_none(),
            now,
        )
        .await?;
        let canonical_missing = receipt_audit.is_some_and(|audit| {
            audit.outcome == RebateSourceReceiptOutcome::Created && !audit.canonical_exists
        });
        created_cases += reconcile_topup_issue(
            state,
            "topup_outbox_stalled",
            &event_key,
            order_id,
            elapsed >= TOPUP_EVENT_STALLED_SECONDS
                && receipt_outcome.is_none()
                && outbox_status.as_deref() == Some("pending"),
            now,
        )
        .await?;
        created_cases += reconcile_topup_issue(
            state,
            "topup_outbox_dead",
            &event_key,
            order_id,
            outbox_status.as_deref() == Some("dead"),
            now,
        )
        .await?;
        created_cases += reconcile_topup_issue(
            state,
            "topup_event_receipt_missing",
            &event_key,
            order_id,
            elapsed >= TOPUP_EVENT_DELIVERY_GRACE_SECONDS && receipt_outcome.is_none(),
            now,
        )
        .await?;
        created_cases += reconcile_topup_issue(
            state,
            "topup_canonical_record_missing",
            &event_key,
            order_id,
            canonical_missing,
            now,
        )
        .await?;
        created_cases += reconcile_topup_issue(
            state,
            "topup_receipt_integrity_drift",
            &event_key,
            order_id,
            receipt_audit.is_some_and(|audit| !audit.integrity_valid) && !canonical_missing,
            now,
        )
        .await?;

        let processed = SyncCursor {
            last_time: order_time,
            last_id: order_id,
        };
        if sync_cursor_advanced(processed, max_processed) {
            max_processed = processed;
        }
    }

    update_topup_reconciliation_checkpoint(state, max_processed, now).await?;
    tracing::info!(
        cursor_time = max_processed.last_time,
        cursor_id = max_processed.last_id,
        created_case_count = created_cases,
        "Topup read-only reconciliation completed"
    );
    Ok(created_cases)
}

async fn load_open_topup_case_order_ids(state: &AppState) -> Result<Vec<i64>> {
    sqlx::query_scalar(
        r#"
        SELECT DISTINCT source_order_id
        FROM rebate_reconciliation_cases
        WHERE case_state IN ('open', 'approved')
          AND issue_code LIKE 'topup\_%' ESCAPE '\'
          AND source_event_key IS NOT NULL
          AND source_order_id IS NOT NULL
        ORDER BY source_order_id
        "#,
    )
    .fetch_all(&state.pool)
    .await
    .map_err(AppError::from)
}

async fn load_rebate_source_receipt_audits(
    state: &AppState,
    event_keys: &[String],
) -> Result<HashMap<String, RebateSourceReceiptAudit>> {
    if event_keys.is_empty() {
        return Ok(HashMap::new());
    }
    let rows = sqlx::query(
        r#"
        SELECT receipt.event_key, receipt.outcome,
               canonical.id IS NOT NULL AS canonical_exists,
               CASE receipt.outcome
                   WHEN 'created' THEN
                       receipt.event_type = 'topup.completed'
                       AND receipt.source_schema_version IN (1, 2)
                       AND receipt.event_key = 'newapi:topup:' || receipt.source_order_id::TEXT
                       AND linked.id = receipt.rebate_record_id
                       AND canonical.id = linked.id
                       AND linked.order_type = 'topup'
                       AND linked.order_id = receipt.source_order_id
                       AND linked.canonical_event_key = receipt.event_key
                       AND linked.source_payload_sha256 = receipt.source_payload_sha256
                       AND linked.source_schema_version = receipt.source_schema_version
                       AND snapshot.rebate_record_id = linked.id
                       AND snapshot.inviter_group = linked.user_group
                       AND snapshot.rule_type = 'topup'
                       AND snapshot.order_amount_minor = COALESCE(linked.order_amount_cents, 0)
                       AND snapshot.credited_quota = linked.credited_quota
                       AND linked.credited_quota IS NOT NULL
                       AND snapshot.rebate_quota = linked.rebate_amount
                       AND snapshot.rebate_quota =
                           ROUND(snapshot.credited_quota::NUMERIC * snapshot.rebate_rate)::BIGINT
                       AND snapshot.quota_per_unit > 0
                       AND snapshot.quota_per_unit % 100 = 0
                       AND snapshot.source_revision = CASE receipt.source_schema_version
                           WHEN 1 THEN 'rebate-source-event:v1'
                           WHEN 2 THEN 'rebate-source-event:v2'
                       END
                       AND (
                           linked.rebate_rate = snapshot.rebate_rate
                           OR (linked.rebate_rate IS NULL AND snapshot.rebate_rate = 0)
                       )
                   WHEN 'ignored_no_inviter' THEN
                       receipt.event_type = 'topup.completed'
                       AND receipt.source_schema_version IN (1, 2)
                       AND receipt.event_key = 'newapi:topup:' || receipt.source_order_id::TEXT
                       AND receipt.rebate_record_id IS NULL
                       AND canonical.id IS NULL
                   ELSE FALSE
               END AS integrity_valid
        FROM rebate_source_event_receipts receipt
        LEFT JOIN rebate_records linked ON linked.id = receipt.rebate_record_id
        LEFT JOIN rebate_records canonical
          ON canonical.canonical_event_key = receipt.event_key
        LEFT JOIN rebate_calculation_snapshots snapshot
          ON snapshot.rebate_record_id = linked.id
        WHERE receipt.event_key::TEXT = ANY($1::TEXT[])
        "#,
    )
    .bind(event_keys)
    .fetch_all(&state.pool)
    .await?;
    let mut outcomes = HashMap::with_capacity(rows.len());
    for row in rows {
        let event_key: String = row.try_get("event_key")?;
        let outcome = match row.try_get::<String, _>("outcome")?.as_str() {
            "created" => RebateSourceReceiptOutcome::Created,
            "ignored_no_inviter" => RebateSourceReceiptOutcome::IgnoredNoInviter,
            value => {
                return Err(AppError::Internal(format!(
                    "返利源事件 receipt outcome 无效: {value}"
                )))
            }
        };
        outcomes.insert(
            event_key,
            RebateSourceReceiptAudit {
                outcome,
                canonical_exists: row.try_get("canonical_exists")?,
                integrity_valid: row.try_get("integrity_valid")?,
            },
        );
    }
    Ok(outcomes)
}

async fn reconcile_topup_issue(
    state: &AppState,
    issue_code: &str,
    event_key: &str,
    source_order_id: i64,
    issue_present: bool,
    now: i64,
) -> Result<i64> {
    let evidence_sha256 = hex::encode(Sha256::digest(
        format!("topup-reconciliation:v1:{issue_code}:{event_key}").as_bytes(),
    ));
    if !issue_present {
        sqlx::query(
            r#"
            UPDATE rebate_reconciliation_cases
            SET case_state = 'resolved', resolved_at = $3, updated_at = $3
            WHERE issue_code = $1 AND evidence_sha256 = $2
              AND case_state IN ('open', 'approved')
            "#,
        )
        .bind(issue_code)
        .bind(&evidence_sha256)
        .bind(now)
        .execute(&state.pool)
        .await?;
        return Ok(0);
    }

    let inserted = sqlx::query(
        r#"
        INSERT INTO rebate_reconciliation_cases (
            issue_code, case_state, evidence_sha256,
            source_event_key, source_order_id, created_at, updated_at
        )
        SELECT $1, 'open', $2, $3, $4, $5, $5
        WHERE NOT EXISTS (
            SELECT 1
            FROM rebate_reconciliation_cases
            WHERE issue_code = $1 AND evidence_sha256 = $2
              AND case_state IN ('open', 'approved')
        )
        "#,
    )
    .bind(issue_code)
    .bind(evidence_sha256)
    .bind(event_key)
    .bind(source_order_id)
    .bind(now)
    .execute(&state.pool)
    .await?
    .rows_affected();
    Ok(inserted as i64)
}

async fn topup_reconciliation_cursor(state: &AppState) -> Result<SyncCursor> {
    let row = sqlx::query(
        r#"
        SELECT cursor_time, cursor_id
        FROM rebate_sync_checkpoints
        WHERE sync_type = 'topup' AND source_namespace = $1
        "#,
    )
    .bind(TOPUP_RECONCILIATION_SOURCE)
    .fetch_optional(&state.pool)
    .await?;
    Ok(row.map_or(
        SyncCursor {
            last_id: 0,
            last_time: 0,
        },
        |row| SyncCursor {
            last_id: row.get("cursor_id"),
            last_time: row.get("cursor_time"),
        },
    ))
}

async fn update_topup_reconciliation_checkpoint(
    state: &AppState,
    cursor: SyncCursor,
    now: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO rebate_sync_checkpoints (
            sync_type, source_namespace, cursor_time, cursor_id, lookback_seconds, updated_at
        ) VALUES ('topup', $1, $2, $3, $4, $5)
        ON CONFLICT (sync_type, source_namespace) DO UPDATE
        SET cursor_time = GREATEST(rebate_sync_checkpoints.cursor_time, EXCLUDED.cursor_time),
            cursor_id = CASE
                WHEN EXCLUDED.cursor_time > rebate_sync_checkpoints.cursor_time
                    THEN EXCLUDED.cursor_id
                WHEN EXCLUDED.cursor_time = rebate_sync_checkpoints.cursor_time
                    THEN GREATEST(rebate_sync_checkpoints.cursor_id, EXCLUDED.cursor_id)
                ELSE rebate_sync_checkpoints.cursor_id
            END,
            lookback_seconds = EXCLUDED.lookback_seconds,
            updated_at = EXCLUDED.updated_at
        "#,
    )
    .bind(TOPUP_RECONCILIATION_SOURCE)
    .bind(cursor.last_time)
    .bind(cursor.last_id)
    .bind(TOPUP_RECONCILIATION_LOOKBACK_SECONDS)
    .bind(now)
    .execute(&state.pool)
    .await?;
    Ok(())
}

async fn sync_subscription_orders(state: &AppState, now: i64) -> Result<i64> {
    let last_sync_id = last_sync_id(state, "subscription").await?;
    let orders = sqlx::query(
        r#"
        SELECT
            s.id,
            s.user_id,
            s.money,
            COALESCE(NULLIF(s.complete_time, 0), NULLIF(s.create_time, 0), 0) AS order_time,
            u.inviter_id,
            u."group"
        FROM subscription_orders s
        JOIN users u ON s.user_id = u.id
        WHERE s.id > $1
          AND s.status = 'success'
          AND u.inviter_id IS NOT NULL
          AND u.inviter_id > 0
        ORDER BY s.id
        LIMIT 1000
        "#,
    )
    .bind(last_sync_id)
    .fetch_all(&state.newapi_pool)
    .await?;

    tracing::info!(
        last_sync_id,
        fetched_count = orders.len(),
        "Subscription rebate sync loaded new-api orders"
    );

    let mut synced = 0;
    let mut max_processed_id = last_sync_id;

    for order in orders {
        let order_id: i64 = order.try_get("id")?;
        let user_id: i64 = order.try_get("user_id")?;
        let money: f64 = order.try_get("money")?;
        let order_time: i64 = order.try_get("order_time")?;
        let inviter_id: i64 = order.try_get("inviter_id")?;
        let group: Option<String> = order.try_get("group")?;

        match insert_rebate_record(
            state,
            InsertRebateRecord {
                order_type: SUBSCRIPTION_ORDER_TYPE,
                order_id,
                user_id,
                inviter_id,
                money,
                order_time,
                group: group.as_deref().unwrap_or("default"),
                now,
            },
        )
        .await?
        {
            InsertOutcome::Inserted => {
                synced += 1;
                max_processed_id = order_id;
            }
            InsertOutcome::Duplicate => {
                max_processed_id = order_id;
            }
        }
    }

    update_sync_log_if_advanced(state, "subscription", max_processed_id, last_sync_id, now).await?;
    tracing::info!(
        last_sync_id,
        max_processed_id,
        inserted_count = synced,
        "Subscription rebate sync processed"
    );
    Ok(synced)
}

async fn sync_redemption_orders(state: &AppState, now: i64) -> Result<i64> {
    let cursor = last_sync_cursor(state, REDEMPTION_ORDER_TYPE).await?;
    let orders = sqlx::query(
        r#"
        SELECT
            r.id::BIGINT AS id,
            r.used_user_id::BIGINT AS user_id,
            r.quota::BIGINT AS quota,
            r.redeemed_time AS order_time,
            u.inviter_id,
            u."group"
        FROM redemptions r
        JOIN users u ON r.used_user_id = u.id
        WHERE r.status = 3
          AND r.used_user_id > 0
          AND r.redeemed_time > 0
          AND (
              r.redeemed_time > $1
              OR (r.redeemed_time = $1 AND r.id > $2)
          )
          AND u.inviter_id IS NOT NULL
          AND u.inviter_id > 0
        ORDER BY r.redeemed_time, r.id
        LIMIT 1000
        "#,
    )
    .bind(cursor.last_time)
    .bind(cursor.last_id)
    .fetch_all(&state.newapi_pool)
    .await?;

    tracing::info!(
        last_sync_id = cursor.last_id,
        last_sync_time = cursor.last_time,
        fetched_count = orders.len(),
        "Redemption rebate sync loaded new-api orders"
    );

    let mut synced = 0;
    let mut max_processed = cursor;

    for order in orders {
        let order_id: i64 = order.try_get("id")?;
        let user_id: i64 = order.try_get("user_id")?;
        let quota: i64 = order.try_get("quota")?;
        let order_time: i64 = order.try_get("order_time")?;
        let inviter_id: i64 = order.try_get("inviter_id")?;
        let group: Option<String> = order.try_get("group")?;
        let money = quota_to_yuan(quota, state.config.quota_per_unit);

        match insert_rebate_record(
            state,
            InsertRebateRecord {
                order_type: REDEMPTION_ORDER_TYPE,
                order_id,
                user_id,
                inviter_id,
                money,
                order_time,
                group: group.as_deref().unwrap_or("default"),
                now,
            },
        )
        .await?
        {
            InsertOutcome::Inserted => {
                synced += 1;
                max_processed = SyncCursor {
                    last_id: order_id,
                    last_time: order_time,
                };
            }
            InsertOutcome::Duplicate => {
                max_processed = SyncCursor {
                    last_id: order_id,
                    last_time: order_time,
                };
            }
        }
    }

    update_sync_cursor_if_advanced(state, REDEMPTION_ORDER_TYPE, max_processed, cursor, now)
        .await?;
    tracing::info!(
        last_sync_id = cursor.last_id,
        last_sync_time = cursor.last_time,
        max_processed_id = max_processed.last_id,
        max_processed_time = max_processed.last_time,
        inserted_count = synced,
        "Redemption rebate sync processed"
    );
    Ok(synced)
}

#[derive(Debug, PartialEq, Eq)]
enum InsertOutcome {
    Inserted,
    Duplicate,
}

#[derive(Clone, Copy)]
struct SyncCursor {
    last_id: i64,
    last_time: i64,
}

struct InsertRebateRecord<'a> {
    order_type: &'a str,
    order_id: i64,
    user_id: i64,
    inviter_id: i64,
    money: f64,
    order_time: i64,
    group: &'a str,
    now: i64,
}

async fn insert_rebate_record(
    state: &AppState,
    params: InsertRebateRecord<'_>,
) -> Result<InsertOutcome> {
    let ulid = ulid::Ulid::new().to_string();
    let order_time = if params.order_time > 0 {
        params.order_time
    } else {
        params.now
    };
    let scan_started_at = params.now;
    let initialization_ends_at = scan_started_at + INITIALIZATION_SECONDS;
    let rebate_effective_at = order_time + REBATE_EFFECTIVE_DELAY_SECONDS;

    let result = sqlx::query(
        r#"
        INSERT INTO rebate_records
        (
            ulid, inviter_id, invitee_id, user_group, order_type, order_id,
            order_amount, order_time, rebate_effective_at, rebate_amount, rebate_rate,
            status, scan_started_at, initialization_ends_at, created_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 0, NULL, 'initializing', $10, $11, $10, $10)
        ON CONFLICT (order_type, order_id) DO NOTHING
        "#,
    )
    .bind(&ulid)
    .bind(params.inviter_id)
    .bind(params.user_id)
    .bind(params.group)
    .bind(params.order_type)
    .bind(params.order_id)
    .bind(params.money)
    .bind(order_time)
    .bind(rebate_effective_at)
    .bind(scan_started_at)
    .bind(initialization_ends_at)
    .execute(&state.pool)
    .await?;

    if result.rows_affected() == 0 {
        Ok(InsertOutcome::Duplicate)
    } else {
        Ok(InsertOutcome::Inserted)
    }
}

fn quota_to_yuan(quota: i64, quota_per_unit: i64) -> f64 {
    quota as f64 / quota_per_unit as f64
}

async fn last_sync_id(state: &AppState, sync_type: &str) -> Result<i64> {
    sqlx::query_scalar("SELECT COALESCE(MAX(last_sync_id), 0) FROM sync_log WHERE sync_type = $1")
        .bind(sync_type)
        .fetch_one(&state.pool)
        .await
        .map_err(AppError::from)
}

async fn last_sync_cursor(state: &AppState, sync_type: &str) -> Result<SyncCursor> {
    let row = sqlx::query(
        r#"
        SELECT COALESCE(last_sync_id, 0) AS last_sync_id,
               COALESCE(last_sync_time, 0) AS last_sync_time
        FROM sync_log
        WHERE sync_type = $1
        ORDER BY last_sync_time DESC, last_sync_id DESC
        LIMIT 1
        "#,
    )
    .bind(sync_type)
    .fetch_optional(&state.pool)
    .await?;

    Ok(match row {
        Some(row) => SyncCursor {
            last_id: row.try_get("last_sync_id")?,
            last_time: row.try_get("last_sync_time")?,
        },
        None => SyncCursor {
            last_id: 0,
            last_time: 0,
        },
    })
}

async fn update_sync_log_if_advanced(
    state: &AppState,
    sync_type: &str,
    max_processed_id: i64,
    last_sync_id: i64,
    now: i64,
) -> Result<()> {
    if max_processed_id <= last_sync_id {
        return Ok(());
    }

    sqlx::query(
        r#"
        INSERT INTO sync_log (sync_type, last_sync_id, last_sync_time, created_at)
        VALUES ($1, $2, $3, $3)
        "#,
    )
    .bind(sync_type)
    .bind(max_processed_id)
    .bind(now)
    .execute(&state.pool)
    .await?;

    Ok(())
}

async fn update_sync_cursor_if_advanced(
    state: &AppState,
    sync_type: &str,
    max_processed: SyncCursor,
    previous: SyncCursor,
    now: i64,
) -> Result<()> {
    if !sync_cursor_advanced(max_processed, previous) {
        return Ok(());
    }

    sqlx::query(
        r#"
        INSERT INTO sync_log (sync_type, last_sync_id, last_sync_time, created_at)
        VALUES ($1, $2, $3, $4)
        "#,
    )
    .bind(sync_type)
    .bind(max_processed.last_id)
    .bind(max_processed.last_time)
    .bind(now)
    .execute(&state.pool)
    .await?;

    Ok(())
}

fn sync_cursor_advanced(next: SyncCursor, previous: SyncCursor) -> bool {
    next.last_time > previous.last_time
        || (next.last_time == previous.last_time && next.last_id > previous.last_id)
}

fn authorize_sync(state: &AppState, headers: &HeaderMap, query_secret: Option<&str>) -> Result<()> {
    let expected = state
        .config
        .sync_secret
        .as_deref()
        .ok_or_else(|| AppError::Forbidden)?;

    let provided = headers
        .get("x-sync-secret")
        .and_then(|value| value.to_str().ok())
        .or_else(|| bearer_secret(headers))
        .or(query_secret);

    if provided == Some(expected) {
        Ok(())
    } else {
        Err(AppError::Forbidden)
    }
}

fn bearer_secret(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get("authorization")?.to_str().ok()?;
    value.strip_prefix("Bearer ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bearer_secret_extracts_token() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer sync-secret".parse().unwrap());

        assert_eq!(bearer_secret(&headers), Some("sync-secret"));
    }

    #[test]
    fn insert_outcome_distinguishes_duplicate() {
        assert_eq!(InsertOutcome::Duplicate, InsertOutcome::Duplicate);
    }

    #[test]
    fn skipped_sync_response_has_zero_counts() {
        let response = SyncResponse::skipped();

        assert!(response.skipped);
        assert_eq!(response.finalized, 0);
        assert_eq!(response.invitation_registrations_synced, 0);
        assert_eq!(response.invitation_signup_synced, 0);
        assert_eq!(response.invitation_signup_unlocked, 0);
        assert_eq!(response.topup_synced, 0);
        assert_eq!(response.subscription_synced, 0);
        assert_eq!(response.redemption_synced, 0);
    }

    #[test]
    fn converts_redemption_quota_to_yuan() {
        assert_eq!(quota_to_yuan(2_500_000, 500_000), 5.0);
        assert_eq!(quota_to_yuan(500_000, 500_000), 1.0);
    }

    #[test]
    fn compares_sync_cursors_by_time_then_id() {
        let previous = SyncCursor {
            last_id: 10,
            last_time: 100,
        };
        assert!(sync_cursor_advanced(
            SyncCursor {
                last_id: 1,
                last_time: 101,
            },
            previous,
        ));
        assert!(sync_cursor_advanced(
            SyncCursor {
                last_id: 11,
                last_time: 100,
            },
            previous,
        ));
        assert!(!sync_cursor_advanced(
            SyncCursor {
                last_id: 10,
                last_time: 100,
            },
            previous,
        ));
        assert!(!sync_cursor_advanced(
            SyncCursor {
                last_id: 99,
                last_time: 99,
            },
            previous,
        ));
    }
}
