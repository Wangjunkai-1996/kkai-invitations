use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::Json;
use serde::{Deserialize, Serialize};
use sqlx::Row;

use crate::error::{AppError, Result};
use crate::invitation_signup::{
    fetch_paid_invitee_ids, insert_invitation_signup_reward_record,
    resolve_invitation_signup_reward_unlocked_at, unlock_paid_invitation_signup_rewards,
    upsert_invitation_registration, InsertInvitationSignupReward, UpsertInvitationRegistration,
};
use crate::rebate_lifecycle::{
    finalize_ready_initializing_records, INITIALIZATION_SECONDS, REBATE_EFFECTIVE_DELAY_SECONDS,
};
use crate::rebate_types::{
    INVITATION_SIGNUP_INVITEE_ORDER_TYPE, INVITATION_SIGNUP_INVITER_ORDER_TYPE,
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
    let mut lock_conn = state.pool.acquire().await?;
    let lock_acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(REBATE_SYNC_ADVISORY_LOCK_ID)
        .fetch_one(&mut *lock_conn)
        .await?;

    if !lock_acquired {
        tracing::info!("Rebate sync skipped: another sync is already running");
        return Ok(SyncResponse::skipped());
    }

    let result = run_rebate_sync_unlocked(state).await;
    let unlock_result: std::result::Result<bool, sqlx::Error> =
        sqlx::query_scalar("SELECT pg_advisory_unlock($1)")
            .bind(REBATE_SYNC_ADVISORY_LOCK_ID)
            .fetch_one(&mut *lock_conn)
            .await;

    match (result, unlock_result) {
        (Ok(response), Ok(true)) => Ok(response),
        (Ok(response), Ok(false)) => {
            tracing::warn!("Rebate sync advisory lock was not held while unlocking");
            Ok(response)
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
    let (topup_synced, subscription_synced) = if order_rebate_enabled {
        let topup_synced = sync_topup_orders(state, now).await?;
        tracing::info!(synced_count = topup_synced, "Topup orders synced");

        let subscription_synced = sync_subscription_orders(state, now).await?;
        tracing::info!(
            synced_count = subscription_synced,
            "Subscription orders synced"
        );

        (topup_synced, subscription_synced)
    } else {
        tracing::info!("Order rebate sync skipped: disabled");
        (0, 0)
    };

    Ok(SyncResponse {
        skipped: false,
        finalized,
        invitation_registrations_synced,
        invitation_signup_synced,
        invitation_signup_unlocked,
        topup_synced,
        subscription_synced,
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
    let last_sync_id = last_sync_id(state, "topup").await?;
    let orders = sqlx::query(
        r#"
        SELECT
            t.id,
            t.user_id,
            t.money,
            COALESCE(NULLIF(t.complete_time, 0), NULLIF(t.create_time, 0), 0) AS order_time,
            u.inviter_id,
            u."group"
        FROM top_ups t
        JOIN users u ON t.user_id = u.id
        WHERE t.id > $1
          AND t.status = 'success'
          AND u.inviter_id IS NOT NULL
          AND u.inviter_id > 0
          AND NOT EXISTS (
              SELECT 1
              FROM subscription_orders s
              WHERE s.trade_no = t.trade_no
          )
        ORDER BY t.id
        LIMIT 1000
        "#,
    )
    .bind(last_sync_id)
    .fetch_all(&state.newapi_pool)
    .await?;

    tracing::info!(
        last_sync_id,
        fetched_count = orders.len(),
        "Topup rebate sync loaded new-api orders"
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
                order_type: "topup",
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

    update_sync_log_if_advanced(state, "topup", max_processed_id, last_sync_id, now).await?;
    tracing::info!(
        last_sync_id,
        max_processed_id,
        inserted_count = synced,
        "Topup rebate sync processed"
    );
    Ok(synced)
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
                order_type: "subscription",
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

#[derive(Debug, PartialEq, Eq)]
enum InsertOutcome {
    Inserted,
    Duplicate,
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

async fn last_sync_id(state: &AppState, sync_type: &str) -> Result<i64> {
    sqlx::query_scalar("SELECT COALESCE(MAX(last_sync_id), 0) FROM sync_log WHERE sync_type = $1")
        .bind(sync_type)
        .fetch_one(&state.pool)
        .await
        .map_err(AppError::from)
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
    }
}
