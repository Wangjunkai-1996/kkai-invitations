use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::Json;
use serde::{Deserialize, Serialize};
use sqlx::Row;

use crate::error::{AppError, Result};
use crate::response::{message, ApiResponse};
use crate::AppState;

const QUOTA_UNITS_PER_YUAN: f64 = 500_000.0;

// POST /api/system/sync-rebates
pub async fn sync_rebates(
    State(state): State<AppState>,
    Query(params): Query<SyncParams>,
    headers: HeaderMap,
) -> Result<Json<ApiResponse<SyncResponse>>> {
    authorize_sync(&state, &headers, params.secret.as_deref())?;

    let now = chrono::Utc::now().timestamp();
    tracing::info!("Starting rebate sync");

    let topup_synced = sync_topup_orders(&state, now).await?;
    tracing::info!(synced_count = topup_synced, "Topup orders synced");

    let subscription_synced = sync_subscription_orders(&state, now).await?;
    tracing::info!(
        synced_count = subscription_synced,
        "Subscription orders synced"
    );

    Ok(message(
        SyncResponse {
            topup_synced,
            subscription_synced,
        },
        "同步完成",
    ))
}

#[derive(Deserialize)]
pub struct SyncParams {
    pub secret: Option<String>,
}

#[derive(Serialize)]
pub struct SyncResponse {
    pub topup_synced: i64,
    pub subscription_synced: i64,
}

async fn sync_topup_orders(state: &AppState, now: i64) -> Result<i64> {
    let last_sync_id = last_sync_id(state, "topup").await?;
    let orders = sqlx::query(
        r#"
        SELECT
            t.id,
            t.user_id,
            t.money,
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

    let mut synced = 0;
    let mut max_processed_id = last_sync_id;

    for order in orders {
        let order_id: i64 = order.try_get("id")?;
        let user_id: i64 = order.try_get("user_id")?;
        let money: f64 = order.try_get("money")?;
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
            InsertOutcome::MissingRule => {
                tracing::warn!(
                    order_type = "topup",
                    order_id,
                    group = group.as_deref().unwrap_or("default"),
                    "Stopping topup sync before checkpoint because rebate rule is missing"
                );
                break;
            }
        }
    }

    update_sync_log_if_advanced(state, "topup", max_processed_id, last_sync_id, now).await?;
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

    let mut synced = 0;
    let mut max_processed_id = last_sync_id;

    for order in orders {
        let order_id: i64 = order.try_get("id")?;
        let user_id: i64 = order.try_get("user_id")?;
        let money: f64 = order.try_get("money")?;
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
            InsertOutcome::MissingRule => {
                tracing::warn!(
                    order_type = "subscription",
                    order_id,
                    group = group.as_deref().unwrap_or("default"),
                    "Stopping subscription sync before checkpoint because rebate rule is missing"
                );
                break;
            }
        }
    }

    update_sync_log_if_advanced(state, "subscription", max_processed_id, last_sync_id, now).await?;
    Ok(synced)
}

#[derive(Debug, PartialEq, Eq)]
enum InsertOutcome {
    Inserted,
    Duplicate,
    MissingRule,
}

struct InsertRebateRecord<'a> {
    order_type: &'a str,
    order_id: i64,
    user_id: i64,
    inviter_id: i64,
    money: f64,
    group: &'a str,
    now: i64,
}

async fn insert_rebate_record(
    state: &AppState,
    params: InsertRebateRecord<'_>,
) -> Result<InsertOutcome> {
    let rule = sqlx::query(
        "SELECT rebate_rate FROM rebate_rules WHERE user_group = $1 AND rule_type = $2",
    )
    .bind(params.group)
    .bind(params.order_type)
    .fetch_optional(&state.pool)
    .await?;

    let Some(rule_row) = rule else {
        return Ok(InsertOutcome::MissingRule);
    };

    let rebate_rate: f64 = rule_row.try_get("rebate_rate")?;
    let rebate_amount = (params.money * rebate_rate * QUOTA_UNITS_PER_YUAN).round() as i64;
    let ulid = ulid::Ulid::new().to_string();

    let result = sqlx::query(
        r#"
        INSERT INTO rebate_records
        (ulid, inviter_id, invitee_id, order_type, order_id, order_amount, rebate_amount, rebate_rate, status, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending', $9, $9)
        ON CONFLICT (order_type, order_id) DO NOTHING
        "#,
    )
    .bind(&ulid)
    .bind(params.inviter_id)
    .bind(params.user_id)
    .bind(params.order_type)
    .bind(params.order_id)
    .bind(params.money)
    .bind(rebate_amount)
    .bind(rebate_rate)
    .bind(params.now)
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
}
