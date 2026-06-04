use std::collections::HashSet;

use sqlx::{PgPool, Row};

use crate::error::{AppError, Result};
use crate::rebate_types::{
    INVITATION_SIGNUP_INVITEE_ORDER_TYPE, INVITATION_SIGNUP_INVITER_ORDER_TYPE,
};

#[derive(Debug, PartialEq, Eq)]
pub enum RewardInsertOutcome {
    Inserted { record_id: i64 },
    Duplicate { record_id: Option<i64> },
}

impl RewardInsertOutcome {
    pub fn inserted_count(&self) -> i64 {
        match self {
            Self::Inserted { .. } => 1,
            Self::Duplicate { .. } => 0,
        }
    }

    pub fn record_id(&self) -> Option<i64> {
        match self {
            Self::Inserted { record_id } => Some(*record_id),
            Self::Duplicate { record_id } => *record_id,
        }
    }
}

pub struct InsertInvitationSignupReward<'a> {
    pub order_type: &'a str,
    pub reward_owner_id: i64,
    pub invitee_id: i64,
    pub user_group: &'a str,
    pub reward_amount: i64,
    pub order_time: i64,
    pub unlock_required: bool,
    pub unlocked_at: Option<i64>,
    pub now: i64,
}

pub async fn insert_invitation_signup_reward_record(
    pool: &PgPool,
    params: InsertInvitationSignupReward<'_>,
) -> Result<RewardInsertOutcome> {
    let ulid = ulid::Ulid::new().to_string();
    let order_time = resolve_invitation_signup_reward_order_time(params.order_time, params.now);
    let unlocked_at = if params.unlock_required {
        params.unlocked_at
    } else {
        Some(params.now)
    };

    let inserted_id: Option<i64> = sqlx::query_scalar(
        r#"
        INSERT INTO rebate_records
        (
            ulid, inviter_id, invitee_id, user_group, order_type, order_id,
            order_amount, order_time, rebate_effective_at, rebate_amount, rebate_rate,
            status, scan_started_at, initialization_ends_at, finalized_at, unlock_required,
            unlocked_at, created_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $3, 0, $6, $6, $8, NULL, 'pending', $7, $7, $7, $9, $10, $7, $7)
        ON CONFLICT (order_type, order_id) DO NOTHING
        RETURNING id
        "#,
    )
    .bind(&ulid)
    .bind(params.reward_owner_id)
    .bind(params.invitee_id)
    .bind(params.user_group)
    .bind(params.order_type)
    .bind(order_time)
    .bind(params.now)
    .bind(params.reward_amount)
    .bind(params.unlock_required)
    .bind(unlocked_at)
    .fetch_optional(pool)
    .await?;

    if let Some(record_id) = inserted_id {
        return Ok(RewardInsertOutcome::Inserted { record_id });
    }

    let record_id: Option<i64> =
        sqlx::query_scalar("SELECT id FROM rebate_records WHERE order_type = $1 AND order_id = $2")
            .bind(params.order_type)
            .bind(params.invitee_id)
            .fetch_optional(pool)
            .await?;

    Ok(RewardInsertOutcome::Duplicate { record_id })
}

pub fn resolve_invitation_signup_reward_order_time(order_time: i64, now: i64) -> i64 {
    if order_time > 0 {
        order_time
    } else {
        now
    }
}

pub fn resolve_invitation_signup_reward_unlocked_at(
    unlock_required: bool,
    invitee_has_paid_order: bool,
    now: i64,
) -> Option<i64> {
    if !unlock_required || invitee_has_paid_order {
        Some(now)
    } else {
        None
    }
}

pub async fn fetch_paid_invitee_ids(
    newapi_pool: &PgPool,
    invitee_ids: &[i64],
) -> Result<HashSet<i64>> {
    if invitee_ids.is_empty() {
        return Ok(HashSet::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT DISTINCT user_id
        FROM (
            SELECT user_id
            FROM top_ups
            WHERE status = 'success'
              AND user_id = ANY($1)
            UNION
            SELECT user_id
            FROM subscription_orders
            WHERE status = 'success'
              AND user_id = ANY($1)
            UNION
            SELECT user_id
            FROM user_subscriptions
            WHERE status IN ('active', 'expired')
              AND user_id = ANY($1)
        ) paid_users
        ORDER BY user_id
        "#,
    )
    .bind(invitee_ids)
    .fetch_all(newapi_pool)
    .await?;

    rows.iter()
        .map(|row| row.try_get::<i64, _>("user_id"))
        .collect::<std::result::Result<HashSet<_>, _>>()
        .map_err(AppError::from)
}

pub async fn unlock_paid_invitation_signup_rewards(
    pool: &PgPool,
    newapi_pool: &PgPool,
    now: i64,
) -> Result<u64> {
    repair_invitation_signup_reward_order_times(pool, now).await?;

    let locked_rows = sqlx::query(
        r#"
        SELECT DISTINCT invitee_id
        FROM rebate_records
        WHERE order_type IN ($1, $2)
          AND unlock_required = TRUE
          AND unlocked_at IS NULL
        ORDER BY invitee_id
        "#,
    )
    .bind(INVITATION_SIGNUP_INVITER_ORDER_TYPE)
    .bind(INVITATION_SIGNUP_INVITEE_ORDER_TYPE)
    .fetch_all(pool)
    .await?;

    let invitee_ids = locked_rows
        .iter()
        .map(|row| row.try_get::<i64, _>("invitee_id"))
        .collect::<std::result::Result<Vec<_>, _>>()?;

    if invitee_ids.is_empty() {
        return Ok(0);
    }

    let paid_user_ids = fetch_paid_invitee_ids(newapi_pool, &invitee_ids).await?;
    if paid_user_ids.is_empty() {
        return Ok(0);
    }

    let paid_invitee_ids = invitee_ids
        .into_iter()
        .filter(|invitee_id| paid_user_ids.contains(invitee_id))
        .collect::<Vec<_>>();

    let updated = sqlx::query(
        r#"
        UPDATE rebate_records
        SET unlocked_at = $1, updated_at = $1
        WHERE order_type IN ($2, $3)
          AND unlock_required = TRUE
          AND unlocked_at IS NULL
          AND invitee_id = ANY($4)
        "#,
    )
    .bind(now)
    .bind(INVITATION_SIGNUP_INVITER_ORDER_TYPE)
    .bind(INVITATION_SIGNUP_INVITEE_ORDER_TYPE)
    .bind(&paid_invitee_ids)
    .execute(pool)
    .await?
    .rows_affected();

    Ok(updated)
}

async fn repair_invitation_signup_reward_order_times(pool: &PgPool, now: i64) -> Result<u64> {
    sqlx::query(
        r#"
        UPDATE rebate_records rr
        SET order_time = ir.invited_at,
            updated_at = $1
        FROM invitation_registrations ir
        WHERE rr.invitee_id = ir.invitee_id
          AND rr.order_type IN ($2, $3)
          AND ir.invited_at > 0
          AND rr.order_time <> ir.invited_at
        "#,
    )
    .bind(now)
    .bind(INVITATION_SIGNUP_INVITER_ORDER_TYPE)
    .bind(INVITATION_SIGNUP_INVITEE_ORDER_TYPE)
    .execute(pool)
    .await
    .map(|result| result.rows_affected())
    .map_err(AppError::from)
}

pub struct UpsertInvitationRegistration<'a> {
    pub inviter_id: i64,
    pub invitee_id: i64,
    pub user_group: &'a str,
    pub invited_at: i64,
    pub now: i64,
}

pub async fn upsert_invitation_registration(
    pool: &PgPool,
    params: UpsertInvitationRegistration<'_>,
) -> Result<i64> {
    sqlx::query(
        r#"
        INSERT INTO invitation_registrations
            (inviter_id, invitee_id, user_group, invited_at, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $5)
        ON CONFLICT (invitee_id)
        DO UPDATE SET
            inviter_id = EXCLUDED.inviter_id,
            user_group = EXCLUDED.user_group,
            invited_at = EXCLUDED.invited_at,
            updated_at = EXCLUDED.updated_at
        "#,
    )
    .bind(params.inviter_id)
    .bind(params.invitee_id)
    .bind(params.user_group)
    .bind(params.invited_at)
    .bind(params.now)
    .execute(pool)
    .await?;

    sqlx::query("SELECT id FROM invitation_registrations WHERE invitee_id = $1")
        .bind(params.invitee_id)
        .fetch_one(pool)
        .await?
        .try_get("id")
        .map_err(AppError::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reward_insert_outcome_reports_insert_count() {
        assert_eq!(
            RewardInsertOutcome::Inserted { record_id: 1 }.inserted_count(),
            1
        );
        assert_eq!(
            RewardInsertOutcome::Duplicate { record_id: Some(1) }.inserted_count(),
            0
        );
    }

    #[test]
    fn reward_insert_outcome_exposes_record_id() {
        assert_eq!(
            RewardInsertOutcome::Inserted { record_id: 7 }.record_id(),
            Some(7)
        );
        assert_eq!(
            RewardInsertOutcome::Duplicate { record_id: Some(8) }.record_id(),
            Some(8)
        );
        assert_eq!(
            RewardInsertOutcome::Duplicate { record_id: None }.record_id(),
            None
        );
    }

    #[test]
    fn resolves_signup_reward_unlock_timestamp() {
        assert_eq!(
            resolve_invitation_signup_reward_unlocked_at(false, false, 1_000),
            Some(1_000)
        );
        assert_eq!(
            resolve_invitation_signup_reward_unlocked_at(true, false, 1_000),
            None
        );
        assert_eq!(
            resolve_invitation_signup_reward_unlocked_at(true, true, 1_000),
            Some(1_000)
        );
    }

    #[test]
    fn resolves_signup_reward_order_timestamp() {
        assert_eq!(resolve_invitation_signup_reward_order_time(900, 1_000), 900);
        assert_eq!(resolve_invitation_signup_reward_order_time(0, 1_000), 1_000);
        assert_eq!(
            resolve_invitation_signup_reward_order_time(-1, 1_000),
            1_000
        );
    }
}
