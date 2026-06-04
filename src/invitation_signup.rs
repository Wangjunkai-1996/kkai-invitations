use sqlx::{PgPool, Row};

use crate::error::{AppError, Result};

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
    pub unlock_required: bool,
    pub now: i64,
}

pub async fn insert_invitation_signup_reward_record(
    pool: &PgPool,
    params: InsertInvitationSignupReward<'_>,
) -> Result<RewardInsertOutcome> {
    let ulid = ulid::Ulid::new().to_string();
    let unlocked_at = if params.unlock_required {
        None
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
        VALUES ($1, $2, $3, $4, $5, $3, 0, $6, $6, $7, NULL, 'pending', $6, $6, $6, $8, $9, $6, $6)
        ON CONFLICT (order_type, order_id) DO NOTHING
        RETURNING id
        "#,
    )
    .bind(&ulid)
    .bind(params.reward_owner_id)
    .bind(params.invitee_id)
    .bind(params.user_group)
    .bind(params.order_type)
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
}
