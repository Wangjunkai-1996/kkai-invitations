use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, RoundingStrategy};
use sqlx::{PgPool, Row};

use crate::error::{AppError, Result};

pub const INITIALIZATION_SECONDS: i64 = 5 * 60;
pub const REBATE_EFFECTIVE_DELAY_SECONDS: i64 = 7 * 24 * 60 * 60;
pub const ALL_USER_GROUP: &str = "__all__";

const PREVIOUS_REBATE_EFFECTIVE_DELAY_SECONDS: i64 = 24 * 60 * 60;
const LEGACY_INITIALIZATION_SECONDS: i64 = 2 * 24 * 60 * 60;
const LEGACY_REBATE_EFFECTIVE_DELAY_SECONDS: i64 = 7 * 24 * 60 * 60;
const QUOTA_UNITS_PER_YUAN: i64 = 500_000;

pub(crate) async fn order_rebate_effective_at<'e, E>(
    executor: E,
    order_time: i64,
) -> std::result::Result<i64, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let cutover: String = sqlx::query_scalar(
        "SELECT value FROM system_config WHERE key = 'order_rebate_seven_day_cutover_at'",
    )
    .fetch_one(executor)
    .await?;
    let cutover = cutover
        .parse::<i64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| sqlx::Error::Protocol("返利七天冷却切换时间无效".into()))?;
    rebate_effective_at_for_cutover(order_time, cutover)
}

pub(crate) fn rebate_effective_at_for_cutover(
    order_time: i64,
    cutover: i64,
) -> std::result::Result<i64, sqlx::Error> {
    if order_time <= 0 || cutover <= 0 {
        return Err(sqlx::Error::Protocol("返利订单或冷却切换时间无效".into()));
    }
    // The migration boundary, not delivery time, determines delayed orders' policy.
    let delay = if order_time < cutover {
        PREVIOUS_REBATE_EFFECTIVE_DELAY_SECONDS
    } else {
        REBATE_EFFECTIVE_DELAY_SECONDS
    };
    order_time
        .checked_add(delay)
        .ok_or_else(|| sqlx::Error::Protocol("返利生效时间超出范围".into()))
}

pub(crate) fn matches_supported_rebate_schedule(
    order_time: i64,
    initialization_ends_at: i64,
    rebate_effective_at: i64,
) -> bool {
    matches_rebate_schedule(
        order_time,
        initialization_ends_at,
        rebate_effective_at,
        INITIALIZATION_SECONDS,
        REBATE_EFFECTIVE_DELAY_SECONDS,
    ) || matches_rebate_schedule(
        order_time,
        initialization_ends_at,
        rebate_effective_at,
        INITIALIZATION_SECONDS,
        PREVIOUS_REBATE_EFFECTIVE_DELAY_SECONDS,
    ) || matches_rebate_schedule(
        order_time,
        initialization_ends_at,
        rebate_effective_at,
        LEGACY_INITIALIZATION_SECONDS,
        LEGACY_REBATE_EFFECTIVE_DELAY_SECONDS,
    )
}

fn matches_rebate_schedule(
    order_time: i64,
    initialization_ends_at: i64,
    rebate_effective_at: i64,
    initialization_seconds: i64,
    effective_delay_seconds: i64,
) -> bool {
    order_time.checked_add(initialization_seconds) == Some(initialization_ends_at)
        && order_time.checked_add(effective_delay_seconds) == Some(rebate_effective_at)
}

struct InitializingRebateRecord {
    id: i64,
    user_group: String,
    order_type: String,
    order_amount_cents: i64,
    admin_adjusted: bool,
    snapshot_rate: Option<Decimal>,
    snapshot_rebate_quota: Option<i64>,
}

pub async fn finalize_ready_initializing_records(pool: &PgPool, now: i64) -> Result<i64> {
    let rows = sqlx::query(
        r#"
        SELECT record.id, record.user_group, record.order_type,
               COALESCE(record.order_amount_cents, ROUND(record.order_amount * 100)::BIGINT)
                   AS order_amount_cents,
               record.admin_adjusted,
               snapshot.rebate_rate::TEXT AS snapshot_rate,
               snapshot.rebate_quota AS snapshot_rebate_quota
        FROM rebate_records record
        LEFT JOIN rebate_calculation_snapshots snapshot
          ON snapshot.rebate_record_id = record.id
        WHERE record.status = 'initializing' AND record.initialization_ends_at <= $1
        ORDER BY record.initialization_ends_at, record.id
        LIMIT 1000
        "#,
    )
    .bind(now)
    .fetch_all(pool)
    .await?;

    finalize_rows(pool, rows_to_initializing_records(rows)?, now).await
}

pub async fn finalize_ready_topup_event_records(pool: &PgPool, now: i64) -> Result<i64> {
    let rows = sqlx::query(
        r#"
        SELECT record.id, record.user_group, record.order_type,
               COALESCE(record.order_amount_cents, ROUND(record.order_amount * 100)::BIGINT)
                   AS order_amount_cents,
               record.admin_adjusted,
               snapshot.rebate_rate::TEXT AS snapshot_rate,
               snapshot.rebate_quota AS snapshot_rebate_quota
        FROM rebate_records record
        LEFT JOIN rebate_calculation_snapshots snapshot
          ON snapshot.rebate_record_id = record.id
        WHERE record.status = 'initializing'
          AND record.initialization_ends_at <= $1
          AND record.order_type = 'topup'
          AND (
              record.canonical_event_key LIKE 'newapi:topup:%'
              OR record.canonical_event_key LIKE 'newapi:redemption:%'
          )
        ORDER BY record.initialization_ends_at, record.id
        LIMIT 1000
        "#,
    )
    .bind(now)
    .fetch_all(pool)
    .await?;

    finalize_rows(pool, rows_to_initializing_records(rows)?, now).await
}

pub async fn finalize_initializing_records_by_ids(
    pool: &PgPool,
    record_ids: &[i64],
    now: i64,
) -> Result<i64> {
    let rows = sqlx::query(
        r#"
        SELECT record.id, record.user_group, record.order_type,
               COALESCE(record.order_amount_cents, ROUND(record.order_amount * 100)::BIGINT)
                   AS order_amount_cents,
               record.admin_adjusted,
               snapshot.rebate_rate::TEXT AS snapshot_rate,
               snapshot.rebate_quota AS snapshot_rebate_quota
        FROM rebate_records record
        LEFT JOIN rebate_calculation_snapshots snapshot
          ON snapshot.rebate_record_id = record.id
        WHERE record.id = ANY($1) AND record.status = 'initializing'
        ORDER BY record.id
        "#,
    )
    .bind(record_ids)
    .fetch_all(pool)
    .await?;

    finalize_rows(pool, rows_to_initializing_records(rows)?, now).await
}

pub async fn current_rebate_rate(
    pool: &PgPool,
    user_group: &str,
    order_type: &str,
) -> Result<Option<Decimal>> {
    let row = sqlx::query(
        r#"
        SELECT rebate_rate::TEXT AS rebate_rate
        FROM rebate_rules
        WHERE rule_type = $2
          AND user_group IN ($1, $3)
        ORDER BY CASE
            WHEN user_group = $1 THEN 0
            WHEN user_group = $3 THEN 1
            ELSE 2
        END
        LIMIT 1
        "#,
    )
    .bind(user_group)
    .bind(order_type)
    .bind(ALL_USER_GROUP)
    .fetch_optional(pool)
    .await?;

    row.map(|row| {
        let rebate_rate: String = row.try_get("rebate_rate")?;
        rebate_rate
            .parse::<Decimal>()
            .map_err(|_| AppError::Internal("返利规则格式错误".to_string()))
    })
    .transpose()
}

pub fn rebate_amount_for_cents(
    order_amount_cents: i64,
    rebate_rate: Decimal,
    quota_per_unit: i64,
) -> Result<i64> {
    Decimal::from(order_amount_cents)
        .checked_mul(rebate_rate)
        .and_then(|value| value.checked_mul(Decimal::from(quota_per_unit)))
        .and_then(|value| value.checked_div(Decimal::from(100)))
        .ok_or_else(|| AppError::Internal("返利金额超出可计算范围".to_string()))?
        .round_dp_with_strategy(0, RoundingStrategy::MidpointAwayFromZero)
        .to_i64()
        .ok_or_else(|| AppError::Internal("返利金额超出整数范围".to_string()))
}

pub fn rebate_amount_for_credited_quota(credited_quota: i64, rebate_rate: Decimal) -> Result<i64> {
    if credited_quota <= 0 {
        return Err(AppError::Internal(
            "credited_quota 必须为正整数".to_string(),
        ));
    }
    Decimal::from(credited_quota)
        .checked_mul(rebate_rate)
        .ok_or_else(|| AppError::Internal("返利额度超出可计算范围".to_string()))?
        .round_dp_with_strategy(0, RoundingStrategy::MidpointAwayFromZero)
        .to_i64()
        .ok_or_else(|| AppError::Internal("返利额度超出整数范围".to_string()))
}

fn rows_to_initializing_records(
    rows: Vec<sqlx::postgres::PgRow>,
) -> Result<Vec<InitializingRebateRecord>> {
    rows.iter()
        .map(|row| {
            let snapshot_rate = row
                .try_get::<Option<String>, _>("snapshot_rate")?
                .map(|value| {
                    value
                        .parse::<Decimal>()
                        .map_err(|_| AppError::Internal("返利快照格式错误".to_string()))
                })
                .transpose()?;
            Ok(InitializingRebateRecord {
                id: row.try_get("id")?,
                user_group: row.try_get("user_group")?,
                order_type: row.try_get("order_type")?,
                order_amount_cents: row.try_get("order_amount_cents")?,
                admin_adjusted: row.try_get("admin_adjusted")?,
                snapshot_rate,
                snapshot_rebate_quota: row.try_get("snapshot_rebate_quota")?,
            })
        })
        .collect()
}

async fn finalize_rows(
    pool: &PgPool,
    rows: Vec<InitializingRebateRecord>,
    now: i64,
) -> Result<i64> {
    let mut finalized = 0;
    let boundary = crate::credit_currency::boundary(pool).await?;

    for record in rows {
        let rows_affected = if record.admin_adjusted {
            sqlx::query(
                r#"
                UPDATE rebate_records
                SET status = 'pending', finalized_at = $1, updated_at = $1
                WHERE id = $2 AND status = 'initializing'
                "#,
            )
            .bind(now)
            .bind(record.id)
            .execute(pool)
            .await?
            .rows_affected()
        } else if let (Some(rebate_rate), Some(rebate_amount)) =
            (record.snapshot_rate, record.snapshot_rebate_quota)
        {
            sqlx::query(
                r#"
                UPDATE rebate_records
                SET status = 'pending',
                    rebate_rate = $1::NUMERIC,
                    rebate_amount = $2,
                    finalized_at = $3,
                    updated_at = $3
                WHERE id = $4 AND status = 'initializing'
                "#,
            )
            .bind(rebate_rate.to_string())
            .bind(rebate_amount)
            .bind(now)
            .bind(record.id)
            .execute(pool)
            .await?
            .rows_affected()
        } else if let Some(rebate_rate) =
            current_rebate_rate(pool, &record.user_group, &record.order_type).await?
        {
            let rebate_amount = rebate_amount_for_cents(
                record.order_amount_cents,
                rebate_rate,
                QUOTA_UNITS_PER_YUAN,
            )?;
            let rebate_amount = if boundary
                .as_ref()
                .is_some_and(|b| record.id <= b.max_rebate_id)
            {
                crate::credit_currency::convert(rebate_amount)
            } else {
                rebate_amount
            };

            sqlx::query(
                r#"
                UPDATE rebate_records
                SET status = 'pending',
                    rebate_rate = $1::NUMERIC,
                    rebate_amount = $2,
                    finalized_at = $3,
                    updated_at = $3
                WHERE id = $4 AND status = 'initializing'
                "#,
            )
            .bind(rebate_rate.to_string())
            .bind(rebate_amount)
            .bind(now)
            .bind(record.id)
            .execute(pool)
            .await?
            .rows_affected()
        } else {
            sqlx::query(
                r#"
                UPDATE rebate_records
                SET status = 'pending',
                    rebate_rate = NULL,
                    rebate_amount = 0,
                    finalized_at = $1,
                    updated_at = $1
                WHERE id = $2 AND status = 'initializing'
                "#,
            )
            .bind(now)
            .bind(record.id)
            .execute(pool)
            .await?
            .rows_affected()
        };

        if rows_affected > 0 {
            finalized += 1;
        }
    }

    Ok(finalized)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calculates_rebate_amount_in_new_api_quota_units() {
        assert_eq!(
            rebate_amount_for_cents(
                10_000,
                "0.05".parse::<Decimal>().unwrap(),
                QUOTA_UNITS_PER_YUAN
            )
            .unwrap(),
            2_500_000
        );
        assert_eq!(
            rebate_amount_for_credited_quota(10, "0.05".parse::<Decimal>().unwrap()).unwrap(),
            1
        );
    }

    #[test]
    fn assigns_cooldown_from_payment_time_across_the_persisted_cutover() {
        let cutover = 1_800_000_000;
        for (order_time, expected_effective_at) in [
            (cutover - 1, cutover - 1 + 86_400),
            (cutover, cutover + 604_800),
            (cutover + 1, cutover + 1 + 604_800),
        ] {
            assert_eq!(
                rebate_effective_at_for_cutover(order_time, cutover).unwrap(),
                expected_effective_at
            );
        }
        assert!(rebate_effective_at_for_cutover(0, cutover).is_err());
        assert!(rebate_effective_at_for_cutover(cutover, 0).is_err());
        assert!(rebate_effective_at_for_cutover(i64::MAX, cutover).is_err());
    }

    #[tokio::test]
    async fn cooldown_uses_the_stored_boundary_and_rejects_missing_or_invalid_configuration() {
        let Ok(database_url) = std::env::var("TEST_DATABASE_URL") else {
            eprintln!("skipped cooldown database check: TEST_DATABASE_URL is not set");
            return;
        };
        let pool = PgPool::connect(&database_url).await.unwrap();
        let mut transaction = pool.begin().await.unwrap();
        sqlx::query("CREATE TEMP TABLE system_config (key TEXT PRIMARY KEY, value TEXT NOT NULL) ON COMMIT DROP")
            .execute(&mut *transaction).await.unwrap();
        assert!(matches!(
            order_rebate_effective_at(&mut *transaction, 1_800_000_000).await,
            Err(sqlx::Error::RowNotFound)
        ));
        sqlx::query(
            "INSERT INTO system_config VALUES ('order_rebate_seven_day_cutover_at', '1800000000')",
        )
        .execute(&mut *transaction)
        .await
        .unwrap();
        assert_eq!(
            order_rebate_effective_at(&mut *transaction, 1_799_999_999)
                .await
                .unwrap(),
            1_800_086_399
        );
        assert_eq!(
            order_rebate_effective_at(&mut *transaction, 1_800_000_000)
                .await
                .unwrap(),
            1_800_604_800
        );
        for invalid in ["0", "-1", "not-a-timestamp"] {
            sqlx::query("UPDATE system_config SET value = $1")
                .bind(invalid)
                .execute(&mut *transaction)
                .await
                .unwrap();
            assert!(matches!(
                order_rebate_effective_at(&mut *transaction, 1_800_000_000).await,
                Err(sqlx::Error::Protocol(_))
            ));
        }
        transaction.rollback().await.unwrap();
        pool.close().await;
    }

    #[test]
    fn accepts_current_and_legacy_schedules_without_mixing_them() {
        let order_time = 1_000;
        assert!(matches_supported_rebate_schedule(
            order_time,
            order_time + 300,
            order_time + 604_800,
        ));
        assert!(matches_supported_rebate_schedule(
            order_time,
            order_time + 300,
            order_time + 86_400,
        ));
        assert!(matches_supported_rebate_schedule(
            order_time,
            order_time + LEGACY_INITIALIZATION_SECONDS,
            order_time + LEGACY_REBATE_EFFECTIVE_DELAY_SECONDS,
        ));
        assert!(!matches_supported_rebate_schedule(
            order_time,
            order_time + LEGACY_INITIALIZATION_SECONDS,
            order_time + PREVIOUS_REBATE_EFFECTIVE_DELAY_SECONDS,
        ));
        assert!(!matches_supported_rebate_schedule(
            order_time,
            order_time + 300,
            order_time + 604_799,
        ));
        assert!(!matches_supported_rebate_schedule(i64::MAX, 0, 0));
    }
}
