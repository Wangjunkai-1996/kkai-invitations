use sqlx::{PgPool, Row};

use crate::error::{AppError, Result};

pub const INITIALIZATION_SECONDS: i64 = 2 * 24 * 60 * 60;
pub const REBATE_EFFECTIVE_DELAY_SECONDS: i64 = 7 * 24 * 60 * 60;

const QUOTA_UNITS_PER_YUAN: f64 = 500_000.0;

struct InitializingRebateRecord {
    id: i64,
    user_group: String,
    order_type: String,
    order_amount: f64,
    admin_adjusted: bool,
}

pub async fn finalize_ready_initializing_records(pool: &PgPool, now: i64) -> Result<i64> {
    let rows = sqlx::query(
        r#"
        SELECT id, user_group, order_type, order_amount::TEXT AS order_amount, admin_adjusted
        FROM rebate_records
        WHERE status = 'initializing' AND initialization_ends_at <= $1
        ORDER BY initialization_ends_at, id
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
        SELECT id, user_group, order_type, order_amount::TEXT AS order_amount, admin_adjusted
        FROM rebate_records
        WHERE id = ANY($1) AND status = 'initializing'
        ORDER BY id
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
) -> Result<Option<f64>> {
    let row = sqlx::query(
        "SELECT rebate_rate::TEXT AS rebate_rate FROM rebate_rules WHERE user_group = $1 AND rule_type = $2",
    )
    .bind(user_group)
    .bind(order_type)
    .fetch_optional(pool)
    .await?;

    row.map(|row| {
        let rebate_rate: String = row.try_get("rebate_rate")?;
        rebate_rate
            .parse::<f64>()
            .map_err(|_| AppError::Internal("返利规则格式错误".to_string()))
    })
    .transpose()
}

pub fn rebate_amount_for_yuan(order_amount_yuan: f64, rebate_rate: f64) -> i64 {
    (order_amount_yuan * rebate_rate * QUOTA_UNITS_PER_YUAN).round() as i64
}

fn rows_to_initializing_records(
    rows: Vec<sqlx::postgres::PgRow>,
) -> Result<Vec<InitializingRebateRecord>> {
    rows.iter()
        .map(|row| {
            let order_amount: String = row.try_get("order_amount")?;
            Ok(InitializingRebateRecord {
                id: row.try_get("id")?,
                user_group: row.try_get("user_group")?,
                order_type: row.try_get("order_type")?,
                order_amount: order_amount.parse::<f64>().unwrap_or_default(),
                admin_adjusted: row.try_get("admin_adjusted")?,
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
        } else if let Some(rebate_rate) =
            current_rebate_rate(pool, &record.user_group, &record.order_type).await?
        {
            let rebate_amount = rebate_amount_for_yuan(record.order_amount, rebate_rate);

            sqlx::query(
                r#"
                UPDATE rebate_records
                SET status = 'pending',
                    rebate_rate = $1,
                    rebate_amount = $2,
                    finalized_at = $3,
                    updated_at = $3
                WHERE id = $4 AND status = 'initializing'
                "#,
            )
            .bind(rebate_rate)
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
        assert_eq!(rebate_amount_for_yuan(100.0, 0.05), 2_500_000);
    }
}
