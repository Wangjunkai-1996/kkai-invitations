use axum::extract::State;
use axum::Json;
use sqlx::Row;

use crate::error::Result;
use crate::AppState;

// POST /api/system/sync-rebates
pub async fn sync_rebates(State(state): State<AppState>) -> Result<Json<serde_json::Value>> {
    let now = chrono::Utc::now().timestamp();

    tracing::info!("开始同步返利记录");

    // 同步充值订单
    let topup_synced = sync_topup_orders(&state, now).await?;
    tracing::info!("充值订单同步完成，新增 {} 条返利记录", topup_synced);

    // 同步订阅订单
    let subscription_synced = sync_subscription_orders(&state, now).await?;
    tracing::info!("订阅订单同步完成，新增 {} 条返利记录", subscription_synced);

    Ok(Json(serde_json::json!({
        "success": true,
        "message": "同步完成",
        "topup_synced": topup_synced,
        "subscription_synced": subscription_synced
    })))
}

async fn sync_topup_orders(state: &AppState, now: i64) -> Result<i64> {
    // 获取上次同步的最大 ID
    let last_sync_id: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(last_sync_id), 0) FROM sync_log WHERE sync_type = 'topup'",
    )
    .fetch_one(&state.pool)
    .await?;

    // 查询新的充值订单
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
        ORDER BY t.id
        LIMIT 1000
        "#,
    )
    .bind(last_sync_id)
    .fetch_all(&state.newapi_pool)
    .await?;

    if orders.is_empty() {
        return Ok(0);
    }

    let mut synced = 0;
    let mut max_id = last_sync_id;

    for order in orders {
        let order_id: i64 = order.try_get("id")?;
        let user_id: i64 = order.try_get("user_id")?;
        let money: f64 = order.try_get("money")?;
        let inviter_id: Option<i64> = order.try_get("inviter_id")?;
        let group: Option<String> = order.try_get("group")?;

        max_id = order_id.max(max_id);

        // 查询返利规则
        let rule = sqlx::query(
            "SELECT rebate_rate FROM rebate_rules WHERE user_group = $1 AND rule_type = 'topup'",
        )
        .bind(group.as_deref().unwrap_or("default"))
        .fetch_optional(&state.pool)
        .await?;

        if let Some(rule_row) = rule {
            let rebate_rate: f64 = rule_row.try_get("rebate_rate")?;
            // 返利金额计算：订单金额 * 返利比例，四舍五入
            // money 是实际金额（元），需要转换为 new-api 的额度单位（通常 1 元 = 500000 额度）
            let rebate_amount = (money * rebate_rate * 500000.0).round() as i64;

            // 插入返利记录
            let result = sqlx::query(
                r#"
                INSERT INTO rebate_records
                (inviter_id, invitee_id, order_type, order_id, order_amount, rebate_amount, rebate_rate, status, created_at, updated_at)
                VALUES ($1, $2, 'topup', $3, $4, $5, $6, 'pending', $7, $7)
                ON CONFLICT (order_type, order_id) DO NOTHING
                "#
            )
            .bind(inviter_id)
            .bind(user_id)
            .bind(order_id)
            .bind(money)
            .bind(rebate_amount)
            .bind(rebate_rate)
            .bind(now)
            .execute(&state.pool)
            .await;

            if result.is_ok() {
                synced += 1;
            }
        }
    }

    // 更新同步日志
    sqlx::query(
        r#"
        INSERT INTO sync_log (sync_type, last_sync_id, last_sync_time, created_at)
        VALUES ('topup', $1, $2, $2)
        "#,
    )
    .bind(max_id)
    .bind(now)
    .execute(&state.pool)
    .await?;

    Ok(synced)
}

async fn sync_subscription_orders(state: &AppState, now: i64) -> Result<i64> {
    // 获取上次同步的最大 ID
    let last_sync_id: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(last_sync_id), 0) FROM sync_log WHERE sync_type = 'subscription'",
    )
    .fetch_one(&state.pool)
    .await?;

    // 查询新的订阅订单
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

    if orders.is_empty() {
        return Ok(0);
    }

    let mut synced = 0;
    let mut max_id = last_sync_id;

    for order in orders {
        let order_id: i64 = order.try_get("id")?;
        let user_id: i64 = order.try_get("user_id")?;
        let money: f64 = order.try_get("money")?;
        let inviter_id: Option<i64> = order.try_get("inviter_id")?;
        let group: Option<String> = order.try_get("group")?;

        max_id = order_id.max(max_id);

        // 查询返利规则
        let rule = sqlx::query("SELECT rebate_rate FROM rebate_rules WHERE user_group = $1 AND rule_type = 'subscription'")
            .bind(group.as_deref().unwrap_or("default"))
            .fetch_optional(&state.pool)
            .await?;

        if let Some(rule_row) = rule {
            let rebate_rate: f64 = rule_row.try_get("rebate_rate")?;
            // 返利金额计算：订单金额 * 返利比例，四舍五入
            // money 是实际金额（元），需要转换为 new-api 的额度单位（通常 1 元 = 500000 额度）
            let rebate_amount = (money * rebate_rate * 500000.0).round() as i64;

            // 插入返利记录
            let result = sqlx::query(
                r#"
                INSERT INTO rebate_records
                (inviter_id, invitee_id, order_type, order_id, order_amount, rebate_amount, rebate_rate, status, created_at, updated_at)
                VALUES ($1, $2, 'subscription', $3, $4, $5, $6, 'pending', $7, $7)
                ON CONFLICT (order_type, order_id) DO NOTHING
                "#
            )
            .bind(inviter_id)
            .bind(user_id)
            .bind(order_id)
            .bind(money)
            .bind(rebate_amount)
            .bind(rebate_rate)
            .bind(now)
            .execute(&state.pool)
            .await;

            if result.is_ok() {
                synced += 1;
            }
        }
    }

    // 更新同步日志
    sqlx::query(
        r#"
        INSERT INTO sync_log (sync_type, last_sync_id, last_sync_time, created_at)
        VALUES ('subscription', $1, $2, $2)
        "#,
    )
    .bind(max_id)
    .bind(now)
    .execute(&state.pool)
    .await?;

    Ok(synced)
}
