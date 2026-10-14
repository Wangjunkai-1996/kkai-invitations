use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use axum::Json;
use chrono::{TimeZone, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};

use crate::credit_currency;
use crate::error::{AppError, Result};
use crate::response::{ok, ApiResponse, PaginatedResponse};
use crate::session::SessionUser;
use crate::system_config::{
    get_config_bool, INVITATION_SIGNUP_REWARD_ENABLED_KEY, ORDER_REBATE_ENABLED_KEY,
};
use crate::AppState;

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RebateAmounts {
    cooling: i64,
    available: i64,
    pending_approval: i64,
    approved: i64,
    issued: i64,
    locked: i64,
    closed: i64,
    pending_calculation_count: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Customer {
    user_id: i64,
    user_name: String,
    invited_at: String,
    first_recharge_at: Option<String>,
    effective: bool,
    recharge_amount: i64,
    unverified_recharge_count: i64,
    rebates: RebateAmounts,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CustomerSummary {
    invited_count: i64,
    effective_invited_count: i64,
    recharge_amount: i64,
    unverified_recharge_count: i64,
    rebates: RebateAmounts,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Customers {
    items: Vec<Customer>,
    total: i64,
    page: i64,
    page_size: i64,
    summary: CustomerSummary,
    credit_currency: &'static str,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CustomerPagination {
    page: Option<i64>,
    page_size: Option<i64>,
}

impl CustomerPagination {
    fn values(&self) -> Result<(i64, i64, i64)> {
        let page = self.page.unwrap_or(1).max(1);
        let size = self.page_size.unwrap_or(20).clamp(1, 100);
        let offset = (page - 1)
            .checked_mul(size)
            .ok_or_else(|| AppError::BadRequest("页码过大".into()))?;
        Ok((page, size, offset))
    }
}

// Qualification never depends on today's wallet balance, rebate activation ID,
// or the existence of a rebate. Subscription mirrors and wallet-funded orders
// are not genuine wallet recharges. Deleted accounts remain in historical counts.
const QUALIFIED_ORDERS: &str = r#"
    SELECT t.*, COALESCE(NULLIF(t.complete_time, 0), NULLIF(t.create_time, 0), 0) AS paid_at
    FROM top_ups t JOIN users u ON u.id = t.user_id
    WHERE u.inviter_id = $1 AND t.status = 'success' AND t.amount > 0 AND t.money > 0
      AND COALESCE(t.payment_provider, '') NOT IN ('balance', 'redemption')
      AND COALESCE(t.payment_method, '') NOT IN ('balance', 'redemption')
      AND NOT EXISTS (SELECT 1 FROM subscription_orders s WHERE s.trade_no = t.trade_no)
"#;

// Only immutable credit snapshots are additive. Provider-specific payment money
// and today's quota conversion cannot reconstruct historical wallet credits.
async fn credited_orders_sql(pool: &PgPool) -> Result<String> {
    let has_outbox: bool = sqlx::query_scalar("SELECT to_regclass('kkai_outbox') IS NOT NULL")
        .fetch_one(pool)
        .await?;
    let outbox = if has_outbox {
        "SELECT event_key, payload::JSONB AS payload FROM kkai_outbox"
    } else {
        "SELECT NULL::TEXT AS event_key, NULL::JSONB AS payload WHERE FALSE"
    };
    Ok(format!(
        r#"
        WITH qualified AS ({QUALIFIED_ORDERS}), credited AS (
            SELECT t.*, CASE
                WHEN e.payload->>'event_type' = 'topup.completed'
                 AND e.payload->>'source_order_id' = t.id::TEXT
                 AND e.payload->>'invitee_id' = t.user_id::TEXT
                 AND e.payload->>'credited_quota' ~ '^[0-9]+$'
                 AND (e.payload->>'credited_quota')::NUMERIC > 0
                 AND (e.payload->>'credited_quota')::NUMERIC <= 9223372036854775807
                THEN CASE WHEN $2::BIGINT IS NOT NULL AND t.id <= $2
                    THEN ROUND((e.payload->>'credited_quota')::NUMERIC * 3 / 40)
                    ELSE (e.payload->>'credited_quota')::NUMERIC END
                ELSE NULL END AS credited_quota
            FROM qualified t LEFT JOIN ({outbox}) e ON e.event_key = 'newapi:topup:' || t.id::TEXT
        )
    "#
    ))
}

async fn credit_context(state: &AppState) -> Result<(Option<i64>, &'static str)> {
    credit_currency::verify_runtime(&state.pool, &state.newapi_pool)
        .await
        .map_err(|_| AppError::Conflict("站内币制尚未对齐，暂不能汇总充值额度".into()))?;
    let raw: Option<String> =
        sqlx::query_scalar("SELECT value FROM options WHERE key='KKAIUSDCreditCutover'")
            .fetch_optional(&state.newapi_pool)
            .await?;
    let Some(raw) = raw.filter(|v| !v.is_empty()) else {
        return Ok((None, credit_currency::LEGACY));
    };
    let value: serde_json::Value =
        serde_json::from_str(&raw).map_err(|_| AppError::Conflict("站内币制记录无效".into()))?;
    let cutoff = value["legacy_max_ids"]["top_ups"]
        .as_i64()
        .filter(|v| *v >= 0)
        .ok_or_else(|| AppError::Conflict("充值币制边界缺失".into()))?;
    Ok((Some(cutoff), credit_currency::USD))
}

pub async fn customers(
    State(state): State<AppState>,
    user: SessionUser,
    Query(params): Query<CustomerPagination>,
) -> Result<Json<ApiResponse<Customers>>> {
    if !crate::sales::profile(&state.pool, user.id)
        .await?
        .registered
    {
        return Err(AppError::Forbidden);
    }
    let (page, page_size, offset) = params.values()?;
    let (legacy_cutoff, credit_currency) = credit_context(&state).await?;
    let orders_sql = credited_orders_sql(&state.newapi_pool).await?;
    // Each database is read at a consistent snapshot. Delivery between the two
    // services remains asynchronous; absent commission is not presented as zero.
    let mut source = state.newapi_pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *source)
        .await?;
    let summary = sqlx::query(&format!(
        r#"{orders_sql}
        SELECT (SELECT COUNT(*) FROM users WHERE inviter_id=$1) AS invited_count,
               COUNT(DISTINCT user_id) AS effective_count,
               ROUND(COALESCE(SUM(credited_quota),0)/5000)::BIGINT AS amount,
               COUNT(*) FILTER (WHERE credited_quota IS NULL) AS unverified_count
        FROM credited"#
    ))
    .bind(user.id)
    .bind(legacy_cutoff)
    .fetch_one(&mut *source)
    .await?;
    let rows = sqlx::query(&format!(r#"{orders_sql}
        SELECT u.id::BIGINT AS user_id, COALESCE(NULLIF(u.display_name,''),u.username,'') AS user_name,
               u.created_at, MIN(t.paid_at) AS first_recharge_at, COUNT(t.id) > 0 AS effective,
               ROUND(COALESCE(SUM(t.credited_quota),0)/5000)::BIGINT AS amount,
               COUNT(t.id) FILTER (WHERE t.credited_quota IS NULL) AS unverified_count
        FROM (SELECT id, username, display_name, created_at FROM users WHERE inviter_id=$1
              ORDER BY created_at DESC, id DESC LIMIT $3 OFFSET $4) u
        LEFT JOIN credited t ON t.user_id=u.id
        GROUP BY u.id,u.username,u.display_name,u.created_at ORDER BY u.created_at DESC,u.id DESC"#))
        .bind(user.id).bind(legacy_cutoff).bind(page_size).bind(offset).fetch_all(&mut *source).await?;
    source.commit().await?;
    let ids = rows
        .iter()
        .map(|r| r.try_get::<i64, _>("user_id"))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut amounts = rebate_amounts(&state.pool, user.id, &ids, Utc::now().timestamp()).await?;
    let items = rows
        .iter()
        .map(|r| {
            let id = r.try_get("user_id")?;
            Ok(Customer {
                user_id: id,
                user_name: r.try_get("user_name")?,
                invited_at: timestamp(r.try_get("created_at")?),
                first_recharge_at: r
                    .try_get::<Option<i64>, _>("first_recharge_at")?
                    .map(timestamp),
                effective: r.try_get("effective")?,
                recharge_amount: r.try_get("amount")?,
                unverified_recharge_count: r.try_get("unverified_count")?,
                rebates: amounts.remove(&Some(id)).unwrap_or_default(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let total = summary.try_get("invited_count")?;
    Ok(ok(Customers {
        items,
        total,
        page,
        page_size,
        credit_currency,
        summary: CustomerSummary {
            invited_count: total,
            effective_invited_count: summary.try_get("effective_count")?,
            recharge_amount: summary.try_get("amount")?,
            unverified_recharge_count: summary.try_get("unverified_count")?,
            rebates: amounts.remove(&None).unwrap_or_default(),
        },
    }))
}

// The classification is shared by customer rows, global totals and order detail.
const REBATE_BUCKET: &str = r#"
    CASE
      WHEN r.status='closed' THEN 'closed'
      WHEN d.request_id IS NOT NULL OR r.status='completed' THEN 'issued'
      WHEN r.status='requested' THEN 'pending_approval'
      WHEN r.status='approved' THEN 'approved'
      WHEN r.unlock_required AND r.unlocked_at IS NULL THEN 'locked'
      WHEN (r.order_type IN ('topup','subscription','redemption') AND NOT $3)
        OR (r.order_type IN ('invite_inviter','invite_invitee') AND NOT $4) THEN 'locked'
      WHEN r.rebate_rate IS NULL AND r.order_type NOT IN ('invite_inviter','invite_invitee') THEN 'pending_calculation'
      WHEN r.status='initializing' OR r.rebate_effective_at > $2 THEN 'cooling'
      WHEN r.status='pending' THEN 'available'
      ELSE 'locked'
    END
"#;

async fn rebate_amounts(
    pool: &PgPool,
    user_id: i64,
    ids: &[i64],
    now: i64,
) -> Result<HashMap<Option<i64>, RebateAmounts>> {
    let order_enabled = get_config_bool(pool, ORDER_REBATE_ENABLED_KEY).await?;
    let signup_enabled = get_config_bool(pool, INVITATION_SIGNUP_REWARD_ENABLED_KEY).await?;
    let rows = sqlx::query(&format!(r#"
        WITH classified AS (
          SELECT r.invitee_id, r.rebate_amount, {REBATE_BUCKET} AS bucket
          FROM rebate_records r
          LEFT JOIN rebate_record_request_ownership o ON o.rebate_record_id=r.id AND o.ownership_state='active'
          LEFT JOIN sales_disbursements d ON d.request_id=o.request_id
          WHERE r.inviter_id=$1 AND r.order_type <> 'invite_invitee' AND r.superseded_by_record_id IS NULL
        )
        SELECT invitee_id,bucket,ROUND(SUM(rebate_amount)::NUMERIC/5000)::BIGINT AS amount,COUNT(*) AS count
        FROM classified GROUP BY GROUPING SETS ((invitee_id,bucket),(bucket))
        HAVING GROUPING(invitee_id)=1 OR invitee_id=ANY($5)
    "#))
        .bind(user_id).bind(now).bind(order_enabled).bind(signup_enabled).bind(ids).fetch_all(pool).await?;
    let mut result = HashMap::<Option<i64>, RebateAmounts>::new();
    for row in rows {
        let item = result.entry(row.try_get("invitee_id")?).or_default();
        let amount = row.try_get("amount")?;
        match row.try_get::<String, _>("bucket")?.as_str() {
            "cooling" => item.cooling = amount,
            "available" => item.available = amount,
            "pending_approval" => item.pending_approval = amount,
            "approved" => item.approved = amount,
            "issued" => item.issued = amount,
            "locked" => item.locked = amount,
            "closed" => item.closed = amount,
            _ => item.pending_calculation_count = row.try_get("count")?,
        }
    }
    Ok(result)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Recharge {
    id: i64,
    completed_at: String,
    amount: Option<i64>,
    original_amount: String,
    payment_provider: String,
    credit_epoch: &'static str,
    rebate_amount: Option<i64>,
    rebate_status: String,
    effective_at: Option<String>,
}

pub async fn recharges(
    State(state): State<AppState>,
    user: SessionUser,
    Path(customer_id): Path<i64>,
    Query(params): Query<CustomerPagination>,
) -> Result<Json<ApiResponse<PaginatedResponse<Recharge>>>> {
    if !crate::sales::profile(&state.pool, user.id)
        .await?
        .registered
    {
        return Err(AppError::Forbidden);
    }
    let owns: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM users WHERE id=$1 AND inviter_id=$2)")
            .bind(customer_id)
            .bind(user.id)
            .fetch_one(&state.newapi_pool)
            .await?;
    if !owns {
        return Err(AppError::NotFound("受邀客户不存在".into()));
    }
    let (page, page_size, offset) = params.values()?;
    let (legacy_cutoff, epoch) = credit_context(&state).await?;
    let orders_sql = credited_orders_sql(&state.newapi_pool).await?;
    let mut source = state.newapi_pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *source)
        .await?;
    let total: i64 = sqlx::query_scalar(&format!(
        "WITH qualified AS ({QUALIFIED_ORDERS}) SELECT COUNT(*) FROM qualified WHERE user_id=$2"
    ))
    .bind(user.id)
    .bind(customer_id)
    .fetch_one(&mut *source)
    .await?;
    let rows = sqlx::query(&format!(r#"{orders_sql}
        SELECT id::BIGINT,paid_at,ROUND(credited_quota/5000)::BIGINT AS amount,
               money::TEXT AS original_amount,COALESCE(NULLIF(payment_provider,''),payment_method,'') AS provider
        FROM credited WHERE user_id=$3 ORDER BY paid_at DESC,id DESC LIMIT $4 OFFSET $5"#))
        .bind(user.id).bind(legacy_cutoff).bind(customer_id).bind(page_size).bind(offset).fetch_all(&mut *source).await?;
    source.commit().await?;
    let ids = rows
        .iter()
        .map(|r| r.try_get::<i64, _>("id"))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let order_enabled = get_config_bool(&state.pool, ORDER_REBATE_ENABLED_KEY).await?;
    let signup_enabled = get_config_bool(&state.pool, INVITATION_SIGNUP_REWARD_ENABLED_KEY).await?;
    let records = sqlx::query(&format!(r#"
        SELECT r.order_id,ROUND(r.rebate_amount::NUMERIC/5000)::BIGINT AS amount,
               {REBATE_BUCKET} AS bucket,r.rebate_effective_at
        FROM rebate_records r
        LEFT JOIN rebate_record_request_ownership o ON o.rebate_record_id=r.id AND o.ownership_state='active'
        LEFT JOIN sales_disbursements d ON d.request_id=o.request_id
        WHERE r.inviter_id=$1 AND r.order_type='topup' AND r.order_id=ANY($5) AND r.invitee_id=$6
          AND r.superseded_by_record_id IS NULL"#))
        .bind(user.id).bind(Utc::now().timestamp()).bind(order_enabled).bind(signup_enabled).bind(&ids).bind(customer_id)
        .fetch_all(&state.pool).await?;
    let mut rebates = HashMap::new();
    for r in records {
        rebates.insert(r.try_get::<i64, _>("order_id")?, r);
    }
    let items = rows
        .iter()
        .map(|r| {
            let id = r.try_get("id")?;
            let record = rebates.get(&id);
            let status = record
                .map(|v| v.try_get::<String, _>("bucket"))
                .transpose()?
                .unwrap_or_else(|| "not_generated".into());
            let rebate_amount = if status == "pending_calculation" {
                None
            } else {
                record.map(|v| v.try_get("amount")).transpose()?
            };
            Ok(Recharge {
                id,
                completed_at: timestamp(r.try_get("paid_at")?),
                amount: r.try_get("amount")?,
                original_amount: r.try_get("original_amount")?,
                payment_provider: r.try_get("provider")?,
                credit_epoch: if legacy_cutoff.is_some_and(|v| id <= v) {
                    credit_currency::LEGACY
                } else {
                    epoch
                },
                rebate_amount,
                rebate_status: status,
                effective_at: record
                    .map(|v| v.try_get("rebate_effective_at"))
                    .transpose()?
                    .map(timestamp),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ok(PaginatedResponse {
        items,
        total,
        page,
        page_size,
    }))
}

fn timestamp(value: i64) -> String {
    Utc.timestamp_opt(value, 0)
        .single()
        .map(|date| date.to_rfc3339())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, SettlementMode};
    use sqlx::postgres::PgPoolOptions;

    #[test]
    fn pagination_rejects_overflow_and_caps_size() {
        assert_eq!(
            CustomerPagination {
                page: Some(0),
                page_size: Some(1000)
            }
            .values()
            .unwrap(),
            (1, 100, 0)
        );
        assert!(CustomerPagination {
            page: Some(i64::MAX),
            page_size: Some(100)
        }
        .values()
        .is_err());
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn customer_statistics_keep_lifetime_qualification_and_financial_states_separate() {
        let url = std::env::var("TEST_DATABASE_URL").expect("isolated local PostgreSQL URL");
        // One connection per pool keeps TEMP tables isolated from every other test.
        let source = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .unwrap();
        let ledger = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .unwrap();
        sqlx::raw_sql(r#"
            CREATE TEMP TABLE users(id BIGINT,username TEXT,display_name TEXT,created_at BIGINT,inviter_id BIGINT,quota BIGINT);
            CREATE TEMP TABLE options(key TEXT,value TEXT);
            CREATE TEMP TABLE top_ups(id BIGINT,user_id BIGINT,amount BIGINT,money NUMERIC,trade_no TEXT,status TEXT,
                complete_time BIGINT,create_time BIGINT,payment_provider TEXT,payment_method TEXT);
            CREATE TEMP TABLE subscription_orders(trade_no TEXT);
            CREATE TEMP TABLE kkai_outbox(event_key TEXT,payload TEXT);
            INSERT INTO users VALUES (11,'seller','Seller',1,0,0),(12,'spent','Spent',2,11,0),
                (13,'gift','Gift',3,11,500000),(14,'someone_else','Someone else',4,99,0),(15,'old','Old',5,11,0);
            INSERT INTO top_ups VALUES
                (1,12,1,1,'one','success',1000000001,1000000000,'epay','alipay'),
                (2,12,2,2,'two','success',1000000002,1000000001,'epay','alipay'),
                (3,13,1,1,'subscription','success',1000000003,1000000002,'epay','alipay'),
                (4,13,1,1,'failed','failed',0,1000000003,'epay','alipay'),
                (5,13,1,1,'balance','success',1000000005,1000000004,'balance','balance'),
                (6,15,1,1,'no-snapshot','success',1000000006,1000000005,'epay','alipay'),
                (7,14,1,1,'private','success',1000000007,1000000006,'epay','alipay'),
                (8,13,0,1,'mirror','success',1000000008,1000000007,'epay','alipay'),
                (9,13,1,1,'code','success',1000000009,1000000008,'redemption','redemption');
            INSERT INTO subscription_orders VALUES ('subscription');
            INSERT INTO kkai_outbox VALUES
                ('newapi:topup:1','{"event_type":"topup.completed","source_order_id":1,"invitee_id":12,"credited_quota":500000}'),
                ('newapi:topup:2','{"event_type":"topup.completed","source_order_id":2,"invitee_id":12,"credited_quota":1000000}'),
                ('newapi:topup:7','{"event_type":"topup.completed","source_order_id":7,"invitee_id":14,"credited_quota":500000}');
        "#).execute(&source).await.unwrap();
        sqlx::raw_sql(r#"
            CREATE TEMP TABLE system_config(key TEXT,value TEXT);
            INSERT INTO system_config VALUES ('order_rebate_enabled','true'),('invitation_signup_reward_enabled','true');
            CREATE TEMP TABLE sales_accounts(user_id BIGINT,active BOOLEAN);
            INSERT INTO sales_accounts VALUES(11,TRUE);
            CREATE TEMP TABLE rebate_records(id BIGINT,inviter_id BIGINT,invitee_id BIGINT,order_id BIGINT,order_type TEXT,
                rebate_amount BIGINT,status TEXT,rebate_rate NUMERIC,rebate_effective_at BIGINT,unlock_required BOOLEAN,
                unlocked_at BIGINT,superseded_by_record_id BIGINT);
            CREATE TEMP TABLE rebate_record_request_ownership(rebate_record_id BIGINT,request_id BIGINT,ownership_state TEXT);
            CREATE TEMP TABLE sales_disbursements(request_id BIGINT);
            INSERT INTO rebate_records VALUES
                (1,11,12,1,'topup',500000,'pending',0.1,9000000000,FALSE,NULL,NULL),
                (2,11,12,2,'topup',1000000,'pending',0.1,1,FALSE,NULL,NULL),
                (3,11,12,30,'topup',1500000,'requested',0.1,1,FALSE,NULL,NULL),
                (4,11,12,40,'topup',2000000,'approved',0.1,1,FALSE,NULL,NULL),
                (5,11,12,50,'topup',2500000,'approved',0.1,1,FALSE,NULL,NULL),
                (6,11,12,60,'invite_inviter',3000000,'pending',NULL,1,TRUE,NULL,NULL),
                (7,11,12,70,'topup',3500000,'closed',0.1,1,FALSE,NULL,NULL),
                (8,11,12,80,'topup',0,'initializing',NULL,9000000000,FALSE,NULL,NULL),
                (9,99,14,7,'topup',9000000,'pending',0.1,1,FALSE,NULL,NULL),
                (10,11,12,100,'topup',9000000,'closed',0.1,1,FALSE,NULL,1),
                (11,11,11,11,'invite_invitee',5000000,'pending',NULL,1,FALSE,NULL,NULL);
            INSERT INTO rebate_record_request_ownership VALUES(5,100,'active'),(4,101,'released');
            INSERT INTO sales_disbursements VALUES(100),(101);
        "#).execute(&ledger).await.unwrap();
        let state = AppState {
            config: Config {
                listen_addr: "127.0.0.1:0".into(),
                base_path: String::new(),
                database_url: url.clone(),
                newapi_database_url: url,
                session_secret: "test".into(),
                rebate_event_ingest_secret: "test".into(),
                sync_secret: None,
                auto_sync_interval_seconds: 0,
                auto_sync_initial_delay_seconds: 0,
                topup_reconciliation_interval_seconds: 0,
                topup_reconciliation_initial_delay_seconds: 0,
                topup_rebate_active_from_id: 1000,
                settlement_mode: SettlementMode::ReadOnly,
                quota_per_unit: 500000,
                balance_api: None,
                balance_dispatch_interval_milliseconds: 1000,
            },
            pool: ledger.clone(),
            newapi_pool: source.clone(),
            balance_client: None,
        };
        let user = SessionUser {
            id: 11,
            username: "seller".into(),
            role: 1,
            status: 1,
            group: "default".into(),
        };
        let result = customers(
            State(state.clone()),
            user.clone(),
            Query(CustomerPagination {
                page: Some(1),
                page_size: Some(1),
            }),
        )
        .await
        .unwrap()
        .0
        .data;
        assert_eq!(result.total, 3);
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0].user_id, 15);
        assert!(result.items[0].effective);
        assert_eq!(result.items[0].unverified_recharge_count, 1);
        assert_eq!(result.summary.effective_invited_count, 2);
        assert_eq!(result.summary.recharge_amount, 300);
        assert_eq!(result.summary.unverified_recharge_count, 1);
        assert_eq!(result.summary.rebates.cooling, 100);
        assert_eq!(result.summary.rebates.available, 200);
        assert_eq!(result.summary.rebates.pending_approval, 300);
        assert_eq!(result.summary.rebates.approved, 400);
        assert_eq!(result.summary.rebates.issued, 500);
        assert_eq!(result.summary.rebates.locked, 600);
        assert_eq!(result.summary.rebates.closed, 700);
        assert_eq!(result.summary.rebates.pending_calculation_count, 1);
        let detail = recharges(
            State(state.clone()),
            user.clone(),
            Path(12),
            Query(CustomerPagination {
                page: None,
                page_size: None,
            }),
        )
        .await
        .unwrap()
        .0
        .data;
        assert_eq!(detail.total, 2);
        assert_eq!(detail.items[0].amount, Some(200));
        assert_eq!(detail.items[0].rebate_status, "available");
        assert_eq!(detail.items[0].rebate_amount, Some(200));
        assert!(matches!(
            recharges(
                State(state.clone()),
                user.clone(),
                Path(14),
                Query(CustomerPagination {
                    page: None,
                    page_size: None
                })
            )
            .await,
            Err(AppError::NotFound(_))
        ));
        let mut ordinary = user.clone();
        ordinary.id = 12;
        assert!(matches!(
            customers(
                State(state.clone()),
                ordinary,
                Query(CustomerPagination {
                    page: None,
                    page_size: None
                })
            )
            .await,
            Err(AppError::Forbidden)
        ));
        // An inactive sales account retains read access to its financial history.
        sqlx::query("UPDATE sales_accounts SET active=FALSE")
            .execute(&ledger)
            .await
            .unwrap();
        assert!(customers(
            State(state.clone()),
            user.clone(),
            Query(CustomerPagination {
                page: None,
                page_size: None
            })
        )
        .await
        .is_ok());
        let orders = credited_orders_sql(&source).await.unwrap();
        let amounts: Vec<Option<i64>> = sqlx::query_scalar(&format!(
            "{orders} SELECT credited_quota::BIGINT FROM credited ORDER BY id"
        ))
        .bind(user.id)
        .bind(Some(1i64))
        .fetch_all(&source)
        .await
        .unwrap();
        assert_eq!(amounts, vec![Some(37500), Some(1000000), None]);
        // Older supported NewAPI databases have no source outbox: preserve the
        // effective count and explicitly expose unknown amounts, never guess.
        sqlx::query("DROP TABLE pg_temp.kkai_outbox")
            .execute(&source)
            .await
            .unwrap();
        let result = customers(
            State(state),
            user,
            Query(CustomerPagination {
                page: None,
                page_size: None,
            }),
        )
        .await
        .unwrap()
        .0
        .data;
        assert_eq!(result.summary.effective_invited_count, 2);
        assert_eq!(result.summary.unverified_recharge_count, 3);
        assert_eq!(result.summary.recharge_amount, 0);
        source.close().await;
        ledger.close().await;
    }
}
