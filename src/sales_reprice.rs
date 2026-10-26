use std::collections::HashSet;
use std::io::Read;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres, Row, Transaction};

use crate::config;
use crate::credit_currency::{self, USD};
use crate::rebate_source_events::calculate_rebate_from_credited_quota;

pub const RECEIPT_KEY: &str = "sales_reprice_20261026";
const VERSION: i32 = 1;
const EXPECTED_COUNT: usize = 21;
const EXPECTED_OLD_TOTAL: i64 = 7_400_000;
const EXPECTED_TARGET_TOTAL: i64 = 22_200_000;
const OLD_RATE: &str = "0.0500";
const TARGET_RATE: &str = "0.1500";

#[derive(Debug, thiserror::Error)]
pub enum SalesRepriceError {
    #[error("invalid sales reprice: {0}")]
    Invalid(String),
    #[error("sales reprice source changed: {0}")]
    SourceChanged(String),
    #[error("sales reprice verification failed: {0}")]
    Verification(String),
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("input error: {0}")]
    Input(#[from] std::io::Error),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Entry {
    pub record_id: i64,
    pub order_id: i64,
    pub canonical_event_key: String,
    pub source_payload_sha256: String,
    pub inviter_id: i64,
    pub invitee_id: i64,
    pub order_time: i64,
    pub credited_quota: i64,
    pub old_rate: String,
    pub old_amount: i64,
    pub snapshot_rule_id: Option<i64>,
    pub target_rate: String,
    pub target_amount: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    pub version: i32,
    pub generated_at: i64,
    pub sales_rule_id: i64,
    pub sales_rule_effective_time: i64,
    pub newapi_legacy_max_topup_id: i64,
    pub currency_max_rebate_id: i64,
    pub entries: Vec<Entry>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub state: String,
    pub applied_at: i64,
    pub backup_sha256: String,
    pub plan: Plan,
    pub sha256: String,
}

impl Receipt {
    pub fn entry(&self, record_id: i64) -> Option<&Entry> {
        self.plan
            .entries
            .iter()
            .find(|entry| entry.record_id == record_id)
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    plan: Plan,
    sha256: String,
}

pub async fn load_verified_receipt(pool: &PgPool) -> Result<Option<Receipt>, SalesRepriceError> {
    let raw: Option<String> = sqlx::query_scalar("SELECT value FROM system_config WHERE key=$1")
        .bind(RECEIPT_KEY)
        .fetch_optional(pool)
        .await?;
    raw.map(|value| parse_receipt(&value)).transpose()
}

pub async fn load_verified_receipt_in_transaction(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<Option<Receipt>, SalesRepriceError> {
    let raw: Option<String> = sqlx::query_scalar("SELECT value FROM system_config WHERE key=$1")
        .bind(RECEIPT_KEY)
        .fetch_optional(&mut **tx)
        .await?;
    raw.map(|value| parse_receipt(&value)).transpose()
}

fn parse_receipt(raw: &str) -> Result<Receipt, SalesRepriceError> {
    let receipt: Receipt = serde_json::from_str(raw)?;
    if receipt.state != "applied"
        || receipt.applied_at < receipt.plan.generated_at
        || !valid_sha256(&receipt.backup_sha256)
    {
        return Err(SalesRepriceError::Invalid(
            "invalid receipt state or time".into(),
        ));
    }
    validate_plan(&receipt.plan, &receipt.sha256)?;
    Ok(receipt)
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn digest(plan: &Plan) -> Result<String, SalesRepriceError> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(plan)?)))
}

fn validate_plan(plan: &Plan, sha256: &str) -> Result<(), SalesRepriceError> {
    if plan.version != VERSION
        || plan.generated_at <= 0
        || plan.sales_rule_id <= 0
        || plan.sales_rule_effective_time <= 0
        || plan.sales_rule_effective_time > plan.generated_at
        || plan.newapi_legacy_max_topup_id < 0
        || plan.currency_max_rebate_id < 0
        || plan.entries.len() != EXPECTED_COUNT
        || digest(plan)? != sha256
    {
        return Err(SalesRepriceError::Invalid(
            "manifest identity or checksum differs".into(),
        ));
    }
    let old_rate = OLD_RATE.parse::<Decimal>().expect("constant rate");
    let target_rate = TARGET_RATE.parse::<Decimal>().expect("constant rate");
    let mut seen_records = HashSet::new();
    let mut seen_orders = HashSet::new();
    let mut old_total = 0_i64;
    let mut target_total = 0_i64;
    for (position, entry) in plan.entries.iter().enumerate() {
        if entry.record_id <= plan.currency_max_rebate_id
            || entry.order_id <= plan.newapi_legacy_max_topup_id
            || entry.canonical_event_key != format!("newapi:topup:{}", entry.order_id)
            || entry.source_payload_sha256.len() != 64
            || !entry
                .source_payload_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || entry.inviter_id <= 0
            || entry.invitee_id <= 0
            || entry.order_time <= 0
            || entry.order_time >= plan.sales_rule_effective_time
            || entry.credited_quota <= 0
            || entry.old_rate.parse::<Decimal>().ok() != Some(old_rate)
            || entry.target_rate.parse::<Decimal>().ok() != Some(target_rate)
            || entry.old_amount
                != calculate_rebate_from_credited_quota(entry.credited_quota, old_rate)
                    .map_err(|error| SalesRepriceError::Invalid(error.to_string()))?
            || entry.target_amount
                != calculate_rebate_from_credited_quota(entry.credited_quota, target_rate)
                    .map_err(|error| SalesRepriceError::Invalid(error.to_string()))?
            || !seen_records.insert(entry.record_id)
            || !seen_orders.insert(entry.order_id)
            || (position > 0 && plan.entries[position - 1].record_id >= entry.record_id)
        {
            return Err(SalesRepriceError::Invalid(format!(
                "invalid or duplicate correction entry {}",
                entry.record_id
            )));
        }
        old_total = old_total
            .checked_add(entry.old_amount)
            .ok_or_else(|| SalesRepriceError::Invalid("old quota total overflow".into()))?;
        target_total = target_total
            .checked_add(entry.target_amount)
            .ok_or_else(|| SalesRepriceError::Invalid("target quota total overflow".into()))?;
    }
    if old_total != EXPECTED_OLD_TOTAL || target_total != EXPECTED_TARGET_TOTAL {
        return Err(SalesRepriceError::Invalid(
            "correction totals differ".into(),
        ));
    }
    Ok(())
}

pub async fn run(args: &[String]) -> Result<String, SalesRepriceError> {
    let command = args.first().map(String::as_str).ok_or_else(|| {
        SalesRepriceError::Invalid("usage: invitations sales-reprice <plan|apply|verify>".into())
    })?;
    if !matches!(command, "plan" | "apply" | "verify") {
        return Err(SalesRepriceError::Invalid(
            "unknown sales-reprice command".into(),
        ));
    }
    if command == "apply" && std::env::var("SETTLEMENT_MODE").ok().as_deref() != Some("read_only") {
        return Err(SalesRepriceError::Invalid(
            "apply requires SETTLEMENT_MODE=read_only".into(),
        ));
    }
    let invitations_url =
        config::invitations_database_url_from_env().map_err(SalesRepriceError::Invalid)?;
    let newapi_url = config::newapi_database_url_from_env().map_err(SalesRepriceError::Invalid)?;
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&invitations_url)
        .await?;
    let newapi_pool = crate::db::init_readonly_pool(&newapi_url).await?;
    credit_currency::verify_runtime(&pool, &newapi_pool)
        .await
        .map_err(|error| {
            SalesRepriceError::Invalid(format!("currency identity differs: {error}"))
        })?;
    if credit_currency::current_epoch(&pool).await? != USD {
        return Err(SalesRepriceError::Invalid(
            "USD credit migration is not applied".into(),
        ));
    }
    let cutoff = newapi_cutoff(&newapi_pool).await?;
    let boundary = credit_currency::boundary(&pool)
        .await?
        .ok_or_else(|| SalesRepriceError::Invalid("currency boundary missing".into()))?;
    let output = match command {
        "plan" => {
            if args.len() != 1 {
                return Err(SalesRepriceError::Invalid("plan takes no flags".into()));
            }
            let plan = plan(&pool, cutoff, boundary.max_rebate_id).await?;
            let sha256 = digest(&plan)?;
            validate_plan(&plan, &sha256)?;
            serde_json::to_string_pretty(&Envelope { plan, sha256 })?
        }
        "apply" | "verify" => {
            let expected = args
                .get(1)
                .filter(|flag| flag.as_str() == "--sha256")
                .and_then(|_| args.get(2))
                .ok_or_else(|| {
                    SalesRepriceError::Invalid("expected --sha256 <manifest hash>".into())
                })?;
            let backup_sha256 = if command == "apply" {
                if args.len() != 6
                    || args.get(3).map(String::as_str) != Some("--backup-sha256")
                    || args.get(5).map(String::as_str) != Some("--ingress-isolated")
                {
                    return Err(SalesRepriceError::Invalid(
                        "apply requires --sha256 <manifest hash> --backup-sha256 <backup hash> --ingress-isolated".into(),
                    ));
                }
                let hash = args[4].as_str();
                if !valid_sha256(hash) {
                    return Err(SalesRepriceError::Invalid("invalid backup SHA256".into()));
                }
                Some(hash)
            } else if args.len() == 3 {
                None
            } else {
                return Err(SalesRepriceError::Invalid(
                    "verify accepts only --sha256".into(),
                ));
            };
            let mut raw = String::new();
            std::io::stdin().read_to_string(&mut raw)?;
            let envelope: Envelope = serde_json::from_str(&raw)?;
            if &envelope.sha256 != expected {
                return Err(SalesRepriceError::Invalid(
                    "manifest hash argument differs".into(),
                ));
            }
            validate_plan(&envelope.plan, &envelope.sha256)?;
            if envelope.plan.newapi_legacy_max_topup_id != cutoff
                || envelope.plan.currency_max_rebate_id != boundary.max_rebate_id
            {
                return Err(SalesRepriceError::SourceChanged(
                    "currency boundary changed".into(),
                ));
            }
            let replayed = if command == "apply" {
                apply(
                    &pool,
                    &envelope,
                    backup_sha256.expect("apply backup checked"),
                )
                .await?
            } else {
                verify(&pool, &envelope).await?;
                false
            };
            serde_json::json!({"ok":true,"sha256":envelope.sha256,"count":EXPECTED_COUNT,"replayed":replayed}).to_string()
        }
        _ => unreachable!(),
    };
    pool.close().await;
    newapi_pool.close().await;
    Ok(output)
}

async fn newapi_cutoff(pool: &PgPool) -> Result<i64, SalesRepriceError> {
    let raw: String =
        sqlx::query_scalar("SELECT value FROM options WHERE key='KKAIUSDCreditCutover'")
            .fetch_one(pool)
            .await?;
    let value: serde_json::Value = serde_json::from_str(&raw)?;
    value["legacy_max_ids"]["top_ups"]
        .as_i64()
        .filter(|id| *id >= 0)
        .ok_or_else(|| SalesRepriceError::Invalid("NewAPI topup currency cutoff missing".into()))
}

async fn plan(pool: &PgPool, cutoff: i64, max_rebate_id: i64) -> Result<Plan, SalesRepriceError> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *tx)
        .await?;
    if load_verified_receipt_in_transaction(&mut tx)
        .await?
        .is_some()
    {
        return Err(SalesRepriceError::Invalid(
            "sales reprice is already applied".into(),
        ));
    }
    let (sales_rule_id, sales_rule_effective_time, entries) =
        load_candidates(&mut tx, cutoff, max_rebate_id, false).await?;
    tx.commit().await?;
    Ok(Plan {
        version: VERSION,
        generated_at: chrono::Utc::now().timestamp(),
        sales_rule_id,
        sales_rule_effective_time,
        newapi_legacy_max_topup_id: cutoff,
        currency_max_rebate_id: max_rebate_id,
        entries,
    })
}

async fn load_candidates(
    tx: &mut Transaction<'_, Postgres>,
    cutoff: i64,
    max_rebate_id: i64,
    lock: bool,
) -> Result<(i64, i64, Vec<Entry>), SalesRepriceError> {
    let rule = sqlx::query(
        "SELECT id,effective_time FROM rebate_rules WHERE user_group='__sales__' AND rule_type='topup' AND rebate_rate=0.1500",
    )
    .fetch_one(&mut **tx).await?;
    let rule_id: i64 = rule.try_get("id")?;
    let effective_time: i64 = rule.try_get("effective_time")?;
    let locking = if lock {
        "FOR UPDATE OF r, snapshot"
    } else {
        ""
    };
    let sql = format!(
        r#"
        SELECT r.id, r.order_id, r.canonical_event_key, r.source_payload_sha256,
               r.inviter_id, r.invitee_id, r.order_time, r.credited_quota,
               r.rebate_rate::TEXT AS old_rate, r.rebate_amount AS old_amount,
               snapshot.rule_id AS snapshot_rule_id
        FROM rebate_records r
        JOIN rebate_calculation_snapshots snapshot ON snapshot.rebate_record_id=r.id
        JOIN rebate_source_event_receipts receipt
          ON receipt.event_key=r.canonical_event_key AND receipt.rebate_record_id=r.id
         AND receipt.outcome='created' AND receipt.source_payload_sha256=r.source_payload_sha256
        JOIN sales_accounts sales ON sales.user_id=r.inviter_id
        WHERE r.order_type='topup' AND r.order_id>$1 AND r.id>$2
          AND r.order_time<$3 AND r.status='pending' AND r.admin_adjusted=FALSE
          AND r.superseded_by_record_id IS NULL
          AND r.canonical_event_key='newapi:topup:' || r.order_id::TEXT
          AND r.rebate_rate=0.0500 AND r.credited_quota>0
          AND r.rebate_amount=ROUND(r.credited_quota::NUMERIC*0.05)::BIGINT
          AND snapshot.inviter_group=r.user_group AND snapshot.rule_type='topup'
          AND snapshot.rebate_rate=0.0500
          AND snapshot.rebate_quota=r.rebate_amount
          AND snapshot.credited_quota=r.credited_quota
          AND snapshot.order_amount_minor=COALESCE(r.order_amount_cents,0)
          AND (SELECT event.active FROM sales_account_events event
               WHERE event.user_id=r.inviter_id AND event.created_at<r.order_time
               ORDER BY event.created_at DESC,event.id DESC LIMIT 1)=TRUE
          AND NOT EXISTS(SELECT 1 FROM rebate_request_items item WHERE item.rebate_record_id=r.id)
          AND NOT EXISTS(SELECT 1 FROM rebate_record_request_ownership owner WHERE owner.rebate_record_id=r.id)
          AND NOT EXISTS(SELECT 1 FROM invitation_rebate_payouts payout WHERE payout.rebate_record_id=r.id)
          AND NOT EXISTS(SELECT 1 FROM balance_operations operation WHERE operation.rebate_record_id=r.id)
          AND NOT EXISTS(SELECT 1 FROM rebate_reconciliation_cases issue
                         WHERE issue.source_order_id=r.order_id AND issue.case_state IN ('open','approved'))
        ORDER BY r.id {locking}
    "#
    );
    let rows = sqlx::query(&sql)
        .bind(cutoff)
        .bind(max_rebate_id)
        .bind(effective_time)
        .fetch_all(&mut **tx)
        .await?;
    let target_rate = TARGET_RATE.parse::<Decimal>().expect("constant rate");
    let entries = rows
        .iter()
        .map(|row| -> Result<Entry, SalesRepriceError> {
            let credited_quota: i64 = row.try_get("credited_quota")?;
            Ok(Entry {
                record_id: row.try_get("id")?,
                order_id: row.try_get("order_id")?,
                canonical_event_key: row.try_get("canonical_event_key")?,
                source_payload_sha256: row.try_get("source_payload_sha256")?,
                inviter_id: row.try_get("inviter_id")?,
                invitee_id: row.try_get("invitee_id")?,
                order_time: row.try_get("order_time")?,
                credited_quota,
                old_rate: row.try_get("old_rate")?,
                old_amount: row.try_get("old_amount")?,
                snapshot_rule_id: row.try_get("snapshot_rule_id")?,
                target_rate: TARGET_RATE.to_string(),
                target_amount: calculate_rebate_from_credited_quota(credited_quota, target_rate)
                    .map_err(|error| SalesRepriceError::Invalid(error.to_string()))?,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((rule_id, effective_time, entries))
}

async fn apply(
    pool: &PgPool,
    envelope: &Envelope,
    backup_sha256: &str,
) -> Result<bool, SalesRepriceError> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
        .bind(RECEIPT_KEY)
        .execute(&mut *tx)
        .await?;
    if let Some(existing) = load_verified_receipt_in_transaction(&mut tx).await? {
        if existing.sha256 != envelope.sha256 || existing.backup_sha256 != backup_sha256 {
            return Err(SalesRepriceError::SourceChanged(
                "different receipt already applied".into(),
            ));
        }
        verify_entries(&mut tx, &existing).await?;
        tx.commit().await?;
        return Ok(true);
    }
    sqlx::query("LOCK TABLE rebate_rules, sales_account_events IN SHARE MODE")
        .execute(&mut *tx)
        .await?;
    let (rule_id, effective_time, entries) = load_candidates(
        &mut tx,
        envelope.plan.newapi_legacy_max_topup_id,
        envelope.plan.currency_max_rebate_id,
        true,
    )
    .await?;
    sqlx::query("LOCK TABLE rebate_requests, rebate_request_items, rebate_record_request_ownership, invitation_rebate_payouts, balance_operations, rebate_reconciliation_cases IN SHARE ROW EXCLUSIVE MODE")
        .execute(&mut *tx)
        .await?;
    let (_, _, current_entries) = load_candidates(
        &mut tx,
        envelope.plan.newapi_legacy_max_topup_id,
        envelope.plan.currency_max_rebate_id,
        false,
    )
    .await?;
    if rule_id != envelope.plan.sales_rule_id
        || effective_time != envelope.plan.sales_rule_effective_time
        || entries != envelope.plan.entries
        || current_entries != envelope.plan.entries
    {
        return Err(SalesRepriceError::SourceChanged(
            "eligible records changed after plan".into(),
        ));
    }
    let receipt = Receipt {
        state: "applied".into(),
        applied_at: chrono::Utc::now().timestamp(),
        backup_sha256: backup_sha256.to_string(),
        plan: envelope.plan.clone(),
        sha256: envelope.sha256.clone(),
    };
    let raw = serde_json::to_string(&receipt)?;
    let inserted = sqlx::query("INSERT INTO system_config(key,value,description,updated_at) VALUES($1,$2,'Sales historical topup reprice receipt',$3) ON CONFLICT(key) DO NOTHING")
        .bind(RECEIPT_KEY).bind(&raw).bind(receipt.applied_at)
        .execute(&mut *tx).await?.rows_affected();
    if inserted != 1 {
        return Err(SalesRepriceError::SourceChanged(
            "receipt was inserted concurrently".into(),
        ));
    }
    for entry in &receipt.plan.entries {
        let affected = sqlx::query(
            r#"
            UPDATE rebate_records
            SET rebate_rate=0.1500, rebate_amount=$1, admin_adjusted=TRUE, updated_at=$2
            WHERE id=$3 AND status='pending' AND admin_adjusted=FALSE
              AND rebate_rate=0.0500 AND rebate_amount=$4
              AND credited_quota=$5 AND source_payload_sha256=$6
        "#,
        )
        .bind(entry.target_amount)
        .bind(receipt.applied_at)
        .bind(entry.record_id)
        .bind(entry.old_amount)
        .bind(entry.credited_quota)
        .bind(&entry.source_payload_sha256)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if affected != 1 {
            return Err(SalesRepriceError::SourceChanged(format!(
                "record {} changed before update",
                entry.record_id
            )));
        }
    }
    verify_entries(&mut tx, &receipt).await?;
    tx.commit().await?;
    Ok(false)
}

async fn verify(pool: &PgPool, envelope: &Envelope) -> Result<(), SalesRepriceError> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *tx)
        .await?;
    let receipt = load_verified_receipt_in_transaction(&mut tx)
        .await?
        .ok_or_else(|| SalesRepriceError::Verification("receipt missing".into()))?;
    if receipt.sha256 != envelope.sha256 {
        return Err(SalesRepriceError::Verification(
            "receipt hash differs".into(),
        ));
    }
    verify_entries(&mut tx, &receipt).await?;
    tx.commit().await?;
    Ok(())
}

async fn verify_entries(
    tx: &mut Transaction<'_, Postgres>,
    receipt: &Receipt,
) -> Result<(), SalesRepriceError> {
    let ids = receipt
        .plan
        .entries
        .iter()
        .map(|entry| entry.record_id)
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        r#"
        SELECT r.id, r.order_id, r.canonical_event_key, r.source_payload_sha256,
               r.inviter_id, r.invitee_id, r.order_time, r.credited_quota,
               r.rebate_rate::TEXT AS record_rate, r.rebate_amount, r.admin_adjusted,
               snapshot.rule_id, snapshot.rebate_rate::TEXT AS snapshot_rate,
               snapshot.rebate_quota AS snapshot_amount, snapshot.credited_quota AS snapshot_credit
        FROM rebate_records r
        JOIN rebate_calculation_snapshots snapshot ON snapshot.rebate_record_id=r.id
        WHERE r.id=ANY($1) ORDER BY r.id
    "#,
    )
    .bind(&ids)
    .fetch_all(&mut **tx)
    .await?;
    if rows.len() != EXPECTED_COUNT {
        return Err(SalesRepriceError::Verification(
            "record count differs".into(),
        ));
    }
    for (row, entry) in rows.iter().zip(&receipt.plan.entries) {
        if row.try_get::<i64, _>("id")? != entry.record_id
            || row.try_get::<i64, _>("order_id")? != entry.order_id
            || row.try_get::<String, _>("canonical_event_key")? != entry.canonical_event_key
            || row.try_get::<String, _>("source_payload_sha256")? != entry.source_payload_sha256
            || row.try_get::<i64, _>("inviter_id")? != entry.inviter_id
            || row.try_get::<i64, _>("invitee_id")? != entry.invitee_id
            || row.try_get::<i64, _>("order_time")? != entry.order_time
            || row.try_get::<i64, _>("credited_quota")? != entry.credited_quota
            || row
                .try_get::<String, _>("record_rate")?
                .parse::<Decimal>()
                .ok()
                != entry.target_rate.parse::<Decimal>().ok()
            || row.try_get::<i64, _>("rebate_amount")? != entry.target_amount
            || !row.try_get::<bool, _>("admin_adjusted")?
            || row.try_get::<Option<i64>, _>("rule_id")? != entry.snapshot_rule_id
            || row
                .try_get::<String, _>("snapshot_rate")?
                .parse::<Decimal>()
                .ok()
                != entry.old_rate.parse::<Decimal>().ok()
            || row.try_get::<i64, _>("snapshot_amount")? != entry.old_amount
            || row.try_get::<i64, _>("snapshot_credit")? != entry.credited_quota
        {
            return Err(SalesRepriceError::Verification(format!(
                "record {} differs from correction receipt",
                entry.record_id
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use sqlx::Executor;

    use super::*;

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn reprice_is_atomic_audited_and_repeatable() {
        let url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL");
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .unwrap();
        let schema = format!(
            "sales_reprice_{}",
            ulid::Ulid::new().to_string().to_lowercase()
        );
        admin
            .execute(format!("CREATE SCHEMA {schema}").as_str())
            .await
            .unwrap();
        let path = schema.clone();
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .after_connect(move |conn, _| {
                let statement = format!("SET search_path TO {path}");
                Box::pin(async move { conn.execute(statement.as_str()).await.map(|_| ()) })
            })
            .connect(&url)
            .await
            .unwrap();
        sqlx::raw_sql(r#"
            CREATE TABLE system_config(key TEXT PRIMARY KEY,value TEXT,description TEXT,updated_at BIGINT);
            CREATE TABLE rebate_rules(id BIGINT,user_group TEXT,rule_type TEXT,rebate_rate NUMERIC,effective_time BIGINT);
            CREATE TABLE sales_accounts(user_id BIGINT PRIMARY KEY);
            CREATE TABLE sales_account_events(id BIGINT,user_id BIGINT,active BOOLEAN,created_at BIGINT);
            CREATE TABLE rebate_records(id BIGINT PRIMARY KEY,order_id BIGINT,order_type TEXT,
                canonical_event_key TEXT,source_payload_sha256 TEXT,inviter_id BIGINT,invitee_id BIGINT,
                user_group TEXT,order_time BIGINT,credited_quota BIGINT,order_amount_cents BIGINT,
                rebate_rate NUMERIC,rebate_amount BIGINT,status TEXT,admin_adjusted BOOLEAN,
                superseded_by_record_id BIGINT,updated_at BIGINT);
            CREATE TABLE rebate_calculation_snapshots(rebate_record_id BIGINT,rule_id BIGINT,
                inviter_group TEXT,rule_type TEXT,rebate_rate NUMERIC,rebate_quota BIGINT,
                credited_quota BIGINT,order_amount_minor BIGINT);
            CREATE TABLE rebate_source_event_receipts(event_key TEXT,rebate_record_id BIGINT,
                outcome TEXT,source_payload_sha256 TEXT);
            CREATE TABLE rebate_requests(id BIGINT);
            CREATE TABLE rebate_request_items(rebate_record_id BIGINT);
            CREATE TABLE rebate_record_request_ownership(rebate_record_id BIGINT);
            CREATE TABLE invitation_rebate_payouts(rebate_record_id BIGINT);
            CREATE TABLE balance_operations(rebate_record_id BIGINT);
            CREATE TABLE rebate_reconciliation_cases(source_order_id BIGINT,case_state TEXT);
            INSERT INTO rebate_rules VALUES(1,'__sales__','topup',0.1500,1700000100);
            INSERT INTO sales_accounts VALUES(7);
            INSERT INTO sales_account_events VALUES(1,7,TRUE,1699999999);
            INSERT INTO rebate_records
            SELECT 1000+n,10000+n,'topup','newapi:topup:'||(10000+n)::TEXT,
                   repeat('a',64),7,2000+n,'default',1700000000,
                   CASE WHEN n=21 THEN 48000000 ELSE 5000000 END,10000,0.0500,
                   CASE WHEN n=21 THEN 2400000 ELSE 250000 END,
                   'pending',FALSE,NULL,1700000000
            FROM generate_series(1,21) n;
            INSERT INTO rebate_calculation_snapshots
            SELECT id,2,'default','topup',0.0500,rebate_amount,credited_quota,10000
            FROM rebate_records;
            INSERT INTO rebate_source_event_receipts
            SELECT canonical_event_key,id,'created',source_payload_sha256 FROM rebate_records;
        "#).execute(&pool).await.unwrap();

        let plan = plan(&pool, 9319, 999).await.unwrap();
        let envelope = Envelope {
            sha256: digest(&plan).unwrap(),
            plan,
        };
        validate_plan(&envelope.plan, &envelope.sha256).unwrap();
        let backup = "b".repeat(64);
        sqlx::query("INSERT INTO rebate_request_items VALUES(1001)")
            .execute(&pool)
            .await
            .unwrap();
        assert!(matches!(
            apply(&pool, &envelope, &backup).await,
            Err(SalesRepriceError::SourceChanged(_))
        ));
        assert!(load_verified_receipt(&pool).await.unwrap().is_none());
        sqlx::query("DELETE FROM rebate_request_items")
            .execute(&pool)
            .await
            .unwrap();
        assert!(!apply(&pool, &envelope, &backup).await.unwrap());
        assert!(apply(&pool, &envelope, &backup).await.unwrap());
        assert!(matches!(
            apply(&pool, &envelope, &"c".repeat(64)).await,
            Err(SalesRepriceError::SourceChanged(_))
        ));
        verify(&pool, &envelope).await.unwrap();
        let values: (i64, String, bool, i64, String) = sqlx::query_as(
            "SELECT r.rebate_amount,r.rebate_rate::TEXT,r.admin_adjusted,s.rebate_quota,s.rebate_rate::TEXT FROM rebate_records r JOIN rebate_calculation_snapshots s ON s.rebate_record_id=r.id WHERE r.id=1001",
        ).fetch_one(&pool).await.unwrap();
        assert_eq!(
            values,
            (750_000, "0.1500".into(), true, 250_000, "0.0500".into())
        );

        sqlx::query("UPDATE rebate_records SET rebate_amount=rebate_amount+1 WHERE id=1001")
            .execute(&pool)
            .await
            .unwrap();
        assert!(matches!(
            verify(&pool, &envelope).await,
            Err(SalesRepriceError::Verification(_))
        ));
        sqlx::query("UPDATE rebate_records SET rebate_amount=750000 WHERE id=1001")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE system_config SET value=replace(value,'0.1500','0.1600') WHERE key=$1")
            .bind(RECEIPT_KEY)
            .execute(&pool)
            .await
            .unwrap();
        assert!(load_verified_receipt(&pool).await.is_err());

        pool.close().await;
        admin
            .execute(format!("DROP SCHEMA {schema} CASCADE").as_str())
            .await
            .unwrap();
        admin.close().await;
    }
}
