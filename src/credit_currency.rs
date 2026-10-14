use std::collections::BTreeMap;
use std::io::Read;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{Executor, PgPool, Postgres, Row, Transaction};

pub const RECEIPT_KEY: &str = "kkai_usd_credit_migration";
pub const LEGACY: &str = "legacy_075";
pub const USD: &str = "usd_credit_v1";
const MONEY_CONFIG: &[&str] = &[
    "min_rebate_request_amount",
    "invitation_signup_reward_amount",
    "invitation_signup_inviter_reward_amount",
    "invitation_signup_invitee_reward_amount",
];
// All quota projections move together. Original event receipts, operation payloads,
// operation hashes, and paid payout/reversal identities remain immutable history.
const TABLES: &[(&str, &str, &[&str])] = &[
    ("rebate_records", "id", &["rebate_amount", "credited_quota"]),
    ("rebate_requests", "id", &["total_amount"]),
    (
        "rebate_calculation_snapshots",
        "rebate_record_id",
        &["rebate_quota", "credited_quota"],
    ),
    (
        "rebate_request_money_snapshots",
        "rebate_request_id",
        &["total_quota"],
    ),
];

type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct MoneyRow {
    id: i64,
    values: BTreeMap<String, Option<i64>>,
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct RequestItem {
    request_id: i64,
    rebate_record_id: i64,
    membership_state: String,
    amount_quota_snapshot: i64,
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    items: Vec<RequestItem>,
    tables: BTreeMap<String, Vec<MoneyRow>>,
    config: BTreeMap<String, String>,
    max_operation_id: i64,
    operation_count: i64,
    max_payout_id: i64,
    payout_count: i64,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    version: u32,
    migration_id: String,
    source_epoch: String,
    target_epoch: String,
    numerator: i64,
    denominator: i64,
    before: Snapshot,
    after: Snapshot,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    plan: Plan,
    sha256: String,
}
#[derive(Debug, Deserialize, Serialize)]
struct Receipt {
    applied_at: i64,
    state: String,
    envelope: Envelope,
    backup_sha256: String,
    newapi_plan_hash: String,
}

pub async fn current_epoch(pool: &PgPool) -> Result<&'static str, sqlx::Error> {
    Ok(if boundary(pool).await?.is_some() {
        USD
    } else {
        LEGACY
    })
}

pub fn convert(value: i64) -> i64 {
    let n = i128::from(value);
    let rounded = (n.abs() * 3 + 20) / 40;
    if n < 0 {
        -(rounded as i64)
    } else {
        rounded as i64
    }
}

fn convert_snapshot(source: &Snapshot) -> Result<Snapshot, Error> {
    let mut target = source.clone();
    for rows in target.tables.values_mut() {
        for row in rows {
            for value in row.values.values_mut() {
                *value = value.map(convert);
            }
        }
    }
    let mut totals = BTreeMap::<i64, i64>::new();
    for item in &mut target.items {
        item.amount_quota_snapshot = convert(item.amount_quota_snapshot);
        if item.membership_state == "active" {
            let total = totals.entry(item.request_id).or_default();
            *total = total
                .checked_add(item.amount_quota_snapshot)
                .ok_or("request total overflow")?;
        }
    }
    // Derive request totals from each converted entitlement, avoiding a second
    // rounding of their aggregate that would break exact accounting equality.
    if let Some(rows) = target.tables.get_mut("rebate_requests") {
        for row in rows {
            row.values.insert(
                "total_amount".into(),
                Some(*totals.get(&row.id).unwrap_or(&0)),
            );
        }
    }
    for value in target.config.values_mut() {
        *value = convert(value.parse()?).to_string();
    }
    Ok(target)
}

async fn snapshot(tx: &mut Transaction<'_, Postgres>) -> Result<Snapshot, Error> {
    let mut tables = BTreeMap::new();
    for (table, key, columns) in TABLES {
        let sql = format!(
            "SELECT {key} AS id, {} FROM {table} ORDER BY {key}",
            columns.join(",")
        );
        let rows = sqlx::query(&sql).fetch_all(&mut **tx).await?;
        let mut money = Vec::with_capacity(rows.len());
        for row in rows {
            let mut values = BTreeMap::new();
            for column in *columns {
                values.insert(column.to_string(), row.try_get::<Option<i64>, _>(*column)?);
            }
            money.push(MoneyRow {
                id: row.try_get("id")?,
                values,
            });
        }
        tables.insert(table.to_string(), money);
    }
    let mut config = BTreeMap::new();
    for key in MONEY_CONFIG {
        let value: Option<String> =
            sqlx::query_scalar("SELECT value FROM system_config WHERE key = $1")
                .bind(key)
                .fetch_optional(&mut **tx)
                .await?;
        if let Some(value) = value {
            config.insert(key.to_string(), value);
        }
    }
    let (max_operation_id, operation_count): (i64, i64) =
        sqlx::query_as("SELECT COALESCE(max(id),0),count(*) FROM balance_operations")
            .fetch_one(&mut **tx)
            .await?;
    let (max_payout_id, payout_count): (i64, i64) =
        sqlx::query_as("SELECT COALESCE(max(id),0),count(*) FROM invitation_rebate_payouts")
            .fetch_one(&mut **tx)
            .await?;
    let item_rows = sqlx::query("SELECT request_id,rebate_record_id,membership_state,amount_quota_snapshot FROM rebate_request_items ORDER BY request_id,rebate_record_id").fetch_all(&mut **tx).await?;
    let items = item_rows
        .into_iter()
        .map(|r| {
            Ok(RequestItem {
                request_id: r.try_get("request_id")?,
                rebate_record_id: r.try_get("rebate_record_id")?,
                membership_state: r.try_get("membership_state")?,
                amount_quota_snapshot: r.try_get("amount_quota_snapshot")?,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()?;
    Ok(Snapshot {
        items,
        tables,
        config,
        max_operation_id,
        operation_count,
        max_payout_id,
        payout_count,
    })
}

fn parse_receipt(raw: &str) -> Result<Receipt, Error> {
    let receipt: Receipt = serde_json::from_str(raw)?;
    validate(&receipt.envelope)?;
    if !matches!(receipt.state.as_str(), "applied" | "restored")
        || receipt.applied_at <= 0
        || receipt.backup_sha256.len() != 64
        || !receipt.backup_sha256.bytes().all(|b| b.is_ascii_hexdigit())
        || receipt.newapi_plan_hash.is_empty()
    {
        return Err("invalid currency receipt state or identity".into());
    }
    Ok(receipt)
}

async fn stored_receipt<'e, E>(executor: E) -> Result<Option<Receipt>, Error>
where
    E: Executor<'e, Database = Postgres>,
{
    let raw: Option<String> = sqlx::query_scalar("SELECT value FROM system_config WHERE key=$1")
        .bind(RECEIPT_KEY)
        .fetch_optional(executor)
        .await?;
    raw.map(|s| parse_receipt(&s)).transpose()
}

pub struct CreditBoundary {
    pub applied_at: i64,
    pub migration_id: String,
    pub max_payout_id: i64,
    pub max_rebate_id: i64,
}

pub async fn boundary<'e, E>(executor: E) -> Result<Option<CreditBoundary>, sqlx::Error>
where
    E: Executor<'e, Database = Postgres>,
{
    Ok(stored_receipt(executor)
        .await
        .map_err(|_| sqlx::Error::Protocol("invalid credit currency receipt".into()))?
        .filter(|r| r.state == "applied")
        .map(|r| CreditBoundary {
            applied_at: r.applied_at,
            migration_id: r.envelope.plan.migration_id,
            max_payout_id: r.envelope.plan.before.max_payout_id,
            max_rebate_id: r
                .envelope
                .plan
                .before
                .tables
                .get("rebate_records")
                .and_then(|rows| rows.last())
                .map(|row| row.id)
                .unwrap_or(0),
        }))
}

pub async fn source_epoch(
    pool: &PgPool,
    operation_id: i64,
    original: Option<&str>,
) -> Result<&'static str, sqlx::Error> {
    let receipt = stored_receipt(pool)
        .await
        .map_err(|_| sqlx::Error::Protocol("invalid credit currency receipt".to_string()))?;
    let Some(receipt) = receipt.filter(|r| r.state == "applied") else {
        return Ok(LEGACY);
    };
    let id = if let Some(original) = original {
        sqlx::query_scalar::<_, i64>("SELECT id FROM balance_operations WHERE operation_ulid=$1")
            .bind(original)
            .fetch_one(pool)
            .await?
    } else {
        operation_id
    };
    Ok(if id <= receipt.envelope.plan.before.max_operation_id {
        LEGACY
    } else {
        USD
    })
}

pub async fn verify_runtime(pool: &PgPool, newapi_pool: &PgPool) -> Result<(), Error> {
    let raw: Option<String> =
        sqlx::query_scalar("SELECT value FROM options WHERE key='KKAIUSDCreditCutover'")
            .fetch_optional(newapi_pool)
            .await?;
    let local = stored_receipt(pool).await?.filter(|r| r.state == "applied");
    let external: Option<serde_json::Value> = raw
        .filter(|s| !s.is_empty())
        .map(|s| serde_json::from_str(&s))
        .transpose()?;
    match (local, external) {
        (None, None) => Ok(()),
        (Some(local), Some(external))
            if external["migration_id"].as_str()
                == Some(local.envelope.plan.migration_id.as_str())
                && external["plan_hash"].as_str() == Some(local.newapi_plan_hash.as_str())
                && external["target_epoch"].as_str() == Some(USD)
                && external["source_epoch"].as_str() == Some(LEGACY)
                && external["version"].as_i64() == Some(1)
                && external["numerator"].as_i64() == Some(3)
                && external["denominator"].as_i64() == Some(40) =>
        {
            Ok(())
        }
        _ => Err("Invitations and NewAPI currency receipts differ; keep settlement stopped".into()),
    }
}

fn digest(plan: &Plan) -> Result<String, Error> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(plan)?)))
}

fn validate(envelope: &Envelope) -> Result<(), Error> {
    let p = &envelope.plan;
    if p.version != 1
        || p.migration_id.is_empty()
        || p.migration_id.len() > 128
        || p.source_epoch != LEGACY
        || p.target_epoch != USD
        || p.numerator != 3
        || p.denominator != 40
        || envelope.sha256 != digest(p)?
        || p.after != convert_snapshot(&p.before)?
    {
        return Err("invalid credit migration plan or checksum".into());
    }
    Ok(())
}

async fn write_snapshot(
    tx: &mut Transaction<'_, Postgres>,
    snapshot: &Snapshot,
) -> Result<(), Error> {
    // The one immutable projection trigger is disabled only inside the locked
    // offline transaction; rollback restores it, and commit always re-enables it.
    let enabled: Option<String> = sqlx::query_scalar("SELECT tgenabled::text FROM pg_trigger WHERE tgrelid='rebate_request_items'::regclass AND tgname='trg_rebate_request_item_snapshot_immutable'")
        .fetch_optional(&mut **tx).await?;
    if enabled.as_deref() != Some("O") {
        return Err("expected immutable snapshot trigger is not enabled".into());
    }
    sqlx::query("ALTER TABLE rebate_request_items DISABLE TRIGGER trg_rebate_request_item_snapshot_immutable").execute(&mut **tx).await?;
    for item in &snapshot.items {
        let affected = sqlx::query("UPDATE rebate_request_items SET amount_quota_snapshot=$1 WHERE request_id=$2 AND rebate_record_id=$3 AND membership_state=$4")
            .bind(item.amount_quota_snapshot).bind(item.request_id).bind(item.rebate_record_id).bind(&item.membership_state)
            .execute(&mut **tx).await?.rows_affected();
        if affected != 1 {
            return Err("request item changed during migration".into());
        }
    }
    sqlx::query("ALTER TABLE rebate_request_items ENABLE TRIGGER trg_rebate_request_item_snapshot_immutable").execute(&mut **tx).await?;
    for (table, key, columns) in TABLES {
        let rows = snapshot.tables.get(*table).ok_or("missing money table")?;
        for row in rows {
            for column in *columns {
                let value = row.values.get(*column).ok_or("missing quota field")?;
                let sql = format!("UPDATE {table} SET {column}=$1 WHERE {key}=$2");
                let count = sqlx::query(&sql)
                    .bind(value)
                    .bind(row.id)
                    .execute(&mut **tx)
                    .await?
                    .rows_affected();
                if count != 1 {
                    return Err("money row missing during migration".into());
                }
            }
        }
    }
    for (key, value) in &snapshot.config {
        let count = sqlx::query("UPDATE system_config SET value=$1 WHERE key=$2")
            .bind(value)
            .bind(key)
            .execute(&mut **tx)
            .await?
            .rows_affected();
        if count != 1 {
            return Err("money configuration missing".into());
        }
    }
    Ok(())
}

async fn ensure_sales_currency_restore_safe(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<(), Error> {
    let usd_issued: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM sales_disbursements WHERE credit_currency='usd_credit_v1')",
    )
    .fetch_one(&mut **tx)
    .await?;
    if usd_issued {
        return Err("sales issuance occurred in USD credits; currency restore is unsafe".into());
    }
    Ok(())
}

// The existing maintenance operator owns service shutdown and backup receipt.
// This command owns one PostgreSQL transaction, exact drift checks and receipts.
// Plan/verify are read-only. Only the immutable projection trigger is toggled transactionally; NewAPI remains read-only.
pub async fn run(args: &[String]) -> Result<String, Error> {
    let command = args
        .first()
        .map(String::as_str)
        .ok_or("expected credit-migrate plan|apply|verify|restore")?;
    let mut args = args.iter().skip(1);
    let mut id = String::new();
    let mut confirmed = String::new();
    let mut backup = String::new();
    let mut maintenance = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--migration-id" => id = args.next().ok_or("missing migration ID")?.clone(),
            "--confirm-sha256" => confirmed = args.next().ok_or("missing plan hash")?.clone(),
            "--backup-sha256" => backup = args.next().ok_or("missing backup hash")?.clone(),
            "--maintenance" => maintenance = true,
            _ => return Err("unknown credit migration argument".into()),
        }
    }
    if !matches!(command, "plan" | "apply" | "verify" | "restore") {
        return Err("unknown credit migration command".into());
    }
    let mut envelope = None;
    if command != "plan" {
        let mut input = String::new();
        std::io::stdin()
            .take(32 * 1024 * 1024)
            .read_to_string(&mut input)?;
        let parsed: Envelope = serde_json::from_str(&input)?;
        validate(&parsed)?;
        if confirmed != parsed.sha256 {
            return Err("exact plan hash confirmation required".into());
        }
        envelope = Some(parsed);
    }
    let mutating = matches!(command, "apply" | "restore");
    if mutating
        && (!maintenance
            || crate::config::settlement_mode_from_env()?.is_enabled()
            || backup.len() != 64
            || !backup.bytes().all(|b| b.is_ascii_hexdigit()))
    {
        return Err("offline maintenance, disabled settlement and backup SHA256 required".into());
    }
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&crate::config::invitations_database_url_from_env()?)
        .await?;
    let mut tx = pool.begin().await?;
    if !mutating {
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await?;
    }
    sqlx::query("SET LOCAL lock_timeout='15s'")
        .execute(&mut *tx)
        .await?;
    sqlx::query("SET LOCAL statement_timeout='60s'")
        .execute(&mut *tx)
        .await?;
    if mutating {
        sqlx::query("SELECT pg_advisory_xact_lock(1263358281,20260711)")
            .execute(&mut *tx)
            .await?;
        sqlx::query("LOCK TABLE rebate_records,rebate_requests,rebate_calculation_snapshots,rebate_request_money_snapshots,rebate_request_items,system_config,balance_operations,balance_operation_outbox,invitation_rebate_payouts,sales_disbursements IN EXCLUSIVE MODE").execute(&mut *tx).await?;
        let pending: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM balance_operation_outbox WHERE dispatched_at IS NULL",
        )
        .fetch_one(&mut *tx)
        .await?;
        if pending != 0 {
            return Err("undrained balance outbox; do not migrate ambiguous credits".into());
        }
    }
    if command == "restore" {
        ensure_sales_currency_restore_safe(&mut tx).await?;
    }
    let current = snapshot(&mut tx).await?;
    let stored: Option<String> = sqlx::query_scalar("SELECT value FROM system_config WHERE key=$1")
        .bind(RECEIPT_KEY)
        .fetch_optional(&mut *tx)
        .await?;
    if command == "plan" {
        if stored.is_some() || id.is_empty() {
            return Err("migration ID required and no prior receipt may exist".into());
        }
        let plan = Plan {
            version: 1,
            migration_id: id,
            source_epoch: LEGACY.into(),
            target_epoch: USD.into(),
            numerator: 3,
            denominator: 40,
            after: convert_snapshot(&current)?,
            before: current,
        };
        let result = Envelope {
            sha256: digest(&plan)?,
            plan,
        };
        tx.rollback().await?;
        return Ok(serde_json::to_string_pretty(&result)?);
    }
    let envelope = envelope.ok_or("missing plan")?;
    let prior: Option<Receipt> = stored.map(|s| parse_receipt(&s)).transpose()?;
    if command == "verify" {
        let prior = prior.ok_or("receipt missing")?;
        if prior.state != "applied"
            || prior.envelope.sha256 != envelope.sha256
            || current != envelope.plan.after
        {
            return Err("receipt or converted quota verification failed".into());
        }
        tx.rollback().await?;
        return Ok(serde_json::json!({"verified":true,"sha256":envelope.sha256}).to_string());
    }
    let (expected, target, state) = if command == "apply" {
        (&envelope.plan.before, &envelope.plan.after, "applied")
    } else {
        (&envelope.plan.after, &envelope.plan.before, "restored")
    };
    if let Some(prior) = &prior {
        if prior.envelope.sha256 != envelope.sha256 || prior.backup_sha256 != backup {
            return Err("another migration or backup receipt exists".into());
        }
        if prior.state == state && &current == target {
            tx.rollback().await?;
            return Ok(
                serde_json::json!({"state":state,"replayed":true,"sha256":envelope.sha256})
                    .to_string(),
            );
        }
        if command == "apply" || prior.state != "applied" {
            return Err("receipt state does not allow this transition".into());
        }
    } else if command == "restore" {
        return Err("restore requires applied receipt".into());
    }
    if &current != expected {
        return Err("currency rows or operation identities changed since plan".into());
    }
    let newapi_pool =
        crate::db::init_readonly_pool(&crate::config::newapi_database_url_from_env()?).await?;
    let raw: String =
        sqlx::query_scalar("SELECT value FROM options WHERE key='KKAIUSDCreditCutover'")
            .fetch_one(&newapi_pool)
            .await?;
    let cutover: serde_json::Value = serde_json::from_str(&raw)?;
    if cutover["version"].as_i64() != Some(1)
        || cutover["source_epoch"].as_str() != Some(LEGACY)
        || cutover["migration_id"].as_str() != Some(envelope.plan.migration_id.as_str())
        || cutover["target_epoch"].as_str() != Some(USD)
        || cutover["numerator"].as_i64() != Some(3)
        || cutover["denominator"].as_i64() != Some(40)
    {
        return Err("NewAPI currency receipt differs from this migration".into());
    }
    let newapi_plan_hash = cutover["plan_hash"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or("NewAPI plan hash missing")?
        .to_owned();
    write_snapshot(&mut tx, target).await?;
    if snapshot(&mut tx).await? != *target {
        return Err("converted quota verification failed".into());
    }
    let hash = envelope.sha256.clone();
    let receipt = Receipt {
        applied_at: cutover["applied_at"]
            .as_i64()
            .filter(|v| *v > 0)
            .ok_or("NewAPI applied_at missing")?,
        state: state.into(),
        envelope,
        backup_sha256: backup,
        newapi_plan_hash,
    };
    sqlx::query("INSERT INTO system_config(key,value,description,updated_at) VALUES($1,$2,'Offline credit migration receipt',EXTRACT(EPOCH FROM NOW())::bigint) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at")
        .bind(RECEIPT_KEY).bind(serde_json::to_string(&receipt)?).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(serde_json::json!({"state":state,"sha256":hash}).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn conversion_is_exact_signed_and_bounded() {
        for (input, want) in [
            (1330, 100),
            (13_300_000, 997_500),
            (20, 2),
            (-20, -2),
            (0, 0),
            (i64::MIN, -691752902764108186),
        ] {
            assert_eq!(convert(input), want);
        }
    }
    #[test]
    fn plan_rejects_changed_conversion_and_tampering() {
        let mut tables = BTreeMap::new();
        tables.insert(
            "rebate_records".into(),
            vec![MoneyRow {
                id: 1,
                values: BTreeMap::from([("rebate_amount".into(), Some(1330))]),
            }],
        );
        let before = Snapshot {
            items: vec![],
            tables,
            config: BTreeMap::new(),
            max_operation_id: 8,
            operation_count: 8,
            max_payout_id: 8,
            payout_count: 8,
        };
        let plan = Plan {
            version: 1,
            migration_id: "test".into(),
            source_epoch: LEGACY.into(),
            target_epoch: USD.into(),
            numerator: 3,
            denominator: 40,
            after: convert_snapshot(&before).unwrap(),
            before,
        };
        let mut envelope = Envelope {
            sha256: digest(&plan).unwrap(),
            plan,
        };
        assert!(validate(&envelope).is_ok());
        envelope
            .plan
            .after
            .tables
            .get_mut("rebate_records")
            .unwrap()[0]
            .values
            .insert("rebate_amount".into(), Some(99));
        envelope.sha256 = digest(&envelope.plan).unwrap();
        assert!(validate(&envelope).is_err());
    }
    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn credit_projection_transaction_preserves_ledger_and_restores_exactly() {
        let url = std::env::var("TEST_DATABASE_URL").unwrap();
        let admin = PgPool::connect(&url).await.unwrap();
        let schema = format!(
            "credit_test_{}",
            ulid::Ulid::new().to_string().to_lowercase()
        );
        let external_schema = format!("{}_external", schema);
        sqlx::raw_sql(&format!(
            "CREATE SCHEMA {schema}; CREATE SCHEMA {external_schema}"
        ))
        .execute(&admin)
        .await
        .unwrap();
        let search_path = schema.clone();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(3)
            .after_connect(move |c, _| {
                let sql = format!("SET search_path TO {search_path}");
                Box::pin(async move {
                    sqlx::query(&sql).execute(c).await?;
                    Ok(())
                })
            })
            .connect(&url)
            .await
            .unwrap();
        let search_path = external_schema.clone();
        let external = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .after_connect(move |c, _| {
                let sql = format!("SET search_path TO {search_path}");
                Box::pin(async move {
                    sqlx::query(&sql).execute(c).await?;
                    Ok(())
                })
            })
            .connect(&url)
            .await
            .unwrap();
        sqlx::raw_sql(include_str!("../tests/fixtures/latest_legacy.sql"))
            .execute(&pool)
            .await
            .unwrap();
        crate::db::MIGRATOR.run(&pool).await.unwrap();
        sqlx::raw_sql(include_str!("../tests/fixtures/newapi_balance_marks.sql"))
            .execute(&external)
            .await
            .unwrap();
        sqlx::raw_sql("UPDATE rebate_records SET status='completed' WHERE id=101; UPDATE rebate_requests SET status='completed',completed_at=1700000001 WHERE id=201;
            INSERT INTO invitation_rebate_payouts(id,rebate_record_id,rebate_request_id,inviter_user_id,invitee_user_id,order_id,order_type,amount_cents,quota_delta,kind,status,request_id,newapi_mark_id,created_at,updated_at,completed_at)
            VALUES(301,101,201,11,12,501,'topup',1000,5000000,'payout','completed','credit-test',401,1700000001,1700000001,1700000001);")
            .execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO kkai_invitation_rebate_balance_marks(id,kind,rebate_record_id,payout_id,user_id,quota_delta,amount_cents,request_id,created_at) VALUES(401,'payout',101,301,11,5000000,1000,'credit-test',1700000001)").execute(&external).await.unwrap();
        sqlx::raw_sql("INSERT INTO rebate_records(id,inviter_id,invitee_id,user_group,order_type,order_id,order_amount,order_time,rebate_effective_at,rebate_amount,status,scan_started_at,initialization_ends_at,created_at,updated_at) VALUES(901,11,12,'default','invite_inviter',901,0,1,1,7,'approved',1,1,1,1),(902,11,12,'default','invite_inviter',902,0,1,1,7,'approved',1,1,1,1),(903,11,12,'default','invite_inviter',903,0,1,1,5000,'approved',1,1,1,1);
            INSERT INTO rebate_requests(id,user_id,rebate_record_ids,total_amount,status,request_kind,created_at,updated_at,approved_at) VALUES(901,11,ARRAY[901,902]::BIGINT[],14,'approved','sales',1,1,1),(902,11,ARRAY[903]::BIGINT[],5000,'approved','sales',1,1,1);
            INSERT INTO sales_disbursements(request_id,user_id,amount_quota,credit_currency,record_amounts,confirmed_by,confirmed_at,note) VALUES(901,11,14,'legacy_075','{\"901\":7,\"902\":7}',99,1,'legacy paid');")
            .execute(&pool).await.unwrap();
        let original_issued: serde_json::Value = sqlx::query_scalar("SELECT row_to_json(issued)::JSONB FROM sales_disbursements issued WHERE request_id=901")
            .fetch_one(&pool).await.unwrap();
        let mut tx = pool.begin().await.unwrap();
        let before = snapshot(&mut tx).await.unwrap();
        let after = convert_snapshot(&before).unwrap();
        assert_eq!(after.items[0].amount_quota_snapshot, 375000);
        assert_eq!(
            after.tables["rebate_requests"]
                .iter()
                .find(|r| r.id == 901)
                .unwrap()
                .values["total_amount"],
            Some(2),
            "sum converted item amounts instead of rounding the aggregate"
        );
        write_snapshot(&mut tx, &after).await.unwrap();
        assert_eq!(snapshot(&mut tx).await.unwrap(), after);
        tx.rollback().await.unwrap();
        let mut tx = pool.begin().await.unwrap();
        assert_eq!(snapshot(&mut tx).await.unwrap(), before);
        write_snapshot(&mut tx, &after).await.unwrap();
        let plan = Plan {
            version: 1,
            migration_id: "credit-test".into(),
            source_epoch: LEGACY.into(),
            target_epoch: USD.into(),
            numerator: 3,
            denominator: 40,
            before: before.clone(),
            after: after.clone(),
        };
        let receipt = Receipt {
            applied_at: 1700000002,
            state: "applied".into(),
            envelope: Envelope {
                sha256: digest(&plan).unwrap(),
                plan,
            },
            backup_sha256: "a".repeat(64),
            newapi_plan_hash: "newapi-plan".into(),
        };
        sqlx::query("INSERT INTO system_config(key,value,updated_at) VALUES($1,$2,1)")
            .bind(RECEIPT_KEY)
            .bind(serde_json::to_string(&receipt).unwrap())
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert!(
            sqlx::query(
                "UPDATE rebate_request_items SET amount_quota_snapshot=1 WHERE request_id=201"
            )
            .execute(&pool)
            .await
            .is_err(),
            "snapshot trigger must be enabled after commit"
        );
        let original: i64 =
            sqlx::query_scalar("SELECT quota_delta FROM invitation_rebate_payouts WHERE id=301")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            original, 5000000,
            "original payout identity must never change"
        );
        let mut local_connection = pool.acquire().await.unwrap();
        let mut external_connection = external.acquire().await.unwrap();
        let report = crate::preflight::run(
            &mut local_connection,
            &mut external_connection,
            crate::preflight::PreflightMode::ReportOnly,
        )
        .await
        .unwrap();
        assert!(report.safe_to_migrate, "{report:?}");
        drop(local_connection);
        drop(external_connection);
        let converted_issued: serde_json::Value = sqlx::query_scalar("SELECT row_to_json(issued)::JSONB FROM sales_disbursements issued WHERE request_id=901")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(
            converted_issued, original_issued,
            "sales issuance evidence is never rewritten during conversion"
        );
        let mut tx = pool.begin().await.unwrap();
        ensure_sales_currency_restore_safe(&mut tx).await.unwrap();
        sqlx::query("INSERT INTO sales_disbursements(request_id,user_id,amount_quota,credit_currency,record_amounts,confirmed_by,confirmed_at,note) VALUES(902,11,375,'usd_credit_v1','{\"903\":375}',99,1700000003,'USD paid')")
            .execute(&mut *tx).await.unwrap();
        assert_eq!(
            snapshot(&mut tx).await.unwrap(),
            after,
            "external issuance changes audit facts even without changing money projections"
        );
        assert!(ensure_sales_currency_restore_safe(&mut tx)
            .await
            .unwrap_err()
            .to_string()
            .contains("currency restore is unsafe"));
        tx.rollback().await.unwrap();
        let mut tx = pool.begin().await.unwrap();
        write_snapshot(&mut tx, &before).await.unwrap();
        assert_eq!(snapshot(&mut tx).await.unwrap(), before);
        let restored_issued: serde_json::Value = sqlx::query_scalar("SELECT row_to_json(issued)::JSONB FROM sales_disbursements issued WHERE request_id=901")
            .fetch_one(&mut *tx).await.unwrap();
        assert_eq!(restored_issued, original_issued);
        tx.commit().await.unwrap();
        assert!(sqlx::query(
            "UPDATE rebate_request_items SET amount_quota_snapshot=1 WHERE request_id=201"
        )
        .execute(&pool)
        .await
        .is_err());
        pool.close().await;
        external.close().await;
        sqlx::raw_sql(&format!(
            "DROP SCHEMA {schema} CASCADE; DROP SCHEMA {external_schema} CASCADE"
        ))
        .execute(&admin)
        .await
        .unwrap();
        admin.close().await;
    }
}
