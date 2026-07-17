use std::collections::{HashMap, HashSet};
use std::io::Read;

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres, Row, Transaction};

use crate::config::{self, SettlementMode};
use crate::rebate_lifecycle::{
    ALL_USER_GROUP, INITIALIZATION_SECONDS, REBATE_EFFECTIVE_DELAY_SECONDS,
};
use crate::rebate_source_events::{
    attach_legacy_rebate_source_event_in_transaction, calculate_rebate_from_credited_quota,
    payload_hash_for_event, IngestError, IngestOutcome, RebateSourceEvent,
};
use crate::rebate_types::TOPUP_ORDER_TYPE;

const MANIFEST_VERSION: i32 = 2;
const SOURCE_NAMESPACE: &str = "newapi";

#[derive(Debug, thiserror::Error)]
pub enum ReconciliationCliError {
    #[error("invalid command: {0}")]
    InvalidCommand(String),
    #[error("invalid manifest: {0}")]
    InvalidManifest(String),
    #[error("source data changed: {0}")]
    SourceChanged(String),
    #[error("verification failed: {0}")]
    Verification(String),
    #[error("configuration error: {0}")]
    Configuration(String),
    #[error("completion evidence required: {0}")]
    CompletionEvidenceRequired(String),
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("event ingest error: {0}")]
    Ingest(#[from] IngestError),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("stdin read error: {0}")]
    Stdin(#[from] std::io::Error),
}

impl ReconciliationCliError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidCommand(_) => "invalid_command",
            Self::InvalidManifest(_) => "invalid_manifest",
            Self::SourceChanged(_) => "source_data_changed",
            Self::Verification(_) => "verification_failed",
            Self::Configuration(_) => "configuration_error",
            Self::CompletionEvidenceRequired(_) => "completion_evidence_required",
            Self::Database(_) => "database_error",
            Self::Ingest(_) => "event_ingest_error",
            Self::Serialization(_) => "serialization_error",
            Self::Stdin(_) => "stdin_read_error",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReconciliationManifest {
    pub manifest_version: i32,
    pub source_namespace: String,
    pub generated_at: i64,
    pub cutoff_topup_id: i64,
    pub quota_per_unit: i64,
    pub orders: Vec<ManifestOrder>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestOrder {
    pub source_order_id: i64,
    pub invitee_id: i64,
    pub inviter_id: i64,
    pub inviter_group: String,
    pub order_amount_cents: Option<i64>,
    pub credited_quota: i64,
    pub credited_quota_source_field: String,
    pub credited_quota_source_value: String,
    pub completed_at: i64,
    pub payment_provider: String,
    pub source_row_sha256: String,
    pub existing_record: Option<ManifestRecordState>,
    pub rebate_rate: String,
    pub expected_rebate_quota: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestRecordState {
    pub record_id: i64,
    pub status: String,
    pub canonical_event_key: Option<String>,
    pub credited_quota: Option<i64>,
    pub rebate_amount: i64,
    pub rebate_rate: Option<String>,
    pub order_time: i64,
    pub initialization_ends_at: i64,
    pub rebate_effective_at: i64,
    pub admin_adjusted: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ManifestEnvelope {
    manifest: ReconciliationManifest,
    sha256: String,
}

#[derive(Debug, Serialize)]
struct ApplyReport {
    ok: bool,
    manifest_sha256: String,
    order_count: usize,
    created: usize,
    replayed: usize,
    ignored: usize,
}

#[derive(Debug, Serialize)]
struct VerifyReport {
    ok: bool,
    manifest_sha256: String,
    order_count: usize,
    verified: usize,
}

#[derive(Clone)]
struct SourceOrder {
    source_order_id: i64,
    invitee_id: i64,
    inviter_id: i64,
    inviter_group: String,
    amount_text: String,
    money_text: String,
    completed_at: i64,
    payment_provider: String,
    trade_no: String,
    status: String,
}

#[derive(Serialize)]
struct SourceRowIdentity<'a> {
    source_order_id: i64,
    invitee_id: i64,
    inviter_id: i64,
    inviter_group: &'a str,
    credited_quota_source_field: &'a str,
    credited_quota_source_value: &'a str,
    credited_quota: i64,
    completed_at: i64,
    payment_provider: &'a str,
    trade_no: &'a str,
    status: &'a str,
}

struct CreditedQuotaEvidence {
    source_field: &'static str,
    source_value: String,
    credited_quota: i64,
}

pub async fn run(args: &[String]) -> Result<String, ReconciliationCliError> {
    let command = args.first().map(String::as_str).ok_or_else(|| {
        ReconciliationCliError::InvalidCommand(
            "usage: invitations reconcile <plan|apply|verify>".to_string(),
        )
    })?;
    let settlement_mode = std::env::var("SETTLEMENT_MODE").ok();
    let settlement_mode = validate_reconcile_settlement_mode(command, settlement_mode.as_deref())?;
    let invitations_url = config::invitations_database_url_from_env()
        .map_err(ReconciliationCliError::Configuration)?;
    let newapi_url =
        config::newapi_database_url_from_env().map_err(ReconciliationCliError::Configuration)?;
    let quota_per_unit = config::quota_per_unit_from_env();
    let invitations_pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&invitations_url)
        .await?;
    let newapi_pool = crate::db::init_readonly_pool(&newapi_url).await?;

    match command {
        "plan" => {
            let cutoff = required_positive_i64_flag(args, "--cutoff-topup-id")?;
            let manifest = plan(&invitations_pool, &newapi_pool, quota_per_unit, cutoff).await?;
            let envelope = envelope(manifest)?;
            Ok(serde_json::to_string_pretty(&envelope)?)
        }
        "apply" => {
            let expected = required_sha256_flag(args)?;
            let envelope = read_manifest_from_stdin()?;
            validate_envelope(&envelope, &expected)?;
            let report = apply(&invitations_pool, &newapi_pool, envelope, settlement_mode).await?;
            Ok(serde_json::to_string_pretty(&report)?)
        }
        "verify" => {
            let expected = required_sha256_flag(args)?;
            let envelope = read_manifest_from_stdin()?;
            validate_envelope(&envelope, &expected)?;
            let report = verify(&invitations_pool, &newapi_pool, envelope).await?;
            Ok(serde_json::to_string_pretty(&report)?)
        }
        value => Err(ReconciliationCliError::InvalidCommand(format!(
            "unsupported reconcile command: {value}"
        ))),
    }
}

fn validate_reconcile_settlement_mode(
    command: &str,
    raw_value: Option<&str>,
) -> Result<SettlementMode, ReconciliationCliError> {
    let mode = config::settlement_mode_from_value(raw_value)
        .map_err(ReconciliationCliError::Configuration)?;
    if command == "apply" && (mode != SettlementMode::ReadOnly || raw_value != Some("read_only")) {
        return Err(ReconciliationCliError::Configuration(
            "reconcile apply requires SETTLEMENT_MODE=read_only exactly".to_string(),
        ));
    }
    Ok(mode)
}

pub async fn plan(
    invitations_pool: &PgPool,
    newapi_pool: &PgPool,
    quota_per_unit: i64,
    cutoff_topup_id: i64,
) -> Result<ReconciliationManifest, ReconciliationCliError> {
    let source_orders = load_source_orders(newapi_pool, Some(cutoff_topup_id), None).await?;
    let mut rules = HashMap::new();
    let mut orders = Vec::with_capacity(source_orders.len());
    for source in source_orders {
        let credit_evidence = credited_quota_evidence(&source, quota_per_unit)?;
        let rate = match rules.get(&source.inviter_group) {
            Some(rate) => *rate,
            None => {
                let rate = load_rule_rate(invitations_pool, &source.inviter_group).await?;
                rules.insert(source.inviter_group.clone(), rate);
                rate
            }
        };
        let expected_rebate_quota =
            calculate_rebate_from_credited_quota(credit_evidence.credited_quota, rate)?;
        let existing_record =
            load_existing_record(invitations_pool, source.source_order_id).await?;
        validate_existing_record_for_plan(source.source_order_id, existing_record.as_ref())?;
        orders.push(ManifestOrder {
            source_order_id: source.source_order_id,
            invitee_id: source.invitee_id,
            inviter_id: source.inviter_id,
            inviter_group: source.inviter_group.clone(),
            order_amount_cents: None,
            credited_quota: credit_evidence.credited_quota,
            credited_quota_source_field: credit_evidence.source_field.to_string(),
            credited_quota_source_value: credit_evidence.source_value.clone(),
            completed_at: source.completed_at,
            payment_provider: source.payment_provider.clone(),
            source_row_sha256: source_row_sha256(&source, &credit_evidence)?,
            existing_record,
            rebate_rate: rate.to_string(),
            expected_rebate_quota,
        });
    }
    Ok(ReconciliationManifest {
        manifest_version: MANIFEST_VERSION,
        source_namespace: SOURCE_NAMESPACE.to_string(),
        generated_at: chrono::Utc::now().timestamp(),
        cutoff_topup_id,
        quota_per_unit,
        orders,
    })
}

async fn apply(
    invitations_pool: &PgPool,
    newapi_pool: &PgPool,
    envelope: ManifestEnvelope,
    settlement_mode: SettlementMode,
) -> Result<ApplyReport, ReconciliationCliError> {
    if settlement_mode != SettlementMode::ReadOnly {
        return Err(ReconciliationCliError::Configuration(
            "reconcile apply requires read_only settlement mode".to_string(),
        ));
    }
    revalidate_source(newapi_pool, &envelope.manifest).await?;
    let mut transaction = invitations_pool.begin().await?;
    sqlx::query("LOCK TABLE rebate_rules IN SHARE MODE")
        .execute(&mut *transaction)
        .await?;
    revalidate_manifest_rules(&mut transaction, &envelope.manifest).await?;
    revalidate_existing_records(&mut transaction, &envelope.manifest).await?;
    let mut report = ApplyReport {
        ok: true,
        manifest_sha256: envelope.sha256,
        order_count: envelope.manifest.orders.len(),
        created: 0,
        replayed: 0,
        ignored: 0,
    };
    for order in &envelope.manifest.orders {
        match attach_legacy_rebate_source_event_in_transaction(
            &mut transaction,
            manifest_event(order),
            envelope.manifest.quota_per_unit,
        )
        .await?
        {
            IngestOutcome::Created => report.created += 1,
            IngestOutcome::Replay => report.replayed += 1,
            IngestOutcome::IgnoredNoInviter => report.ignored += 1,
        }
    }
    revalidate_source(newapi_pool, &envelope.manifest).await?;
    transaction.commit().await?;
    Ok(report)
}

async fn verify(
    invitations_pool: &PgPool,
    newapi_pool: &PgPool,
    envelope: ManifestEnvelope,
) -> Result<VerifyReport, ReconciliationCliError> {
    revalidate_source(newapi_pool, &envelope.manifest).await?;
    for order in &envelope.manifest.orders {
        verify_order(invitations_pool, order, envelope.manifest.quota_per_unit).await?;
    }
    Ok(VerifyReport {
        ok: true,
        manifest_sha256: envelope.sha256,
        order_count: envelope.manifest.orders.len(),
        verified: envelope.manifest.orders.len(),
    })
}

#[cfg(test)]
pub(crate) async fn apply_for_test(
    invitations_pool: &PgPool,
    newapi_pool: &PgPool,
    manifest: ReconciliationManifest,
) -> Result<(usize, usize), ReconciliationCliError> {
    let report = apply(
        invitations_pool,
        newapi_pool,
        envelope(manifest)?,
        SettlementMode::ReadOnly,
    )
    .await?;
    Ok((report.created, report.replayed))
}

#[cfg(test)]
pub(crate) async fn verify_for_test(
    invitations_pool: &PgPool,
    newapi_pool: &PgPool,
    manifest: ReconciliationManifest,
) -> Result<usize, ReconciliationCliError> {
    let report = verify(invitations_pool, newapi_pool, envelope(manifest)?).await?;
    Ok(report.verified)
}

async fn load_source_orders(
    pool: &PgPool,
    cutoff_topup_id: Option<i64>,
    order_ids: Option<&[i64]>,
) -> Result<Vec<SourceOrder>, ReconciliationCliError> {
    let rows = sqlx::query(
        r#"
        SELECT t.id,
               t.user_id AS invitee_id,
               invitee.inviter_id,
               COALESCE(NULLIF(inviter."group", ''), 'default') AS inviter_group,
               t.amount::TEXT AS amount_text,
               t.money::TEXT AS money_text,
               t.complete_time AS completed_at,
               COALESCE(t.payment_provider, '') AS payment_provider,
               t.trade_no,
               t.status
        FROM top_ups t
        JOIN users invitee ON invitee.id = t.user_id
        JOIN users inviter ON inviter.id = invitee.inviter_id
        WHERE t.status = 'success'
          AND invitee.inviter_id IS NOT NULL
          AND invitee.inviter_id > 0
          AND ($1::BIGINT IS NULL OR t.id <= $1)
          AND ($2::BIGINT[] IS NULL OR t.id = ANY($2))
          AND NOT EXISTS (
              SELECT 1 FROM subscription_orders subscription
              WHERE subscription.trade_no = t.trade_no
          )
        ORDER BY t.id
        "#,
    )
    .bind(cutoff_topup_id)
    .bind(order_ids)
    .fetch_all(pool)
    .await?;
    let orders = rows
        .into_iter()
        .map(|row| {
            Ok(SourceOrder {
                source_order_id: row.try_get("id")?,
                invitee_id: row.try_get("invitee_id")?,
                inviter_id: row.try_get("inviter_id")?,
                inviter_group: row.try_get("inviter_group")?,
                amount_text: row.try_get("amount_text")?,
                money_text: row.try_get("money_text")?,
                completed_at: row.try_get("completed_at")?,
                payment_provider: row
                    .try_get::<String, _>("payment_provider")?
                    .trim()
                    .to_string(),
                trade_no: row.try_get("trade_no")?,
                status: row.try_get("status")?,
            })
        })
        .collect::<Result<Vec<_>, ReconciliationCliError>>()?;
    validate_completion_evidence(&orders)?;
    Ok(orders)
}

async fn revalidate_source(
    newapi_pool: &PgPool,
    manifest: &ReconciliationManifest,
) -> Result<(), ReconciliationCliError> {
    validate_manifest(manifest)?;
    let order_ids = manifest
        .orders
        .iter()
        .map(|order| order.source_order_id)
        .collect::<Vec<_>>();
    let rows = load_source_orders(
        newapi_pool,
        Some(manifest.cutoff_topup_id),
        Some(&order_ids),
    )
    .await?;
    if rows.len() != manifest.orders.len() {
        return Err(ReconciliationCliError::SourceChanged(
            "source order count changed".to_string(),
        ));
    }
    let actual = rows
        .iter()
        .map(|row| {
            let credit_evidence = credited_quota_evidence(row, manifest.quota_per_unit)?;
            Ok((
                row.source_order_id,
                source_row_sha256(row, &credit_evidence)?,
            ))
        })
        .collect::<Result<HashMap<_, _>, ReconciliationCliError>>()?;
    for order in &manifest.orders {
        if actual.get(&order.source_order_id) != Some(&order.source_row_sha256) {
            return Err(ReconciliationCliError::SourceChanged(format!(
                "topup {} no longer matches manifest",
                order.source_order_id
            )));
        }
    }
    Ok(())
}

async fn verify_order(
    pool: &PgPool,
    order: &ManifestOrder,
    quota_per_unit: i64,
) -> Result<(), ReconciliationCliError> {
    let event_key = format!("newapi:topup:{}", order.source_order_id);
    let expected_payload_hash = payload_hash_for_event(manifest_event(order))?;
    let rows = sqlx::query(
        r#"
        SELECT record.inviter_id, record.invitee_id, record.user_group,
               record.order_amount_cents, record.credited_quota, record.order_time,
               record.initialization_ends_at, record.rebate_effective_at,
               record.rebate_amount, record.rebate_rate::TEXT AS record_rate,
               record.admin_adjusted,
               record.source_payload_sha256, record.source_schema_version,
               snapshot.inviter_group AS snapshot_group,
               snapshot.rebate_rate::TEXT AS snapshot_rate,
               snapshot.order_amount_minor, snapshot.credited_quota AS snapshot_credited_quota,
               snapshot.rebate_quota,
               snapshot.quota_per_unit
        FROM rebate_records record
        JOIN rebate_calculation_snapshots snapshot ON snapshot.rebate_record_id = record.id
        WHERE record.canonical_event_key = $1
        "#,
    )
    .bind(&event_key)
    .fetch_all(pool)
    .await?;
    if rows.len() != 1 {
        return Err(ReconciliationCliError::Verification(format!(
            "{event_key} expected exactly one record, got {}",
            rows.len()
        )));
    }
    verify_applied_order_row(
        &rows[0],
        order,
        quota_per_unit,
        &event_key,
        &expected_payload_hash,
    )
}

fn verify_applied_order_row(
    row: &sqlx::postgres::PgRow,
    order: &ManifestOrder,
    quota_per_unit: i64,
    event_key: &str,
    expected_payload_hash: &str,
) -> Result<(), ReconciliationCliError> {
    let snapshot_rate = row
        .try_get::<String, _>("snapshot_rate")?
        .parse::<Decimal>()
        .map_err(|_| {
            ReconciliationCliError::Verification(format!("{event_key} snapshot rate is invalid"))
        })?;
    let manifest_rate = order.rebate_rate.parse::<Decimal>().map_err(|_| {
        ReconciliationCliError::InvalidManifest(format!("{event_key} manifest rate is invalid"))
    })?;
    let record_rate = row
        .try_get::<Option<String>, _>("record_rate")?
        .map(|value| {
            value.parse::<Decimal>().map_err(|_| {
                ReconciliationCliError::Verification(format!("{event_key} record rate is invalid"))
            })
        })
        .transpose()?;
    let matches = row.try_get::<i64, _>("inviter_id")? == order.inviter_id
        && row.try_get::<i64, _>("invitee_id")? == order.invitee_id
        && row.try_get::<String, _>("user_group")? == order.inviter_group
        && row.try_get::<Option<i64>, _>("order_amount_cents")? == order.order_amount_cents
        && row.try_get::<Option<i64>, _>("credited_quota")? == Some(order.credited_quota)
        && row.try_get::<i64, _>("order_time")? == order.completed_at
        && row.try_get::<i64, _>("initialization_ends_at")?
            == order.completed_at + INITIALIZATION_SECONDS
        && row.try_get::<i64, _>("rebate_effective_at")?
            == order.completed_at + REBATE_EFFECTIVE_DELAY_SECONDS
        && row.try_get::<i64, _>("rebate_amount")? == order.expected_rebate_quota
        && record_rate_matches(record_rate, manifest_rate)
        && !row.try_get::<bool, _>("admin_adjusted")?
        && row
            .try_get::<Option<String>, _>("source_payload_sha256")?
            .as_deref()
            == Some(expected_payload_hash)
        && row.try_get::<Option<i32>, _>("source_schema_version")? == Some(2)
        && row.try_get::<String, _>("snapshot_group")? == order.inviter_group
        && snapshot_rate == manifest_rate
        && row.try_get::<i64, _>("order_amount_minor")? == order.order_amount_cents.unwrap_or(0)
        && row.try_get::<Option<i64>, _>("snapshot_credited_quota")? == Some(order.credited_quota)
        && row.try_get::<i64, _>("rebate_quota")? == order.expected_rebate_quota
        && row.try_get::<i64, _>("quota_per_unit")? == quota_per_unit;
    if !matches {
        return Err(ReconciliationCliError::Verification(format!(
            "{event_key} financial identity or snapshot drifted"
        )));
    }
    Ok(())
}

fn record_rate_matches(record_rate: Option<Decimal>, expected_rate: Decimal) -> bool {
    record_rate == Some(expected_rate) || (record_rate.is_none() && expected_rate.is_zero())
}

async fn load_rule_rate(
    pool: &PgPool,
    inviter_group: &str,
) -> Result<Decimal, ReconciliationCliError> {
    let rate: Option<String> = sqlx::query_scalar(
        r#"
        SELECT rebate_rate::TEXT
        FROM rebate_rules
        WHERE rule_type = $2 AND user_group IN ($1, $3)
        ORDER BY CASE WHEN user_group = $1 THEN 0 ELSE 1 END
        LIMIT 1
        "#,
    )
    .bind(inviter_group)
    .bind(TOPUP_ORDER_TYPE)
    .bind(ALL_USER_GROUP)
    .fetch_optional(pool)
    .await?;
    let rate = rate
        .unwrap_or_else(|| "0".to_string())
        .parse::<Decimal>()
        .map_err(|_| ReconciliationCliError::InvalidManifest("invalid rebate rate".to_string()))?;
    validate_rebate_rate(rate)?;
    Ok(rate)
}

async fn revalidate_manifest_rules(
    transaction: &mut Transaction<'_, Postgres>,
    manifest: &ReconciliationManifest,
) -> Result<(), ReconciliationCliError> {
    let mut rates = HashMap::new();
    for order in &manifest.orders {
        let manifest_rate = validate_manifest_order_rebate(order)?;
        if let Some(previous) = rates.insert(order.inviter_group.clone(), manifest_rate) {
            if previous != manifest_rate {
                return Err(ReconciliationCliError::InvalidManifest(format!(
                    "inviter group {} has inconsistent manifest rates",
                    order.inviter_group
                )));
            }
        }
    }

    for (group, expected_rate) in rates {
        let current_rate = load_rule_rate_in_transaction(transaction, &group).await?;
        if current_rate != expected_rate {
            return Err(ReconciliationCliError::SourceChanged(format!(
                "rebate rule changed for inviter group {group}"
            )));
        }
    }
    Ok(())
}

async fn load_rule_rate_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    inviter_group: &str,
) -> Result<Decimal, ReconciliationCliError> {
    let rate: Option<String> = sqlx::query_scalar(
        r#"
        SELECT rebate_rate::TEXT
        FROM rebate_rules
        WHERE rule_type = $2 AND user_group IN ($1, $3)
        ORDER BY CASE WHEN user_group = $1 THEN 0 ELSE 1 END
        LIMIT 1
        "#,
    )
    .bind(inviter_group)
    .bind(TOPUP_ORDER_TYPE)
    .bind(ALL_USER_GROUP)
    .fetch_optional(&mut **transaction)
    .await?;
    let rate = rate
        .unwrap_or_else(|| "0".to_string())
        .parse::<Decimal>()
        .map_err(|_| ReconciliationCliError::InvalidManifest("invalid rebate rate".to_string()))?;
    validate_rebate_rate(rate)?;
    Ok(rate)
}

fn validate_rebate_rate(rate: Decimal) -> Result<(), ReconciliationCliError> {
    if rate < Decimal::ZERO || rate > Decimal::ONE {
        return Err(ReconciliationCliError::InvalidManifest(
            "rebate rate must be between zero and one".to_string(),
        ));
    }
    Ok(())
}

fn validate_manifest_order_rebate(
    order: &ManifestOrder,
) -> Result<Decimal, ReconciliationCliError> {
    let rate = order.rebate_rate.parse::<Decimal>().map_err(|_| {
        ReconciliationCliError::InvalidManifest(format!(
            "topup {} has an invalid rebate rate",
            order.source_order_id
        ))
    })?;
    validate_rebate_rate(rate)?;
    let expected_quota = calculate_rebate_from_credited_quota(order.credited_quota, rate)?;
    if expected_quota != order.expected_rebate_quota {
        return Err(ReconciliationCliError::InvalidManifest(format!(
            "topup {} rebate expectation is inconsistent",
            order.source_order_id
        )));
    }
    Ok(rate)
}

async fn load_existing_record(
    pool: &PgPool,
    source_order_id: i64,
) -> Result<Option<ManifestRecordState>, ReconciliationCliError> {
    let rows = sqlx::query(
        r#"
        SELECT id, status, canonical_event_key, credited_quota, rebate_amount,
               rebate_rate::TEXT AS rebate_rate, order_time,
               initialization_ends_at, rebate_effective_at, admin_adjusted
        FROM rebate_records
        WHERE (order_type = 'topup' AND order_id = $1)
           OR canonical_event_key = $2
        ORDER BY CASE WHEN canonical_event_key = $2 THEN 0 ELSE 1 END, id
        "#,
    )
    .bind(source_order_id)
    .bind(format!("newapi:topup:{source_order_id}"))
    .fetch_all(pool)
    .await?;
    manifest_record_state_from_rows(rows, source_order_id)
}

fn validate_existing_record_for_plan(
    source_order_id: i64,
    record: Option<&ManifestRecordState>,
) -> Result<(), ReconciliationCliError> {
    let Some(record) = record else {
        return Ok(());
    };
    if record.status != "initializing" {
        return Err(ReconciliationCliError::SourceChanged(format!(
            "topup {source_order_id} existing rebate record is not initializing"
        )));
    }
    if record.admin_adjusted {
        return Err(ReconciliationCliError::SourceChanged(format!(
            "topup {source_order_id} existing rebate record was admin-adjusted"
        )));
    }
    Ok(())
}

async fn revalidate_existing_records(
    transaction: &mut Transaction<'_, Postgres>,
    manifest: &ReconciliationManifest,
) -> Result<(), ReconciliationCliError> {
    for order in &manifest.orders {
        let event_key = format!("newapi:topup:{}", order.source_order_id);
        let expected_payload_hash = payload_hash_for_event(manifest_event(order))?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(&event_key)
            .execute(&mut **transaction)
            .await?;
        let receipt_hash: Option<String> = sqlx::query_scalar(
            r#"
            SELECT source_payload_sha256
            FROM rebate_source_event_receipts
            WHERE event_key = $1
            FOR UPDATE
            "#,
        )
        .bind(&event_key)
        .fetch_optional(&mut **transaction)
        .await?;
        if let Some(receipt_hash) = receipt_hash {
            if receipt_hash != expected_payload_hash {
                return Err(ReconciliationCliError::SourceChanged(format!(
                    "event receipt changed for topup {}",
                    order.source_order_id
                )));
            }
            verify_applied_order_in_transaction(transaction, order, manifest.quota_per_unit)
                .await?;
            continue;
        }
        let rows = sqlx::query(
            r#"
            SELECT id, status, canonical_event_key, credited_quota, rebate_amount,
                   rebate_rate::TEXT AS rebate_rate, order_time,
                   initialization_ends_at, rebate_effective_at, admin_adjusted
            FROM rebate_records
            WHERE (order_type = 'topup' AND order_id = $1)
               OR canonical_event_key = $2
            ORDER BY CASE WHEN canonical_event_key = $2 THEN 0 ELSE 1 END, id
            FOR UPDATE
            "#,
        )
        .bind(order.source_order_id)
        .bind(event_key)
        .fetch_all(&mut **transaction)
        .await?;
        let current = manifest_record_state_from_rows(rows, order.source_order_id)?;
        if current != order.existing_record {
            return Err(ReconciliationCliError::SourceChanged(format!(
                "rebate record changed for topup {}",
                order.source_order_id
            )));
        }
    }
    Ok(())
}

async fn verify_applied_order_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    order: &ManifestOrder,
    quota_per_unit: i64,
) -> Result<(), ReconciliationCliError> {
    let event_key = format!("newapi:topup:{}", order.source_order_id);
    let expected_payload_hash = payload_hash_for_event(manifest_event(order))?;
    let rows = sqlx::query(
        r#"
        SELECT record.inviter_id, record.invitee_id, record.user_group,
               record.order_amount_cents, record.credited_quota, record.order_time,
               record.initialization_ends_at, record.rebate_effective_at,
               record.rebate_amount, record.rebate_rate::TEXT AS record_rate,
               record.admin_adjusted,
               record.source_payload_sha256, record.source_schema_version,
               snapshot.inviter_group AS snapshot_group,
               snapshot.rebate_rate::TEXT AS snapshot_rate,
               snapshot.order_amount_minor, snapshot.credited_quota AS snapshot_credited_quota,
               snapshot.rebate_quota,
               snapshot.quota_per_unit
        FROM rebate_records record
        JOIN rebate_calculation_snapshots snapshot ON snapshot.rebate_record_id = record.id
        WHERE record.canonical_event_key = $1
        FOR UPDATE OF record, snapshot
        "#,
    )
    .bind(&event_key)
    .fetch_all(&mut **transaction)
    .await?;
    if rows.len() != 1 {
        return Err(ReconciliationCliError::SourceChanged(format!(
            "{event_key} expected exactly one applied record, got {}",
            rows.len()
        )));
    }
    verify_applied_order_row(
        &rows[0],
        order,
        quota_per_unit,
        &event_key,
        &expected_payload_hash,
    )
    .map_err(|error| ReconciliationCliError::SourceChanged(error.to_string()))
}

fn manifest_record_state_from_rows(
    rows: Vec<sqlx::postgres::PgRow>,
    source_order_id: i64,
) -> Result<Option<ManifestRecordState>, ReconciliationCliError> {
    if rows.len() > 1 {
        return Err(ReconciliationCliError::Verification(format!(
            "topup {source_order_id} maps to multiple rebate records"
        )));
    }
    rows.into_iter()
        .next()
        .map(|row| {
            Ok(ManifestRecordState {
                record_id: row.try_get("id")?,
                status: row.try_get("status")?,
                canonical_event_key: row.try_get("canonical_event_key")?,
                credited_quota: row.try_get("credited_quota")?,
                rebate_amount: row.try_get("rebate_amount")?,
                rebate_rate: row.try_get("rebate_rate")?,
                order_time: row.try_get("order_time")?,
                initialization_ends_at: row.try_get("initialization_ends_at")?,
                rebate_effective_at: row.try_get("rebate_effective_at")?,
                admin_adjusted: row.try_get("admin_adjusted")?,
            })
        })
        .transpose()
}

fn manifest_event(order: &ManifestOrder) -> RebateSourceEvent {
    RebateSourceEvent {
        schema_version: 2,
        event_key: format!("newapi:topup:{}", order.source_order_id),
        event_type: "topup.completed".to_string(),
        source_order_id: order.source_order_id,
        invitee_id: order.invitee_id,
        inviter_id: Some(order.inviter_id),
        inviter_group: order.inviter_group.clone(),
        rebate_base_amount_cents: None,
        credited_quota: Some(order.credited_quota),
        completed_at: order.completed_at,
        payment_provider: order.payment_provider.clone(),
    }
}

fn source_row_sha256(
    source: &SourceOrder,
    credit_evidence: &CreditedQuotaEvidence,
) -> Result<String, ReconciliationCliError> {
    let identity = SourceRowIdentity {
        source_order_id: source.source_order_id,
        invitee_id: source.invitee_id,
        inviter_id: source.inviter_id,
        inviter_group: &source.inviter_group,
        credited_quota_source_field: credit_evidence.source_field,
        credited_quota_source_value: &credit_evidence.source_value,
        credited_quota: credit_evidence.credited_quota,
        completed_at: source.completed_at,
        payment_provider: &source.payment_provider,
        trade_no: &source.trade_no,
        status: &source.status,
    };
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(&identity)?)))
}

fn validate_completion_evidence(
    source_orders: &[SourceOrder],
) -> Result<(), ReconciliationCliError> {
    let missing = source_orders
        .iter()
        .filter(|source| source.completed_at <= 0)
        .map(|source| {
            format!(
                "topup_id={} trade_no={:?}",
                source.source_order_id, source.trade_no
            )
        })
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(());
    }
    Err(ReconciliationCliError::CompletionEvidenceRequired(format!(
        "successful topups have no verified complete_time: {}",
        missing.join(", ")
    )))
}

fn credited_quota_evidence(
    source: &SourceOrder,
    quota_per_unit: i64,
) -> Result<CreditedQuotaEvidence, ReconciliationCliError> {
    let label = format!(
        "topup {} trade_no {:?}",
        source.source_order_id, source.trade_no
    );
    let source_field = credited_quota_source_field(&source.payment_provider, &label)?;
    let source_value = match source_field {
        "amount" => &source.amount_text,
        "money" => &source.money_text,
        _ => unreachable!("validated credited quota source field"),
    };
    credited_quota_evidence_from_value(
        &source.payment_provider,
        source_field,
        source_value,
        quota_per_unit,
        &label,
    )
}

fn credited_quota_source_field(
    payment_provider: &str,
    label: &str,
) -> Result<&'static str, ReconciliationCliError> {
    match payment_provider.trim() {
        "epay" | "waffo" | "waffo_pancake" | "creem" => Ok("amount"),
        "stripe" => Ok("money"),
        provider => Err(ReconciliationCliError::InvalidManifest(format!(
            "{label} has unsupported payment_provider {provider:?}"
        ))),
    }
}

fn credited_quota_evidence_from_value(
    payment_provider: &str,
    source_field: &'static str,
    source_value: &str,
    quota_per_unit: i64,
    label: &str,
) -> Result<CreditedQuotaEvidence, ReconciliationCliError> {
    let provider = payment_provider.trim();
    let (normalized_value, credited_quota) = match provider {
        "epay" | "waffo" | "waffo_pancake" => {
            let amount = positive_source_i64(source_value, source_field, label)?;
            let credited_quota = amount.checked_mul(quota_per_unit).ok_or_else(|| {
                ReconciliationCliError::InvalidManifest(format!(
                    "{label} credited quota is out of range"
                ))
            })?;
            (amount.to_string(), credited_quota)
        }
        "creem" => {
            let credited_quota = positive_source_i64(source_value, source_field, label)?;
            (credited_quota.to_string(), credited_quota)
        }
        "stripe" => {
            let money = source_value.parse::<Decimal>().map_err(|_| {
                ReconciliationCliError::InvalidManifest(format!(
                    "{label} has invalid money value {source_value:?}"
                ))
            })?;
            if money <= Decimal::ZERO {
                return Err(ReconciliationCliError::InvalidManifest(format!(
                    "{label} money must be positive"
                )));
            }
            let rounded = money
                .checked_mul(Decimal::from(quota_per_unit))
                .ok_or_else(|| {
                    ReconciliationCliError::InvalidManifest(format!(
                        "{label} credited quota is out of range"
                    ))
                })?
                .round_dp_with_strategy(0, RoundingStrategy::MidpointAwayFromZero);
            let credited_quota = rounded.to_i64().filter(|value| *value > 0).ok_or_else(|| {
                ReconciliationCliError::InvalidManifest(format!(
                    "{label} credited quota is out of range"
                ))
            })?;
            (money.normalize().to_string(), credited_quota)
        }
        _ => {
            return Err(ReconciliationCliError::InvalidManifest(format!(
                "{label} has unsupported payment_provider {provider:?}"
            )))
        }
    };
    Ok(CreditedQuotaEvidence {
        source_field,
        source_value: normalized_value,
        credited_quota,
    })
}

fn positive_source_i64(
    value: &str,
    field: &str,
    label: &str,
) -> Result<i64, ReconciliationCliError> {
    let parsed = value.parse::<i64>().map_err(|_| {
        ReconciliationCliError::InvalidManifest(format!(
            "{label} has invalid {field} value {value:?}"
        ))
    })?;
    if parsed <= 0 {
        return Err(ReconciliationCliError::InvalidManifest(format!(
            "{label} {field} must be positive"
        )));
    }
    Ok(parsed)
}

fn validate_manifest_credit_evidence(
    order: &ManifestOrder,
    quota_per_unit: i64,
) -> Result<(), ReconciliationCliError> {
    let label = format!("manifest topup {}", order.source_order_id);
    let expected_field = credited_quota_source_field(&order.payment_provider, &label)?;
    if order.credited_quota_source_field != expected_field {
        return Err(ReconciliationCliError::InvalidManifest(format!(
            "{label} credited quota source field is inconsistent"
        )));
    }
    let evidence = credited_quota_evidence_from_value(
        &order.payment_provider,
        expected_field,
        &order.credited_quota_source_value,
        quota_per_unit,
        &label,
    )?;
    if evidence.source_value != order.credited_quota_source_value
        || evidence.credited_quota != order.credited_quota
    {
        return Err(ReconciliationCliError::InvalidManifest(format!(
            "{label} credited quota evidence is inconsistent"
        )));
    }
    Ok(())
}

fn envelope(manifest: ReconciliationManifest) -> Result<ManifestEnvelope, ReconciliationCliError> {
    let sha256 = manifest_sha256(&manifest)?;
    Ok(ManifestEnvelope { manifest, sha256 })
}

fn manifest_sha256(manifest: &ReconciliationManifest) -> Result<String, ReconciliationCliError> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(manifest)?)))
}

fn read_manifest_from_stdin() -> Result<ManifestEnvelope, ReconciliationCliError> {
    let mut input = String::new();
    std::io::stdin().read_to_string(&mut input)?;
    Ok(serde_json::from_str(&input)?)
}

fn validate_envelope(
    envelope: &ManifestEnvelope,
    expected_sha256: &str,
) -> Result<(), ReconciliationCliError> {
    validate_manifest(&envelope.manifest)?;
    let actual = manifest_sha256(&envelope.manifest)?;
    if envelope.sha256 != actual || expected_sha256 != actual {
        return Err(ReconciliationCliError::InvalidManifest(
            "manifest SHA-256 mismatch".to_string(),
        ));
    }
    Ok(())
}

fn validate_manifest(manifest: &ReconciliationManifest) -> Result<(), ReconciliationCliError> {
    if manifest.manifest_version != MANIFEST_VERSION
        || manifest.source_namespace != SOURCE_NAMESPACE
        || manifest.generated_at <= 0
        || manifest.cutoff_topup_id <= 0
        || manifest.quota_per_unit <= 0
        || manifest.quota_per_unit % 100 != 0
    {
        return Err(ReconciliationCliError::InvalidManifest(
            "unsupported manifest header".to_string(),
        ));
    }
    let mut ids = HashSet::new();
    for order in &manifest.orders {
        let expected_event_key = format!("newapi:topup:{}", order.source_order_id);
        if order.source_order_id <= 0
            || order.source_order_id > manifest.cutoff_topup_id
            || !ids.insert(order.source_order_id)
            || order.source_row_sha256.len() != 64
            || !order
                .source_row_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || order.invitee_id <= 0
            || order.inviter_id <= 0
            || order.invitee_id == order.inviter_id
            || order.inviter_group.trim().is_empty()
            || order.inviter_group.len() > 64
            || order.order_amount_cents.is_some_and(|amount| amount <= 0)
            || order.credited_quota <= 0
            || order.credited_quota_source_field.len() > 16
            || order.credited_quota_source_value.is_empty()
            || order.credited_quota_source_value.len() > 128
            || order.completed_at <= 0
            || order.payment_provider.trim().is_empty()
            || order.payment_provider.len() > 64
            || order.existing_record.as_ref().is_some_and(|record| {
                record.record_id <= 0
                    || record.status != "initializing"
                    || record.admin_adjusted
                    || record.rebate_amount < 0
                    || record.credited_quota.is_some_and(|quota| quota <= 0)
                    || record.order_time <= 0
                    || record.initialization_ends_at <= 0
                    || record.rebate_effective_at <= 0
                    || record
                        .canonical_event_key
                        .as_deref()
                        .is_some_and(|event_key| event_key != expected_event_key)
                    || record.rebate_rate.as_deref().is_some_and(|rate| {
                        rate.parse::<Decimal>()
                            .map(|rate| rate < Decimal::ZERO || rate > Decimal::ONE)
                            .unwrap_or(true)
                    })
            })
        {
            return Err(ReconciliationCliError::InvalidManifest(
                "invalid or duplicate manifest order".to_string(),
            ));
        }
        validate_manifest_credit_evidence(order, manifest.quota_per_unit)?;
        validate_manifest_order_rebate(order)?;
    }
    Ok(())
}

fn required_positive_i64_flag(args: &[String], name: &str) -> Result<i64, ReconciliationCliError> {
    let Some(index) = args.iter().position(|arg| arg == name) else {
        return Err(ReconciliationCliError::InvalidCommand(format!(
            "plan requires {name}"
        )));
    };
    let value = args.get(index + 1).ok_or_else(|| {
        ReconciliationCliError::InvalidCommand(format!("{name} requires a value"))
    })?;
    let value = value.parse::<i64>().map_err(|_| {
        ReconciliationCliError::InvalidCommand(format!("{name} must be an integer"))
    })?;
    if value <= 0 {
        return Err(ReconciliationCliError::InvalidCommand(format!(
            "{name} must be positive"
        )));
    }
    Ok(value)
}

fn required_sha256_flag(args: &[String]) -> Result<String, ReconciliationCliError> {
    let index = args
        .iter()
        .position(|arg| arg == "--expected-sha256")
        .ok_or_else(|| {
            ReconciliationCliError::InvalidCommand(
                "apply and verify require --expected-sha256".to_string(),
            )
        })?;
    let value = args.get(index + 1).ok_or_else(|| {
        ReconciliationCliError::InvalidCommand("--expected-sha256 requires a value".to_string())
    })?;
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ReconciliationCliError::InvalidCommand(
            "--expected-sha256 must be a 64-character hex digest".to_string(),
        ));
    }
    Ok(value.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source_order(payment_provider: &str, amount: &str, money: &str) -> SourceOrder {
        SourceOrder {
            source_order_id: 842,
            invitee_id: 3418,
            inviter_id: 1001,
            inviter_group: "default".to_string(),
            amount_text: amount.to_string(),
            money_text: money.to_string(),
            completed_at: 1_784_211_072,
            payment_provider: payment_provider.to_string(),
            trade_no: "trade-842".to_string(),
            status: "success".to_string(),
        }
    }

    #[test]
    fn manifest_hash_detects_content_changes() {
        let manifest = ReconciliationManifest {
            manifest_version: 2,
            source_namespace: "newapi".to_string(),
            generated_at: 1,
            cutoff_topup_id: 1,
            quota_per_unit: 500_000,
            orders: Vec::new(),
        };
        let mut envelope = envelope(manifest).unwrap();
        let expected = envelope.sha256.clone();
        assert!(validate_envelope(&envelope, &expected).is_ok());
        envelope.manifest.generated_at = 2;
        assert!(validate_envelope(&envelope, &expected).is_err());
    }

    #[test]
    fn credited_quota_uses_provider_specific_source_semantics() {
        for provider in ["epay", "waffo", "waffo_pancake"] {
            let evidence =
                credited_quota_evidence(&source_order(provider, "75", "12.34"), 500_000).unwrap();
            assert_eq!(evidence.source_field, "amount");
            assert_eq!(evidence.source_value, "75");
            assert_eq!(evidence.credited_quota, 37_500_000);
        }

        let stripe =
            credited_quota_evidence(&source_order("stripe", "999", "0.000001"), 500_000).unwrap();
        assert_eq!(stripe.source_field, "money");
        assert_eq!(stripe.source_value, "0.000001");
        assert_eq!(stripe.credited_quota, 1);

        let creem =
            credited_quota_evidence(&source_order("creem", "12345", "99.00"), 500_000).unwrap();
        assert_eq!(creem.source_field, "amount");
        assert_eq!(creem.credited_quota, 12_345);

        for provider in ["", "unknown", "balance"] {
            assert!(
                credited_quota_evidence(&source_order(provider, "75", "12.34"), 500_000).is_err()
            );
        }
        assert!(credited_quota_evidence(
            &source_order("epay", "9223372036854775807", "1"),
            500_000
        )
        .is_err());
        assert!(credited_quota_evidence(
            &source_order("stripe", "1", "79228162514264337593543950335"),
            500_000
        )
        .is_err());
    }

    #[test]
    fn source_hash_tracks_only_the_provider_accounting_basis() {
        let epay = source_order("epay", "75", "12.34");
        let epay_evidence = credited_quota_evidence(&epay, 500_000).unwrap();
        let epay_hash = source_row_sha256(&epay, &epay_evidence).unwrap();
        let epay_money_changed = source_order("epay", "75", "99.99");
        let changed_evidence = credited_quota_evidence(&epay_money_changed, 500_000).unwrap();
        assert_eq!(
            epay_hash,
            source_row_sha256(&epay_money_changed, &changed_evidence).unwrap()
        );

        let epay_amount_changed = source_order("epay", "76", "12.34");
        let changed_evidence = credited_quota_evidence(&epay_amount_changed, 500_000).unwrap();
        assert_ne!(
            epay_hash,
            source_row_sha256(&epay_amount_changed, &changed_evidence).unwrap()
        );

        let stripe = source_order("stripe", "75", "12.34");
        let stripe_evidence = credited_quota_evidence(&stripe, 500_000).unwrap();
        let stripe_hash = source_row_sha256(&stripe, &stripe_evidence).unwrap();
        let stripe_money_changed = source_order("stripe", "75", "12.35");
        let changed_evidence = credited_quota_evidence(&stripe_money_changed, 500_000).unwrap();
        assert_ne!(
            stripe_hash,
            source_row_sha256(&stripe_money_changed, &changed_evidence).unwrap()
        );
    }

    #[test]
    fn missing_completion_time_lists_required_source_evidence() {
        let mut source = source_order("epay", "75", "12.34");
        source.completed_at = 0;
        let error = validate_completion_evidence(&[source]).unwrap_err();
        assert_eq!(error.code(), "completion_evidence_required");
        assert!(error.to_string().contains("topup_id=842"));
        assert!(error.to_string().contains("trade-842"));
    }

    #[test]
    fn plan_requires_an_explicit_positive_cutoff() {
        let missing = vec!["plan".to_string()];
        assert!(required_positive_i64_flag(&missing, "--cutoff-topup-id").is_err());

        let valid = vec![
            "plan".to_string(),
            "--cutoff-topup-id".to_string(),
            "842".to_string(),
        ];
        assert_eq!(
            required_positive_i64_flag(&valid, "--cutoff-topup-id").unwrap(),
            842
        );
    }

    #[test]
    fn manifest_rejects_an_inconsistent_rebate_expectation() {
        let manifest = ReconciliationManifest {
            manifest_version: 2,
            source_namespace: "newapi".to_string(),
            generated_at: 1,
            cutoff_topup_id: 842,
            quota_per_unit: 500_000,
            orders: vec![ManifestOrder {
                source_order_id: 842,
                invitee_id: 3418,
                inviter_id: 1001,
                inviter_group: "default".to_string(),
                order_amount_cents: None,
                credited_quota: 37_500_000,
                credited_quota_source_field: "amount".to_string(),
                credited_quota_source_value: "75".to_string(),
                completed_at: 1_784_211_072,
                payment_provider: "epay".to_string(),
                source_row_sha256: "a".repeat(64),
                existing_record: None,
                rebate_rate: "0.05".to_string(),
                expected_rebate_quota: 1,
            }],
        };

        assert!(validate_manifest(&manifest).is_err());
    }

    #[test]
    fn apply_requires_exact_read_only_settlement_mode() {
        assert!(validate_reconcile_settlement_mode("apply", Some("read_only")).is_ok());
        for value in [None, Some("disabled"), Some("enabled"), Some(" read_only ")] {
            assert!(validate_reconcile_settlement_mode("apply", value).is_err());
        }
        assert!(validate_reconcile_settlement_mode("apply", Some("invalid")).is_err());
        assert!(validate_reconcile_settlement_mode("plan", None).is_ok());
        assert!(validate_reconcile_settlement_mode("verify", Some("enabled")).is_ok());
        assert!(validate_reconcile_settlement_mode("plan", Some("invalid")).is_err());
    }
}
