use axum::extract::rejection::JsonRejection;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{Postgres, Row, Transaction};
use subtle::ConstantTimeEq;

use crate::error::{AppError, Result};
use crate::rebate_lifecycle::{
    matches_supported_rebate_schedule, ALL_USER_GROUP, INITIALIZATION_SECONDS,
    REBATE_EFFECTIVE_DELAY_SECONDS,
};
use crate::rebate_types::TOPUP_ORDER_TYPE;
use crate::AppState;

const LEGACY_SCHEMA_VERSION: i32 = 1;
const CREDITED_QUOTA_SCHEMA_VERSION: i32 = 2;
const SUPPORTED_EVENT_TYPE: &str = "topup.completed";
const DEFAULT_INVITER_GROUP: &str = "default";
const REDEMPTION_PAYMENT_PROVIDER: &str = "redemption";
const SOURCE_REVISION_V1: &str = "rebate-source-event:v1";
const SOURCE_REVISION_V2: &str = "rebate-source-event:v2";
const RECEIPT_OUTCOME_CREATED: &str = "created";
const RECEIPT_OUTCOME_IGNORED_NO_INVITER: &str = "ignored_no_inviter";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RebateSourceEvent {
    pub schema_version: i32,
    pub event_key: String,
    pub event_type: String,
    pub source_order_id: i64,
    pub invitee_id: i64,
    #[serde(default)]
    pub inviter_id: Option<i64>,
    #[serde(default)]
    pub inviter_group: String,
    #[serde(default)]
    pub rebate_base_amount_cents: Option<i64>,
    #[serde(default)]
    pub credited_quota: Option<i64>,
    pub completed_at: i64,
    pub payment_provider: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IngestOutcome {
    Created,
    Replay,
    IgnoredNoInviter,
}

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("invalid rebate source event: {0}")]
    Invalid(String),
    #[error("rebate source event conflict: {0}")]
    Conflict(String),
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

#[derive(Clone, Debug, Serialize)]
struct ValidatedEvent {
    schema_version: i32,
    event_key: String,
    event_type: String,
    source_order_id: i64,
    invitee_id: i64,
    inviter_id: Option<i64>,
    inviter_group: String,
    rebate_base_amount_cents: Option<i64>,
    credited_quota: Option<i64>,
    completed_at: i64,
    payment_provider: String,
}

struct RebateRuleSnapshot {
    rule_id: Option<i64>,
    rate: Decimal,
}

struct ExistingRecord {
    id: i64,
    order_type: String,
    order_id: i64,
    canonical_event_key: Option<String>,
    source_payload_sha256: Option<String>,
    source_schema_version: Option<i32>,
    inviter_id: i64,
    invitee_id: i64,
    user_group: String,
    stored_order_amount_cents: Option<i64>,
    credited_quota: Option<i64>,
    rebate_amount: i64,
    rebate_rate: Option<String>,
    status: String,
    order_time: i64,
    initialization_ends_at: i64,
    rebate_effective_at: i64,
    admin_adjusted: bool,
}

struct ExistingReceipt {
    source_payload_sha256: String,
}

#[derive(Clone, Copy)]
enum ExistingRecordPolicy {
    StrictOnline,
    AuditedLegacyAttach,
}

struct ExistingRecordIngestContext<'a> {
    inviter_id: i64,
    payload_hash: &'a str,
    quota_per_unit: i64,
    policy: ExistingRecordPolicy,
}

pub async fn receive_rebate_source_event(
    State(state): State<AppState>,
    headers: HeaderMap,
    payload: std::result::Result<Json<RebateSourceEvent>, JsonRejection>,
) -> Result<StatusCode> {
    authorize_bearer(&headers, &state.config.rebate_event_ingest_secret)?;
    let Json(event) = payload
        .map_err(|_| AppError::UnprocessableEntity("返利源事件 JSON 格式无效".to_string()))?;

    match ingest_rebate_source_event(
        &state.pool,
        event,
        state.config.quota_per_unit,
        state.config.topup_rebate_active_from_id,
    )
    .await
    {
        Ok(IngestOutcome::Created) => Ok(StatusCode::CREATED),
        Ok(IngestOutcome::Replay) => Ok(StatusCode::OK),
        Ok(IngestOutcome::IgnoredNoInviter) => Ok(StatusCode::NO_CONTENT),
        Err(IngestError::Invalid(message)) => Err(AppError::UnprocessableEntity(message)),
        Err(IngestError::Conflict(message)) => Err(AppError::Conflict(message)),
        Err(IngestError::Database(error)) => Err(AppError::Database(error)),
    }
}

pub async fn ingest_rebate_source_event(
    pool: &sqlx::PgPool,
    event: RebateSourceEvent,
    quota_per_unit: i64,
    topup_rebate_active_from_id: i64,
) -> std::result::Result<IngestOutcome, IngestError> {
    let mut transaction = pool.begin().await?;
    let outcome = ingest_rebate_source_event_with_policy(
        &mut transaction,
        event,
        quota_per_unit,
        topup_rebate_active_from_id,
        ExistingRecordPolicy::StrictOnline,
    )
    .await?;
    transaction.commit().await?;
    Ok(outcome)
}

pub(crate) async fn attach_legacy_rebate_source_event_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    event: RebateSourceEvent,
    quota_per_unit: i64,
    topup_rebate_active_from_id: i64,
) -> std::result::Result<IngestOutcome, IngestError> {
    ingest_rebate_source_event_with_policy(
        transaction,
        event,
        quota_per_unit,
        topup_rebate_active_from_id,
        ExistingRecordPolicy::AuditedLegacyAttach,
    )
    .await
}

async fn ingest_rebate_source_event_with_policy(
    transaction: &mut Transaction<'_, Postgres>,
    event: RebateSourceEvent,
    quota_per_unit: i64,
    topup_rebate_active_from_id: i64,
    existing_record_policy: ExistingRecordPolicy,
) -> std::result::Result<IngestOutcome, IngestError> {
    let event = validate_event(event)?;
    validate_quota_per_unit(quota_per_unit)?;
    validate_topup_rebate_scope(&event, topup_rebate_active_from_id)?;
    let payload_hash = source_payload_hash(&event)?;
    lock_event_identity(transaction, &event.event_key).await?;
    if let Some(receipt) = lock_existing_receipt(transaction, &event.event_key).await? {
        return if receipt.source_payload_sha256 == payload_hash {
            Ok(IngestOutcome::Replay)
        } else {
            Err(IngestError::Conflict(
                "相同 event_key 的事件内容不一致".to_string(),
            ))
        };
    }

    let Some(inviter_id) = event.inviter_id else {
        insert_event_receipt(
            transaction,
            &event,
            &payload_hash,
            RECEIPT_OUTCOME_IGNORED_NO_INVITER,
            None,
        )
        .await?;
        return Ok(IngestOutcome::IgnoredNoInviter);
    };

    let existing = lock_existing_record(transaction, &event).await?;
    if let Some(record) = existing {
        let (outcome, record_id) = ingest_existing_record(
            transaction,
            &event,
            record,
            ExistingRecordIngestContext {
                inviter_id,
                payload_hash: &payload_hash,
                quota_per_unit,
                policy: existing_record_policy,
            },
        )
        .await?;
        insert_event_receipt(
            transaction,
            &event,
            &payload_hash,
            RECEIPT_OUTCOME_CREATED,
            Some(record_id),
        )
        .await?;
        return Ok(outcome);
    }

    let rule = load_rebate_rule(transaction, &event.inviter_group).await?;
    let credited_quota = credited_quota_for_event(&event, quota_per_unit)?;
    let rebate_quota = calculate_rebate_from_credited_quota(credited_quota, rule.rate)?;
    let record_id = insert_rebate_record(
        transaction,
        &event,
        inviter_id,
        &payload_hash,
        &rule,
        rebate_quota,
        credited_quota,
    )
    .await?;
    insert_calculation_snapshot(
        transaction,
        record_id,
        &event,
        &rule,
        rebate_quota,
        credited_quota,
        quota_per_unit,
    )
    .await?;
    insert_event_receipt(
        transaction,
        &event,
        &payload_hash,
        RECEIPT_OUTCOME_CREATED,
        Some(record_id),
    )
    .await?;
    Ok(IngestOutcome::Created)
}

pub(crate) fn payload_hash_for_event(
    event: RebateSourceEvent,
) -> std::result::Result<String, IngestError> {
    source_payload_hash(&validate_event(event)?)
}

async fn lock_event_identity(
    transaction: &mut Transaction<'_, Postgres>,
    event_key: &str,
) -> std::result::Result<(), IngestError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(event_key)
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

async fn lock_existing_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    event_key: &str,
) -> std::result::Result<Option<ExistingReceipt>, IngestError> {
    let row = sqlx::query(
        r#"
        SELECT source_payload_sha256
        FROM rebate_source_event_receipts
        WHERE event_key = $1
        FOR UPDATE
        "#,
    )
    .bind(event_key)
    .fetch_optional(&mut **transaction)
    .await?;
    row.map(|row| {
        Ok(ExistingReceipt {
            source_payload_sha256: row.try_get("source_payload_sha256")?,
        })
    })
    .transpose()
}

async fn insert_event_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    event: &ValidatedEvent,
    payload_hash: &str,
    outcome: &str,
    rebate_record_id: Option<i64>,
) -> std::result::Result<(), IngestError> {
    sqlx::query(
        r#"
        INSERT INTO rebate_source_event_receipts (
            event_key, event_type, source_order_id, source_payload_sha256,
            source_schema_version, outcome, rebate_record_id, received_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#,
    )
    .bind(&event.event_key)
    .bind(&event.event_type)
    .bind(event.source_order_id)
    .bind(payload_hash)
    .bind(event.schema_version)
    .bind(outcome)
    .bind(rebate_record_id)
    .bind(chrono::Utc::now().timestamp())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn authorize_bearer(headers: &HeaderMap, expected_secret: &str) -> Result<()> {
    let candidate = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .unwrap_or_default();
    let expected_hash = Sha256::digest(expected_secret.as_bytes());
    let candidate_hash = Sha256::digest(candidate.as_bytes());
    if bool::from(expected_hash.ct_eq(&candidate_hash)) {
        return Ok(());
    }
    Err(AppError::Unauthorized)
}

fn validate_event(event: RebateSourceEvent) -> std::result::Result<ValidatedEvent, IngestError> {
    if !matches!(
        event.schema_version,
        LEGACY_SCHEMA_VERSION | CREDITED_QUOTA_SCHEMA_VERSION
    ) {
        return Err(IngestError::Invalid("不支持的 schema_version".to_string()));
    }
    if event.event_type != SUPPORTED_EVENT_TYPE {
        return Err(IngestError::Invalid("不支持的 event_type".to_string()));
    }
    if event.source_order_id <= 0 || event.invitee_id <= 0 {
        return Err(IngestError::Invalid("订单和用户 ID 必须为正数".to_string()));
    }
    let payment_provider = event.payment_provider.trim();
    if payment_provider.is_empty() || payment_provider.len() > 64 {
        return Err(IngestError::Invalid("支付渠道无效".to_string()));
    }
    let event_key_source = if payment_provider == REDEMPTION_PAYMENT_PROVIDER {
        "redemption"
    } else {
        "topup"
    };
    let expected_event_key = format!("newapi:{event_key_source}:{}", event.source_order_id);
    if event.event_key != expected_event_key {
        return Err(IngestError::Invalid("event_key 与源订单不匹配".to_string()));
    }
    if event.event_key.len() > 160 {
        return Err(IngestError::Invalid("event_key 过长".to_string()));
    }
    if let Some(inviter_id) = event.inviter_id {
        if inviter_id <= 0 || inviter_id == event.invitee_id {
            return Err(IngestError::Invalid("邀请人 ID 无效".to_string()));
        }
    }
    match event.schema_version {
        LEGACY_SCHEMA_VERSION
            if event
                .rebate_base_amount_cents
                .is_none_or(|amount| amount <= 0)
                || event.credited_quota.is_some() =>
        {
            return Err(IngestError::Invalid(
                "schema v1 必须且只能提供正整数分返利基数".to_string(),
            ));
        }
        CREDITED_QUOTA_SCHEMA_VERSION
            if event.credited_quota.is_none_or(|quota| quota <= 0)
                || event.rebate_base_amount_cents.is_some() =>
        {
            return Err(IngestError::Invalid(
                "schema v2 必须提供正整数 credited_quota".to_string(),
            ));
        }
        _ => {}
    }
    if event.completed_at <= 0
        || event
            .completed_at
            .checked_add(REBATE_EFFECTIVE_DELAY_SECONDS)
            .is_none()
        || event
            .completed_at
            .checked_add(INITIALIZATION_SECONDS)
            .is_none()
    {
        return Err(IngestError::Invalid("completed_at 必须为正数".to_string()));
    }
    let inviter_group = match event.inviter_group.trim() {
        "" => DEFAULT_INVITER_GROUP.to_string(),
        value if value.len() <= 64 => value.to_string(),
        _ => return Err(IngestError::Invalid("邀请人分组过长".to_string())),
    };
    Ok(ValidatedEvent {
        schema_version: event.schema_version,
        event_key: event.event_key,
        event_type: event.event_type,
        source_order_id: event.source_order_id,
        invitee_id: event.invitee_id,
        inviter_id: event.inviter_id,
        inviter_group,
        rebate_base_amount_cents: event.rebate_base_amount_cents,
        credited_quota: event.credited_quota,
        completed_at: event.completed_at,
        payment_provider: payment_provider.to_string(),
    })
}

fn source_revision(event: &ValidatedEvent) -> &'static str {
    match event.schema_version {
        LEGACY_SCHEMA_VERSION => SOURCE_REVISION_V1,
        CREDITED_QUOTA_SCHEMA_VERSION => SOURCE_REVISION_V2,
        _ => unreachable!("validated schema version"),
    }
}

fn validate_quota_per_unit(quota_per_unit: i64) -> std::result::Result<(), IngestError> {
    if quota_per_unit <= 0 || quota_per_unit % 100 != 0 {
        return Err(IngestError::Invalid("quota_per_unit 配置无效".to_string()));
    }
    Ok(())
}

fn validate_topup_rebate_scope(
    event: &ValidatedEvent,
    topup_rebate_active_from_id: i64,
) -> std::result::Result<(), IngestError> {
    if topup_rebate_active_from_id <= 0 {
        return Err(IngestError::Invalid(
            "topup rebate active boundary 配置无效".to_string(),
        ));
    }
    if event.payment_provider != REDEMPTION_PAYMENT_PROVIDER
        && event.source_order_id < topup_rebate_active_from_id
    {
        return Err(IngestError::Invalid(format!(
            "topup {} 早于在线充值返利生效边界 {}",
            event.source_order_id, topup_rebate_active_from_id
        )));
    }
    Ok(())
}

fn rebate_record_order_id(event: &ValidatedEvent) -> i64 {
    if event.payment_provider == REDEMPTION_PAYMENT_PROVIDER {
        -event.source_order_id
    } else {
        event.source_order_id
    }
}

fn source_payload_hash(event: &ValidatedEvent) -> std::result::Result<String, IngestError> {
    let payload = serde_json::to_vec(event)
        .map_err(|error| IngestError::Invalid(format!("事件序列化失败: {error}")))?;
    Ok(hex::encode(Sha256::digest(payload)))
}

pub(crate) fn calculate_rebate_from_credited_quota(
    credited_quota: i64,
    rate: Decimal,
) -> std::result::Result<i64, IngestError> {
    if credited_quota <= 0 {
        return Err(IngestError::Invalid(
            "credited_quota 必须为正整数".to_string(),
        ));
    }
    Decimal::from(credited_quota)
        .checked_mul(rate)
        .ok_or_else(|| IngestError::Invalid("返利额度超出可计算范围".to_string()))?
        .round_dp_with_strategy(0, RoundingStrategy::MidpointAwayFromZero)
        .to_i64()
        .ok_or_else(|| IngestError::Invalid("返利额度超出整数范围".to_string()))
}

fn credited_quota_for_event(
    event: &ValidatedEvent,
    quota_per_unit: i64,
) -> std::result::Result<i64, IngestError> {
    if let Some(credited_quota) = event.credited_quota {
        return Ok(credited_quota);
    }
    let amount_cents = event
        .rebate_base_amount_cents
        .ok_or_else(|| IngestError::Invalid("schema v1 缺少返利基数".to_string()))?;
    amount_cents
        .checked_mul(quota_per_unit / 100)
        .ok_or_else(|| IngestError::Invalid("credited_quota 超出整数范围".to_string()))
}

async fn lock_existing_record(
    transaction: &mut Transaction<'_, Postgres>,
    event: &ValidatedEvent,
) -> std::result::Result<Option<ExistingRecord>, IngestError> {
    let rows = sqlx::query(
        r#"
        SELECT id, order_type, order_id, canonical_event_key,
               source_payload_sha256, source_schema_version,
               inviter_id, invitee_id, user_group,
               order_amount_cents AS stored_order_amount_cents,
               credited_quota,
               rebate_amount, rebate_rate::TEXT AS rebate_rate, status, order_time,
               initialization_ends_at, rebate_effective_at, admin_adjusted
        FROM rebate_records
        WHERE canonical_event_key = $1
           OR (order_type = 'topup' AND order_id = $2)
        ORDER BY CASE WHEN canonical_event_key = $1 THEN 0 ELSE 1 END, id
        FOR UPDATE
        "#,
    )
    .bind(&event.event_key)
    .bind(rebate_record_order_id(event))
    .fetch_all(&mut **transaction)
    .await?;
    if rows.len() > 1 {
        return Err(IngestError::Conflict(
            "事件键与订单号指向不同返利记录".to_string(),
        ));
    }
    rows.into_iter()
        .next()
        .map(|row| {
            Ok(ExistingRecord {
                id: row.try_get("id")?,
                order_type: row.try_get("order_type")?,
                order_id: row.try_get("order_id")?,
                canonical_event_key: row.try_get("canonical_event_key")?,
                source_payload_sha256: row.try_get("source_payload_sha256")?,
                source_schema_version: row.try_get("source_schema_version")?,
                inviter_id: row.try_get("inviter_id")?,
                invitee_id: row.try_get("invitee_id")?,
                user_group: row.try_get("user_group")?,
                stored_order_amount_cents: row.try_get("stored_order_amount_cents")?,
                credited_quota: row.try_get("credited_quota")?,
                rebate_amount: row.try_get("rebate_amount")?,
                rebate_rate: row.try_get("rebate_rate")?,
                status: row.try_get("status")?,
                order_time: row.try_get("order_time")?,
                initialization_ends_at: row.try_get("initialization_ends_at")?,
                rebate_effective_at: row.try_get("rebate_effective_at")?,
                admin_adjusted: row.try_get("admin_adjusted")?,
            })
        })
        .transpose()
}

async fn ingest_existing_record(
    transaction: &mut Transaction<'_, Postgres>,
    event: &ValidatedEvent,
    record: ExistingRecord,
    context: ExistingRecordIngestContext<'_>,
) -> std::result::Result<(IngestOutcome, i64), IngestError> {
    if record.canonical_event_key.as_deref() == Some(event.event_key.as_str()) {
        validate_canonical_record_and_snapshot(
            transaction,
            event,
            context.inviter_id,
            context.payload_hash,
            &record,
        )
        .await?;
        return Ok((IngestOutcome::Replay, record.id));
    }
    if record.canonical_event_key.is_some()
        || record.inviter_id != context.inviter_id
        || record.invitee_id != event.invitee_id
    {
        return Err(IngestError::Conflict(
            "源订单与已有返利记录的经济身份不一致".to_string(),
        ));
    }
    let rule = load_rebate_rule(transaction, &event.inviter_group).await?;
    let credited_quota = credited_quota_for_event(event, context.quota_per_unit)?;
    let rebate_quota = calculate_rebate_from_credited_quota(credited_quota, rule.rate)?;
    validate_existing_record_for_policy(
        event,
        context.inviter_id,
        &record,
        context.policy,
        &rule,
        rebate_quota,
        credited_quota,
    )?;
    let rate_text = rule.rule_id.map(|_| rule.rate.to_string());
    let updated = sqlx::query(
        r#"
        UPDATE rebate_records
        SET canonical_event_key = $1,
            source_payload_sha256 = $2,
            source_schema_version = $3,
            order_amount_cents = $4,
            credited_quota = $5,
            user_group = $6,
            order_time = $7,
            initialization_ends_at = $8,
            rebate_effective_at = $9,
            rebate_rate = $10::NUMERIC,
            rebate_amount = $11,
            updated_at = GREATEST(updated_at, $12)
        WHERE id = $13 AND canonical_event_key IS NULL
          AND status = 'initializing' AND admin_adjusted = FALSE
        "#,
    )
    .bind(&event.event_key)
    .bind(context.payload_hash)
    .bind(event.schema_version)
    .bind(event.rebate_base_amount_cents)
    .bind(credited_quota)
    .bind(&event.inviter_group)
    .bind(event.completed_at)
    .bind(event.completed_at + INITIALIZATION_SECONDS)
    .bind(event.completed_at + REBATE_EFFECTIVE_DELAY_SECONDS)
    .bind(rate_text)
    .bind(rebate_quota)
    .bind(chrono::Utc::now().timestamp())
    .bind(record.id)
    .execute(&mut **transaction)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(IngestError::Conflict(
            "已有返利记录在接收期间发生变化".to_string(),
        ));
    }
    insert_calculation_snapshot(
        transaction,
        record.id,
        event,
        &rule,
        rebate_quota,
        credited_quota,
        context.quota_per_unit,
    )
    .await?;
    Ok((IngestOutcome::Created, record.id))
}

fn validate_existing_record_for_policy(
    event: &ValidatedEvent,
    inviter_id: i64,
    record: &ExistingRecord,
    policy: ExistingRecordPolicy,
    rule: &RebateRuleSnapshot,
    rebate_quota: i64,
    credited_quota: i64,
) -> std::result::Result<(), IngestError> {
    if record.inviter_id != inviter_id || record.invitee_id != event.invitee_id {
        return Err(IngestError::Conflict(
            "源订单与已有返利记录的经济身份不一致".to_string(),
        ));
    }
    if record.status != "initializing" || record.admin_adjusted {
        return Err(IngestError::Conflict(
            "已有返利记录不是可安全接入的未调整初始化记录".to_string(),
        ));
    }
    if matches!(policy, ExistingRecordPolicy::AuditedLegacyAttach) {
        return Ok(());
    }
    if event.schema_version == CREDITED_QUOTA_SCHEMA_VERSION
        && is_pristine_legacy_placeholder(record)
    {
        return Ok(());
    }

    let expected_rate = rule.rule_id.map(|_| rule.rate);
    let actual_rate = record
        .rebate_rate
        .as_deref()
        .map(|value| {
            value
                .parse::<Decimal>()
                .map_err(|_| IngestError::Conflict("已有返利记录的费率格式无效".to_string()))
        })
        .transpose()?;
    if record.user_group != event.inviter_group
        || record.stored_order_amount_cents != event.rebate_base_amount_cents
        || record.credited_quota != Some(credited_quota)
        || actual_rate != expected_rate
        || record.rebate_amount != rebate_quota
        || record.order_time != event.completed_at
        || !matches_supported_rebate_schedule(
            event.completed_at,
            record.initialization_ends_at,
            record.rebate_effective_at,
        )
    {
        return Err(IngestError::Conflict(
            "已有返利记录的会计字段与源事件不一致".to_string(),
        ));
    }
    Ok(())
}

fn is_pristine_legacy_placeholder(record: &ExistingRecord) -> bool {
    record.canonical_event_key.is_none()
        && record.source_payload_sha256.is_none()
        && record.source_schema_version.is_none()
        && record.stored_order_amount_cents.is_none()
        && record.credited_quota.is_none()
        && record.rebate_amount == 0
        && record.rebate_rate.is_none()
        && record.status == "initializing"
        && !record.admin_adjusted
}

async fn validate_canonical_record_and_snapshot(
    transaction: &mut Transaction<'_, Postgres>,
    event: &ValidatedEvent,
    inviter_id: i64,
    payload_hash: &str,
    record: &ExistingRecord,
) -> std::result::Result<(), IngestError> {
    if record.order_type != TOPUP_ORDER_TYPE
        || record.order_id != rebate_record_order_id(event)
        || record.source_payload_sha256.as_deref() != Some(payload_hash)
        || record.source_schema_version != Some(event.schema_version)
        || record.inviter_id != inviter_id
        || record.invitee_id != event.invitee_id
        || record.user_group != event.inviter_group
        || record.stored_order_amount_cents != event.rebate_base_amount_cents
        || record.order_time != event.completed_at
        || !matches_supported_rebate_schedule(
            event.completed_at,
            record.initialization_ends_at,
            record.rebate_effective_at,
        )
        || record.admin_adjusted
    {
        return Err(IngestError::Conflict(
            "已有 canonical 返利记录与源事件不一致".to_string(),
        ));
    }

    let row = sqlx::query(
        r#"
        SELECT inviter_group, rule_type, rebate_rate::TEXT AS rebate_rate,
               quota_per_unit, order_amount_minor, credited_quota, rebate_quota,
               source_revision, snapshotted_at
        FROM rebate_calculation_snapshots
        WHERE rebate_record_id = $1
        FOR UPDATE
        "#,
    )
    .bind(record.id)
    .fetch_optional(&mut **transaction)
    .await?;
    let Some(row) = row else {
        return Err(IngestError::Conflict(
            "已有 canonical 返利记录缺少冻结规则快照".to_string(),
        ));
    };
    let snapshot_rate = row
        .try_get::<String, _>("rebate_rate")?
        .parse::<Decimal>()
        .map_err(|_| IngestError::Conflict("冻结规则快照费率格式无效".to_string()))?;
    let record_rate = record
        .rebate_rate
        .as_deref()
        .map(|value| {
            value
                .parse::<Decimal>()
                .map_err(|_| IngestError::Conflict("canonical 记录费率格式无效".to_string()))
        })
        .transpose()?;
    let snapshot_quota_per_unit: i64 = row.try_get("quota_per_unit")?;
    let snapshot_credited_quota: Option<i64> = row.try_get("credited_quota")?;
    let expected_credited_quota = if let Some(credited_quota) = event.credited_quota {
        credited_quota
    } else {
        event
            .rebate_base_amount_cents
            .and_then(|amount| amount.checked_mul(snapshot_quota_per_unit / 100))
            .ok_or_else(|| IngestError::Conflict("冻结 credited_quota 无法校验".to_string()))?
    };
    let calculated_quota =
        calculate_rebate_from_credited_quota(expected_credited_quota, snapshot_rate)?;
    let record_rate_matches =
        record_rate == Some(snapshot_rate) || (record_rate.is_none() && snapshot_rate.is_zero());
    if row.try_get::<String, _>("inviter_group")? == event.inviter_group
        && row.try_get::<String, _>("rule_type")? == TOPUP_ORDER_TYPE
        && snapshot_quota_per_unit > 0
        && snapshot_quota_per_unit % 100 == 0
        && row.try_get::<i64, _>("order_amount_minor")?
            == event.rebate_base_amount_cents.unwrap_or(0)
        && snapshot_credited_quota == Some(expected_credited_quota)
        && record.credited_quota == Some(expected_credited_quota)
        && row.try_get::<i64, _>("rebate_quota")? == record.rebate_amount
        && record.rebate_amount == calculated_quota
        && record_rate_matches
        && row.try_get::<String, _>("source_revision")? == source_revision(event)
        && row.try_get::<i64, _>("snapshotted_at")? > 0
    {
        return Ok(());
    }
    Err(IngestError::Conflict(
        "已有 canonical 返利记录与冻结规则快照不一致".to_string(),
    ))
}

async fn load_rebate_rule(
    transaction: &mut Transaction<'_, Postgres>,
    inviter_group: &str,
) -> std::result::Result<RebateRuleSnapshot, IngestError> {
    let row = sqlx::query(
        r#"
        SELECT id, rebate_rate::TEXT AS rebate_rate
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
    let Some(row) = row else {
        return Ok(RebateRuleSnapshot {
            rule_id: None,
            rate: Decimal::ZERO,
        });
    };
    let rate_text: String = row.try_get("rebate_rate")?;
    let rate = rate_text
        .parse::<Decimal>()
        .map_err(|_| IngestError::Invalid("返利规则格式无效".to_string()))?;
    if rate < Decimal::ZERO || rate > Decimal::ONE {
        return Err(IngestError::Invalid("返利规则超出 0 到 1".to_string()));
    }
    Ok(RebateRuleSnapshot {
        rule_id: Some(row.try_get("id")?),
        rate,
    })
}

async fn insert_rebate_record(
    transaction: &mut Transaction<'_, Postgres>,
    event: &ValidatedEvent,
    inviter_id: i64,
    payload_hash: &str,
    rule: &RebateRuleSnapshot,
    rebate_quota: i64,
    credited_quota: i64,
) -> std::result::Result<i64, IngestError> {
    let now = chrono::Utc::now().timestamp();
    let rate_text = rule.rule_id.map(|_| rule.rate.to_string());
    let record_id = sqlx::query_scalar(
        r#"
        INSERT INTO rebate_records (
            ulid, inviter_id, invitee_id, user_group, order_type, order_id,
            order_amount, order_time, rebate_effective_at, rebate_amount, rebate_rate,
            status, scan_started_at, initialization_ends_at, created_at, updated_at,
            canonical_event_key, source_payload_sha256, source_schema_version,
            order_amount_cents, credited_quota
        ) VALUES (
            $1, $2, $3, $4, 'topup', $5,
            COALESCE($6::NUMERIC / 100, 0), $7, $8, $9, $10::NUMERIC,
            'initializing', $7, $11, $12, $12,
            $13, $14, $15, $6, $16
        )
        RETURNING id
        "#,
    )
    .bind(ulid::Ulid::new().to_string())
    .bind(inviter_id)
    .bind(event.invitee_id)
    .bind(&event.inviter_group)
    .bind(rebate_record_order_id(event))
    .bind(event.rebate_base_amount_cents)
    .bind(event.completed_at)
    .bind(event.completed_at + REBATE_EFFECTIVE_DELAY_SECONDS)
    .bind(rebate_quota)
    .bind(rate_text)
    .bind(event.completed_at + INITIALIZATION_SECONDS)
    .bind(now)
    .bind(&event.event_key)
    .bind(payload_hash)
    .bind(event.schema_version)
    .bind(credited_quota)
    .fetch_one(&mut **transaction)
    .await?;
    Ok(record_id)
}

async fn insert_calculation_snapshot(
    transaction: &mut Transaction<'_, Postgres>,
    record_id: i64,
    event: &ValidatedEvent,
    rule: &RebateRuleSnapshot,
    rebate_quota: i64,
    credited_quota: i64,
    quota_per_unit: i64,
) -> std::result::Result<(), IngestError> {
    let result = sqlx::query(
        r#"
        INSERT INTO rebate_calculation_snapshots (
            rebate_record_id, inviter_group, rule_id, rule_type, rebate_rate,
            quota_per_unit, order_amount_minor, credited_quota,
            rebate_quota, source_revision, snapshotted_at
        ) VALUES ($1, $2, $3, 'topup', $4::NUMERIC, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (rebate_record_id) DO NOTHING
        "#,
    )
    .bind(record_id)
    .bind(&event.inviter_group)
    .bind(rule.rule_id)
    .bind(rule.rate.to_string())
    .bind(quota_per_unit)
    .bind(event.rebate_base_amount_cents.unwrap_or(0))
    .bind(credited_quota)
    .bind(rebate_quota)
    .bind(source_revision(event))
    .bind(chrono::Utc::now().timestamp())
    .execute(&mut **transaction)
    .await?;
    if result.rows_affected() == 1 {
        return Ok(());
    }

    let matches: bool = sqlx::query_scalar(
        r#"
        SELECT inviter_group = $2
           AND rule_id IS NOT DISTINCT FROM $3
           AND rule_type = 'topup'
           AND rebate_rate = $4::NUMERIC
           AND quota_per_unit = $5
           AND order_amount_minor = $6
           AND credited_quota = $7
           AND rebate_quota = $8
           AND source_revision = $9
        FROM rebate_calculation_snapshots
        WHERE rebate_record_id = $1
        "#,
    )
    .bind(record_id)
    .bind(&event.inviter_group)
    .bind(rule.rule_id)
    .bind(rule.rate.to_string())
    .bind(quota_per_unit)
    .bind(event.rebate_base_amount_cents.unwrap_or(0))
    .bind(credited_quota)
    .bind(rebate_quota)
    .bind(source_revision(event))
    .fetch_one(&mut **transaction)
    .await?;
    if matches {
        return Ok(());
    }
    Err(IngestError::Conflict(
        "已有返利规则快照与源事件不一致".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pristine_legacy_placeholder() -> ExistingRecord {
        ExistingRecord {
            id: 1,
            order_type: TOPUP_ORDER_TYPE.to_string(),
            order_id: 842,
            canonical_event_key: None,
            source_payload_sha256: None,
            source_schema_version: None,
            inviter_id: 1001,
            invitee_id: 3418,
            user_group: "default".to_string(),
            stored_order_amount_cents: None,
            credited_quota: None,
            rebate_amount: 0,
            rebate_rate: None,
            status: "initializing".to_string(),
            order_time: 1,
            initialization_ends_at: 1,
            rebate_effective_at: 1,
            admin_adjusted: false,
        }
    }

    fn event() -> RebateSourceEvent {
        RebateSourceEvent {
            schema_version: 1,
            event_key: "newapi:topup:842".to_string(),
            event_type: "topup.completed".to_string(),
            source_order_id: 842,
            invitee_id: 3418,
            inviter_id: Some(1001),
            inviter_group: "default".to_string(),
            rebate_base_amount_cents: Some(7500),
            credited_quota: None,
            completed_at: 1_784_211_072,
            payment_provider: "epay".to_string(),
        }
    }

    fn redemption_event() -> RebateSourceEvent {
        RebateSourceEvent {
            schema_version: 2,
            event_key: "newapi:redemption:7878".to_string(),
            event_type: "topup.completed".to_string(),
            source_order_id: 7878,
            invitee_id: 3120,
            inviter_id: Some(3118),
            inviter_group: "default".to_string(),
            rebate_base_amount_cents: None,
            credited_quota: Some(500_000_000),
            completed_at: 1_784_010_626,
            payment_provider: REDEMPTION_PAYMENT_PROVIDER.to_string(),
        }
    }

    #[test]
    fn validates_canonical_event_identity_and_normalizes_empty_group() {
        let mut input = event();
        input.inviter_group = "  ".to_string();
        let validated = validate_event(input).unwrap();
        assert_eq!(validated.inviter_group, "default");

        let mut invalid = event();
        invalid.event_key = "newapi:topup:843".to_string();
        assert!(matches!(
            validate_event(invalid),
            Err(IngestError::Invalid(_))
        ));

        let mut overflowing = event();
        overflowing.completed_at = i64::MAX;
        assert!(matches!(
            validate_event(overflowing),
            Err(IngestError::Invalid(_))
        ));
    }

    #[test]
    fn rejects_topups_before_the_configured_active_boundary() {
        let validated = validate_event(event()).unwrap();
        assert!(validate_topup_rebate_scope(&validated, 842).is_ok());
        assert!(matches!(
            validate_topup_rebate_scope(&validated, 843),
            Err(IngestError::Invalid(_))
        ));
        assert!(matches!(
            validate_topup_rebate_scope(&validated, 0),
            Err(IngestError::Invalid(_))
        ));
    }

    #[test]
    fn accepts_redemption_event_identity_and_maps_to_legacy_order_id() {
        let validated = validate_event(redemption_event()).unwrap();
        assert_eq!(validated.event_key, "newapi:redemption:7878");
        assert_eq!(validated.payment_provider, REDEMPTION_PAYMENT_PROVIDER);
        assert_eq!(rebate_record_order_id(&validated), -7878);
    }

    #[test]
    fn rejects_redemption_provider_and_event_key_mismatches() {
        let mut redemption_with_topup_key = redemption_event();
        redemption_with_topup_key.event_key = "newapi:topup:7878".to_string();
        assert!(matches!(
            validate_event(redemption_with_topup_key),
            Err(IngestError::Invalid(_))
        ));

        let mut topup_with_redemption_key = event();
        topup_with_redemption_key.event_key = "newapi:redemption:842".to_string();
        assert!(matches!(
            validate_event(topup_with_redemption_key),
            Err(IngestError::Invalid(_))
        ));
    }

    #[test]
    fn redemptions_bypass_the_online_topup_active_boundary() {
        let validated = validate_event(redemption_event()).unwrap();
        assert!(validate_topup_rebate_scope(&validated, 9_000).is_ok());
        assert!(matches!(
            validate_topup_rebate_scope(&validated, 0),
            Err(IngestError::Invalid(_))
        ));
    }

    #[test]
    fn hashes_semantic_payload_and_ignores_unknown_json_fields() {
        let with_unknown: RebateSourceEvent = serde_json::from_value(serde_json::json!({
            "schema_version": 1,
            "event_key": "newapi:topup:842",
            "event_type": "topup.completed",
            "source_order_id": 842,
            "invitee_id": 3418,
            "inviter_id": 1001,
            "inviter_group": "default",
            "rebate_base_amount_cents": 7500,
            "completed_at": 1784211072_i64,
            "payment_provider": "epay",
            "future_field": "accepted"
        }))
        .unwrap();
        assert_eq!(
            source_payload_hash(&validate_event(with_unknown).unwrap()).unwrap(),
            source_payload_hash(&validate_event(event()).unwrap()).unwrap()
        );
    }

    #[test]
    fn rounds_positive_quota_half_away_from_zero_without_float_input() {
        assert_eq!(
            calculate_rebate_from_credited_quota(10, "0.05".parse().unwrap()).unwrap(),
            1
        );
    }

    #[test]
    fn schema_v2_accepts_only_credited_quota_as_the_rebate_base() {
        let mut input = event();
        input.schema_version = 2;
        input.rebate_base_amount_cents = None;
        input.credited_quota = Some(37_500_000);
        let validated = validate_event(input).unwrap();
        assert_eq!(validated.credited_quota, Some(37_500_000));

        let mut invalid = event();
        invalid.schema_version = 2;
        invalid.credited_quota = Some(37_500_000);
        assert!(matches!(
            validate_event(invalid),
            Err(IngestError::Invalid(_))
        ));
    }

    #[test]
    fn strict_online_ingest_accepts_only_pristine_legacy_placeholders() {
        let record = pristine_legacy_placeholder();
        assert!(is_pristine_legacy_placeholder(&record));
        let mut schema_v2 = event();
        schema_v2.schema_version = 2;
        schema_v2.rebate_base_amount_cents = None;
        schema_v2.credited_quota = Some(37_500_000);
        let schema_v2 = validate_event(schema_v2).unwrap();
        let rule = RebateRuleSnapshot {
            rule_id: Some(1),
            rate: "0.05".parse().unwrap(),
        };
        assert!(validate_existing_record_for_policy(
            &schema_v2,
            1001,
            &record,
            ExistingRecordPolicy::StrictOnline,
            &rule,
            1_875_000,
            37_500_000,
        )
        .is_ok());

        let schema_v1 = validate_event(event()).unwrap();
        assert!(matches!(
            validate_existing_record_for_policy(
                &schema_v1,
                1001,
                &record,
                ExistingRecordPolicy::StrictOnline,
                &rule,
                1_875_000,
                37_500_000,
            ),
            Err(IngestError::Conflict(_))
        ));

        for mutate in [
            |record: &mut ExistingRecord| record.rebate_amount = 1,
            |record: &mut ExistingRecord| record.rebate_rate = Some("0.05".to_string()),
            |record: &mut ExistingRecord| record.credited_quota = Some(1),
            |record: &mut ExistingRecord| record.stored_order_amount_cents = Some(1),
            |record: &mut ExistingRecord| record.source_schema_version = Some(2),
            |record: &mut ExistingRecord| record.source_payload_sha256 = Some("a".repeat(64)),
            |record: &mut ExistingRecord| {
                record.canonical_event_key = Some("newapi:topup:842".to_string())
            },
            |record: &mut ExistingRecord| record.status = "pending".to_string(),
            |record: &mut ExistingRecord| record.admin_adjusted = true,
        ] {
            let mut changed = pristine_legacy_placeholder();
            mutate(&mut changed);
            assert!(!is_pristine_legacy_placeholder(&changed));
        }
    }

    #[test]
    fn strict_online_ingest_accepts_the_previous_complete_schedule() {
        let event = validate_event(event()).unwrap();
        let rule = RebateRuleSnapshot {
            rule_id: Some(1),
            rate: "0.05".parse().unwrap(),
        };
        let mut record = pristine_legacy_placeholder();
        record.user_group = event.inviter_group.clone();
        record.stored_order_amount_cents = event.rebate_base_amount_cents;
        record.credited_quota = Some(37_500_000);
        record.rebate_amount = 1_875_000;
        record.rebate_rate = Some("0.05".to_string());
        record.order_time = event.completed_at;
        record.initialization_ends_at = event.completed_at + 2 * 24 * 60 * 60;
        record.rebate_effective_at = event.completed_at + 7 * 24 * 60 * 60;

        assert!(validate_existing_record_for_policy(
            &event,
            1001,
            &record,
            ExistingRecordPolicy::StrictOnline,
            &rule,
            1_875_000,
            37_500_000,
        )
        .is_ok());

        record.rebate_effective_at = event.completed_at + REBATE_EFFECTIVE_DELAY_SECONDS;
        assert!(matches!(
            validate_existing_record_for_policy(
                &event,
                1001,
                &record,
                ExistingRecordPolicy::StrictOnline,
                &rule,
                1_875_000,
                37_500_000,
            ),
            Err(IngestError::Conflict(_))
        ));
    }

    #[test]
    fn bearer_authentication_accepts_only_the_dedicated_secret() {
        let secret = "0123456789abcdef0123456789abcdef";
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {secret}").parse().unwrap(),
        );
        assert!(authorize_bearer(&headers, secret).is_ok());

        headers.insert(
            axum::http::header::AUTHORIZATION,
            "Bearer wrong-secret".parse().unwrap(),
        );
        assert!(matches!(
            authorize_bearer(&headers, secret),
            Err(AppError::Unauthorized)
        ));
    }
}
