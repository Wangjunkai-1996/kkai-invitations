use std::collections::{HashMap, HashSet};

use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::Json;
use chrono::{TimeZone, Utc};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgRow;
use sqlx::Row;

use crate::error::{
    AppError, Result, FUNDS_MAINTENANCE_MESSAGE, LEGACY_COMPLETION_DISABLED_MESSAGE,
};
use crate::invitation_signup::{
    fetch_paid_invitee_ids, insert_invitation_signup_reward_record,
    resolve_invitation_signup_reward_unlocked_at, unlock_paid_invitation_signup_rewards,
    upsert_invitation_registration, InsertInvitationSignupReward, RewardInsertOutcome,
    UpsertInvitationRegistration,
};
use crate::middleware::AdminUser;
use crate::payout::{pay_rebate_record, request_id_from_headers, PayRecordInput, PayResult};
use crate::rebate_lifecycle::{
    finalize_initializing_records_by_ids, rebate_amount_for_cents,
    rebate_amount_for_credited_quota, rebate_effective_at_for_cutover, ALL_USER_GROUP,
    INITIALIZATION_SECONDS, SALES_USER_GROUP,
};
use crate::rebate_types::{
    is_invitation_signup_reward_order_type, is_order_rebate_order_type,
    INVITATION_SIGNUP_INVITEE_ORDER_TYPE, INVITATION_SIGNUP_INVITER_ORDER_TYPE,
    REDEMPTION_ORDER_TYPE,
};
use crate::response::{message, ok, ApiResponse, PaginatedResponse};
use crate::routes::user::{
    cents_to_quota, count_rebate_records, count_user_related_rebate_records, fetch_rebate_records,
    fetch_user_related_rebate_records, quota_to_cents, PaginationParams, RebateRecord,
    RebateRecordSourceFilter,
};
use crate::sales_reprice::load_verified_receipt_in_transaction;
use crate::system_config::{
    get_config_bool, get_config_i64, get_config_i64_or_fallback,
    INVITATION_SIGNUP_INVITEE_REWARD_AMOUNT_KEY,
    INVITATION_SIGNUP_INVITEE_REWARD_REQUIRES_PAID_ORDER_KEY,
    INVITATION_SIGNUP_INVITER_REWARD_AMOUNT_KEY,
    INVITATION_SIGNUP_INVITER_REWARD_REQUIRES_PAID_ORDER_KEY, INVITATION_SIGNUP_REWARD_AMOUNT_KEY,
    INVITATION_SIGNUP_REWARD_ENABLED_KEY, INVITATION_SIGNUP_REWARD_REVIEW_REQUIRED_KEY,
    ORDER_REBATE_ENABLED_KEY, REBATE_TO_BALANCE_ENABLED_KEY, USER_INVITATION_REBATE_ENABLED_KEY,
};
use crate::AppState;

const MAX_APPROVE_AND_PAY_BATCH_SIZE: usize = 100;
const MAX_PAYOUT_REQUEST_ID_LEN: usize = 128;

#[derive(Serialize, Deserialize)]
pub struct RebateRule {
    pub id: i64,
    pub user_group: String,
    pub rule_type: String,
    pub rebate_rate: String,
    pub effective_time: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Deserialize)]
pub struct RebateRuleRequest {
    pub user_group: String,
    pub rule_type: String,
    pub rebate_rate: serde_json::Value,
}

struct ValidatedRebateRuleRequest {
    user_group: String,
    rule_type: String,
    rebate_rate: f64,
}

#[derive(Serialize)]
pub struct UserGroup {
    pub name: String,
    pub user_count: i64,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SystemConfig {
    pub min_rebate_request_amount: i64,
    pub rebate_request_frequency_days: i64,
    pub user_invitation_rebate_enabled: bool,
    pub order_rebate_enabled: bool,
    pub invitation_signup_reward_enabled: bool,
    #[serde(default)]
    pub invitation_signup_reward_amount: i64,
    #[serde(default)]
    pub invitation_signup_inviter_reward_amount: i64,
    #[serde(default)]
    pub invitation_signup_invitee_reward_amount: i64,
    pub invitation_signup_reward_review_required: bool,
    pub invitation_signup_inviter_reward_requires_paid_order: bool,
    pub invitation_signup_invitee_reward_requires_paid_order: bool,
    pub rebate_to_balance_enabled: bool,
}

#[derive(Deserialize)]
pub struct ApproveRequest {
    pub note: Option<String>,
    pub reason: Option<String>,
}

#[derive(Deserialize)]
pub struct ApproveAndPayRequest {
    pub note: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchApproveAndPayRequest {
    pub request_ids: Vec<i64>,
    pub note: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApproveAndPayRecordResult {
    pub record_id: i64,
    pub outcome: String,
    pub amount_cents: Option<i64>,
    pub quota_delta: Option<i64>,
    pub payout_id: Option<i64>,
    pub newapi_log_id: Option<i64>,
    pub newapi_mark_id: Option<i64>,
    pub error: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApproveAndPayResponse {
    pub request_id: i64,
    pub status: String,
    pub total_amount: i64,
    pub paid_count: usize,
    pub already_paid_count: usize,
    pub pending_count: usize,
    pub failed_count: usize,
    pub items: Vec<ApproveAndPayRecordResult>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchApproveAndPayError {
    pub request_id: i64,
    pub error: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchApproveAndPayResponse {
    pub total_requests: usize,
    pub succeeded_requests: usize,
    pub failed_requests: usize,
    pub paid_count: usize,
    pub already_paid_count: usize,
    pub pending_count: usize,
    pub failed_count: usize,
    pub items: Vec<ApproveAndPayResponse>,
    pub errors: Vec<BatchApproveAndPayError>,
}

enum PreparedApproveAndPayRequest {
    Pay {
        rebate_record_ids: Vec<i64>,
        total_amount: i64,
    },
    AlreadyCompleted {
        rebate_record_ids: Vec<i64>,
        total_amount: i64,
    },
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RebateRequestDetail {
    pub id: i64,
    pub user_id: i64,
    pub user_name: String,
    pub amount: i64,
    pub status: String,
    pub rebate_record_ids: Vec<i64>,
    pub created_at: String,
    pub updated_at: String,
    pub approved_at: Option<String>,
    pub completed_at: Option<String>,
    pub rejected_at: Option<String>,
    pub review_note: Option<String>,
    pub reject_reason: Option<String>,
    pub request_kind: String,
    pub issued_at: Option<String>,
    pub issue_note: Option<String>,
    pub credit_currency: &'static str,
}

#[derive(Serialize)]
pub struct RebateStats {
    pub total_rebate: i64,
    pub completed_rebate: i64,
    pub pending_rebate: i64,
    pub requested_rebate: i64,
    pub approved_rebate: i64,
    pub total_invitations: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminRebateOrderRecord {
    pub order_type: String,
    pub order_id: i64,
    pub inviter_id: i64,
    pub inviter_name: Option<String>,
    pub invitee_id: i64,
    pub invitee_name: Option<String>,
    pub user_group: String,
    pub order_amount: i64,
    pub rebate_amount: i64,
    pub rebate_ratio: Option<f64>,
    pub status: String,
    pub rule_missing: bool,
    pub local_rebate_record_id: Option<i64>,
    pub local_rebate_status: Option<String>,
    pub order_time: String,
    pub effective_at: String,
    pub scan_started_at: String,
    pub initialization_ends_at: String,
    pub finalized_at: Option<String>,
    pub closed_at: Option<String>,
    pub admin_adjusted: bool,
    pub can_modify: bool,
    pub can_close: bool,
    pub can_reopen: bool,
    pub can_end_initialization: bool,
    pub can_extend_initialization: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminInvitationRegistration {
    pub id: i64,
    pub inviter_id: i64,
    pub inviter_name: Option<String>,
    pub invitee_id: i64,
    pub invitee_name: Option<String>,
    pub user_group: String,
    pub invited_at: String,
    pub total_reward_amount: i64,
    pub inviter_reward_generated: bool,
    pub inviter_reward_record_id: Option<i64>,
    pub inviter_reward_amount: i64,
    pub inviter_reward_status: Option<String>,
    pub invitee_reward_generated: bool,
    pub invitee_reward_record_id: Option<i64>,
    pub invitee_reward_amount: i64,
    pub invitee_reward_status: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InvitationRegistrationRewardResponse {
    pub generated: bool,
    pub record_id: Option<i64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InvitationRegistrationRewardRevokeResponse {
    pub revoked: bool,
    pub record_id: Option<i64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RebateRequestListParams {
    pub request_kind: Option<String>,
    pub status: Option<String>,
    pub page: Option<i64>,
    pub page_size: Option<i64>,
}

impl RebateRequestListParams {
    fn request_kind(&self) -> Result<&str> {
        match self.request_kind.as_deref().unwrap_or("ordinary") {
            "ordinary" => Ok("ordinary"),
            "sales" => Ok("sales"),
            _ => Err(AppError::BadRequest("返利申请类别无效".into())),
        }
    }
    fn page(&self) -> i64 {
        self.page.unwrap_or(1).max(1)
    }

    fn page_size(&self) -> i64 {
        self.page_size.unwrap_or(100).clamp(1, 1000)
    }

    fn offset(&self) -> i64 {
        (self.page() - 1).saturating_mul(self.page_size())
    }

    fn status_filter(&self) -> Option<&str> {
        match self.status.as_deref() {
            Some("pending" | "approved" | "rejected" | "completed") => self.status.as_deref(),
            _ => None,
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminRebateRecordListParams {
    pub status: Option<String>,
    pub source: Option<String>,
    pub page: Option<i64>,
    pub page_size: Option<i64>,
}

impl AdminRebateRecordListParams {
    fn page(&self) -> i64 {
        self.page.unwrap_or(1).max(1)
    }

    fn page_size(&self) -> i64 {
        self.page_size.unwrap_or(100).clamp(1, 1000)
    }

    fn offset(&self) -> i64 {
        (self.page() - 1).saturating_mul(self.page_size())
    }

    fn status_filter(&self) -> Option<&str> {
        match self.status.as_deref() {
            Some("pending" | "requested" | "approved" | "completed" | "rejected") => {
                self.status.as_deref()
            }
            _ => None,
        }
    }

    fn source_filter(&self) -> Option<RebateRecordSourceFilter> {
        match self.source.as_deref() {
            Some("order") => Some(RebateRecordSourceFilter {
                order_rebate_enabled: true,
                invitation_signup_reward_enabled: false,
            }),
            Some("signup") => Some(RebateRecordSourceFilter {
                order_rebate_enabled: false,
                invitation_signup_reward_enabled: true,
            }),
            _ => None,
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RebateOrderRecordParams {
    pub order_type: Option<String>,
    pub page: Option<i64>,
    pub page_size: Option<i64>,
}

impl RebateOrderRecordParams {
    fn page(&self) -> i64 {
        self.page.unwrap_or(1).max(1)
    }

    fn page_size(&self) -> i64 {
        self.page_size.unwrap_or(20).clamp(1, 100)
    }

    fn offset(&self) -> i64 {
        (self.page() - 1).saturating_mul(self.page_size())
    }

    fn order_type_filter(&self) -> Option<&str> {
        match self.order_type.as_deref() {
            Some(order_type) if is_order_rebate_order_type(order_type) => {
                self.order_type.as_deref()
            }
            _ => None,
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InvitationRegistrationParams {
    pub page: Option<i64>,
    pub page_size: Option<i64>,
}

impl InvitationRegistrationParams {
    fn page(&self) -> i64 {
        self.page.unwrap_or(1).max(1)
    }

    fn page_size(&self) -> i64 {
        self.page_size.unwrap_or(20).clamp(1, 100)
    }

    fn offset(&self) -> i64 {
        (self.page() - 1).saturating_mul(self.page_size())
    }
}

struct LocalRebateRecord {
    id: i64,
    order_type: String,
    order_id: i64,
    canonical_event_key: Option<String>,
    superseded_by_record_id: Option<i64>,
    rebate_amount: i64,
    rebate_ratio: Option<f64>,
    status: String,
    order_time: i64,
    rebate_effective_at: i64,
    scan_started_at: i64,
    initialization_ends_at: i64,
    finalized_at: Option<i64>,
    closed_at: Option<i64>,
    admin_adjusted: bool,
}

#[derive(Clone)]
struct NewApiRebateOrder {
    order_type: String,
    order_id: i64,
    inviter_id: i64,
    inviter_name: Option<String>,
    invitee_id: i64,
    invitee_name: Option<String>,
    user_group: String,
    order_amount: i64,
    order_time: i64,
}

struct NewApiRebateOrderDiagnostics {
    invited_users: i64,
    topup_success: i64,
    topup_invited: i64,
    topup_rebate_orders: i64,
    subscription_success: i64,
    subscription_invited: i64,
    redemption_used: i64,
    redemption_invited: i64,
}

struct NewApiInvitationRegistration {
    inviter_id: i64,
    inviter_name: Option<String>,
    invitee_id: i64,
    invitee_name: Option<String>,
    user_group: String,
    invited_at: i64,
}

struct LocalInvitationRegistration {
    id: i64,
    inviter_id: i64,
    invitee_id: i64,
    user_group: String,
    invited_at: i64,
}

struct InvitationRegistrationReward {
    record_id: i64,
    rebate_amount: i64,
    status: String,
}

struct InvitationSignupRewardAmounts {
    inviter: i64,
    invitee: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateRebateOrderRecordsRequest {
    pub record_ids: Vec<i64>,
    pub rebate_amount: Option<i64>,
    pub rebate_ratio: Option<f64>,
}

#[derive(Clone, Copy)]
enum AdminRebateAccountingBase {
    CreditedQuota(i64),
    OrderAmountCents(i64),
}

impl AdminRebateAccountingBase {
    fn from_record(
        source_schema_version: Option<i32>,
        credited_quota: Option<i64>,
        order_amount_cents: i64,
    ) -> Result<Self> {
        match source_schema_version {
            Some(2) => credited_quota
                .filter(|quota| *quota > 0)
                .map(Self::CreditedQuota)
                .ok_or_else(|| {
                    AppError::Internal("schema v2 返利记录缺少有效 credited_quota".to_string())
                }),
            None | Some(1) => Ok(Self::OrderAmountCents(order_amount_cents)),
            Some(version) => Err(AppError::Internal(format!(
                "返利记录 source_schema_version 不受支持: {version}"
            ))),
        }
    }

    fn rebate_amount(self, rate: Decimal, quota_per_unit: i64) -> Result<i64> {
        match self {
            Self::CreditedQuota(credited_quota) => {
                rebate_amount_for_credited_quota(credited_quota, rate)
            }
            Self::OrderAmountCents(order_amount_cents) => {
                rebate_amount_for_cents(order_amount_cents, rate, quota_per_unit)
            }
        }
    }

    fn ratio_for_amount(self, amount_cents: i64, quota_per_unit: i64) -> Result<Option<f64>> {
        match self {
            Self::CreditedQuota(credited_quota) => {
                let rebate_quota =
                    rebate_amount_for_cents(amount_cents, Decimal::ONE, quota_per_unit)?;
                Decimal::from(rebate_quota)
                    .checked_div(Decimal::from(credited_quota))
                    .and_then(|ratio| ratio.to_f64())
                    .map(Some)
                    .ok_or_else(|| AppError::Internal("返利比例超出可计算范围".to_string()))
            }
            Self::OrderAmountCents(order_amount_cents) => Ok(derive_ratio_from_amount(
                Some(amount_cents),
                order_amount_cents,
            )),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RebateOrderRecordIdsRequest {
    pub record_ids: Vec<i64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExtendRebateInitializationRequest {
    pub record_ids: Vec<i64>,
    pub initialization_ends_at: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RebateOrderRecordBatchResponse {
    pub updated: i64,
}

// GET /api/admin/rebate-rules
pub async fn get_rebate_rules(
    State(state): State<AppState>,
    _admin: AdminUser,
) -> Result<Json<ApiResponse<Vec<RebateRule>>>> {
    let rows = sqlx::query(
        r#"
        SELECT id, user_group, rule_type, rebate_rate::TEXT as rebate_rate,
               effective_time, created_at, updated_at
        FROM rebate_rules
        ORDER BY CASE WHEN user_group = $1 THEN 0 ELSE 1 END, user_group, rule_type
        "#,
    )
    .bind(ALL_USER_GROUP)
    .fetch_all(&state.pool)
    .await?;

    Ok(ok(rows
        .iter()
        .map(rebate_rule_from_row)
        .collect::<Result<Vec<_>>>()?))
}

// POST /api/admin/rebate-rules
pub async fn create_rebate_rule(
    State(state): State<AppState>,
    admin: AdminUser,
    Json(req): Json<RebateRuleRequest>,
) -> Result<Json<ApiResponse<RebateRule>>> {
    let validated = validate_rebate_rule_request(&req)?;
    let now = Utc::now().timestamp();

    let row = sqlx::query(
        r#"
        INSERT INTO rebate_rules (user_group, rule_type, rebate_rate, effective_time, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $4, $4)
        ON CONFLICT (user_group, rule_type)
        DO UPDATE SET rebate_rate = $3, effective_time = $4, updated_at = $4
        WHERE rebate_rules.user_group <> $5
        RETURNING id, user_group, rule_type, rebate_rate::TEXT as rebate_rate,
                  effective_time, created_at, updated_at
        "#,
    )
    .bind(&validated.user_group)
    .bind(&validated.rule_type)
    .bind(validated.rebate_rate)
    .bind(now)
    .bind(SALES_USER_GROUP)
    .fetch_optional(&state.pool)
    .await?
    .ok_or_else(|| AppError::Conflict("销售返利规则创建后不可覆盖".to_string()))?;

    tracing::info!(
        admin_id = admin.0.id,
        user_group = validated.user_group,
        rule_type = validated.rule_type,
        "Admin saved rebate rule"
    );

    Ok(message(rebate_rule_from_row(&row)?, "返利规则已保存"))
}

// PUT /api/admin/rebate-rules/:id
pub async fn update_rebate_rule(
    State(state): State<AppState>,
    admin: AdminUser,
    Path(id): Path<i64>,
    Json(req): Json<RebateRuleRequest>,
) -> Result<Json<ApiResponse<RebateRule>>> {
    if id <= 0 {
        return Err(AppError::BadRequest("无效的规则 ID".to_string()));
    }
    let validated = validate_rebate_rule_request(&req)?;
    if validated.user_group == SALES_USER_GROUP {
        return Err(AppError::Conflict("销售返利规则创建后不可修改".to_string()));
    }
    let now = Utc::now().timestamp();

    let row = sqlx::query(
        r#"
        UPDATE rebate_rules
        SET user_group = $1, rule_type = $2, rebate_rate = $3, effective_time = $4, updated_at = $4
        WHERE id = $5 AND user_group <> $6
        RETURNING id, user_group, rule_type, rebate_rate::TEXT as rebate_rate,
                  effective_time, created_at, updated_at
        "#,
    )
    .bind(&validated.user_group)
    .bind(&validated.rule_type)
    .bind(validated.rebate_rate)
    .bind(now)
    .bind(id)
    .bind(SALES_USER_GROUP)
    .fetch_optional(&state.pool)
    .await?;
    let row = match row {
        Some(row) => row,
        None => {
            let protected: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM rebate_rules WHERE id=$1 AND user_group=$2)",
            )
            .bind(id)
            .bind(SALES_USER_GROUP)
            .fetch_one(&state.pool)
            .await?;
            if protected {
                return Err(AppError::Conflict("销售返利规则创建后不可修改".to_string()));
            }
            return Err(AppError::NotFound("返利规则不存在".to_string()));
        }
    };

    tracing::info!(
        admin_id = admin.0.id,
        rule_id = id,
        "Admin updated rebate rule"
    );

    Ok(message(rebate_rule_from_row(&row)?, "返利规则已更新"))
}

// DELETE /api/admin/rebate-rules/:id
pub async fn delete_rebate_rule(
    State(state): State<AppState>,
    admin: AdminUser,
    Path(id): Path<i64>,
) -> Result<Json<ApiResponse<serde_json::Value>>> {
    if id <= 0 {
        return Err(AppError::BadRequest("无效的规则 ID".to_string()));
    }

    let rows = sqlx::query("DELETE FROM rebate_rules WHERE id = $1 AND user_group <> $2")
        .bind(id)
        .bind(SALES_USER_GROUP)
        .execute(&state.pool)
        .await?
        .rows_affected();

    if rows == 0 {
        let protected: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM rebate_rules WHERE id=$1 AND user_group=$2)",
        )
        .bind(id)
        .bind(SALES_USER_GROUP)
        .fetch_one(&state.pool)
        .await?;
        if protected {
            return Err(AppError::Conflict("销售返利规则创建后不可删除".to_string()));
        }
        return Err(AppError::NotFound("返利规则不存在".to_string()));
    }

    tracing::info!(
        admin_id = admin.0.id,
        rule_id = id,
        "Admin deleted rebate rule"
    );

    Ok(message(serde_json::json!({}), "返利规则已删除"))
}

// GET /api/admin/user-groups
pub async fn get_user_groups(
    State(state): State<AppState>,
    _admin: AdminUser,
) -> Result<Json<ApiResponse<Vec<UserGroup>>>> {
    let rows = sqlx::query(
        r#"
        SELECT "group" as name, COUNT(*) as user_count
        FROM users
        GROUP BY "group"
        ORDER BY "group"
        "#,
    )
    .fetch_all(&state.newapi_pool)
    .await?;

    Ok(ok(rows
        .iter()
        .map(|row| {
            Ok(UserGroup {
                name: row.try_get("name")?,
                user_count: row.try_get("user_count")?,
            })
        })
        .collect::<Result<Vec<_>>>()?))
}

// GET /api/admin/system-config
pub async fn get_system_config(
    State(state): State<AppState>,
    _admin: AdminUser,
) -> Result<Json<ApiResponse<SystemConfig>>> {
    let min_amount = get_config_i64(&state.pool, "min_rebate_request_amount").await?;
    let frequency_days = get_config_i64(&state.pool, "rebate_request_frequency_days").await?;
    let user_invitation_rebate_enabled =
        get_config_bool(&state.pool, USER_INVITATION_REBATE_ENABLED_KEY).await?;
    let order_rebate_enabled = get_config_bool(&state.pool, ORDER_REBATE_ENABLED_KEY).await?;
    let invitation_signup_reward_enabled =
        get_config_bool(&state.pool, INVITATION_SIGNUP_REWARD_ENABLED_KEY).await?;
    let invitation_signup_reward_amount =
        get_config_i64(&state.pool, INVITATION_SIGNUP_REWARD_AMOUNT_KEY).await?;
    let invitation_signup_inviter_reward_amount = get_config_i64_or_fallback(
        &state.pool,
        INVITATION_SIGNUP_INVITER_REWARD_AMOUNT_KEY,
        INVITATION_SIGNUP_REWARD_AMOUNT_KEY,
    )
    .await?;
    let invitation_signup_invitee_reward_amount = get_config_i64_or_fallback(
        &state.pool,
        INVITATION_SIGNUP_INVITEE_REWARD_AMOUNT_KEY,
        INVITATION_SIGNUP_REWARD_AMOUNT_KEY,
    )
    .await?;
    let invitation_signup_reward_review_required =
        get_config_bool(&state.pool, INVITATION_SIGNUP_REWARD_REVIEW_REQUIRED_KEY).await?;
    let invitation_signup_inviter_reward_requires_paid_order = get_config_bool(
        &state.pool,
        INVITATION_SIGNUP_INVITER_REWARD_REQUIRES_PAID_ORDER_KEY,
    )
    .await?;
    let invitation_signup_invitee_reward_requires_paid_order = get_config_bool(
        &state.pool,
        INVITATION_SIGNUP_INVITEE_REWARD_REQUIRES_PAID_ORDER_KEY,
    )
    .await?;
    let rebate_to_balance_enabled =
        get_config_bool(&state.pool, REBATE_TO_BALANCE_ENABLED_KEY).await?;

    Ok(ok(SystemConfig {
        min_rebate_request_amount: quota_to_cents(min_amount),
        rebate_request_frequency_days: frequency_days,
        user_invitation_rebate_enabled,
        order_rebate_enabled,
        invitation_signup_reward_enabled,
        invitation_signup_reward_amount: quota_to_cents(invitation_signup_reward_amount),
        invitation_signup_inviter_reward_amount: quota_to_cents(
            invitation_signup_inviter_reward_amount,
        ),
        invitation_signup_invitee_reward_amount: quota_to_cents(
            invitation_signup_invitee_reward_amount,
        ),
        invitation_signup_reward_review_required,
        invitation_signup_inviter_reward_requires_paid_order,
        invitation_signup_invitee_reward_requires_paid_order,
        rebate_to_balance_enabled,
    }))
}

// POST/PUT /api/admin/system-config
pub async fn update_system_config(
    State(state): State<AppState>,
    admin: AdminUser,
    Json(config): Json<SystemConfig>,
) -> Result<Json<ApiResponse<SystemConfig>>> {
    if config.min_rebate_request_amount < 0 {
        return Err(AppError::BadRequest(
            "最低返利申请金额不能为负数".to_string(),
        ));
    }
    if config.rebate_request_frequency_days < 0 {
        return Err(AppError::BadRequest(
            "返利申请频率限制不能为负数".to_string(),
        ));
    }
    if config.invitation_signup_reward_amount < 0 {
        return Err(AppError::BadRequest(
            "邀请注册奖励金额不能为负数".to_string(),
        ));
    }
    if config.invitation_signup_inviter_reward_amount < 0 {
        return Err(AppError::BadRequest(
            "邀请人注册奖励金额不能为负数".to_string(),
        ));
    }
    if config.invitation_signup_invitee_reward_amount < 0 {
        return Err(AppError::BadRequest(
            "被邀请人注册奖励金额不能为负数".to_string(),
        ));
    }

    let (inviter_reward_amount, invitee_reward_amount) =
        resolve_signup_reward_amounts_for_update(&config);
    let legacy_reward_amount = inviter_reward_amount.max(invitee_reward_amount);

    let now = Utc::now().timestamp();
    upsert_config_value(
        &state.pool,
        "min_rebate_request_amount",
        &cents_to_quota(config.min_rebate_request_amount)?.to_string(),
        "最低返利申请金额（人民币分，API 层；数据库存 new-api 额度单位）",
        now,
    )
    .await?;
    upsert_config_value(
        &state.pool,
        "rebate_request_frequency_days",
        &config.rebate_request_frequency_days.to_string(),
        "返利申请频率限制（天数）",
        now,
    )
    .await?;
    upsert_config_value(
        &state.pool,
        USER_INVITATION_REBATE_ENABLED_KEY,
        if config.user_invitation_rebate_enabled {
            "true"
        } else {
            "false"
        },
        "用户侧邀请返利界面开关",
        now,
    )
    .await?;
    upsert_config_value(
        &state.pool,
        ORDER_REBATE_ENABLED_KEY,
        if config.order_rebate_enabled {
            "true"
        } else {
            "false"
        },
        "订单返利开关",
        now,
    )
    .await?;
    upsert_config_value(
        &state.pool,
        INVITATION_SIGNUP_REWARD_ENABLED_KEY,
        if config.invitation_signup_reward_enabled {
            "true"
        } else {
            "false"
        },
        "邀请注册奖励开关",
        now,
    )
    .await?;
    upsert_config_value(
        &state.pool,
        REBATE_TO_BALANCE_ENABLED_KEY,
        if config.rebate_to_balance_enabled {
            "true"
        } else {
            "false"
        },
        "返利到余额开关",
        now,
    )
    .await?;
    upsert_config_value(
        &state.pool,
        INVITATION_SIGNUP_REWARD_AMOUNT_KEY,
        &cents_to_quota(legacy_reward_amount)?.to_string(),
        "邀请注册奖励金额兼容值（人民币分，API 层；数据库存 new-api 额度单位）",
        now,
    )
    .await?;
    upsert_config_value(
        &state.pool,
        INVITATION_SIGNUP_INVITER_REWARD_AMOUNT_KEY,
        &cents_to_quota(inviter_reward_amount)?.to_string(),
        "邀请人注册奖励金额（人民币分，API 层；数据库存 new-api 额度单位）",
        now,
    )
    .await?;
    upsert_config_value(
        &state.pool,
        INVITATION_SIGNUP_INVITEE_REWARD_AMOUNT_KEY,
        &cents_to_quota(invitee_reward_amount)?.to_string(),
        "被邀请人注册奖励金额（人民币分，API 层；数据库存 new-api 额度单位）",
        now,
    )
    .await?;
    upsert_config_value(
        &state.pool,
        INVITATION_SIGNUP_REWARD_REVIEW_REQUIRED_KEY,
        if config.invitation_signup_reward_review_required {
            "true"
        } else {
            "false"
        },
        "邀请注册奖励是否需要审核",
        now,
    )
    .await?;
    upsert_config_value(
        &state.pool,
        INVITATION_SIGNUP_INVITER_REWARD_REQUIRES_PAID_ORDER_KEY,
        if config.invitation_signup_inviter_reward_requires_paid_order {
            "true"
        } else {
            "false"
        },
        "邀请人注册奖励是否等待被邀请人首次充值/订阅",
        now,
    )
    .await?;
    upsert_config_value(
        &state.pool,
        INVITATION_SIGNUP_INVITEE_REWARD_REQUIRES_PAID_ORDER_KEY,
        if config.invitation_signup_invitee_reward_requires_paid_order {
            "true"
        } else {
            "false"
        },
        "被邀请人注册奖励是否等待首次充值/订阅",
        now,
    )
    .await?;

    tracing::info!(admin_id = admin.0.id, "Admin updated system config");

    Ok(message(
        SystemConfig {
            invitation_signup_reward_amount: legacy_reward_amount,
            invitation_signup_inviter_reward_amount: inviter_reward_amount,
            invitation_signup_invitee_reward_amount: invitee_reward_amount,
            ..config
        },
        "系统配置已更新",
    ))
}

// GET /api/admin/rebate-requests
pub async fn get_rebate_requests(
    State(state): State<AppState>,
    _admin: AdminUser,
    Query(params): Query<RebateRequestListParams>,
) -> Result<Json<ApiResponse<PaginatedResponse<RebateRequestDetail>>>> {
    let total =
        count_rebate_requests(&state, params.status_filter(), params.request_kind()?).await?;
    let rebate_request_rows =
        fetch_rebate_requests(&state, params.status_filter(), &params).await?;
    let user_map = fetch_user_map(&state, &rebate_request_rows).await?;

    let credit_currency = crate::credit_currency::current_epoch(&state.pool).await?;
    let result = rebate_request_rows
        .iter()
        .map(|row| rebate_request_detail_from_row(row, &user_map, credit_currency))
        .collect::<Result<Vec<_>>>()?;

    Ok(ok(PaginatedResponse {
        items: result,
        total,
        page: params.page(),
        page_size: params.page_size(),
    }))
}

// POST /api/admin/rebate-requests/:id/approve
pub async fn approve_rebate_request(
    State(state): State<AppState>,
    admin: AdminUser,
    Path(id): Path<i64>,
    Json(req): Json<ApproveRequest>,
) -> Result<Json<ApiResponse<serde_json::Value>>> {
    validate_id(id)?;
    let now = Utc::now().timestamp();
    let mut tx = state.pool.begin().await?;

    let row = lock_rebate_request_for_review(&mut tx, id).await?;
    ensure_rebate_request_not_completed(&row)?;
    lock_and_validate_request_records(&mut tx, &row, true, false).await?;
    let rebate_record_ids: Vec<i64> = row.try_get("rebate_record_ids")?;

    sqlx::query(
        r#"
        UPDATE rebate_requests
        SET status = 'approved',
            admin_id = $1,
            admin_note = $2,
            approved_at = $3,
            rejected_at = NULL,
            updated_at = $3
        WHERE id = $4
        "#,
    )
    .bind(admin.0.id)
    .bind(&req.note)
    .bind(now)
    .bind(id)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        r#"
        UPDATE rebate_records
        SET status = 'approved', updated_at = $1
        WHERE id = ANY($2)
          AND status IN ('requested', 'pending', 'approved', 'rejected')
        "#,
    )
    .bind(now)
    .bind(&rebate_record_ids)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    tracing::info!(
        admin_id = admin.0.id,
        request_id = id,
        note = ?req.note,
        "Admin approved rebate request"
    );

    Ok(message(
        serde_json::json!({ "id": id }),
        "返利申请已通过，等待处理",
    ))
}

// POST /api/admin/rebate-requests/:id/approve-and-pay
pub async fn approve_and_pay_rebate_request(
    State(state): State<AppState>,
    admin: AdminUser,
    Path(id): Path<i64>,
    headers: HeaderMap,
    Json(req): Json<ApproveAndPayRequest>,
) -> Result<Json<ApiResponse<ApproveAndPayResponse>>> {
    validate_id(id)?;
    let base_request_id = request_id_from_headers(&headers)?;
    let result =
        approve_and_pay_request(&state, &admin, id, req.note.as_deref(), &base_request_id).await?;
    let response_message = approve_and_pay_message(&result);

    Ok(message(result, response_message))
}

// POST /api/admin/rebate-requests/approve-and-pay-batch
pub async fn batch_approve_and_pay_rebate_requests(
    State(state): State<AppState>,
    admin: AdminUser,
    headers: HeaderMap,
    Json(req): Json<BatchApproveAndPayRequest>,
) -> Result<Json<ApiResponse<BatchApproveAndPayResponse>>> {
    let request_ids = validate_approve_and_pay_batch_ids(&req.request_ids)?;
    let base_request_id = request_id_from_headers(&headers)?;

    let mut items = Vec::new();
    let mut errors = Vec::new();
    let mut paid_count = 0;
    let mut already_paid_count = 0;
    let mut failed_count = 0;

    for request_id in request_ids.iter().copied() {
        match approve_and_pay_request(
            &state,
            &admin,
            request_id,
            req.note.as_deref(),
            &derive_payout_request_id(&base_request_id, request_id, 0),
        )
        .await
        {
            Ok(item) => {
                paid_count += item.paid_count;
                already_paid_count += item.already_paid_count;
                failed_count += item.failed_count;
                items.push(item);
            }
            Err(error) => {
                errors.push(BatchApproveAndPayError {
                    request_id,
                    error: app_error_message(&error).to_string(),
                });
            }
        }
    }

    let result = BatchApproveAndPayResponse {
        total_requests: request_ids.len(),
        succeeded_requests: items.len(),
        failed_requests: errors.len(),
        paid_count,
        already_paid_count,
        pending_count: aggregate_pending_count(&items),
        failed_count,
        items,
        errors,
    };
    let response_message = batch_approve_and_pay_message(&result);

    Ok(message(result, response_message))
}

// POST /api/admin/rebate-requests/:id/reject
pub async fn reject_rebate_request(
    State(state): State<AppState>,
    admin: AdminUser,
    Path(id): Path<i64>,
    Json(req): Json<ApproveRequest>,
) -> Result<Json<ApiResponse<serde_json::Value>>> {
    validate_id(id)?;
    let now = Utc::now().timestamp();
    let note = req.reason.as_ref().or(req.note.as_ref()).cloned();
    if note.as_deref().unwrap_or("").trim().is_empty() {
        return Err(AppError::BadRequest("请填写拒绝原因".to_string()));
    }

    let mut tx = state.pool.begin().await?;
    let row = lock_rebate_request_for_review(&mut tx, id).await?;
    ensure_rebate_request_not_completed(&row)?;
    if row.try_get::<String, _>("status")? == "rejected" {
        tx.commit().await?;
        return Ok(message(serde_json::json!({ "id": id }), "返利申请已拒绝"));
    }
    lock_and_validate_request_records(&mut tx, &row, false, false).await?;
    let rebate_record_ids: Vec<i64> = row.try_get("rebate_record_ids")?;

    sqlx::query(
        r#"
        UPDATE rebate_requests
        SET status = 'rejected',
            admin_id = $1,
            admin_note = $2,
            approved_at = NULL,
            rejected_at = $3,
            updated_at = $3
        WHERE id = $4
        "#,
    )
    .bind(admin.0.id)
    .bind(&note)
    .bind(now)
    .bind(id)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        r#"
        UPDATE rebate_records
        SET status = 'pending', updated_at = $1
        WHERE id = ANY($2)
          AND status IN ('requested', 'pending', 'approved', 'rejected')
        "#,
    )
    .bind(now)
    .bind(&rebate_record_ids)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    tracing::info!(
        admin_id = admin.0.id,
        request_id = id,
        note = ?note,
        "Admin rejected rebate request"
    );

    Ok(message(serde_json::json!({ "id": id }), "返利申请已拒绝"))
}

// POST /api/admin/rebate-requests/:id/reset
pub async fn reset_rebate_request_review(
    State(state): State<AppState>,
    admin: AdminUser,
    Path(id): Path<i64>,
) -> Result<Json<ApiResponse<serde_json::Value>>> {
    validate_id(id)?;
    let now = Utc::now().timestamp();
    let mut tx = state.pool.begin().await?;

    let row = lock_rebate_request_for_review(&mut tx, id).await?;
    ensure_rebate_request_not_completed(&row)?;
    lock_and_validate_request_records(&mut tx, &row, true, false).await?;
    let rebate_record_ids: Vec<i64> = row.try_get("rebate_record_ids")?;

    sqlx::query(
        r#"
        UPDATE rebate_requests
        SET status = 'pending',
            admin_id = $1,
            admin_note = NULL,
            approved_at = NULL,
            rejected_at = NULL,
            updated_at = $2
        WHERE id = $3
        "#,
    )
    .bind(admin.0.id)
    .bind(now)
    .bind(id)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        r#"
        UPDATE rebate_records
        SET status = 'requested', updated_at = $1
        WHERE id = ANY($2)
          AND status IN ('requested', 'pending', 'approved', 'rejected')
        "#,
    )
    .bind(now)
    .bind(&rebate_record_ids)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    tracing::info!(
        admin_id = admin.0.id,
        request_id = id,
        "Admin reset rebate request review"
    );

    Ok(message(
        serde_json::json!({ "id": id }),
        "返利申请审核已撤回",
    ))
}

// POST /api/admin/rebate-requests/:id/complete
pub async fn complete_rebate_request() -> Result<Json<ApiResponse<serde_json::Value>>> {
    Err(AppError::LegacyCompletionDisabled)
}

// POST /api/admin/rebate-requests/:id/undo-complete
pub async fn undo_complete_rebate_request() -> Result<Json<ApiResponse<serde_json::Value>>> {
    Err(AppError::LegacyCompletionDisabled)
}

// GET /api/admin/rebate-stats
pub async fn get_rebate_stats(
    State(state): State<AppState>,
    _admin: AdminUser,
) -> Result<Json<ApiResponse<RebateStats>>> {
    let row = sqlx::query(
        r#"
        SELECT
            COALESCE(SUM(rebate_amount), 0)::BIGINT as total,
            COALESCE(SUM(CASE WHEN status = 'completed' OR issued.request_id IS NOT NULL THEN rebate_amount ELSE 0 END), 0)::BIGINT as completed,
            COALESCE(SUM(CASE WHEN status = 'pending' THEN rebate_amount ELSE 0 END), 0)::BIGINT as pending,
            COALESCE(SUM(CASE WHEN status = 'requested' THEN rebate_amount ELSE 0 END), 0)::BIGINT as requested,
            COALESCE(SUM(CASE WHEN status = 'approved' AND issued.request_id IS NULL THEN rebate_amount ELSE 0 END), 0)::BIGINT as approved
        FROM rebate_records
        LEFT JOIN rebate_record_request_ownership owner ON owner.rebate_record_id=rebate_records.id AND owner.ownership_state='active'
        LEFT JOIN sales_disbursements issued ON issued.request_id=owner.request_id
        WHERE status NOT IN ('initializing', 'closed')
        "#,
    )
    .fetch_one(&state.pool)
    .await?;

    let total_invitations: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM users WHERE inviter_id IS NOT NULL AND inviter_id > 0",
    )
    .fetch_one(&state.newapi_pool)
    .await?;

    Ok(ok(RebateStats {
        total_rebate: quota_to_cents(row.try_get("total")?),
        completed_rebate: quota_to_cents(row.try_get("completed")?),
        pending_rebate: quota_to_cents(row.try_get("pending")?),
        requested_rebate: quota_to_cents(row.try_get("requested")?),
        approved_rebate: quota_to_cents(row.try_get("approved")?),
        total_invitations,
    }))
}

// GET /api/admin/rebate-records
pub async fn get_rebate_records(
    State(state): State<AppState>,
    _admin: AdminUser,
    Query(params): Query<AdminRebateRecordListParams>,
) -> Result<Json<ApiResponse<PaginatedResponse<RebateRecord>>>> {
    let source_filter = params.source_filter();
    unlock_paid_invitation_signup_rewards(&state.pool, &state.newapi_pool, Utc::now().timestamp())
        .await?;

    let records = fetch_rebate_records(
        &state.pool,
        None,
        params.status_filter(),
        true,
        source_filter,
        params.page_size(),
        params.offset(),
    )
    .await?;
    let total = count_rebate_records(
        &state.pool,
        None,
        params.status_filter(),
        true,
        source_filter,
    )
    .await?;

    Ok(ok(PaginatedResponse {
        items: records,
        total,
        page: params.page(),
        page_size: params.page_size(),
    }))
}

// POST /api/admin/rebate-records/:id/revoke-signup-reward
pub async fn revoke_signup_reward_rebate_record(
    State(state): State<AppState>,
    admin: AdminUser,
    Path(id): Path<i64>,
) -> Result<Json<ApiResponse<InvitationRegistrationRewardRevokeResponse>>> {
    validate_id(id)?;
    let now = Utc::now().timestamp();
    let mut tx = state.pool.begin().await?;

    let row = sqlx::query(
        r#"
        SELECT id, order_type, status
        FROM rebate_records
        WHERE id = $1
        "#,
    )
    .bind(id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| AppError::NotFound("返利记录不存在".to_string()))?;

    let record_id: i64 = row.try_get("id")?;
    let order_type: String = row.try_get("order_type")?;
    if !is_invitation_signup_reward_order_type(&order_type) {
        return Err(AppError::BadRequest(
            "只有邀请注册奖励记录可以撤回".to_string(),
        ));
    }

    let linked_request_count =
        revoke_invitation_signup_reward_record_in_tx(&mut tx, record_id, now).await?;

    tx.commit().await?;

    tracing::info!(
        admin_id = admin.0.id,
        record_id,
        order_type,
        linked_request_count,
        "Admin revoked invitation signup reward rebate record"
    );

    Ok(message(
        InvitationRegistrationRewardRevokeResponse {
            revoked: true,
            record_id: Some(record_id),
        },
        "邀请注册奖励已撤回",
    ))
}

// GET /api/admin/rebate-order-records
pub async fn get_rebate_order_records(
    State(state): State<AppState>,
    admin: AdminUser,
    Query(params): Query<RebateOrderRecordParams>,
) -> Result<Json<ApiResponse<PaginatedResponse<AdminRebateOrderRecord>>>> {
    let order_type = params.order_type_filter();
    let page = params.page();
    let page_size = params.page_size();
    let offset = params.offset();

    tracing::info!(
        admin_id = admin.0.id,
        order_type = order_type.unwrap_or("all"),
        page,
        page_size,
        offset,
        "Admin rebate order records requested"
    );

    let total = count_newapi_rebate_orders(&state, order_type).await?;
    tracing::info!(
        admin_id = admin.0.id,
        order_type = order_type.unwrap_or("all"),
        total,
        "New-api rebate order count loaded"
    );

    if total == 0 {
        let diagnostics = fetch_newapi_rebate_order_diagnostics(&state).await?;
        tracing::warn!(
            admin_id = admin.0.id,
            order_type = order_type.unwrap_or("all"),
            invited_users = diagnostics.invited_users,
            topup_success = diagnostics.topup_success,
            topup_invited = diagnostics.topup_invited,
            topup_rebate_orders = diagnostics.topup_rebate_orders,
            subscription_success = diagnostics.subscription_success,
            subscription_invited = diagnostics.subscription_invited,
            redemption_used = diagnostics.redemption_used,
            redemption_invited = diagnostics.redemption_invited,
            "No new-api rebate orders matched admin rebate order query"
        );
    }

    let orders = fetch_newapi_rebate_orders(&state, order_type, page_size, offset).await?;
    tracing::info!(
        admin_id = admin.0.id,
        order_type = order_type.unwrap_or("all"),
        fetched_count = orders.len(),
        first_order_type = orders.first().map(|order| order.order_type.as_str()),
        first_order_id = orders.first().map(|order| order.order_id),
        "New-api rebate order page loaded"
    );

    let now = Utc::now().timestamp();
    let local_record_map = fetch_local_rebate_record_map(&state, &orders).await?;
    tracing::info!(
        admin_id = admin.0.id,
        local_record_count = local_record_map.len(),
        "Local rebate record map loaded for admin order page"
    );

    let cutover: String = sqlx::query_scalar(
        "SELECT value FROM system_config WHERE key='order_rebate_seven_day_cutover_at'",
    )
    .fetch_one(&state.pool)
    .await?;
    let cutover = cutover
        .parse::<i64>()
        .map_err(|_| AppError::Internal("返利冷却政策配置无效".into()))?;
    let records = orders
        .iter()
        .map(|order| {
            let local = local_record_map.get(&(order.order_type.clone(), order.order_id));
            let effective_at = match local {
                Some(record) => record.rebate_effective_at,
                None => rebate_effective_at_for_cutover(order.order_time, cutover)?,
            };
            Ok(admin_rebate_order_record_from_parts(
                order,
                local,
                now,
                effective_at,
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    tracing::info!(
        admin_id = admin.0.id,
        returned_count = records.len(),
        total,
        page,
        page_size,
        "Admin rebate order records response ready"
    );

    Ok(ok(PaginatedResponse {
        items: records,
        total,
        page,
        page_size,
    }))
}

// PATCH /api/admin/rebate-order-records
pub async fn update_rebate_order_records(
    State(state): State<AppState>,
    admin: AdminUser,
    Json(req): Json<UpdateRebateOrderRecordsRequest>,
) -> Result<Json<ApiResponse<RebateOrderRecordBatchResponse>>> {
    let record_ids = validate_record_ids(&req.record_ids)?;
    if req.rebate_amount.is_none() && req.rebate_ratio.is_none() {
        return Err(AppError::BadRequest("请填写返利金额或返利比例".to_string()));
    }
    if let Some(amount) = req.rebate_amount {
        if amount < 0 {
            return Err(AppError::BadRequest("返利金额不能为负数".to_string()));
        }
    }
    if let Some(ratio) = req.rebate_ratio {
        validate_rebate_ratio(ratio)?;
    }

    let now = Utc::now().timestamp();
    let mut tx = state.pool.begin().await?;
    let rows = sqlx::query(
        r#"
        SELECT id,
               COALESCE(order_amount_cents, ROUND(order_amount * 100)::BIGINT)
                   AS order_amount_cents,
               credited_quota,
               source_schema_version,
               status
        FROM rebate_records
        WHERE id = ANY($1)
        ORDER BY id
        FOR UPDATE
        "#,
    )
    .bind(&record_ids)
    .fetch_all(&mut *tx)
    .await?;

    validate_operation_rows(&rows, &record_ids, is_rebate_record_mutable)?;

    let reprice_receipt = load_verified_receipt_in_transaction(&mut tx)
        .await
        .map_err(|error| AppError::Conflict(format!("销售返利更正回执无效: {error}")))?;
    if reprice_receipt
        .as_ref()
        .is_some_and(|receipt| record_ids.iter().any(|id| receipt.entry(*id).is_some()))
    {
        return Err(AppError::Conflict(
            "历史销售返利更正记录不能再次调整".into(),
        ));
    }

    let currency_boundary = crate::credit_currency::boundary(&mut *tx).await?;

    let mut updated = 0;
    for row in &rows {
        let id: i64 = row.try_get("id")?;
        let order_amount_cents: i64 = row.try_get("order_amount_cents")?;
        let mut accounting_base = AdminRebateAccountingBase::from_record(
            row.try_get("source_schema_version")?,
            row.try_get("credited_quota")?,
            order_amount_cents,
        )?;
        if let AdminRebateAccountingBase::OrderAmountCents(cents) = accounting_base {
            if currency_boundary
                .as_ref()
                .is_some_and(|b| id <= b.max_rebate_id)
            {
                let base =
                    rebate_amount_for_cents(cents, Decimal::ONE, state.config.quota_per_unit)?;
                accounting_base =
                    AdminRebateAccountingBase::CreditedQuota(crate::credit_currency::convert(base));
            }
        }
        let direct_rebate_quota = req
            .rebate_amount
            .map(|amount_cents| {
                rebate_amount_for_cents(amount_cents, Decimal::ONE, state.config.quota_per_unit)
            })
            .transpose()?;
        let rebate_ratio = match (req.rebate_ratio, req.rebate_amount) {
            (Some(ratio), _) => Some(ratio),
            (None, Some(amount_cents)) => {
                accounting_base.ratio_for_amount(amount_cents, state.config.quota_per_unit)?
            }
            (None, None) => None,
        };
        if let Some(ratio) = rebate_ratio {
            validate_rebate_ratio(ratio)?;
        }
        let rebate_amount = if let Some(rebate_quota) = direct_rebate_quota {
            rebate_quota
        } else {
            let ratio = rebate_ratio.unwrap_or_default();
            let rate = Decimal::from_f64(ratio)
                .ok_or_else(|| AppError::BadRequest("返利比例格式无效".to_string()))?;
            accounting_base.rebate_amount(rate, state.config.quota_per_unit)?
        };

        let rows_affected = sqlx::query(
            r#"
            UPDATE rebate_records
            SET rebate_amount = $1,
                rebate_rate = $2,
                admin_adjusted = TRUE,
                updated_at = $3
            WHERE id = $4
            "#,
        )
        .bind(rebate_amount)
        .bind(rebate_ratio)
        .bind(now)
        .bind(id)
        .execute(&mut *tx)
        .await?
        .rows_affected();

        updated += rows_affected as i64;
    }

    tx.commit().await?;

    tracing::info!(
        admin_id = admin.0.id,
        record_count = record_ids.len(),
        updated,
        "Admin updated rebate order records"
    );

    Ok(message(
        RebateOrderRecordBatchResponse { updated },
        "返利记录已更新",
    ))
}

// POST /api/admin/rebate-order-records/close
pub async fn close_rebate_order_records(
    State(state): State<AppState>,
    admin: AdminUser,
    Json(req): Json<RebateOrderRecordIdsRequest>,
) -> Result<Json<ApiResponse<RebateOrderRecordBatchResponse>>> {
    let record_ids = validate_record_ids(&req.record_ids)?;
    let now = Utc::now().timestamp();
    let mut tx = state.pool.begin().await?;
    let rows = lock_rebate_records(&mut tx, &record_ids).await?;
    validate_operation_rows(&rows, &record_ids, is_rebate_record_mutable)?;

    let updated = sqlx::query(
        r#"
        UPDATE rebate_records
        SET status = 'closed', closed_at = $1, updated_at = $1
        WHERE id = ANY($2)
        "#,
    )
    .bind(now)
    .bind(&record_ids)
    .execute(&mut *tx)
    .await?
    .rows_affected() as i64;

    tx.commit().await?;

    tracing::info!(
        admin_id = admin.0.id,
        record_count = record_ids.len(),
        updated,
        "Admin closed rebate order records"
    );

    Ok(message(
        RebateOrderRecordBatchResponse { updated },
        "返利订单已关闭",
    ))
}

// POST /api/admin/rebate-order-records/reopen
pub async fn reopen_rebate_order_records(
    State(state): State<AppState>,
    admin: AdminUser,
    Json(req): Json<RebateOrderRecordIdsRequest>,
) -> Result<Json<ApiResponse<RebateOrderRecordBatchResponse>>> {
    let record_ids = validate_record_ids(&req.record_ids)?;
    let now = Utc::now().timestamp();
    let mut tx = state.pool.begin().await?;
    let rows = lock_rebate_records(&mut tx, &record_ids).await?;
    validate_reopen_rows(&rows, &record_ids, state.config.topup_rebate_active_from_id)?;

    let updated = sqlx::query(
        r#"
        UPDATE rebate_records
        SET status = CASE
                WHEN finalized_at IS NULL THEN 'initializing'
                ELSE 'pending'
            END,
            closed_at = NULL,
            updated_at = $1
        WHERE id = ANY($2)
        "#,
    )
    .bind(now)
    .bind(&record_ids)
    .execute(&mut *tx)
    .await?
    .rows_affected() as i64;

    tx.commit().await?;

    tracing::info!(
        admin_id = admin.0.id,
        record_count = record_ids.len(),
        updated,
        "Admin reopened rebate order records"
    );

    Ok(message(
        RebateOrderRecordBatchResponse { updated },
        "返利订单已开启",
    ))
}

// POST /api/admin/rebate-order-records/end-initialization
pub async fn end_rebate_order_initialization(
    State(state): State<AppState>,
    admin: AdminUser,
    Json(req): Json<RebateOrderRecordIdsRequest>,
) -> Result<Json<ApiResponse<RebateOrderRecordBatchResponse>>> {
    let record_ids = validate_record_ids(&req.record_ids)?;
    let rows = sqlx::query("SELECT id, status FROM rebate_records WHERE id = ANY($1) ORDER BY id")
        .bind(&record_ids)
        .fetch_all(&state.pool)
        .await?;
    validate_operation_rows(&rows, &record_ids, |status| status == "initializing")?;

    let now = Utc::now().timestamp();
    let updated = finalize_initializing_records_by_ids(&state.pool, &record_ids, now).await?;

    tracing::info!(
        admin_id = admin.0.id,
        record_count = record_ids.len(),
        updated,
        "Admin ended rebate order initialization"
    );

    Ok(message(
        RebateOrderRecordBatchResponse { updated },
        "订单初始化已结束",
    ))
}

// POST /api/admin/rebate-order-records/extend-initialization
pub async fn extend_rebate_order_initialization(
    State(state): State<AppState>,
    admin: AdminUser,
    Json(req): Json<ExtendRebateInitializationRequest>,
) -> Result<Json<ApiResponse<RebateOrderRecordBatchResponse>>> {
    let record_ids = validate_record_ids(&req.record_ids)?;
    let now = Utc::now().timestamp();
    if req.initialization_ends_at <= now {
        return Err(AppError::BadRequest(
            "初始化结束时间必须晚于当前时间".to_string(),
        ));
    }

    let mut tx = state.pool.begin().await?;
    let rows = sqlx::query(
        r#"
        SELECT id, status, initialization_ends_at
        FROM rebate_records
        WHERE id = ANY($1)
        ORDER BY id
        FOR UPDATE
        "#,
    )
    .bind(&record_ids)
    .fetch_all(&mut *tx)
    .await?;

    validate_operation_rows(&rows, &record_ids, can_extend_initialization)?;
    for row in &rows {
        let current_ends_at: i64 = row.try_get("initialization_ends_at")?;
        let status: String = row.try_get("status")?;
        if status == "initializing" && req.initialization_ends_at <= current_ends_at {
            return Err(AppError::BadRequest(
                "新的初始化结束时间必须晚于当前初始化结束时间".to_string(),
            ));
        }
    }

    let updated = sqlx::query(
        r#"
        UPDATE rebate_records
        SET status = 'initializing',
            initialization_ends_at = $1,
            finalized_at = NULL,
            updated_at = $2
        WHERE id = ANY($3)
        "#,
    )
    .bind(req.initialization_ends_at)
    .bind(now)
    .bind(&record_ids)
    .execute(&mut *tx)
    .await?
    .rows_affected() as i64;

    tx.commit().await?;

    tracing::info!(
        admin_id = admin.0.id,
        record_count = record_ids.len(),
        updated,
        "Admin extended rebate order initialization"
    );

    Ok(message(
        RebateOrderRecordBatchResponse { updated },
        "订单初始化时间已延长",
    ))
}

// GET /api/admin/invitation-registrations
pub async fn get_invitation_registrations(
    State(state): State<AppState>,
    _admin: AdminUser,
    Query(params): Query<InvitationRegistrationParams>,
) -> Result<Json<ApiResponse<PaginatedResponse<AdminInvitationRegistration>>>> {
    let now = Utc::now().timestamp();
    let total = count_newapi_invitation_registrations(&state).await?;
    let registrations =
        fetch_newapi_invitation_registrations(&state, params.page_size(), params.offset()).await?;

    for registration in &registrations {
        upsert_invitation_registration(
            &state.pool,
            UpsertInvitationRegistration {
                inviter_id: registration.inviter_id,
                invitee_id: registration.invitee_id,
                user_group: &registration.user_group,
                invited_at: registration.invited_at,
                now,
            },
        )
        .await?;
    }

    let invitee_ids = registrations
        .iter()
        .map(|registration| registration.invitee_id)
        .collect::<Vec<_>>();
    unlock_paid_invitation_signup_rewards(&state.pool, &state.newapi_pool, now).await?;
    let local_map = fetch_local_invitation_registration_map(&state, &invitee_ids).await?;
    let reward_map = fetch_invitation_registration_reward_map(&state, &invitee_ids).await?;
    let total_reward_map =
        fetch_invitation_registration_total_reward_map(&state, &invitee_ids).await?;
    let reward_amounts = fetch_invitation_signup_reward_amounts(&state.pool).await?;

    let mut items = Vec::with_capacity(registrations.len());
    for registration in &registrations {
        if let Some(local) = local_map.get(&registration.invitee_id) {
            items.push(admin_invitation_registration_from_parts(
                registration,
                local,
                &reward_map,
                &total_reward_map,
                &reward_amounts,
            )?);
        }
    }

    Ok(ok(PaginatedResponse {
        items,
        total,
        page: params.page(),
        page_size: params.page_size(),
    }))
}

// POST /api/admin/invitation-registrations/:id/inviter-reward
pub async fn generate_invitation_registration_inviter_reward(
    State(state): State<AppState>,
    admin: AdminUser,
    Path(id): Path<i64>,
) -> Result<Json<ApiResponse<InvitationRegistrationRewardResponse>>> {
    generate_invitation_registration_reward(
        &state,
        &admin,
        id,
        INVITATION_SIGNUP_INVITER_ORDER_TYPE,
    )
    .await
}

// POST /api/admin/invitation-registrations/:id/invitee-reward
pub async fn generate_invitation_registration_invitee_reward(
    State(state): State<AppState>,
    admin: AdminUser,
    Path(id): Path<i64>,
) -> Result<Json<ApiResponse<InvitationRegistrationRewardResponse>>> {
    generate_invitation_registration_reward(
        &state,
        &admin,
        id,
        INVITATION_SIGNUP_INVITEE_ORDER_TYPE,
    )
    .await
}

// POST /api/admin/invitation-registrations/:id/inviter-reward/revoke
pub async fn revoke_invitation_registration_inviter_reward(
    State(state): State<AppState>,
    admin: AdminUser,
    Path(id): Path<i64>,
) -> Result<Json<ApiResponse<InvitationRegistrationRewardRevokeResponse>>> {
    revoke_invitation_registration_reward(&state, &admin, id, INVITATION_SIGNUP_INVITER_ORDER_TYPE)
        .await
}

// POST /api/admin/invitation-registrations/:id/invitee-reward/revoke
pub async fn revoke_invitation_registration_invitee_reward(
    State(state): State<AppState>,
    admin: AdminUser,
    Path(id): Path<i64>,
) -> Result<Json<ApiResponse<InvitationRegistrationRewardRevokeResponse>>> {
    revoke_invitation_registration_reward(&state, &admin, id, INVITATION_SIGNUP_INVITEE_ORDER_TYPE)
        .await
}

// GET /api/admin/user-rebates/:user_id
pub async fn get_user_rebates(
    State(state): State<AppState>,
    _admin: AdminUser,
    Path(user_id): Path<i64>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<ApiResponse<PaginatedResponse<RebateRecord>>>> {
    validate_id(user_id)?;
    unlock_paid_invitation_signup_rewards(&state.pool, &state.newapi_pool, Utc::now().timestamp())
        .await?;

    let records = fetch_user_related_rebate_records(
        &state.pool,
        user_id,
        params.page_size(),
        params.offset(),
    )
    .await?;
    let total = count_user_related_rebate_records(&state.pool, user_id).await?;

    Ok(ok(PaginatedResponse {
        items: records,
        total,
        page: params.page(),
        page_size: params.page_size(),
    }))
}

async fn generate_invitation_registration_reward(
    state: &AppState,
    admin: &AdminUser,
    registration_id: i64,
    order_type: &str,
) -> Result<Json<ApiResponse<InvitationRegistrationRewardResponse>>> {
    validate_id(registration_id)?;
    if !get_config_bool(&state.pool, INVITATION_SIGNUP_REWARD_ENABLED_KEY).await? {
        return Err(AppError::BadRequest("邀请注册奖励功能未开启".to_string()));
    }

    let is_inviter_reward = order_type == INVITATION_SIGNUP_INVITER_ORDER_TYPE;
    let reward_amount_key = if is_inviter_reward {
        INVITATION_SIGNUP_INVITER_REWARD_AMOUNT_KEY
    } else {
        INVITATION_SIGNUP_INVITEE_REWARD_AMOUNT_KEY
    };
    let reward_amount = get_config_i64_or_fallback(
        &state.pool,
        reward_amount_key,
        INVITATION_SIGNUP_REWARD_AMOUNT_KEY,
    )
    .await?;
    if reward_amount <= 0 {
        let message = if is_inviter_reward {
            "邀请人注册奖励金额必须大于 0"
        } else {
            "被邀请人注册奖励金额必须大于 0"
        };
        return Err(AppError::BadRequest(message.to_string()));
    }

    let registration = fetch_local_invitation_registration(state, registration_id).await?;
    let reward_owner_id = if is_inviter_reward {
        registration.inviter_id
    } else {
        registration.invitee_id
    };
    let unlock_required_key = if is_inviter_reward {
        INVITATION_SIGNUP_INVITER_REWARD_REQUIRES_PAID_ORDER_KEY
    } else {
        INVITATION_SIGNUP_INVITEE_REWARD_REQUIRES_PAID_ORDER_KEY
    };
    let unlock_required = get_config_bool(&state.pool, unlock_required_key).await?;
    let now = Utc::now().timestamp();
    let paid_invitee_ids = if unlock_required {
        fetch_paid_invitee_ids(&state.newapi_pool, &[registration.invitee_id]).await?
    } else {
        Default::default()
    };

    let outcome = insert_invitation_signup_reward_record(
        &state.pool,
        InsertInvitationSignupReward {
            order_type,
            reward_owner_id,
            invitee_id: registration.invitee_id,
            user_group: &registration.user_group,
            reward_amount,
            order_time: registration.invited_at,
            unlock_required,
            unlocked_at: resolve_invitation_signup_reward_unlocked_at(
                unlock_required,
                paid_invitee_ids.contains(&registration.invitee_id),
                now,
            ),
            now,
        },
    )
    .await?;

    tracing::info!(
        admin_id = admin.0.id,
        registration_id,
        order_type,
        generated = matches!(&outcome, RewardInsertOutcome::Inserted { .. }),
        "Admin generated invitation signup reward"
    );

    let generated = matches!(&outcome, RewardInsertOutcome::Inserted { .. });
    let response = InvitationRegistrationRewardResponse {
        generated,
        record_id: outcome.record_id(),
    };
    let message_text = if generated {
        "邀请注册奖励已生成"
    } else {
        "邀请注册奖励已存在"
    };

    Ok(message(response, message_text))
}

async fn revoke_invitation_registration_reward(
    state: &AppState,
    admin: &AdminUser,
    registration_id: i64,
    order_type: &str,
) -> Result<Json<ApiResponse<InvitationRegistrationRewardRevokeResponse>>> {
    validate_id(registration_id)?;
    let registration = fetch_local_invitation_registration(state, registration_id).await?;
    let reward_owner_id = if order_type == INVITATION_SIGNUP_INVITER_ORDER_TYPE {
        registration.inviter_id
    } else {
        registration.invitee_id
    };
    let now = Utc::now().timestamp();

    let mut tx = state.pool.begin().await?;
    let row = sqlx::query(
        r#"
        SELECT id, status
        FROM rebate_records
        WHERE order_type = $1
          AND order_id = $2
          AND invitee_id = $2
          AND inviter_id = $3
        "#,
    )
    .bind(order_type)
    .bind(registration.invitee_id)
    .bind(reward_owner_id)
    .fetch_optional(&mut *tx)
    .await?;

    let Some(row) = row else {
        tx.commit().await?;
        return Ok(message(
            InvitationRegistrationRewardRevokeResponse {
                revoked: false,
                record_id: None,
            },
            "邀请注册奖励不存在",
        ));
    };

    let record_id: i64 = row.try_get("id")?;
    let linked_request_count =
        revoke_invitation_signup_reward_record_in_tx(&mut tx, record_id, now).await?;

    tx.commit().await?;

    tracing::info!(
        admin_id = admin.0.id,
        registration_id,
        order_type,
        record_id,
        linked_request_count,
        "Admin revoked invitation signup reward"
    );

    Ok(message(
        InvitationRegistrationRewardRevokeResponse {
            revoked: true,
            record_id: Some(record_id),
        },
        "邀请注册奖励已撤回",
    ))
}

async fn count_newapi_invitation_registrations(state: &AppState) -> Result<i64> {
    sqlx::query_scalar("SELECT COUNT(*) FROM users WHERE inviter_id IS NOT NULL AND inviter_id > 0")
        .fetch_one(&state.newapi_pool)
        .await
        .map_err(AppError::from)
}

async fn fetch_newapi_invitation_registrations(
    state: &AppState,
    page_size: i64,
    offset: i64,
) -> Result<Vec<NewApiInvitationRegistration>> {
    let rows = sqlx::query(
        r#"
        SELECT
            u.inviter_id AS inviter_id,
            COALESCE(NULLIF(inviter.display_name, ''), NULLIF(inviter.username, '')) AS inviter_name,
            u.id AS invitee_id,
            COALESCE(NULLIF(u.display_name, ''), NULLIF(u.username, '')) AS invitee_name,
            COALESCE(NULLIF(u."group", ''), 'default') AS user_group,
            COALESCE(NULLIF(u.created_at, 0), 0) AS invited_at
        FROM users u
        LEFT JOIN users inviter ON inviter.id = u.inviter_id
        WHERE u.inviter_id IS NOT NULL
          AND u.inviter_id > 0
        ORDER BY invited_at DESC, u.id DESC
        LIMIT $1 OFFSET $2
        "#,
    )
    .bind(page_size)
    .bind(offset)
    .fetch_all(&state.newapi_pool)
    .await?;

    rows.iter()
        .map(|row| {
            Ok(NewApiInvitationRegistration {
                inviter_id: row.try_get("inviter_id")?,
                inviter_name: row.try_get("inviter_name")?,
                invitee_id: row.try_get("invitee_id")?,
                invitee_name: row.try_get("invitee_name")?,
                user_group: row.try_get("user_group")?,
                invited_at: row.try_get("invited_at")?,
            })
        })
        .collect()
}

async fn fetch_local_invitation_registration(
    state: &AppState,
    registration_id: i64,
) -> Result<LocalInvitationRegistration> {
    let row = sqlx::query(
        r#"
        SELECT id, inviter_id, invitee_id, user_group, invited_at
        FROM invitation_registrations
        WHERE id = $1
        "#,
    )
    .bind(registration_id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or_else(|| AppError::NotFound("邀请注册记录不存在".to_string()))?;

    local_invitation_registration_from_row(&row)
}

async fn fetch_local_invitation_registration_map(
    state: &AppState,
    invitee_ids: &[i64],
) -> Result<HashMap<i64, LocalInvitationRegistration>> {
    if invitee_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT id, inviter_id, invitee_id, user_group, invited_at
        FROM invitation_registrations
        WHERE invitee_id = ANY($1)
        "#,
    )
    .bind(invitee_ids)
    .fetch_all(&state.pool)
    .await?;

    rows.iter()
        .map(|row| {
            let registration = local_invitation_registration_from_row(row)?;
            Ok((registration.invitee_id, registration))
        })
        .collect()
}

async fn fetch_invitation_registration_reward_map(
    state: &AppState,
    invitee_ids: &[i64],
) -> Result<HashMap<(String, i64), InvitationRegistrationReward>> {
    if invitee_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT id, order_type, order_id, rebate_amount,
               CASE WHEN EXISTS(SELECT 1 FROM sales_disbursements issued
                   JOIN rebate_record_request_ownership owner ON owner.request_id=issued.request_id
                   WHERE owner.rebate_record_id=rebate_records.id AND owner.ownership_state='active')
               THEN 'completed' ELSE status END AS status
        FROM rebate_records
        WHERE order_type IN ($1, $2)
          AND order_id = ANY($3)
        "#,
    )
    .bind(INVITATION_SIGNUP_INVITER_ORDER_TYPE)
    .bind(INVITATION_SIGNUP_INVITEE_ORDER_TYPE)
    .bind(invitee_ids)
    .fetch_all(&state.pool)
    .await?;

    rows.iter()
        .map(|row| {
            let order_type: String = row.try_get("order_type")?;
            let invitee_id: i64 = row.try_get("order_id")?;
            Ok((
                (order_type, invitee_id),
                InvitationRegistrationReward {
                    record_id: row.try_get("id")?,
                    rebate_amount: quota_to_cents(row.try_get("rebate_amount")?),
                    status: row.try_get("status")?,
                },
            ))
        })
        .collect()
}

async fn fetch_invitation_registration_total_reward_map(
    state: &AppState,
    invitee_ids: &[i64],
) -> Result<HashMap<(i64, i64), i64>> {
    if invitee_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT inviter_id, invitee_id, COALESCE(SUM(rebate_amount), 0)::BIGINT AS total
        FROM rebate_records
        WHERE invitee_id = ANY($1)
          AND status NOT IN ('initializing', 'closed')
        GROUP BY inviter_id, invitee_id
        "#,
    )
    .bind(invitee_ids)
    .fetch_all(&state.pool)
    .await?;

    rows.iter()
        .map(|row| {
            let inviter_id: i64 = row.try_get("inviter_id")?;
            let invitee_id: i64 = row.try_get("invitee_id")?;
            let total: i64 = row.try_get("total")?;
            Ok(((inviter_id, invitee_id), quota_to_cents(total)))
        })
        .collect()
}

async fn fetch_invitation_signup_reward_amounts(
    pool: &sqlx::PgPool,
) -> Result<InvitationSignupRewardAmounts> {
    Ok(InvitationSignupRewardAmounts {
        inviter: get_config_i64_or_fallback(
            pool,
            INVITATION_SIGNUP_INVITER_REWARD_AMOUNT_KEY,
            INVITATION_SIGNUP_REWARD_AMOUNT_KEY,
        )
        .await?,
        invitee: get_config_i64_or_fallback(
            pool,
            INVITATION_SIGNUP_INVITEE_REWARD_AMOUNT_KEY,
            INVITATION_SIGNUP_REWARD_AMOUNT_KEY,
        )
        .await?,
    })
}

fn admin_invitation_registration_from_parts(
    source: &NewApiInvitationRegistration,
    local: &LocalInvitationRegistration,
    reward_map: &HashMap<(String, i64), InvitationRegistrationReward>,
    total_reward_map: &HashMap<(i64, i64), i64>,
    reward_amounts: &InvitationSignupRewardAmounts,
) -> Result<AdminInvitationRegistration> {
    let inviter_reward = reward_map.get(&(
        INVITATION_SIGNUP_INVITER_ORDER_TYPE.to_string(),
        source.invitee_id,
    ));
    let invitee_reward = reward_map.get(&(
        INVITATION_SIGNUP_INVITEE_ORDER_TYPE.to_string(),
        source.invitee_id,
    ));

    Ok(AdminInvitationRegistration {
        id: local.id,
        inviter_id: source.inviter_id,
        inviter_name: source.inviter_name.clone(),
        invitee_id: source.invitee_id,
        invitee_name: source.invitee_name.clone(),
        user_group: local.user_group.clone(),
        invited_at: timestamp_to_iso(local.invited_at),
        total_reward_amount: total_reward_map
            .get(&(local.inviter_id, local.invitee_id))
            .copied()
            .unwrap_or_default(),
        inviter_reward_generated: inviter_reward.is_some(),
        inviter_reward_record_id: inviter_reward.map(|reward| reward.record_id),
        inviter_reward_amount: inviter_reward
            .map(|reward| reward.rebate_amount)
            .unwrap_or_else(|| quota_to_cents(reward_amounts.inviter)),
        inviter_reward_status: inviter_reward.map(|reward| reward.status.clone()),
        invitee_reward_generated: invitee_reward.is_some(),
        invitee_reward_record_id: invitee_reward.map(|reward| reward.record_id),
        invitee_reward_amount: invitee_reward
            .map(|reward| reward.rebate_amount)
            .unwrap_or_else(|| quota_to_cents(reward_amounts.invitee)),
        invitee_reward_status: invitee_reward.map(|reward| reward.status.clone()),
    })
}

fn local_invitation_registration_from_row(row: &PgRow) -> Result<LocalInvitationRegistration> {
    Ok(LocalInvitationRegistration {
        id: row.try_get("id")?,
        inviter_id: row.try_get("inviter_id")?,
        invitee_id: row.try_get("invitee_id")?,
        user_group: row.try_get("user_group")?,
        invited_at: row.try_get("invited_at")?,
    })
}

async fn count_newapi_rebate_orders(state: &AppState, order_type: Option<&str>) -> Result<i64> {
    sqlx::query_scalar(
        r#"
        SELECT COUNT(*) FROM (
            SELECT 'topup'::TEXT AS order_type
            FROM top_ups t
            JOIN users u ON t.user_id = u.id
            WHERE t.status = 'success'
              AND t.id >= $2
              AND u.inviter_id IS NOT NULL
              AND u.inviter_id > 0
              AND NOT EXISTS (
                  SELECT 1
                  FROM subscription_orders s
                  WHERE s.trade_no = t.trade_no
              )
            UNION ALL
            SELECT 'subscription'::TEXT AS order_type
            FROM subscription_orders s
            JOIN users u ON s.user_id = u.id
            WHERE s.status = 'success'
              AND u.inviter_id IS NOT NULL
              AND u.inviter_id > 0
            UNION ALL
            SELECT 'redemption'::TEXT AS order_type
            FROM redemptions r
            JOIN users u ON r.used_user_id = u.id
            WHERE r.status = 3
              AND r.used_user_id > 0
              AND r.redeemed_time > 0
              AND u.inviter_id IS NOT NULL
              AND u.inviter_id > 0
        ) rebate_orders
        WHERE ($1::TEXT IS NULL OR order_type = $1)
        "#,
    )
    .bind(order_type)
    .bind(state.config.topup_rebate_active_from_id)
    .fetch_one(&state.newapi_pool)
    .await
    .map_err(AppError::from)
}

async fn fetch_newapi_rebate_order_diagnostics(
    state: &AppState,
) -> Result<NewApiRebateOrderDiagnostics> {
    let row = sqlx::query(
        r#"
        SELECT
            (SELECT COUNT(*) FROM users WHERE inviter_id IS NOT NULL AND inviter_id > 0)::BIGINT AS invited_users,
            (SELECT COUNT(*) FROM top_ups WHERE status = 'success')::BIGINT AS topup_success,
            (
                SELECT COUNT(*)
                FROM top_ups t
                JOIN users u ON t.user_id = u.id
                WHERE t.status = 'success'
                  AND t.id >= $1
                  AND u.inviter_id IS NOT NULL
                  AND u.inviter_id > 0
            )::BIGINT AS topup_invited,
            (
                SELECT COUNT(*)
                FROM top_ups t
                JOIN users u ON t.user_id = u.id
                WHERE t.status = 'success'
                  AND t.id >= $1
                  AND u.inviter_id IS NOT NULL
                  AND u.inviter_id > 0
                  AND NOT EXISTS (
                      SELECT 1
                      FROM subscription_orders s
                      WHERE s.trade_no = t.trade_no
                  )
            )::BIGINT AS topup_rebate_orders,
            (SELECT COUNT(*) FROM subscription_orders WHERE status = 'success')::BIGINT AS subscription_success,
            (
                SELECT COUNT(*)
                FROM subscription_orders s
                JOIN users u ON s.user_id = u.id
                WHERE s.status = 'success'
                  AND u.inviter_id IS NOT NULL
                  AND u.inviter_id > 0
            )::BIGINT AS subscription_invited,
            (SELECT COUNT(*) FROM redemptions WHERE status = 3 AND used_user_id > 0 AND redeemed_time > 0)::BIGINT AS redemption_used,
            (
                SELECT COUNT(*)
                FROM redemptions r
                JOIN users u ON r.used_user_id = u.id
                WHERE r.status = 3
                  AND r.used_user_id > 0
                  AND r.redeemed_time > 0
                  AND u.inviter_id IS NOT NULL
                  AND u.inviter_id > 0
            )::BIGINT AS redemption_invited
        "#,
    )
    .bind(state.config.topup_rebate_active_from_id)
    .fetch_one(&state.newapi_pool)
    .await?;

    Ok(NewApiRebateOrderDiagnostics {
        invited_users: row.try_get("invited_users")?,
        topup_success: row.try_get("topup_success")?,
        topup_invited: row.try_get("topup_invited")?,
        topup_rebate_orders: row.try_get("topup_rebate_orders")?,
        subscription_success: row.try_get("subscription_success")?,
        subscription_invited: row.try_get("subscription_invited")?,
        redemption_used: row.try_get("redemption_used")?,
        redemption_invited: row.try_get("redemption_invited")?,
    })
}

async fn fetch_newapi_rebate_orders(
    state: &AppState,
    order_type: Option<&str>,
    page_size: i64,
    offset: i64,
) -> Result<Vec<NewApiRebateOrder>> {
    let rows = sqlx::query(
        r#"
        SELECT
            order_type,
            order_id,
            inviter_id,
            inviter_name,
            invitee_id,
            invitee_name,
            user_group,
            order_amount_cents,
            order_time
        FROM (
            SELECT
                'topup'::TEXT AS order_type,
                t.id AS order_id,
                u.inviter_id AS inviter_id,
                COALESCE(NULLIF(inviter.display_name, ''), NULLIF(inviter.username, '')) AS inviter_name,
                t.user_id AS invitee_id,
                COALESCE(NULLIF(u.display_name, ''), NULLIF(u.username, '')) AS invitee_name,
                COALESCE(NULLIF(inviter."group", ''), 'default') AS user_group,
                ROUND(t.money::NUMERIC * 100)::BIGINT AS order_amount_cents,
                COALESCE(NULLIF(t.complete_time, 0), NULLIF(t.create_time, 0), 0) AS order_time
            FROM top_ups t
            JOIN users u ON t.user_id = u.id
            LEFT JOIN users inviter ON inviter.id = u.inviter_id
            WHERE t.status = 'success'
              AND t.id >= $5
              AND u.inviter_id IS NOT NULL
              AND u.inviter_id > 0
              AND NOT EXISTS (
                  SELECT 1
                  FROM subscription_orders s
                  WHERE s.trade_no = t.trade_no
              )
            UNION ALL
            SELECT
                'subscription'::TEXT AS order_type,
                s.id AS order_id,
                u.inviter_id AS inviter_id,
                COALESCE(NULLIF(inviter.display_name, ''), NULLIF(inviter.username, '')) AS inviter_name,
                s.user_id AS invitee_id,
                COALESCE(NULLIF(u.display_name, ''), NULLIF(u.username, '')) AS invitee_name,
                COALESCE(NULLIF(inviter."group", ''), 'default') AS user_group,
                ROUND(s.money::NUMERIC * 100)::BIGINT AS order_amount_cents,
                COALESCE(NULLIF(s.complete_time, 0), NULLIF(s.create_time, 0), 0) AS order_time
            FROM subscription_orders s
            JOIN users u ON s.user_id = u.id
            LEFT JOIN users inviter ON inviter.id = u.inviter_id
            WHERE s.status = 'success'
              AND u.inviter_id IS NOT NULL
              AND u.inviter_id > 0
            UNION ALL
            SELECT
                'redemption'::TEXT AS order_type,
                r.id::BIGINT AS order_id,
                u.inviter_id AS inviter_id,
                COALESCE(NULLIF(inviter.display_name, ''), NULLIF(inviter.username, '')) AS inviter_name,
                r.used_user_id::BIGINT AS invitee_id,
                COALESCE(NULLIF(u.display_name, ''), NULLIF(u.username, '')) AS invitee_name,
                COALESCE(NULLIF(inviter."group", ''), 'default') AS user_group,
                ROUND(r.quota::NUMERIC * 100 / $2::NUMERIC)::BIGINT AS order_amount_cents,
                COALESCE(NULLIF(r.redeemed_time, 0), NULLIF(r.created_time, 0), 0) AS order_time
            FROM redemptions r
            JOIN users u ON r.used_user_id = u.id
            LEFT JOIN users inviter ON inviter.id = u.inviter_id
            WHERE r.status = 3
              AND r.used_user_id > 0
              AND r.redeemed_time > 0
              AND u.inviter_id IS NOT NULL
              AND u.inviter_id > 0
        ) rebate_orders
        WHERE ($1::TEXT IS NULL OR order_type = $1)
        ORDER BY order_time DESC, order_id DESC
        LIMIT $3 OFFSET $4
        "#,
    )
    .bind(order_type)
    .bind(state.config.quota_per_unit)
    .bind(page_size)
    .bind(offset)
    .bind(state.config.topup_rebate_active_from_id)
    .fetch_all(&state.newapi_pool)
    .await?;

    rows.iter()
        .map(|row| {
            Ok(NewApiRebateOrder {
                order_type: row.try_get("order_type")?,
                order_id: row.try_get("order_id")?,
                inviter_id: row.try_get("inviter_id")?,
                inviter_name: row.try_get("inviter_name")?,
                invitee_id: row.try_get("invitee_id")?,
                invitee_name: row.try_get("invitee_name")?,
                user_group: row.try_get("user_group")?,
                order_amount: row.try_get("order_amount_cents")?,
                order_time: row.try_get("order_time")?,
            })
        })
        .collect::<Result<Vec<_>>>()
}

#[cfg(test)]
pub(crate) async fn fetch_newapi_topup_rebate_order_ids_for_test(
    state: &AppState,
) -> Result<(i64, Vec<i64>)> {
    let total = count_newapi_rebate_orders(state, Some("topup")).await?;
    let orders = fetch_newapi_rebate_orders(state, Some("topup"), 1_000, 0).await?;
    Ok((
        total,
        orders.into_iter().map(|order| order.order_id).collect(),
    ))
}

async fn fetch_local_rebate_record_map(
    state: &AppState,
    orders: &[NewApiRebateOrder],
) -> Result<HashMap<(String, i64), LocalRebateRecord>> {
    let topup_order_ids: Vec<i64> = orders
        .iter()
        .filter(|order| order.order_type == "topup")
        .map(|order| order.order_id)
        .collect();
    let subscription_order_ids: Vec<i64> = orders
        .iter()
        .filter(|order| order.order_type == "subscription")
        .map(|order| order.order_id)
        .collect();
    let redemption_order_ids: Vec<i64> = orders
        .iter()
        .filter(|order| order.order_type == REDEMPTION_ORDER_TYPE)
        .map(|order| order.order_id)
        .collect();
    let redemption_event_keys = redemption_order_ids
        .iter()
        .map(|order_id| format!("newapi:redemption:{order_id}"))
        .collect::<Vec<_>>();

    if topup_order_ids.is_empty()
        && subscription_order_ids.is_empty()
        && redemption_order_ids.is_empty()
    {
        return Ok(HashMap::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT
            id,
            order_type,
            order_id,
            canonical_event_key,
            superseded_by_record_id,
            rebate_amount,
            rebate_rate::TEXT AS rebate_rate,
            status,
            order_time,
            rebate_effective_at,
            scan_started_at,
            initialization_ends_at,
            finalized_at,
            closed_at,
            admin_adjusted
        FROM rebate_records
        WHERE superseded_by_record_id IS NULL
          AND (
              (order_type = 'topup' AND order_id = ANY($1))
              OR (order_type = 'subscription' AND order_id = ANY($2))
              OR canonical_event_key::TEXT = ANY($3::TEXT[])
              OR (order_type = 'redemption' AND order_id = ANY($4))
          )
        "#,
    )
    .bind(&topup_order_ids)
    .bind(&subscription_order_ids)
    .bind(&redemption_event_keys)
    .bind(&redemption_order_ids)
    .fetch_all(&state.pool)
    .await?;

    let mut records = HashMap::with_capacity(rows.len());
    for row in &rows {
        let record = local_rebate_record_from_row(row)?;
        let Some(key) = local_rebate_record_key(&record)? else {
            continue;
        };
        if records.insert(key, record).is_some() {
            return Err(AppError::Internal(
                "同一 New API 订单映射到多条有效返利记录".to_string(),
            ));
        }
    }
    Ok(records)
}

#[cfg(test)]
pub(crate) async fn fetch_local_rebate_record_id_for_test(
    state: &AppState,
    order_type: &str,
    order_id: i64,
) -> Result<Option<i64>> {
    let order = NewApiRebateOrder {
        order_type: order_type.to_string(),
        order_id,
        inviter_id: 1,
        inviter_name: None,
        invitee_id: 2,
        invitee_name: None,
        user_group: "default".to_string(),
        order_amount: 0,
        order_time: 1,
    };
    let records = fetch_local_rebate_record_map(state, &[order]).await?;
    Ok(records
        .get(&(order_type.to_string(), order_id))
        .map(|record| record.id))
}

fn local_rebate_record_from_row(row: &PgRow) -> Result<LocalRebateRecord> {
    let rebate_rate: Option<String> = row.try_get("rebate_rate")?;
    Ok(LocalRebateRecord {
        id: row.try_get("id")?,
        order_type: row.try_get("order_type")?,
        order_id: row.try_get("order_id")?,
        canonical_event_key: row.try_get("canonical_event_key")?,
        superseded_by_record_id: row.try_get("superseded_by_record_id")?,
        rebate_amount: quota_to_cents(row.try_get("rebate_amount")?),
        rebate_ratio: rebate_rate.as_deref().map(text_to_f64),
        status: row.try_get("status")?,
        order_time: row.try_get("order_time")?,
        rebate_effective_at: row.try_get("rebate_effective_at")?,
        scan_started_at: row.try_get("scan_started_at")?,
        initialization_ends_at: row.try_get("initialization_ends_at")?,
        finalized_at: row.try_get("finalized_at")?,
        closed_at: row.try_get("closed_at")?,
        admin_adjusted: row.try_get("admin_adjusted")?,
    })
}

fn local_rebate_record_key(record: &LocalRebateRecord) -> Result<Option<(String, i64)>> {
    if record.superseded_by_record_id.is_some() {
        return Ok(None);
    }
    let Some(event_key) = record.canonical_event_key.as_deref() else {
        return Ok(Some((record.order_type.clone(), record.order_id)));
    };
    let Some(redemption_id) = event_key.strip_prefix("newapi:redemption:") else {
        return Ok(Some((record.order_type.clone(), record.order_id)));
    };
    let redemption_id = redemption_id.parse::<i64>().map_err(|_| {
        AppError::Internal(format!(
            "返利记录 {} 的 canonical redemption event key 无效",
            record.id
        ))
    })?;
    if redemption_id <= 0 {
        return Err(AppError::Internal(format!(
            "返利记录 {} 的 canonical redemption ID 无效",
            record.id
        )));
    }
    Ok(Some((REDEMPTION_ORDER_TYPE.to_string(), redemption_id)))
}

fn admin_rebate_order_record_from_parts(
    order: &NewApiRebateOrder,
    local_record: Option<&LocalRebateRecord>,
    now: i64,
    effective_at_fallback: i64,
) -> AdminRebateOrderRecord {
    let rule_missing = local_record
        .map(|record| record.rebate_ratio.is_none())
        .unwrap_or(true);
    let status = local_record
        .map(|record| {
            derive_admin_rebate_status(
                now,
                record.rebate_effective_at,
                rule_missing,
                &record.status,
            )
        })
        .unwrap_or("initializing")
        .to_string();
    let order_time = local_record
        .map(|record| record.order_time)
        .unwrap_or(order.order_time);
    let rebate_effective_at = local_record
        .map(|record| record.rebate_effective_at)
        .unwrap_or(effective_at_fallback);
    let scan_started_at = local_record
        .map(|record| record.scan_started_at)
        .unwrap_or(now);
    let initialization_ends_at = local_record
        .map(|record| record.initialization_ends_at)
        .unwrap_or(scan_started_at + INITIALIZATION_SECONDS);

    AdminRebateOrderRecord {
        order_type: order.order_type.clone(),
        order_id: order.order_id,
        inviter_id: order.inviter_id,
        inviter_name: order.inviter_name.clone(),
        invitee_id: order.invitee_id,
        invitee_name: order.invitee_name.clone(),
        user_group: order.user_group.clone(),
        order_amount: order.order_amount,
        rebate_amount: local_record
            .map(|record| record.rebate_amount)
            .unwrap_or_default(),
        rebate_ratio: local_record.and_then(|record| record.rebate_ratio),
        status,
        rule_missing,
        local_rebate_record_id: local_record.map(|record| record.id),
        local_rebate_status: local_record.map(|record| record.status.clone()),
        order_time: timestamp_to_iso(order_time),
        effective_at: timestamp_to_iso(rebate_effective_at),
        scan_started_at: timestamp_to_iso(scan_started_at),
        initialization_ends_at: timestamp_to_iso(initialization_ends_at),
        finalized_at: local_record
            .and_then(|record| optional_timestamp_to_iso(record.finalized_at)),
        closed_at: local_record.and_then(|record| optional_timestamp_to_iso(record.closed_at)),
        admin_adjusted: local_record
            .map(|record| record.admin_adjusted)
            .unwrap_or(false),
        can_modify: local_record
            .map(|record| is_rebate_record_mutable(&record.status))
            .unwrap_or(false),
        can_close: local_record
            .map(|record| is_rebate_record_mutable(&record.status))
            .unwrap_or(false),
        can_reopen: local_record
            .map(|record| can_reopen_rebate_record(&record.status, record.superseded_by_record_id))
            .unwrap_or(false),
        can_end_initialization: local_record
            .map(|record| record.status == "initializing")
            .unwrap_or(false),
        can_extend_initialization: local_record
            .map(|record| can_extend_initialization(&record.status))
            .unwrap_or(false),
    }
}

fn derive_admin_rebate_status(
    now: i64,
    effective_at: i64,
    rule_missing: bool,
    local_status: &str,
) -> &'static str {
    match local_status {
        "initializing" => "initializing",
        "closed" => "closed",
        "requested" => "requested",
        "approved" => "approved",
        "completed" => "paid",
        "pending" | "rejected" => {
            if rule_missing || now < effective_at {
                "estimated"
            } else {
                "claimable"
            }
        }
        _ => "paid",
    }
}

fn text_to_f64(value: &str) -> f64 {
    value.parse::<f64>().unwrap_or_default()
}

fn rebate_rule_from_row(row: &PgRow) -> Result<RebateRule> {
    Ok(RebateRule {
        id: row.try_get("id")?,
        user_group: row.try_get("user_group")?,
        rule_type: row.try_get("rule_type")?,
        rebate_rate: row.try_get("rebate_rate")?,
        effective_time: row.try_get("effective_time")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn validate_rebate_rule_request(req: &RebateRuleRequest) -> Result<ValidatedRebateRuleRequest> {
    let user_group = req.user_group.trim();
    if user_group.is_empty() || user_group.len() > 64 {
        return Err(AppError::BadRequest(
            "用户组名称长度必须在 1-64 之间".to_string(),
        ));
    }
    let rule_type = req.rule_type.trim();
    if !is_order_rebate_order_type(rule_type) {
        return Err(AppError::BadRequest(
            "规则类型必须是 topup、subscription 或 redemption".to_string(),
        ));
    }

    let rate = match &req.rebate_rate {
        serde_json::Value::Number(value) => value.as_f64(),
        serde_json::Value::String(value) => value.parse::<f64>().ok(),
        _ => None,
    }
    .ok_or_else(|| AppError::BadRequest("返利比例格式错误".to_string()))?;

    if !(0.0..=1.0).contains(&rate) {
        return Err(AppError::BadRequest("返利比例必须在 0-1 之间".to_string()));
    }
    if user_group == SALES_USER_GROUP && (rule_type != "topup" || rate != 0.15) {
        return Err(AppError::BadRequest(
            "销售返利专用规则仅支持充值 15%".to_string(),
        ));
    }

    Ok(ValidatedRebateRuleRequest {
        user_group: user_group.to_string(),
        rule_type: rule_type.to_string(),
        rebate_rate: rate,
    })
}

async fn upsert_config_value(
    pool: &sqlx::PgPool,
    key: &str,
    value: &str,
    description: &str,
    now: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO system_config (key, value, description, updated_at)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (key)
        DO UPDATE SET value = $2, description = $3, updated_at = $4
        "#,
    )
    .bind(key)
    .bind(value)
    .bind(description)
    .bind(now)
    .execute(pool)
    .await?;

    Ok(())
}

fn resolve_signup_reward_amounts_for_update(config: &SystemConfig) -> (i64, i64) {
    if config.invitation_signup_inviter_reward_amount == 0
        && config.invitation_signup_invitee_reward_amount == 0
        && config.invitation_signup_reward_amount > 0
    {
        return (
            config.invitation_signup_reward_amount,
            config.invitation_signup_reward_amount,
        );
    }

    (
        config.invitation_signup_inviter_reward_amount,
        config.invitation_signup_invitee_reward_amount,
    )
}

async fn count_rebate_requests(
    state: &AppState,
    status: Option<&str>,
    request_kind: &str,
) -> Result<i64> {
    Ok(sqlx::query_scalar("SELECT COUNT(*) FROM rebate_requests WHERE ($1::TEXT IS NULL OR (CASE WHEN EXISTS(SELECT 1 FROM sales_disbursements WHERE request_id=rebate_requests.id) THEN 'completed' ELSE status END)=$1) AND request_kind=$2")
        .bind(status).bind(request_kind).fetch_one(&state.pool).await?)
}

async fn fetch_rebate_requests(
    state: &AppState,
    status: Option<&str>,
    params: &RebateRequestListParams,
) -> Result<Vec<PgRow>> {
    Ok(sqlx::query(r#"
        SELECT id, user_id, rebate_record_ids, total_amount, status, created_at, updated_at,
               approved_at, completed_at, rejected_at, admin_note, request_kind,
               (SELECT confirmed_at FROM sales_disbursements WHERE request_id=rebate_requests.id) AS issued_at,
               (SELECT note FROM sales_disbursements WHERE request_id=rebate_requests.id) AS issue_note
        FROM rebate_requests
        WHERE ($1::TEXT IS NULL OR (CASE WHEN EXISTS(SELECT 1 FROM sales_disbursements WHERE request_id=rebate_requests.id) THEN 'completed' ELSE status END)=$1) AND request_kind=$2
        ORDER BY created_at DESC, id DESC LIMIT $3 OFFSET $4
    "#).bind(status).bind(params.request_kind()?).bind(params.page_size()).bind(params.offset())
    .fetch_all(&state.pool).await?)
}

async fn approve_and_pay_request(
    state: &AppState,
    admin: &AdminUser,
    request_id: i64,
    note: Option<&str>,
    base_payout_request_id: &str,
) -> Result<ApproveAndPayResponse> {
    let prepared = prepare_approve_and_pay_request(state, admin, request_id, note).await?;

    match prepared {
        PreparedApproveAndPayRequest::AlreadyCompleted {
            rebate_record_ids,
            total_amount,
        } => Ok(ApproveAndPayResponse {
            request_id,
            status: "completed".to_string(),
            total_amount: quota_to_cents(total_amount),
            paid_count: 0,
            already_paid_count: rebate_record_ids.len(),
            pending_count: 0,
            failed_count: 0,
            items: rebate_record_ids
                .into_iter()
                .map(already_completed_record_result)
                .collect(),
        }),
        PreparedApproveAndPayRequest::Pay {
            rebate_record_ids,
            total_amount,
        } => {
            let mut items = Vec::with_capacity(rebate_record_ids.len());
            let mut paid_count = 0;
            let mut already_paid_count = 0;
            let mut pending_count = 0;
            let mut failed_count = 0;

            for record_id in rebate_record_ids {
                let payout_request_id =
                    derive_payout_request_id(base_payout_request_id, request_id, record_id);
                match pay_rebate_record(
                    state,
                    PayRecordInput {
                        record_id,
                        request_id: payout_request_id,
                        admin,
                        now: Utc::now().timestamp(),
                    },
                )
                .await
                {
                    Ok(result) => {
                        if result.outcome == "already_paid" {
                            already_paid_count += 1;
                        } else if result.outcome == "paid" {
                            paid_count += 1;
                        } else if result.outcome == "pending" {
                            pending_count += 1;
                        }
                        items.push(record_result_from_pay_result(result));
                    }
                    Err(error) => {
                        failed_count += 1;
                        items.push(failed_record_result(record_id, &error));
                    }
                }
            }

            let finalized_status = finalize_approve_and_pay_request(state, request_id).await?;
            Ok(ApproveAndPayResponse {
                request_id,
                status: finalized_status,
                total_amount: quota_to_cents(total_amount),
                paid_count,
                already_paid_count,
                pending_count,
                failed_count,
                items,
            })
        }
    }
}

fn aggregate_pending_count(items: &[ApproveAndPayResponse]) -> usize {
    items.iter().map(|item| item.pending_count).sum()
}

fn approve_and_pay_message(response: &ApproveAndPayResponse) -> String {
    let details = settlement_outcome_details(
        response.pending_count,
        response.paid_count,
        response.already_paid_count,
        response.failed_count,
    );
    let prefix = if response.pending_count > 0 {
        "返利申请已提交"
    } else {
        "返利申请处理完成"
    };
    response_message(prefix, details)
}

fn batch_approve_and_pay_message(response: &BatchApproveAndPayResponse) -> String {
    let mut details = settlement_outcome_details(
        response.pending_count,
        response.paid_count,
        response.already_paid_count,
        response.failed_count,
    );
    if response.failed_requests > 0 {
        details.push(format!("{} 个申请处理失败", response.failed_requests));
    }
    let prefix = if response.pending_count > 0 {
        "批量返利申请已提交"
    } else {
        "批量返利申请处理完成"
    };
    response_message(prefix, details)
}

fn settlement_outcome_details(
    pending_count: usize,
    paid_count: usize,
    already_paid_count: usize,
    failed_count: usize,
) -> Vec<String> {
    let mut details = Vec::new();
    if pending_count > 0 {
        details.push(format!("{pending_count} 条等待结算"));
    }
    if paid_count > 0 {
        details.push(format!("{paid_count} 条已结算"));
    }
    if already_paid_count > 0 {
        details.push(format!("{already_paid_count} 条无需重复结算"));
    }
    if failed_count > 0 {
        details.push(format!("{failed_count} 条记录处理失败"));
    }
    details
}

fn response_message(prefix: &str, details: Vec<String>) -> String {
    if details.is_empty() {
        prefix.to_string()
    } else {
        format!("{prefix}：{}", details.join("，"))
    }
}

async fn prepare_approve_and_pay_request(
    state: &AppState,
    admin: &AdminUser,
    request_id: i64,
    note: Option<&str>,
) -> Result<PreparedApproveAndPayRequest> {
    let now = Utc::now().timestamp();
    let mut tx = state.pool.begin().await?;
    let row = lock_rebate_request_for_review(&mut tx, request_id).await?;
    if row.try_get::<String, _>("request_kind")? == "sales" {
        return Err(AppError::Conflict(
            "销售申请必须人工确认发放，不能转入站内余额".into(),
        ));
    }
    let status: String = row.try_get("status")?;
    let rebate_record_ids: Vec<i64> = row.try_get("rebate_record_ids")?;

    let total_amount = rebate_records_total_amount(&mut tx, &rebate_record_ids).await?;
    match status.as_str() {
        "completed" => {
            tx.commit().await?;
            Ok(PreparedApproveAndPayRequest::AlreadyCompleted {
                rebate_record_ids,
                total_amount,
            })
        }
        "pending" | "approved" => {
            lock_and_validate_request_records(&mut tx, &row, true, true).await?;
            sqlx::query(
                r#"
                UPDATE rebate_requests
                SET status = 'approved',
                    admin_id = $1,
                    admin_note = $2,
                    approved_at = COALESCE(approved_at, $3),
                    rejected_at = NULL,
                    updated_at = $3
                WHERE id = $4
                "#,
            )
            .bind(admin.0.id)
            .bind(note)
            .bind(now)
            .bind(request_id)
            .execute(&mut *tx)
            .await?;

            sqlx::query(
                r#"
                UPDATE rebate_records
                SET status = 'approved', updated_at = $1
                WHERE id = ANY($2)
                  AND status IN ('requested', 'pending', 'approved', 'rejected')
                "#,
            )
            .bind(now)
            .bind(&rebate_record_ids)
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok(PreparedApproveAndPayRequest::Pay {
                rebate_record_ids,
                total_amount,
            })
        }
        "rejected" => Err(AppError::Conflict("已拒绝的返利申请不能发放".to_string())),
        _ => Err(AppError::Conflict(format!(
            "返利申请状态不支持审批并发放：{status}"
        ))),
    }
}

async fn finalize_approve_and_pay_request(state: &AppState, request_id: i64) -> Result<String> {
    let now = Utc::now().timestamp();
    let mut tx = state.pool.begin().await?;
    let row = lock_rebate_request_for_review(&mut tx, request_id).await?;
    let rebate_record_ids: Vec<i64> = row.try_get("rebate_record_ids")?;
    let all_completed: bool = sqlx::query_scalar(
        r#"
        SELECT NOT EXISTS (
            SELECT 1
            FROM rebate_records
            WHERE id = ANY($1)
              AND status <> 'completed'
        )
        "#,
    )
    .bind(&rebate_record_ids)
    .fetch_one(&mut *tx)
    .await?;

    if !all_completed {
        tx.commit().await?;
        return Ok("approved".to_string());
    }

    sqlx::query(
        r#"
        UPDATE rebate_requests
        SET status = 'completed',
            completed_at = COALESCE(completed_at, $2),
            updated_at = $2
        WHERE id = $1
        "#,
    )
    .bind(request_id)
    .bind(now)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok("completed".to_string())
}

fn validate_approve_and_pay_batch_ids(ids: &[i64]) -> Result<Vec<i64>> {
    if ids.is_empty() {
        return Err(AppError::BadRequest("请选择返利申请".to_string()));
    }
    if ids.len() > MAX_APPROVE_AND_PAY_BATCH_SIZE {
        return Err(AppError::BadRequest(format!(
            "一次最多处理 {MAX_APPROVE_AND_PAY_BATCH_SIZE} 个返利申请"
        )));
    }

    let mut seen = HashSet::new();
    let mut result = Vec::new();
    for id in ids.iter().copied() {
        validate_id(id)?;
        if seen.insert(id) {
            result.push(id);
        }
    }
    Ok(result)
}

fn derive_payout_request_id(base: &str, request_id: i64, record_id: i64) -> String {
    let suffix = format!(":req{request_id}:rec{record_id}");
    let max_base_len = MAX_PAYOUT_REQUEST_ID_LEN.saturating_sub(suffix.len());
    let truncated_base = truncate_utf8_by_bytes(base, max_base_len);
    format!("{truncated_base}{suffix}")
}

fn truncate_utf8_by_bytes(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

fn record_result_from_pay_result(result: PayResult) -> ApproveAndPayRecordResult {
    ApproveAndPayRecordResult {
        record_id: result.record_id,
        outcome: result.outcome,
        amount_cents: Some(result.amount_cents),
        quota_delta: Some(result.quota_delta),
        payout_id: result.payout_id,
        newapi_log_id: result.newapi_log_id,
        newapi_mark_id: result.newapi_mark_id,
        error: None,
    }
}

fn failed_record_result(record_id: i64, error: &AppError) -> ApproveAndPayRecordResult {
    ApproveAndPayRecordResult {
        record_id,
        outcome: "failed".to_string(),
        amount_cents: None,
        quota_delta: None,
        payout_id: None,
        newapi_log_id: None,
        newapi_mark_id: None,
        error: Some(app_error_message(error).to_string()),
    }
}

fn already_completed_record_result(record_id: i64) -> ApproveAndPayRecordResult {
    ApproveAndPayRecordResult {
        record_id,
        outcome: "already_completed".to_string(),
        amount_cents: None,
        quota_delta: None,
        payout_id: None,
        newapi_log_id: None,
        newapi_mark_id: None,
        error: None,
    }
}

fn app_error_message(error: &AppError) -> &str {
    match error {
        AppError::Unauthorized => "未授权",
        AppError::Forbidden => "无权限",
        AppError::FundsMaintenance => FUNDS_MAINTENANCE_MESSAGE,
        AppError::LegacyCompletionDisabled => LEGACY_COMPLETION_DISABLED_MESSAGE,
        AppError::NotFound(message)
        | AppError::BadRequest(message)
        | AppError::Conflict(message)
        | AppError::UnprocessableEntity(message)
        | AppError::Locked(message)
        | AppError::Internal(message) => message,
        AppError::Database(_) => "数据库错误",
    }
}

async fn fetch_user_map(
    state: &AppState,
    rebate_request_rows: &[PgRow],
) -> Result<std::collections::HashMap<i64, String>> {
    let user_ids: Vec<i64> = rebate_request_rows
        .iter()
        .filter_map(|row| row.try_get::<i64, _>("user_id").ok())
        .collect();

    if user_ids.is_empty() {
        return Ok(std::collections::HashMap::new());
    }

    Ok(sqlx::query(
        r#"
            SELECT
                id,
                COALESCE(NULLIF(display_name, ''), NULLIF(username, '')) AS user_name
            FROM users
            WHERE id = ANY($1)
            "#,
    )
    .bind(&user_ids)
    .fetch_all(&state.newapi_pool)
    .await?
    .iter()
    .filter_map(|row| {
        let id = row.try_get::<i64, _>("id").ok()?;
        let user_name = row.try_get::<Option<String>, _>("user_name").ok()??;
        Some((id, user_name))
    })
    .collect())
}

fn rebate_request_detail_from_row(
    row: &PgRow,
    user_map: &std::collections::HashMap<i64, String>,
    credit_currency: &'static str,
) -> Result<RebateRequestDetail> {
    let user_id: i64 = row.try_get("user_id")?;
    let status: String = row.try_get("status")?;
    let admin_note: Option<String> = row.try_get("admin_note")?;

    Ok(RebateRequestDetail {
        credit_currency,
        id: row.try_get("id")?,
        user_id,
        user_name: user_map.get(&user_id).cloned().unwrap_or_default(),
        amount: quota_to_cents(row.try_get("total_amount")?),
        status: status.clone(),
        rebate_record_ids: row.try_get("rebate_record_ids")?,
        created_at: timestamp_to_iso(row.try_get("created_at")?),
        updated_at: timestamp_to_iso(row.try_get("updated_at")?),
        approved_at: optional_timestamp_to_iso(row.try_get("approved_at")?),
        completed_at: optional_timestamp_to_iso(row.try_get("completed_at")?),
        rejected_at: optional_timestamp_to_iso(row.try_get("rejected_at")?),
        review_note: admin_note.clone(),
        request_kind: row.try_get("request_kind")?,
        issued_at: optional_timestamp_to_iso(row.try_get("issued_at")?),
        issue_note: row.try_get("issue_note")?,
        reject_reason: if status == "rejected" {
            admin_note
        } else {
            None
        },
    })
}

fn validate_record_ids(ids: &[i64]) -> Result<Vec<i64>> {
    if ids.is_empty() {
        return Err(AppError::BadRequest("请选择返利记录".to_string()));
    }
    if ids.len() > 1000 {
        return Err(AppError::BadRequest(
            "一次最多操作 1000 条返利记录".to_string(),
        ));
    }
    if ids.iter().any(|id| *id <= 0) {
        return Err(AppError::BadRequest("返利记录 ID 无效".to_string()));
    }

    let unique: HashSet<i64> = ids.iter().copied().collect();
    if unique.len() != ids.len() {
        return Err(AppError::BadRequest("返利记录不能重复选择".to_string()));
    }

    let mut result = ids.to_vec();
    result.sort_unstable();
    Ok(result)
}

async fn lock_rebate_records(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    record_ids: &[i64],
) -> Result<Vec<PgRow>> {
    sqlx::query(
        r#"
        SELECT id, order_type, order_id, status, superseded_by_record_id
        FROM rebate_records
        WHERE id = ANY($1)
        ORDER BY id
        FOR UPDATE
        "#,
    )
    .bind(record_ids)
    .fetch_all(&mut **tx)
    .await
    .map_err(AppError::from)
}

async fn lock_rebate_records_for_review(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    record_ids: &[i64],
) -> Result<Vec<PgRow>> {
    // A dispatcher may hold a record before updating its request. Do not wait
    // on that record while a review/revocation holds the request lock.
    sqlx::query(
        r#"
        SELECT id, order_type, order_id, status, superseded_by_record_id
        FROM rebate_records
        WHERE id = ANY($1)
        ORDER BY id
        FOR UPDATE NOWAIT
        "#,
    )
    .bind(record_ids)
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| match &error {
        sqlx::Error::Database(database) if database.code().as_deref() == Some("55P03") => {
            AppError::Conflict("返利记录正在处理，请稍后重试".into())
        }
        _ => AppError::from(error),
    })
}

async fn rebate_records_total_amount(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    record_ids: &[i64],
) -> Result<i64> {
    if record_ids.is_empty() {
        return Ok(0);
    }

    sqlx::query_scalar(
        "SELECT COALESCE(SUM(rebate_amount), 0)::BIGINT FROM rebate_records WHERE id = ANY($1)",
    )
    .bind(record_ids)
    .fetch_one(&mut **tx)
    .await
    .map_err(AppError::from)
}

async fn revoke_invitation_signup_reward_record_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    record_id: i64,
    now: i64,
) -> Result<usize> {
    // Match review/confirmation lock order. A new request created before the
    // record lock is acquired is detected by the ownership check below.
    let request_rows = sqlx::query(
        r#"
        SELECT id, status, rebate_record_ids, request_kind,
               EXISTS(SELECT 1 FROM sales_disbursements WHERE request_id=rebate_requests.id) AS externally_issued
        FROM rebate_requests
        WHERE $1 = ANY(rebate_record_ids)
        ORDER BY id
        FOR NO KEY UPDATE
        "#,
    )
    .bind(record_id)
    .fetch_all(&mut **tx)
    .await?;
    let mut affected_ids = vec![record_id];
    let mut request_ids = Vec::new();
    for request in &request_rows {
        ensure_rebate_request_not_completed(request)?;
        request_ids.push(request.try_get::<i64, _>("id")?);
        if request.try_get::<String, _>("status")? != "rejected" {
            affected_ids.extend(request.try_get::<Vec<i64>, _>("rebate_record_ids")?);
        }
    }
    affected_ids.sort_unstable();
    affected_ids.dedup();
    let records = lock_rebate_records_for_review(tx, &affected_ids).await?;
    let record = records
        .iter()
        .find(|row| row.try_get::<i64, _>("id").ok() == Some(record_id))
        .ok_or_else(|| AppError::NotFound("返利记录不存在".into()))?;
    if record.try_get::<String, _>("status")? == "completed" {
        return Err(AppError::Conflict("已完成的邀请注册奖励不能撤回".into()));
    }
    let unsafe_settlement: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS(SELECT 1 FROM invitation_rebate_payouts WHERE rebate_record_id=ANY($1))
            OR EXISTS(SELECT 1 FROM balance_operations WHERE rebate_record_id=ANY($1))
            OR EXISTS(SELECT 1 FROM rebate_record_request_ownership
                      WHERE rebate_record_id=$2 AND ownership_state='active'
                        AND NOT (request_id=ANY($3)))
        "#,
    )
    .bind(&affected_ids)
    .bind(record_id)
    .bind(&request_ids)
    .fetch_one(&mut **tx)
    .await?;
    if unsafe_settlement {
        return Err(AppError::Conflict(
            "奖励已有结算记录或申请归属已变化，不能撤回".into(),
        ));
    }
    for request in &request_rows {
        if request.try_get::<String, _>("status")? == "rejected" {
            continue;
        }
        let request_id: i64 = request.try_get("id")?;
        let remaining_ids = without_rebate_record_id(
            &request.try_get::<Vec<i64>, _>("rebate_record_ids")?,
            record_id,
        );
        if remaining_ids.is_empty() {
            // Keep the rejected request and its immutable item snapshots as evidence.
            sqlx::query("UPDATE rebate_requests SET status='rejected',approved_at=NULL,rejected_at=$2,updated_at=$2 WHERE id=$1")
                .bind(request_id).bind(now).execute(&mut **tx).await?;
        } else {
            sqlx::query(
                r#"
                UPDATE rebate_requests
                SET rebate_record_ids=$1,
                    total_amount=(SELECT SUM(amount_quota_snapshot)::BIGINT FROM rebate_request_items
                                  WHERE request_id=$2 AND membership_state='active' AND rebate_record_id=ANY($1)),
                    updated_at=$3
                WHERE id=$2
                "#,
            )
            .bind(&remaining_ids).bind(request_id).bind(now).execute(&mut **tx).await?;
        }
    }
    sqlx::query("UPDATE rebate_records SET status='closed',closed_at=$2,updated_at=$2 WHERE id=$1")
        .bind(record_id)
        .bind(now)
        .execute(&mut **tx)
        .await?;
    Ok(request_rows.len())
}

async fn lock_and_validate_request_records(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    request: &PgRow,
    check_amounts: bool,
    allow_settlement_retry: bool,
) -> Result<()> {
    let request_id: i64 = request.try_get("id")?;
    let ids: Vec<i64> = request.try_get("rebate_record_ids")?;
    let records = lock_rebate_records_for_review(tx, &ids).await?;
    if ids.is_empty() || records.len() != ids.len() {
        return Err(AppError::Conflict("申请记录缺失，请重新申请".into()));
    }
    let rejected = request.try_get::<String, _>("status")? == "rejected";
    let invalid: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM rebate_records record
            LEFT JOIN rebate_request_items item ON item.request_id=$1
                AND item.rebate_record_id=record.id AND item.membership_state='active'
            LEFT JOIN rebate_record_request_ownership owner ON owner.rebate_record_id=record.id
                AND owner.ownership_state='active'
            WHERE record.id=ANY($2) AND (
                record.inviter_id<>$3 OR (record.status NOT IN ('pending','requested','approved','rejected') AND NOT ($7 AND record.status='completed'))
                OR item.rebate_record_id IS NULL
                OR CASE WHEN $4 THEN owner.request_id IS NOT NULL ELSE owner.request_id IS DISTINCT FROM $1 END
                OR ($5 AND record.rebate_amount<>item.amount_quota_snapshot)
            )
        ) OR ($5 AND $6<>(SELECT COALESCE(SUM(amount_quota_snapshot),0)::BIGINT
                         FROM rebate_request_items WHERE request_id=$1 AND membership_state='active'))
          OR (NOT $7 AND (
              EXISTS(SELECT 1 FROM invitation_rebate_payouts WHERE rebate_record_id=ANY($2))
              OR EXISTS(SELECT 1 FROM balance_operations WHERE rebate_record_id=ANY($2))
          ))
        "#,
    )
    .bind(request_id).bind(&ids).bind(request.try_get::<i64, _>("user_id")?)
    .bind(rejected).bind(check_amounts).bind(request.try_get::<i64, _>("total_amount")?)
    .bind(allow_settlement_retry)
    .fetch_one(&mut **tx).await?;
    if invalid {
        return Err(AppError::Conflict(
            "返利记录归属、金额或结算状态已变化，请重新申请或核对".into(),
        ));
    }
    Ok(())
}

async fn lock_rebate_request_for_review(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    id: i64,
) -> Result<PgRow> {
    sqlx::query(
        r#"
        SELECT id, user_id, total_amount, status, rebate_record_ids, request_kind,
               EXISTS(SELECT 1 FROM sales_disbursements WHERE request_id=rebate_requests.id) AS externally_issued
        FROM rebate_requests
        WHERE id = $1
        FOR NO KEY UPDATE
        "#,
    )
    .bind(id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| AppError::NotFound("返利申请不存在".to_string()))
}

fn ensure_rebate_request_not_completed(row: &PgRow) -> Result<()> {
    if row.try_get::<bool, _>("externally_issued")? {
        return Err(AppError::Conflict("已确认发放的销售申请不能修改".into()));
    }
    let status: String = row.try_get("status")?;
    if status == "completed" {
        return Err(AppError::BadRequest("已完成的返利申请不能修改".to_string()));
    }

    Ok(())
}

fn validate_operation_rows(
    rows: &[PgRow],
    record_ids: &[i64],
    status_allowed: impl Fn(&str) -> bool,
) -> Result<()> {
    if rows.len() != record_ids.len() {
        return Err(AppError::BadRequest(
            "部分返利记录不存在，请刷新后重试".to_string(),
        ));
    }

    for row in rows {
        let status: String = row.try_get("status")?;
        if !status_allowed(&status) {
            return Err(AppError::BadRequest(
                "部分返利记录状态不允许执行该操作".to_string(),
            ));
        }
    }

    Ok(())
}

fn validate_reopen_rows(
    rows: &[PgRow],
    record_ids: &[i64],
    topup_rebate_active_from_id: i64,
) -> Result<()> {
    validate_operation_rows(rows, record_ids, |status| status == "closed")?;
    for row in rows {
        let superseded_by_record_id: Option<i64> = row.try_get("superseded_by_record_id")?;
        if superseded_by_record_id.is_some() {
            return Err(AppError::BadRequest(
                "已被主记录取代的返利记录必须永久关闭".to_string(),
            ));
        }
        let order_type: String = row.try_get("order_type")?;
        let order_id: i64 = row.try_get("order_id")?;
        if is_topup_before_active_boundary(&order_type, order_id, topup_rebate_active_from_id) {
            return Err(AppError::BadRequest(
                "在线充值记录早于返利生效边界，必须永久关闭".to_string(),
            ));
        }
    }
    Ok(())
}

fn is_topup_before_active_boundary(
    order_type: &str,
    order_id: i64,
    topup_rebate_active_from_id: i64,
) -> bool {
    order_type == "topup" && order_id > 0 && order_id < topup_rebate_active_from_id
}

fn can_reopen_rebate_record(status: &str, superseded_by_record_id: Option<i64>) -> bool {
    status == "closed" && superseded_by_record_id.is_none()
}

fn is_rebate_record_mutable(status: &str) -> bool {
    matches!(status, "initializing" | "pending" | "rejected")
}

fn without_rebate_record_id(record_ids: &[i64], record_id: i64) -> Vec<i64> {
    record_ids
        .iter()
        .copied()
        .filter(|id| *id != record_id)
        .collect()
}

fn can_extend_initialization(status: &str) -> bool {
    matches!(status, "initializing" | "pending" | "rejected")
}

fn validate_rebate_ratio(ratio: f64) -> Result<()> {
    if !(0.0..=1.0).contains(&ratio) {
        return Err(AppError::BadRequest("返利比例必须在 0-1 之间".to_string()));
    }
    Ok(())
}

fn derive_ratio_from_amount(amount_cents: Option<i64>, order_amount_cents: i64) -> Option<f64> {
    if order_amount_cents <= 0 {
        return None;
    }

    amount_cents.map(|amount| amount as f64 / order_amount_cents as f64)
}

fn validate_id(id: i64) -> Result<()> {
    if id <= 0 {
        return Err(AppError::BadRequest("无效的 ID".to_string()));
    }
    Ok(())
}

fn timestamp_to_iso(timestamp: i64) -> String {
    Utc.timestamp_opt(timestamp, 0)
        .single()
        .unwrap_or_else(Utc::now)
        .to_rfc3339()
}

fn optional_timestamp_to_iso(timestamp: Option<i64>) -> Option<String> {
    timestamp.map(timestamp_to_iso)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn legacy_completion_handlers_return_typed_disabled_error() {
        assert!(matches!(
            complete_rebate_request().await,
            Err(AppError::LegacyCompletionDisabled)
        ));
        assert!(matches!(
            undo_complete_rebate_request().await,
            Err(AppError::LegacyCompletionDisabled)
        ));
    }

    #[test]
    fn validates_string_rebate_rate() {
        let req = RebateRuleRequest {
            user_group: "default".to_string(),
            rule_type: "topup".to_string(),
            rebate_rate: serde_json::json!("0.05"),
        };

        let validated = validate_rebate_rule_request(&req).unwrap();
        assert_eq!(validated.user_group, "default");
        assert_eq!(validated.rule_type, "topup");
        assert_eq!(validated.rebate_rate, 0.05);
    }

    #[test]
    fn validates_all_user_group_rebate_rule() {
        let req = RebateRuleRequest {
            user_group: ALL_USER_GROUP.to_string(),
            rule_type: "subscription".to_string(),
            rebate_rate: serde_json::json!("0.08"),
        };

        let validated = validate_rebate_rule_request(&req).unwrap();
        assert_eq!(validated.user_group, ALL_USER_GROUP);
        assert_eq!(validated.rule_type, "subscription");
        assert_eq!(validated.rebate_rate, 0.08);
    }

    #[test]
    fn validates_redemption_rebate_rule() {
        let req = RebateRuleRequest {
            user_group: "default".to_string(),
            rule_type: "redemption".to_string(),
            rebate_rate: serde_json::json!("0.12"),
        };

        let validated = validate_rebate_rule_request(&req).unwrap();
        assert_eq!(validated.rule_type, "redemption");
        assert_eq!(validated.rebate_rate, 0.12);
    }

    #[test]
    fn rejects_percentage_rebate_rate() {
        let req = RebateRuleRequest {
            user_group: "default".to_string(),
            rule_type: "topup".to_string(),
            rebate_rate: serde_json::json!("5"),
        };

        assert!(validate_rebate_rule_request(&req).is_err());
    }

    #[test]
    fn removes_rebate_record_id_from_request_record_list() {
        assert_eq!(without_rebate_record_id(&[1, 2, 3], 2), vec![1, 3]);
        assert_eq!(without_rebate_record_id(&[7], 7), Vec::<i64>::new());
        assert_eq!(without_rebate_record_id(&[4, 5], 9), vec![4, 5]);
    }

    #[test]
    fn admin_rebate_record_params_accept_record_statuses() {
        let requested = AdminRebateRecordListParams {
            page: None,
            page_size: None,
            status: Some("requested".to_string()),
            source: None,
        };
        assert_eq!(requested.status_filter(), Some("requested"));

        let rejected = AdminRebateRecordListParams {
            page: None,
            page_size: None,
            status: Some("rejected".to_string()),
            source: None,
        };
        assert_eq!(rejected.status_filter(), Some("rejected"));
    }

    #[test]
    fn rebate_order_record_params_accept_redemption_filter() {
        let params = RebateOrderRecordParams {
            order_type: Some("redemption".to_string()),
            page: None,
            page_size: None,
        };

        assert_eq!(params.order_type_filter(), Some("redemption"));
    }

    #[test]
    fn canonical_redemption_master_maps_to_the_newapi_redemption_order() {
        let record = LocalRebateRecord {
            id: 77,
            order_type: "topup".to_string(),
            order_id: -4432,
            canonical_event_key: Some("newapi:redemption:4432".to_string()),
            superseded_by_record_id: None,
            rebate_amount: 250,
            rebate_ratio: Some(0.05),
            status: "pending".to_string(),
            order_time: 1,
            rebate_effective_at: 2,
            scan_started_at: 1,
            initialization_ends_at: 2,
            finalized_at: Some(2),
            closed_at: None,
            admin_adjusted: false,
        };

        assert_eq!(
            local_rebate_record_key(&record).unwrap(),
            Some(("redemption".to_string(), 4432))
        );
    }

    #[test]
    fn superseded_records_are_not_mapped_or_reopened() {
        let record = LocalRebateRecord {
            id: 78,
            order_type: "redemption".to_string(),
            order_id: 4432,
            canonical_event_key: None,
            superseded_by_record_id: Some(77),
            rebate_amount: 0,
            rebate_ratio: None,
            status: "closed".to_string(),
            order_time: 1,
            rebate_effective_at: 2,
            scan_started_at: 1,
            initialization_ends_at: 2,
            finalized_at: None,
            closed_at: Some(2),
            admin_adjusted: false,
        };

        assert_eq!(local_rebate_record_key(&record).unwrap(), None);
        assert!(!can_reopen_rebate_record(
            &record.status,
            record.superseded_by_record_id
        ));
        assert!(can_reopen_rebate_record("closed", None));
    }

    #[test]
    fn topups_before_active_boundary_cannot_be_reopened() {
        assert!(is_topup_before_active_boundary("topup", 426, 444));
        assert!(!is_topup_before_active_boundary("topup", 444, 444));
        assert!(!is_topup_before_active_boundary("topup", -4432, 444));
        assert!(!is_topup_before_active_boundary("redemption", 426, 444));
    }

    #[test]
    fn admin_rebate_record_params_filter_record_sources() {
        let signup = AdminRebateRecordListParams {
            page: None,
            page_size: None,
            status: None,
            source: Some("signup".to_string()),
        };
        let signup_filter = signup.source_filter().expect("signup filter");
        assert!(!signup_filter.order_rebate_enabled);
        assert!(signup_filter.invitation_signup_reward_enabled);

        let order = AdminRebateRecordListParams {
            page: None,
            page_size: None,
            status: None,
            source: Some("order".to_string()),
        };
        let order_filter = order.source_filter().expect("order filter");
        assert!(order_filter.order_rebate_enabled);
        assert!(!order_filter.invitation_signup_reward_enabled);
    }

    #[test]
    fn derives_estimated_status_before_rebate_effective_time() {
        assert_eq!(
            derive_admin_rebate_status(1_000, 2_000, false, "pending"),
            "estimated"
        );
    }

    #[test]
    fn derives_claimable_status_for_effective_unwithdrawn_rebate() {
        assert_eq!(
            derive_admin_rebate_status(2_000, 1_000, false, "pending"),
            "claimable"
        );
        assert_eq!(
            derive_admin_rebate_status(2_000, 1_000, false, "pending"),
            "claimable"
        );
    }

    #[test]
    fn preserves_rebate_request_flow_statuses() {
        assert_eq!(
            derive_admin_rebate_status(1_000, 2_000, false, "requested"),
            "requested"
        );
        assert_eq!(
            derive_admin_rebate_status(1_000, 2_000, false, "approved"),
            "approved"
        );
        assert_eq!(
            derive_admin_rebate_status(1_000, 2_000, false, "completed"),
            "paid"
        );
    }

    #[test]
    fn missing_rule_stays_estimated_even_after_effective_time() {
        assert_eq!(
            derive_admin_rebate_status(2_000, 1_000, true, "pending"),
            "estimated"
        );
    }

    #[test]
    fn derives_initializing_and_closed_statuses() {
        assert_eq!(
            derive_admin_rebate_status(2_000, 1_000, false, "initializing"),
            "initializing"
        );
        assert_eq!(
            derive_admin_rebate_status(2_000, 1_000, false, "closed"),
            "closed"
        );
    }

    #[test]
    fn allows_extend_initialization_for_mutable_records() {
        assert!(can_extend_initialization("initializing"));
        assert!(can_extend_initialization("pending"));
        assert!(can_extend_initialization("rejected"));
        assert!(!can_extend_initialization("closed"));
        assert!(!can_extend_initialization("requested"));
        assert!(!can_extend_initialization("approved"));
        assert!(!can_extend_initialization("completed"));
    }

    #[test]
    fn validates_approve_and_pay_batch_ids() {
        assert_eq!(
            validate_approve_and_pay_batch_ids(&[3, 2, 3, 1]).unwrap(),
            vec![3, 2, 1]
        );
        assert!(validate_approve_and_pay_batch_ids(&[]).is_err());
        assert!(validate_approve_and_pay_batch_ids(&[0]).is_err());

        let too_many = vec![1; MAX_APPROVE_AND_PAY_BATCH_SIZE + 1];
        assert!(validate_approve_and_pay_batch_ids(&too_many).is_err());
    }

    #[test]
    fn approve_and_pay_response_serializes_pending_count_in_camel_case() {
        let response = ApproveAndPayResponse {
            request_id: 10,
            status: "approved".to_string(),
            total_amount: 100,
            paid_count: 0,
            already_paid_count: 0,
            pending_count: 2,
            failed_count: 0,
            items: Vec::new(),
        };

        let json = serde_json::to_value(response).unwrap();
        assert_eq!(json["status"], "approved");
        assert_eq!(json["pendingCount"], 2);
        assert!(json.get("pending_count").is_none());
    }

    #[test]
    fn batch_pending_count_sums_nested_pending_records() {
        let items = vec![
            approve_and_pay_response_for_count_test("pending", 2),
            approve_and_pay_response_for_count_test("pending", 1),
            approve_and_pay_response_for_count_test("completed", 0),
        ];

        assert_eq!(aggregate_pending_count(&items), 3);
        let response = BatchApproveAndPayResponse {
            total_requests: 3,
            succeeded_requests: 3,
            failed_requests: 0,
            paid_count: 0,
            already_paid_count: 0,
            pending_count: aggregate_pending_count(&items),
            failed_count: 0,
            items,
            errors: Vec::new(),
        };
        let json = serde_json::to_value(response).unwrap();
        assert_eq!(json["pendingCount"], 3);
        assert!(json.get("pending_count").is_none());
    }

    #[test]
    fn approve_and_pay_message_reports_actual_settlement_outcomes() {
        let mut response = approve_and_pay_response_for_count_test("approved", 2);
        assert_eq!(
            approve_and_pay_message(&response),
            "返利申请已提交：2 条等待结算"
        );

        response.pending_count = 0;
        response.paid_count = 2;
        assert_eq!(
            approve_and_pay_message(&response),
            "返利申请处理完成：2 条已结算"
        );

        response.paid_count = 0;
        response.already_paid_count = 2;
        assert_eq!(
            approve_and_pay_message(&response),
            "返利申请处理完成：2 条无需重复结算"
        );

        response.pending_count = 1;
        response.already_paid_count = 0;
        response.failed_count = 1;
        assert_eq!(
            approve_and_pay_message(&response),
            "返利申请已提交：1 条等待结算，1 条记录处理失败"
        );
    }

    #[test]
    fn batch_message_reports_record_and_request_failures() {
        let response = BatchApproveAndPayResponse {
            total_requests: 3,
            succeeded_requests: 2,
            failed_requests: 1,
            paid_count: 1,
            already_paid_count: 0,
            pending_count: 2,
            failed_count: 1,
            items: Vec::new(),
            errors: Vec::new(),
        };

        assert_eq!(
            batch_approve_and_pay_message(&response),
            "批量返利申请已提交：2 条等待结算，1 条已结算，1 条记录处理失败，1 个申请处理失败"
        );
    }

    fn approve_and_pay_response_for_count_test(
        status: &str,
        pending_count: usize,
    ) -> ApproveAndPayResponse {
        ApproveAndPayResponse {
            request_id: 1,
            status: status.to_string(),
            total_amount: 0,
            paid_count: 0,
            already_paid_count: 0,
            pending_count,
            failed_count: 0,
            items: Vec::new(),
        }
    }

    #[test]
    fn derives_payout_request_id_within_limit() {
        let base = "x".repeat(200);
        let request_id = derive_payout_request_id(&base, 123, 456);

        assert!(request_id.len() <= MAX_PAYOUT_REQUEST_ID_LEN);
        assert!(request_id.ends_with(":req123:rec456"));
    }

    #[test]
    fn derives_payout_request_id_on_utf8_boundary() {
        let base = "批量".repeat(80);
        let request_id = derive_payout_request_id(&base, 7, 9);

        assert!(request_id.len() <= MAX_PAYOUT_REQUEST_ID_LEN);
        assert!(request_id.ends_with(":req7:rec9"));
        assert!(std::str::from_utf8(request_id.as_bytes()).is_ok());
    }

    #[test]
    fn resolves_split_signup_reward_amounts_with_legacy_fallback() {
        let legacy = test_system_config(300, 0, 0);
        assert_eq!(
            resolve_signup_reward_amounts_for_update(&legacy),
            (300, 300)
        );

        let split = test_system_config(300, 500, 100);
        assert_eq!(resolve_signup_reward_amounts_for_update(&split), (500, 100));

        let disabled = test_system_config(0, 0, 0);
        assert_eq!(resolve_signup_reward_amounts_for_update(&disabled), (0, 0));
    }

    fn test_system_config(
        legacy_amount: i64,
        inviter_amount: i64,
        invitee_amount: i64,
    ) -> SystemConfig {
        SystemConfig {
            min_rebate_request_amount: 0,
            rebate_request_frequency_days: 0,
            user_invitation_rebate_enabled: true,
            order_rebate_enabled: true,
            invitation_signup_reward_enabled: true,
            invitation_signup_reward_amount: legacy_amount,
            invitation_signup_inviter_reward_amount: inviter_amount,
            invitation_signup_invitee_reward_amount: invitee_amount,
            invitation_signup_reward_review_required: true,
            invitation_signup_inviter_reward_requires_paid_order: false,
            invitation_signup_invitee_reward_requires_paid_order: false,
            rebate_to_balance_enabled: true,
        }
    }
}
