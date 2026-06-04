use std::collections::{HashMap, HashSet};

use axum::extract::{Path, Query, State};
use axum::Json;
use chrono::{TimeZone, Utc};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgRow;
use sqlx::Row;

use crate::error::{AppError, Result};
use crate::invitation_signup::{
    insert_invitation_signup_reward_record, upsert_invitation_registration,
    InsertInvitationSignupReward, RewardInsertOutcome, UpsertInvitationRegistration,
};
use crate::middleware::AdminUser;
use crate::rebate_lifecycle::{
    finalize_initializing_records_by_ids, rebate_amount_for_yuan, ALL_USER_GROUP,
    INITIALIZATION_SECONDS, REBATE_EFFECTIVE_DELAY_SECONDS,
};
use crate::rebate_types::{
    INVITATION_SIGNUP_INVITEE_ORDER_TYPE, INVITATION_SIGNUP_INVITER_ORDER_TYPE,
};
use crate::response::{message, ok, ApiResponse, PaginatedResponse};
use crate::routes::user::{
    cents_to_quota, count_rebate_records, fetch_rebate_records, quota_to_cents, PaginationParams,
    RebateRecord,
};
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

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RebateRequestListParams {
    pub status: Option<String>,
    pub page: Option<i64>,
    pub page_size: Option<i64>,
}

impl RebateRequestListParams {
    fn page(&self) -> i64 {
        self.page.unwrap_or(1).max(1)
    }

    fn page_size(&self) -> i64 {
        self.page_size.unwrap_or(100).clamp(1, 1000)
    }

    fn offset(&self) -> i64 {
        (self.page() - 1) * self.page_size()
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
        (self.page() - 1) * self.page_size()
    }

    fn status_filter(&self) -> Option<&str> {
        match self.status.as_deref() {
            Some("pending" | "requested" | "approved" | "completed" | "rejected") => {
                self.status.as_deref()
            }
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
        (self.page() - 1) * self.page_size()
    }

    fn order_type_filter(&self) -> Option<&str> {
        match self.order_type.as_deref() {
            Some("topup" | "subscription") => self.order_type.as_deref(),
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
        (self.page() - 1) * self.page_size()
    }
}

struct LocalRebateRecord {
    id: i64,
    order_type: String,
    order_id: i64,
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
    order_amount_yuan: f64,
    order_time: i64,
}

struct NewApiRebateOrderDiagnostics {
    invited_users: i64,
    topup_success: i64,
    topup_invited: i64,
    topup_rebate_orders: i64,
    subscription_success: i64,
    subscription_invited: i64,
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
        RETURNING id, user_group, rule_type, rebate_rate::TEXT as rebate_rate,
                  effective_time, created_at, updated_at
        "#,
    )
    .bind(&validated.user_group)
    .bind(&validated.rule_type)
    .bind(validated.rebate_rate)
    .bind(now)
    .fetch_one(&state.pool)
    .await?;

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
    let now = Utc::now().timestamp();

    let row = sqlx::query(
        r#"
        UPDATE rebate_rules
        SET user_group = $1, rule_type = $2, rebate_rate = $3, effective_time = $4, updated_at = $4
        WHERE id = $5
        RETURNING id, user_group, rule_type, rebate_rate::TEXT as rebate_rate,
                  effective_time, created_at, updated_at
        "#,
    )
    .bind(&validated.user_group)
    .bind(&validated.rule_type)
    .bind(validated.rebate_rate)
    .bind(now)
    .bind(id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or_else(|| AppError::NotFound("返利规则不存在".to_string()))?;

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

    let rows = sqlx::query("DELETE FROM rebate_rules WHERE id = $1")
        .bind(id)
        .execute(&state.pool)
        .await?
        .rows_affected();

    if rows == 0 {
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
        &cents_to_quota(config.min_rebate_request_amount).to_string(),
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
        &cents_to_quota(legacy_reward_amount).to_string(),
        "邀请注册奖励金额兼容值（人民币分，API 层；数据库存 new-api 额度单位）",
        now,
    )
    .await?;
    upsert_config_value(
        &state.pool,
        INVITATION_SIGNUP_INVITER_REWARD_AMOUNT_KEY,
        &cents_to_quota(inviter_reward_amount).to_string(),
        "邀请人注册奖励金额（人民币分，API 层；数据库存 new-api 额度单位）",
        now,
    )
    .await?;
    upsert_config_value(
        &state.pool,
        INVITATION_SIGNUP_INVITEE_REWARD_AMOUNT_KEY,
        &cents_to_quota(invitee_reward_amount).to_string(),
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
    let total = count_rebate_requests(&state, params.status_filter()).await?;
    let rebate_request_rows =
        fetch_rebate_requests(&state, params.status_filter(), &params).await?;
    let user_map = fetch_user_map(&state, &rebate_request_rows).await?;

    let result = rebate_request_rows
        .iter()
        .map(|row| rebate_request_detail_from_row(row, &user_map))
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
pub async fn complete_rebate_request(
    State(state): State<AppState>,
    admin: AdminUser,
    Path(id): Path<i64>,
) -> Result<Json<ApiResponse<serde_json::Value>>> {
    validate_id(id)?;
    let now = Utc::now().timestamp();
    let mut tx = state.pool.begin().await?;

    let row = sqlx::query(
        "SELECT rebate_record_ids FROM rebate_requests WHERE id = $1 AND status = 'approved' FOR UPDATE",
    )
    .bind(id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| AppError::NotFound("申请不存在或状态不正确".to_string()))?;
    let rebate_record_ids: Vec<i64> = row.try_get("rebate_record_ids")?;

    sqlx::query(
        "UPDATE rebate_requests SET status = 'completed', completed_at = $1, updated_at = $1 WHERE id = $2",
    )
    .bind(now)
    .bind(id)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        "UPDATE rebate_records SET status = 'completed', updated_at = $1 WHERE id = ANY($2) AND status = 'approved'",
    )
    .bind(now)
    .bind(&rebate_record_ids)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    tracing::info!(
        admin_id = admin.0.id,
        request_id = id,
        "Admin marked rebate request as completed"
    );

    Ok(message(serde_json::json!({ "id": id }), "返利处理已完成"))
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
            COALESCE(SUM(CASE WHEN status = 'completed' THEN rebate_amount ELSE 0 END), 0)::BIGINT as completed,
            COALESCE(SUM(CASE WHEN status = 'pending' THEN rebate_amount ELSE 0 END), 0)::BIGINT as pending,
            COALESCE(SUM(CASE WHEN status = 'requested' THEN rebate_amount ELSE 0 END), 0)::BIGINT as requested,
            COALESCE(SUM(CASE WHEN status = 'approved' THEN rebate_amount ELSE 0 END), 0)::BIGINT as approved
        FROM rebate_records
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
    let records = fetch_rebate_records(
        &state.pool,
        None,
        params.status_filter(),
        true,
        None,
        params.page_size(),
        params.offset(),
    )
    .await?;
    let total = count_rebate_records(&state.pool, None, params.status_filter(), true, None).await?;

    Ok(ok(PaginatedResponse {
        items: records,
        total,
        page: params.page(),
        page_size: params.page_size(),
    }))
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
    let order_rebate_enabled = get_config_bool(&state.pool, ORDER_REBATE_ENABLED_KEY).await?;
    let hydrated = if order_rebate_enabled {
        hydrate_newapi_rebate_orders(&state, &orders, now).await?
    } else {
        tracing::info!(
            admin_id = admin.0.id,
            requested_count = orders.len(),
            "Admin order rebate hydration skipped: disabled"
        );
        0
    };
    tracing::info!(
        admin_id = admin.0.id,
        requested_count = orders.len(),
        hydrated_count = hydrated,
        "Admin rebate order list hydration finished"
    );

    let local_record_map = fetch_local_rebate_record_map(&state, &orders).await?;
    tracing::info!(
        admin_id = admin.0.id,
        local_record_count = local_record_map.len(),
        "Local rebate record map loaded for admin order page"
    );

    let records = orders
        .iter()
        .map(|order| {
            admin_rebate_order_record_from_parts(
                order,
                local_record_map.get(&(order.order_type.clone(), order.order_id)),
                now,
            )
        })
        .collect::<Vec<_>>();

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
        SELECT id, order_amount::TEXT AS order_amount, status
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

    let mut updated = 0;
    for row in &rows {
        let id: i64 = row.try_get("id")?;
        let order_amount_yuan = text_to_f64(&row.try_get::<String, _>("order_amount")?);
        let order_amount_cents = (order_amount_yuan * 100.0).round() as i64;
        let rebate_ratio = req
            .rebate_ratio
            .or_else(|| derive_ratio_from_amount(req.rebate_amount, order_amount_cents));
        if let Some(ratio) = rebate_ratio {
            validate_rebate_ratio(ratio)?;
        }
        let rebate_amount = req.rebate_amount.map(cents_to_quota).unwrap_or_else(|| {
            let ratio = rebate_ratio.unwrap_or_default();
            rebate_amount_for_yuan(order_amount_yuan, ratio)
        });

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
    validate_operation_rows(&rows, &record_ids, |status| status == "closed")?;

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

// GET /api/admin/user-rebates/:user_id
pub async fn get_user_rebates(
    State(state): State<AppState>,
    _admin: AdminUser,
    Path(user_id): Path<i64>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<ApiResponse<PaginatedResponse<RebateRecord>>>> {
    validate_id(user_id)?;

    let records = fetch_rebate_records(
        &state.pool,
        Some(user_id),
        None,
        false,
        None,
        params.page_size(),
        params.offset(),
    )
    .await?;
    let total = count_rebate_records(&state.pool, Some(user_id), None, false, None).await?;

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

    let outcome = insert_invitation_signup_reward_record(
        &state.pool,
        InsertInvitationSignupReward {
            order_type,
            reward_owner_id,
            invitee_id: registration.invitee_id,
            user_group: &registration.user_group,
            reward_amount,
            unlock_required,
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
        SELECT id, order_type, order_id, rebate_amount, status
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
        ) rebate_orders
        WHERE ($1::TEXT IS NULL OR order_type = $1)
        "#,
    )
    .bind(order_type)
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
                  AND u.inviter_id IS NOT NULL
                  AND u.inviter_id > 0
            )::BIGINT AS topup_invited,
            (
                SELECT COUNT(*)
                FROM top_ups t
                JOIN users u ON t.user_id = u.id
                WHERE t.status = 'success'
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
            )::BIGINT AS subscription_invited
        "#,
    )
    .fetch_one(&state.newapi_pool)
    .await?;

    Ok(NewApiRebateOrderDiagnostics {
        invited_users: row.try_get("invited_users")?,
        topup_success: row.try_get("topup_success")?,
        topup_invited: row.try_get("topup_invited")?,
        topup_rebate_orders: row.try_get("topup_rebate_orders")?,
        subscription_success: row.try_get("subscription_success")?,
        subscription_invited: row.try_get("subscription_invited")?,
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
            order_amount,
            order_time
        FROM (
            SELECT
                'topup'::TEXT AS order_type,
                t.id AS order_id,
                u.inviter_id AS inviter_id,
                COALESCE(NULLIF(inviter.display_name, ''), NULLIF(inviter.username, '')) AS inviter_name,
                t.user_id AS invitee_id,
                COALESCE(NULLIF(u.display_name, ''), NULLIF(u.username, '')) AS invitee_name,
                COALESCE(NULLIF(u."group", ''), 'default') AS user_group,
                t.money::TEXT AS order_amount,
                COALESCE(NULLIF(t.complete_time, 0), NULLIF(t.create_time, 0), 0) AS order_time
            FROM top_ups t
            JOIN users u ON t.user_id = u.id
            LEFT JOIN users inviter ON inviter.id = u.inviter_id
            WHERE t.status = 'success'
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
                COALESCE(NULLIF(u."group", ''), 'default') AS user_group,
                s.money::TEXT AS order_amount,
                COALESCE(NULLIF(s.complete_time, 0), NULLIF(s.create_time, 0), 0) AS order_time
            FROM subscription_orders s
            JOIN users u ON s.user_id = u.id
            LEFT JOIN users inviter ON inviter.id = u.inviter_id
            WHERE s.status = 'success'
              AND u.inviter_id IS NOT NULL
              AND u.inviter_id > 0
        ) rebate_orders
        WHERE ($1::TEXT IS NULL OR order_type = $1)
        ORDER BY order_time DESC, order_id DESC
        LIMIT $2 OFFSET $3
        "#,
    )
    .bind(order_type)
    .bind(page_size)
    .bind(offset)
    .fetch_all(&state.newapi_pool)
    .await?;

    rows.iter()
        .map(|row| {
            let order_amount_text: String = row.try_get("order_amount")?;
            let order_amount_yuan = text_to_f64(&order_amount_text);
            Ok(NewApiRebateOrder {
                order_type: row.try_get("order_type")?,
                order_id: row.try_get("order_id")?,
                inviter_id: row.try_get("inviter_id")?,
                inviter_name: row.try_get("inviter_name")?,
                invitee_id: row.try_get("invitee_id")?,
                invitee_name: row.try_get("invitee_name")?,
                user_group: row.try_get("user_group")?,
                order_amount: (order_amount_yuan * 100.0).round() as i64,
                order_amount_yuan,
                order_time: row.try_get("order_time")?,
            })
        })
        .collect::<Result<Vec<_>>>()
}

async fn hydrate_newapi_rebate_orders(
    state: &AppState,
    orders: &[NewApiRebateOrder],
    now: i64,
) -> Result<i64> {
    let mut hydrated = 0;

    for order in orders {
        let ulid = ulid::Ulid::new().to_string();
        let order_time = if order.order_time > 0 {
            order.order_time
        } else {
            now
        };
        let scan_started_at = now;
        let initialization_ends_at = scan_started_at + INITIALIZATION_SECONDS;
        let rebate_effective_at = order_time + REBATE_EFFECTIVE_DELAY_SECONDS;

        let rows_affected = sqlx::query(
            r#"
            INSERT INTO rebate_records
            (
                ulid, inviter_id, invitee_id, user_group, order_type, order_id,
                order_amount, order_time, rebate_effective_at, rebate_amount, rebate_rate,
                status, scan_started_at, initialization_ends_at, created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 0, NULL, 'initializing', $10, $11, $10, $10)
            ON CONFLICT (order_type, order_id) DO NOTHING
            "#,
        )
        .bind(&ulid)
        .bind(order.inviter_id)
        .bind(order.invitee_id)
        .bind(&order.user_group)
        .bind(&order.order_type)
        .bind(order.order_id)
        .bind(order.order_amount_yuan)
        .bind(order_time)
        .bind(rebate_effective_at)
        .bind(scan_started_at)
        .bind(initialization_ends_at)
        .execute(&state.pool)
        .await?
        .rows_affected();

        hydrated += rows_affected as i64;
    }

    Ok(hydrated)
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

    if topup_order_ids.is_empty() && subscription_order_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT
            id,
            order_type,
            order_id,
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
        WHERE (order_type = 'topup' AND order_id = ANY($1))
           OR (order_type = 'subscription' AND order_id = ANY($2))
        "#,
    )
    .bind(&topup_order_ids)
    .bind(&subscription_order_ids)
    .fetch_all(&state.pool)
    .await?;

    rows.iter()
        .map(|row| {
            let record = local_rebate_record_from_row(row)?;
            Ok(((record.order_type.clone(), record.order_id), record))
        })
        .collect()
}

fn local_rebate_record_from_row(row: &PgRow) -> Result<LocalRebateRecord> {
    let rebate_rate: Option<String> = row.try_get("rebate_rate")?;
    Ok(LocalRebateRecord {
        id: row.try_get("id")?,
        order_type: row.try_get("order_type")?,
        order_id: row.try_get("order_id")?,
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

fn admin_rebate_order_record_from_parts(
    order: &NewApiRebateOrder,
    local_record: Option<&LocalRebateRecord>,
    now: i64,
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
        .unwrap_or(order_time + REBATE_EFFECTIVE_DELAY_SECONDS);
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
            .map(|record| record.status == "closed")
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
        "requested" | "approved" | "completed" => "paid",
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
    if rule_type != "topup" && rule_type != "subscription" {
        return Err(AppError::BadRequest(
            "规则类型必须是 topup 或 subscription".to_string(),
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

async fn count_rebate_requests(state: &AppState, status: Option<&str>) -> Result<i64> {
    let total = if let Some(status) = status {
        sqlx::query_scalar("SELECT COUNT(*) FROM rebate_requests WHERE status = $1")
            .bind(status)
            .fetch_one(&state.pool)
            .await?
    } else {
        sqlx::query_scalar("SELECT COUNT(*) FROM rebate_requests")
            .fetch_one(&state.pool)
            .await?
    };
    Ok(total)
}

async fn fetch_rebate_requests(
    state: &AppState,
    status: Option<&str>,
    params: &RebateRequestListParams,
) -> Result<Vec<PgRow>> {
    let rows = if let Some(status) = status {
        sqlx::query(
            r#"
            SELECT id, user_id, rebate_record_ids, total_amount, status, created_at, updated_at,
                   approved_at, completed_at, rejected_at, admin_note
            FROM rebate_requests
            WHERE status = $1
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(status)
        .bind(params.page_size())
        .bind(params.offset())
        .fetch_all(&state.pool)
        .await?
    } else {
        sqlx::query(
            r#"
            SELECT id, user_id, rebate_record_ids, total_amount, status, created_at, updated_at,
                   approved_at, completed_at, rejected_at, admin_note
            FROM rebate_requests
            ORDER BY created_at DESC
            LIMIT $1 OFFSET $2
            "#,
        )
        .bind(params.page_size())
        .bind(params.offset())
        .fetch_all(&state.pool)
        .await?
    };
    Ok(rows)
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
) -> Result<RebateRequestDetail> {
    let user_id: i64 = row.try_get("user_id")?;
    let status: String = row.try_get("status")?;
    let admin_note: Option<String> = row.try_get("admin_note")?;

    Ok(RebateRequestDetail {
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
        SELECT id, status
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

async fn lock_rebate_request_for_review(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    id: i64,
) -> Result<PgRow> {
    sqlx::query(
        r#"
        SELECT id, status, rebate_record_ids
        FROM rebate_requests
        WHERE id = $1
        FOR UPDATE
        "#,
    )
    .bind(id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| AppError::NotFound("返利申请不存在".to_string()))
}

fn ensure_rebate_request_not_completed(row: &PgRow) -> Result<()> {
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

fn is_rebate_record_mutable(status: &str) -> bool {
    matches!(status, "initializing" | "pending" | "rejected")
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
    fn rejects_percentage_rebate_rate() {
        let req = RebateRuleRequest {
            user_group: "default".to_string(),
            rule_type: "topup".to_string(),
            rebate_rate: serde_json::json!("5"),
        };

        assert!(validate_rebate_rule_request(&req).is_err());
    }

    #[test]
    fn admin_rebate_record_params_accept_record_statuses() {
        let requested = AdminRebateRecordListParams {
            page: None,
            page_size: None,
            status: Some("requested".to_string()),
        };
        assert_eq!(requested.status_filter(), Some("requested"));

        let rejected = AdminRebateRecordListParams {
            page: None,
            page_size: None,
            status: Some("rejected".to_string()),
        };
        assert_eq!(rejected.status_filter(), Some("rejected"));
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
    fn derives_paid_status_for_rebate_request_flow() {
        assert_eq!(
            derive_admin_rebate_status(1_000, 2_000, false, "requested"),
            "paid"
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
