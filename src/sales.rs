use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use axum::Json;
use chrono::{TimeZone, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};

use crate::error::{AppError, Result};
use crate::middleware::AdminUser;
use crate::response::{message, ok, ApiResponse, PaginatedResponse};
use crate::routes::user::PaginationParams;
use crate::session::SessionUser;
use crate::AppState;

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SalesProfile {
    pub active: bool,
    pub registered: bool,
}

pub async fn profile(pool: &PgPool, user_id: i64) -> Result<SalesProfile> {
    let active: Option<bool> =
        sqlx::query_scalar("SELECT active FROM sales_accounts WHERE user_id = $1")
            .bind(user_id)
            .fetch_optional(pool)
            .await?;
    Ok(SalesProfile {
        active: active.unwrap_or(false),
        registered: active.is_some(),
    })
}

pub async fn my_profile(
    State(state): State<AppState>,
    user: SessionUser,
) -> Result<Json<ApiResponse<SalesProfile>>> {
    Ok(ok(profile(&state.pool, user.id).await?))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SalesAccount {
    pub user_id: i64,
    pub user_name: String,
    pub active: bool,
    pub note: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateSalesAccount {
    pub active: bool,
    #[serde(default)]
    pub note: String,
}

pub async fn list_accounts(
    State(state): State<AppState>,
    _admin: AdminUser,
    Query(params): Query<PaginationParams>,
) -> Result<Json<ApiResponse<PaginatedResponse<SalesAccount>>>> {
    let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sales_accounts")
        .fetch_one(&state.pool)
        .await?;
    let rows = sqlx::query("SELECT user_id, active, note, created_at, updated_at FROM sales_accounts ORDER BY updated_at DESC, user_id DESC LIMIT $1 OFFSET $2")
        .bind(params.page_size()).bind(params.offset()).fetch_all(&state.pool).await?;
    let ids = rows
        .iter()
        .map(|row| row.try_get::<i64, _>("user_id"))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let names: HashMap<i64, String> = sqlx::query("SELECT id, COALESCE(NULLIF(display_name, ''), username) AS name FROM users WHERE id = ANY($1)")
        .bind(&ids).fetch_all(&state.newapi_pool).await?.iter()
        .map(|row| Ok((row.try_get("id")?, row.try_get("name")?)))
        .collect::<std::result::Result<_, sqlx::Error>>()?;
    let items = rows
        .iter()
        .map(|row| {
            let user_id = row.try_get("user_id")?;
            Ok(SalesAccount {
                user_id,
                user_name: names.get(&user_id).cloned().unwrap_or_default(),
                active: row.try_get("active")?,
                note: row.try_get("note")?,
                created_at: timestamp(row.try_get("created_at")?),
                updated_at: timestamp(row.try_get("updated_at")?),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ok(PaginatedResponse {
        items,
        total,
        page: params.page(),
        page_size: params.page_size(),
    }))
}

pub async fn update_account(
    State(state): State<AppState>,
    admin: AdminUser,
    Path(user_id): Path<i64>,
    Json(input): Json<UpdateSalesAccount>,
) -> Result<Json<ApiResponse<SalesProfile>>> {
    if user_id <= 0 || input.note.chars().count() > 500 {
        return Err(AppError::BadRequest("销售账号或备注无效".into()));
    }
    let user_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM users WHERE id = $1 AND deleted_at IS NULL)",
    )
    .bind(user_id)
    .fetch_one(&state.newapi_pool)
    .await?;
    if !user_exists {
        return Err(AppError::NotFound("用户不存在".into()));
    }
    let now = Utc::now().timestamp();
    let mut tx = state.pool.begin().await?;
    sqlx::query("INSERT INTO sales_accounts(user_id, active, note, created_at, updated_at, updated_by) VALUES ($1,$2,$3,$4,$4,$5) ON CONFLICT(user_id) DO UPDATE SET active=EXCLUDED.active,note=EXCLUDED.note,updated_at=EXCLUDED.updated_at,updated_by=EXCLUDED.updated_by")
        .bind(user_id).bind(input.active).bind(input.note.trim()).bind(now).bind(admin.0.id).execute(&mut *tx).await?;
    sqlx::query("INSERT INTO sales_account_events(user_id,active,note,admin_id,created_at) VALUES($1,$2,$3,$4,$5)")
        .bind(user_id).bind(input.active).bind(input.note.trim()).bind(admin.0.id).bind(now).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(message(
        SalesProfile {
            active: input.active,
            registered: true,
        },
        "销售身份已更新",
    ))
}

#[derive(Deserialize)]
pub struct ConfirmIssued {
    pub note: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IssuedConfirmation {
    pub id: i64,
    pub request_kind: &'static str,
    pub issued_at: String,
    pub issue_note: String,
}

pub async fn confirm_issued(
    State(state): State<AppState>,
    admin: AdminUser,
    Path(id): Path<i64>,
    Json(input): Json<ConfirmIssued>,
) -> Result<Json<ApiResponse<IssuedConfirmation>>> {
    crate::payout::require_settlement_writes_enabled(state.config.settlement_mode)?;
    let note = input.note.trim();
    if id <= 0 || note.is_empty() || note.chars().count() > 500 {
        return Err(AppError::BadRequest(
            "请填写 1 至 500 字的发放记录说明".into(),
        ));
    }
    let mut tx = state.pool.begin().await?;
    let credit_currency = if crate::credit_currency::boundary(&mut *tx).await?.is_some() {
        crate::credit_currency::USD
    } else {
        crate::credit_currency::LEGACY
    };
    let request = sqlx::query("SELECT user_id, request_kind, status, total_amount, rebate_record_ids FROM rebate_requests WHERE id=$1 FOR UPDATE")
        .bind(id).fetch_optional(&mut *tx).await?.ok_or_else(|| AppError::NotFound("返利申请不存在".into()))?;
    if request.try_get::<String, _>("request_kind")? != "sales"
        || request.try_get::<String, _>("status")? != "approved"
    {
        return Err(AppError::Conflict(
            "只有已批准的销售申请可以确认发放".into(),
        ));
    }
    if let Some(existing) =
        sqlx::query("SELECT confirmed_at,note FROM sales_disbursements WHERE request_id=$1")
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?
    {
        return Ok(ok(IssuedConfirmation {
            id,
            request_kind: "sales",
            issued_at: timestamp(existing.try_get("confirmed_at")?),
            issue_note: existing.try_get("note")?,
        }));
    }
    let ids: Vec<i64> = request.try_get("rebate_record_ids")?;
    sqlx::query("SELECT id FROM rebate_records WHERE id=ANY($1) ORDER BY id FOR UPDATE")
        .bind(&ids)
        .fetch_all(&mut *tx)
        .await?;
    let amount: i64 = request.try_get("total_amount")?;
    let conflict: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM invitation_rebate_payouts WHERE rebate_record_id=ANY($1)) OR EXISTS(SELECT 1 FROM balance_operations WHERE rebate_record_id=ANY($1))")
        .bind(&ids).fetch_one(&mut *tx).await?;
    if conflict || amount <= 0 {
        return Err(AppError::Conflict(
            "返利已有余额发放记录或金额无效，不能确认外部发放".into(),
        ));
    }
    let now = Utc::now().timestamp();
    sqlx::query("INSERT INTO sales_disbursements(request_id,user_id,amount_quota,confirmed_by,confirmed_at,note,credit_currency,record_amounts) VALUES($1,$2,$3,$4,$5,$6,$7,(SELECT jsonb_object_agg(rebate_record_id::TEXT,amount_quota_snapshot) FROM rebate_request_items WHERE request_id=$1 AND membership_state='active'))")
        .bind(id).bind(request.try_get::<i64, _>("user_id")?).bind(amount).bind(admin.0.id).bind(now).bind(note).bind(credit_currency).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(message(
        IssuedConfirmation {
            id,
            request_kind: "sales",
            issued_at: timestamp(now),
            issue_note: note.into(),
        },
        "销售返利已确认发放",
    ))
}

fn timestamp(value: i64) -> String {
    Utc.timestamp_opt(value, 0)
        .single()
        .map(|date| date.to_rfc3339())
        .unwrap_or_default()
}
