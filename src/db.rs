use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Executor;

const READONLY_CONNECTION_SETUP_SQL: &str = "SET default_transaction_read_only = on";

use crate::system_config::{
    INVITATION_SIGNUP_INVITEE_REWARD_AMOUNT_KEY,
    INVITATION_SIGNUP_INVITEE_REWARD_REQUIRES_PAID_ORDER_KEY,
    INVITATION_SIGNUP_INVITER_REWARD_AMOUNT_KEY,
    INVITATION_SIGNUP_INVITER_REWARD_REQUIRES_PAID_ORDER_KEY, INVITATION_SIGNUP_REWARD_AMOUNT_KEY,
    INVITATION_SIGNUP_REWARD_ENABLED_KEY, INVITATION_SIGNUP_REWARD_REVIEW_REQUIRED_KEY,
    ORDER_REBATE_ENABLED_KEY, REBATE_TO_BALANCE_ENABLED_KEY, USER_INVITATION_REBATE_ENABLED_KEY,
};

pub async fn init_pool(database_url: &str) -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(database_url)
        .await
        .expect("failed to connect to database");

    tracing::info!("Invitations database pool connected");
    migrate(&pool).await;
    pool
}

pub async fn init_readonly_pool(database_url: &str) -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                conn.execute(READONLY_CONNECTION_SETUP_SQL).await?;
                Ok(())
            })
        })
        .connect(database_url)
        .await
        .expect("failed to connect to new-api database");

    tracing::info!("new-api readonly database pool connected");
    pool
}

pub async fn init_external_pool(database_url: &str) -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await
        .expect("failed to connect to external database");

    tracing::info!("external writable database pool connected");
    pool
}

async fn migrate(pool: &PgPool) {
    tracing::info!("Starting invitations database migration");

    // 返利规则表
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS rebate_rules (
            id BIGSERIAL PRIMARY KEY,
            user_group VARCHAR(64) NOT NULL,
            rule_type VARCHAR(20) NOT NULL,
            rebate_rate DECIMAL(5,4) NOT NULL,
            effective_time BIGINT NOT NULL,
            created_at BIGINT NOT NULL,
            updated_at BIGINT NOT NULL,
            UNIQUE(user_group, rule_type)
        )
        "#,
    )
    .execute(pool)
    .await
    .expect("failed to create rebate_rules table");

    sqlx::query("CREATE INDEX IF NOT EXISTS idx_rebate_rules_group ON rebate_rules(user_group)")
        .execute(pool)
        .await
        .ok();

    // 返利记录表
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS rebate_records (
            id BIGSERIAL PRIMARY KEY,
            ulid VARCHAR(26),
            inviter_id BIGINT NOT NULL,
            invitee_id BIGINT NOT NULL,
            user_group VARCHAR(64) NOT NULL,
            order_type VARCHAR(20) NOT NULL,
            order_id BIGINT NOT NULL,
            order_amount DECIMAL(10,2) NOT NULL,
            order_time BIGINT NOT NULL,
            rebate_effective_at BIGINT NOT NULL,
            rebate_amount BIGINT NOT NULL DEFAULT 0,
            rebate_rate DECIMAL(5,4),
            status VARCHAR(20) NOT NULL,
            scan_started_at BIGINT NOT NULL,
            initialization_ends_at BIGINT NOT NULL,
            finalized_at BIGINT,
            admin_adjusted BOOLEAN NOT NULL DEFAULT FALSE,
            closed_at BIGINT,
            unlock_required BOOLEAN NOT NULL DEFAULT FALSE,
            unlocked_at BIGINT,
            created_at BIGINT NOT NULL,
            updated_at BIGINT NOT NULL,
            UNIQUE(order_type, order_id)
        )
        "#,
    )
    .execute(pool)
    .await
    .expect("failed to create rebate_records table");

    sqlx::query(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_rebate_records_ulid ON rebate_records(ulid)",
    )
    .execute(pool)
    .await
    .ok();

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_rebate_records_inviter ON rebate_records(inviter_id)",
    )
    .execute(pool)
    .await
    .ok();

    sqlx::query("CREATE INDEX IF NOT EXISTS idx_rebate_records_status ON rebate_records(status)")
        .execute(pool)
        .await
        .ok();

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_rebate_records_initialization ON rebate_records(status, initialization_ends_at)",
    )
    .execute(pool)
    .await
    .ok();

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_rebate_records_effective ON rebate_records(inviter_id, status, rebate_effective_at)",
    )
    .execute(pool)
    .await
    .ok();

    // 添加复合索引以优化常见查询
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_rebate_records_inviter_status ON rebate_records(inviter_id, status)")
        .execute(pool)
        .await
        .ok();

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_rebate_records_signup_unlock ON rebate_records(order_type, unlock_required, unlocked_at, invitee_id)",
    )
    .execute(pool)
    .await
    .ok();

    // 邀请注册关系表
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS invitation_registrations (
            id BIGSERIAL PRIMARY KEY,
            inviter_id BIGINT NOT NULL,
            invitee_id BIGINT NOT NULL UNIQUE,
            user_group VARCHAR(64) NOT NULL,
            invited_at BIGINT NOT NULL,
            created_at BIGINT NOT NULL,
            updated_at BIGINT NOT NULL
        )
        "#,
    )
    .execute(pool)
    .await
    .expect("failed to create invitation_registrations table");

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_invitation_registrations_inviter ON invitation_registrations(inviter_id)",
    )
    .execute(pool)
    .await
    .ok();

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_invitation_registrations_invited_at ON invitation_registrations(invited_at DESC, id DESC)",
    )
    .execute(pool)
    .await
    .ok();

    // 返利申请表
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS rebate_requests (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            rebate_record_ids BIGINT[] NOT NULL,
            total_amount BIGINT NOT NULL,
            status VARCHAR(20) NOT NULL,
            admin_id BIGINT,
            admin_note TEXT,
            created_at BIGINT NOT NULL,
            updated_at BIGINT NOT NULL,
            approved_at BIGINT,
            rejected_at BIGINT,
            completed_at BIGINT
        )
        "#,
    )
    .execute(pool)
    .await
    .expect("failed to create rebate_requests table");

    sqlx::query("CREATE INDEX IF NOT EXISTS idx_rebate_requests_user ON rebate_requests(user_id)")
        .execute(pool)
        .await
        .ok();

    sqlx::query("CREATE INDEX IF NOT EXISTS idx_rebate_requests_status ON rebate_requests(status)")
        .execute(pool)
        .await
        .ok();

    // 返利余额发放流水表
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS invitation_rebate_payouts (
            id BIGSERIAL PRIMARY KEY,
            rebate_record_id BIGINT NOT NULL,
            rebate_request_id BIGINT,
            inviter_user_id BIGINT NOT NULL,
            invitee_user_id BIGINT NOT NULL,
            order_id BIGINT NOT NULL,
            order_type VARCHAR(64) NOT NULL,
            amount_cents BIGINT NOT NULL CHECK (amount_cents >= 0),
            quota_delta BIGINT NOT NULL CHECK (quota_delta <> 0),
            kind VARCHAR(32) NOT NULL CHECK (kind IN ('payout', 'reversal')),
            status VARCHAR(32) NOT NULL CHECK (status IN ('pending', 'newapi_applied', 'completed', 'reversed', 'failed', 'reversal_pending')),
            request_id VARCHAR(128) NOT NULL,
            admin_user_id BIGINT,
            admin_username TEXT,
            newapi_mark_id BIGINT,
            newapi_log_id BIGINT,
            original_payout_id BIGINT REFERENCES invitation_rebate_payouts(id),
            error_message TEXT,
            created_at BIGINT NOT NULL,
            updated_at BIGINT NOT NULL,
            completed_at BIGINT,
            reversed_at BIGINT
        )
        "#,
    )
    .execute(pool)
    .await
    .expect("failed to create invitation_rebate_payouts table");

    sqlx::query(
        r#"
        CREATE UNIQUE INDEX IF NOT EXISTS uq_invitation_rebate_payouts_record_pay
        ON invitation_rebate_payouts(rebate_record_id)
        WHERE kind = 'payout'
        "#,
    )
    .execute(pool)
    .await
    .ok();

    sqlx::query(
        r#"
        CREATE UNIQUE INDEX IF NOT EXISTS uq_invitation_rebate_payouts_original_reverse
        ON invitation_rebate_payouts(original_payout_id)
        WHERE kind = 'reversal'
        "#,
    )
    .execute(pool)
    .await
    .ok();

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_invitation_rebate_payouts_status ON invitation_rebate_payouts(status, updated_at)",
    )
    .execute(pool)
    .await
    .ok();

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS invitation_rebate_payout_attempts (
            id BIGSERIAL PRIMARY KEY,
            rebate_record_id BIGINT,
            action VARCHAR(32) NOT NULL,
            request_id VARCHAR(128) NOT NULL,
            admin_user_id BIGINT,
            admin_username TEXT,
            status VARCHAR(64) NOT NULL,
            http_status INTEGER NOT NULL,
            error_message TEXT,
            created_at BIGINT NOT NULL,
            completed_at BIGINT
        )
        "#,
    )
    .execute(pool)
    .await
    .expect("failed to create invitation_rebate_payout_attempts table");

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_invitation_rebate_payout_attempts_record ON invitation_rebate_payout_attempts(rebate_record_id, created_at DESC)",
    )
    .execute(pool)
    .await
    .ok();

    // 同步日志表
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS sync_log (
            id BIGSERIAL PRIMARY KEY,
            sync_type VARCHAR(64) NOT NULL,
            last_sync_id BIGINT NOT NULL,
            last_sync_time BIGINT NOT NULL,
            created_at BIGINT NOT NULL
        )
        "#,
    )
    .execute(pool)
    .await
    .expect("failed to create sync_log table");

    sqlx::query("ALTER TABLE sync_log ALTER COLUMN sync_type TYPE VARCHAR(64)")
        .execute(pool)
        .await
        .ok();

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_sync_log_type_id ON sync_log(sync_type, last_sync_id)",
    )
    .execute(pool)
    .await
    .ok();

    // 系统配置表
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS system_config (
            key VARCHAR(64) PRIMARY KEY,
            value TEXT NOT NULL,
            description TEXT,
            updated_at BIGINT NOT NULL
        )
        "#,
    )
    .execute(pool)
    .await
    .expect("failed to create system_config table");

    // 插入默认配置
    sqlx::query(
        r#"
        INSERT INTO system_config (key, value, description, updated_at)
        VALUES
            ('min_rebate_request_amount', '1000000', '最低返利申请金额（new-api 额度单位）', EXTRACT(EPOCH FROM NOW())::BIGINT),
            ('rebate_request_frequency_days', '7', '返利申请频率限制（天数）', EXTRACT(EPOCH FROM NOW())::BIGINT),
            ($1, 'true', '用户侧邀请返利界面开关', EXTRACT(EPOCH FROM NOW())::BIGINT),
            ($2, 'true', '订单返利开关', EXTRACT(EPOCH FROM NOW())::BIGINT),
            ($3, 'false', '邀请注册奖励开关', EXTRACT(EPOCH FROM NOW())::BIGINT),
            ($4, '1000000', '邀请注册奖励金额（new-api 额度单位）', EXTRACT(EPOCH FROM NOW())::BIGINT),
            ($5, '1000000', '邀请人注册奖励金额（new-api 额度单位）', EXTRACT(EPOCH FROM NOW())::BIGINT),
            ($6, '1000000', '被邀请人注册奖励金额（new-api 额度单位）', EXTRACT(EPOCH FROM NOW())::BIGINT),
            ($7, 'true', '邀请注册奖励是否需要审核', EXTRACT(EPOCH FROM NOW())::BIGINT),
            ($8, 'false', '邀请人注册奖励是否等待被邀请人首次充值/订阅', EXTRACT(EPOCH FROM NOW())::BIGINT),
            ($9, 'false', '被邀请人注册奖励是否等待首次充值/订阅', EXTRACT(EPOCH FROM NOW())::BIGINT),
            ($10, 'true', '返利到余额开关', EXTRACT(EPOCH FROM NOW())::BIGINT)
        ON CONFLICT (key) DO NOTHING
        "#,
    )
    .bind(USER_INVITATION_REBATE_ENABLED_KEY)
    .bind(ORDER_REBATE_ENABLED_KEY)
    .bind(INVITATION_SIGNUP_REWARD_ENABLED_KEY)
    .bind(INVITATION_SIGNUP_REWARD_AMOUNT_KEY)
    .bind(INVITATION_SIGNUP_INVITER_REWARD_AMOUNT_KEY)
    .bind(INVITATION_SIGNUP_INVITEE_REWARD_AMOUNT_KEY)
    .bind(INVITATION_SIGNUP_REWARD_REVIEW_REQUIRED_KEY)
    .bind(INVITATION_SIGNUP_INVITER_REWARD_REQUIRES_PAID_ORDER_KEY)
    .bind(INVITATION_SIGNUP_INVITEE_REWARD_REQUIRES_PAID_ORDER_KEY)
    .bind(REBATE_TO_BALANCE_ENABLED_KEY)
    .execute(pool)
    .await
    .ok();

    tracing::info!("Invitations database migration completed");
}

pub async fn migrate_newapi_payout_tables(pool: &PgPool) {
    tracing::info!("Starting new-api payout database migration");

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS kkai_invitation_rebate_balance_marks (
            id BIGSERIAL PRIMARY KEY,
            kind VARCHAR(32) NOT NULL CHECK (kind IN ('payout', 'reversal')),
            rebate_record_id BIGINT NOT NULL,
            payout_id BIGINT NOT NULL,
            original_payout_id BIGINT,
            user_id BIGINT NOT NULL,
            quota_delta BIGINT NOT NULL CHECK (quota_delta <> 0),
            amount_cents BIGINT NOT NULL CHECK (amount_cents >= 0),
            log_id BIGINT,
            request_id VARCHAR(128) NOT NULL,
            created_at BIGINT NOT NULL
        )
        "#,
    )
    .execute(pool)
    .await
    .expect("failed to create kkai_invitation_rebate_balance_marks table");

    sqlx::query(
        r#"
        CREATE UNIQUE INDEX IF NOT EXISTS uq_kkai_invitation_rebate_balance_marks_record_pay
        ON kkai_invitation_rebate_balance_marks(rebate_record_id)
        WHERE kind = 'payout'
        "#,
    )
    .execute(pool)
    .await
    .ok();

    sqlx::query(
        r#"
        CREATE UNIQUE INDEX IF NOT EXISTS uq_kkai_invitation_rebate_balance_marks_original_reverse
        ON kkai_invitation_rebate_balance_marks(original_payout_id)
        WHERE kind = 'reversal'
        "#,
    )
    .execute(pool)
    .await
    .ok();

    sqlx::query(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_kkai_invitation_rebate_balance_marks_request ON kkai_invitation_rebate_balance_marks(request_id)",
    )
    .execute(pool)
    .await
    .ok();

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_kkai_invitation_rebate_balance_marks_user ON kkai_invitation_rebate_balance_marks(user_id, created_at DESC)",
    )
    .execute(pool)
    .await
    .ok();

    tracing::info!("new-api payout database migration completed");
}
