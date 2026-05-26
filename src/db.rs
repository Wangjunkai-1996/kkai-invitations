use sqlx::postgres::{PgPool, PgPoolOptions};

pub async fn init_pool(database_url: &str) -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(database_url)
        .await
        .expect("failed to connect to database");

    migrate(&pool).await;
    pool
}

pub async fn init_readonly_pool(database_url: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await
        .expect("failed to connect to new-api database")
}

async fn migrate(pool: &PgPool) {
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
            inviter_id BIGINT NOT NULL,
            invitee_id BIGINT NOT NULL,
            order_type VARCHAR(20) NOT NULL,
            order_id BIGINT NOT NULL,
            order_amount DECIMAL(10,2) NOT NULL,
            rebate_amount BIGINT NOT NULL,
            rebate_rate DECIMAL(5,4) NOT NULL,
            status VARCHAR(20) NOT NULL,
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
        "CREATE INDEX IF NOT EXISTS idx_rebate_records_inviter ON rebate_records(inviter_id)",
    )
    .execute(pool)
    .await
    .ok();

    sqlx::query("CREATE INDEX IF NOT EXISTS idx_rebate_records_status ON rebate_records(status)")
        .execute(pool)
        .await
        .ok();

    // 添加复合索引以优化常见查询
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_rebate_records_inviter_status ON rebate_records(inviter_id, status)")
        .execute(pool)
        .await
        .ok();

    // 提现申请表
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS withdrawal_requests (
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
            completed_at BIGINT
        )
        "#,
    )
    .execute(pool)
    .await
    .expect("failed to create withdrawal_requests table");

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_withdrawal_requests_user ON withdrawal_requests(user_id)",
    )
    .execute(pool)
    .await
    .ok();

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_withdrawal_requests_status ON withdrawal_requests(status)",
    )
    .execute(pool)
    .await
    .ok();

    // 同步日志表
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS sync_log (
            id BIGSERIAL PRIMARY KEY,
            sync_type VARCHAR(20) NOT NULL,
            last_sync_id BIGINT NOT NULL,
            last_sync_time BIGINT NOT NULL,
            created_at BIGINT NOT NULL
        )
        "#,
    )
    .execute(pool)
    .await
    .expect("failed to create sync_log table");

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
            ('min_withdrawal_amount', '1000000', '最低提现金额（new-api 额度单位）', EXTRACT(EPOCH FROM NOW())::BIGINT),
            ('withdrawal_frequency_days', '7', '提现频率限制（天数）', EXTRACT(EPOCH FROM NOW())::BIGINT)
        ON CONFLICT (key) DO NOTHING
        "#,
    )
    .execute(pool)
    .await
    .ok();
}
