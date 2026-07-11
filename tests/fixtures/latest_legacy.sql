CREATE TABLE rebate_rules (
    id BIGSERIAL PRIMARY KEY,
    user_group VARCHAR(64) NOT NULL,
    rule_type VARCHAR(20) NOT NULL,
    rebate_rate DECIMAL(5,4) NOT NULL,
    effective_time BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    UNIQUE(user_group, rule_type)
);

CREATE TABLE rebate_records (
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
);

CREATE TABLE rebate_requests (
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
);

CREATE TABLE invitation_rebate_payouts (
    id BIGSERIAL PRIMARY KEY,
    rebate_record_id BIGINT NOT NULL,
    rebate_request_id BIGINT,
    inviter_user_id BIGINT NOT NULL,
    invitee_user_id BIGINT NOT NULL,
    order_id BIGINT NOT NULL,
    order_type VARCHAR(64) NOT NULL,
    amount_cents BIGINT NOT NULL,
    quota_delta BIGINT NOT NULL,
    kind VARCHAR(32) NOT NULL,
    status VARCHAR(32) NOT NULL,
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
);

CREATE TABLE sync_log (
    id BIGSERIAL PRIMARY KEY,
    sync_type VARCHAR(64) NOT NULL,
    last_sync_id BIGINT NOT NULL,
    last_sync_time BIGINT NOT NULL,
    created_at BIGINT NOT NULL
);

CREATE TABLE system_config (
    key VARCHAR(64) PRIMARY KEY,
    value TEXT NOT NULL,
    description TEXT,
    updated_at BIGINT NOT NULL
);

INSERT INTO rebate_records (
    id, ulid, inviter_id, invitee_id, user_group, order_type, order_id,
    order_amount, order_time, rebate_effective_at, rebate_amount, rebate_rate,
    status, scan_started_at, initialization_ends_at, created_at, updated_at
) VALUES (
    101, '01J00000000000000000000000', 11, 12, 'legacy-group', 'topup', 501,
    100.00, 1700000000, 1700000000, 5000000, 0.0500,
    'approved', 1700000000, 1700000000, 1700000000, 1700000000
);

INSERT INTO rebate_requests (
    id, user_id, rebate_record_ids, total_amount, status, created_at, updated_at, approved_at
) VALUES (
    201, 11, ARRAY[101]::BIGINT[], 5000000, 'approved', 1700000000, 1700000000, 1700000000
);

INSERT INTO sync_log (sync_type, last_sync_id, last_sync_time, created_at)
VALUES ('topup', 501, 1700000001, 1700000001);
