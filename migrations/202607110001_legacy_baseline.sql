-- Versioned adoption of the schemas created by the former startup DDL.
-- This migration is intentionally expand-only. Historical ambiguity is handled by preflight.

SET LOCAL lock_timeout = '15s';
SET LOCAL statement_timeout = '5min';

CREATE TABLE IF NOT EXISTS rebate_rules (
    id BIGSERIAL PRIMARY KEY,
    user_group VARCHAR(64) NOT NULL,
    rule_type VARCHAR(20) NOT NULL,
    rebate_rate DECIMAL(5,4) NOT NULL,
    effective_time BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);

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
    updated_at BIGINT NOT NULL
);

ALTER TABLE rebate_records ADD COLUMN IF NOT EXISTS ulid VARCHAR(26);
ALTER TABLE rebate_records ADD COLUMN IF NOT EXISTS user_group VARCHAR(64);
ALTER TABLE rebate_records ADD COLUMN IF NOT EXISTS order_time BIGINT;
ALTER TABLE rebate_records ADD COLUMN IF NOT EXISTS rebate_effective_at BIGINT;
ALTER TABLE rebate_records ADD COLUMN IF NOT EXISTS scan_started_at BIGINT;
ALTER TABLE rebate_records ADD COLUMN IF NOT EXISTS initialization_ends_at BIGINT;
ALTER TABLE rebate_records ADD COLUMN IF NOT EXISTS finalized_at BIGINT;
ALTER TABLE rebate_records ADD COLUMN IF NOT EXISTS admin_adjusted BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE rebate_records ADD COLUMN IF NOT EXISTS closed_at BIGINT;
ALTER TABLE rebate_records ADD COLUMN IF NOT EXISTS unlock_required BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE rebate_records ADD COLUMN IF NOT EXISTS unlocked_at BIGINT;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM rebate_records
        WHERE user_group IS NULL OR order_time IS NULL OR rebate_effective_at IS NULL
           OR scan_started_at IS NULL OR initialization_ends_at IS NULL
    ) THEN
        RAISE EXCEPTION 'rebate_records contains rows requiring manual legacy enrichment';
    END IF;
END
$$;

ALTER TABLE rebate_records ALTER COLUMN user_group SET NOT NULL;
ALTER TABLE rebate_records ALTER COLUMN order_time SET NOT NULL;
ALTER TABLE rebate_records ALTER COLUMN rebate_effective_at SET NOT NULL;
ALTER TABLE rebate_records ALTER COLUMN scan_started_at SET NOT NULL;
ALTER TABLE rebate_records ALTER COLUMN initialization_ends_at SET NOT NULL;

CREATE TABLE IF NOT EXISTS invitation_registrations (
    id BIGSERIAL PRIMARY KEY,
    inviter_id BIGINT NOT NULL,
    invitee_id BIGINT NOT NULL,
    user_group VARCHAR(64) NOT NULL,
    invited_at BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);

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
);

ALTER TABLE rebate_requests ADD COLUMN IF NOT EXISTS rejected_at BIGINT;

CREATE TABLE IF NOT EXISTS invitation_rebate_payouts (
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
    original_payout_id BIGINT,
    error_message TEXT,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    completed_at BIGINT,
    reversed_at BIGINT
);

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
);

ALTER TABLE invitation_rebate_payouts
    ADD CONSTRAINT ck_invitation_rebate_payouts_amount_nonnegative
    CHECK (amount_cents >= 0) NOT VALID;
ALTER TABLE invitation_rebate_payouts
    ADD CONSTRAINT ck_invitation_rebate_payouts_delta_nonzero
    CHECK (quota_delta <> 0) NOT VALID;
ALTER TABLE invitation_rebate_payouts
    ADD CONSTRAINT ck_invitation_rebate_payouts_kind
    CHECK (kind IN ('payout', 'reversal')) NOT VALID;
ALTER TABLE invitation_rebate_payouts
    ADD CONSTRAINT ck_invitation_rebate_payouts_status
    CHECK (status IN ('pending', 'newapi_applied', 'completed', 'reversed', 'failed', 'reversal_pending')) NOT VALID;
ALTER TABLE invitation_rebate_payouts
    ADD CONSTRAINT ck_invitation_rebate_payouts_kind_status_original
    CHECK (
        (kind = 'payout' AND original_payout_id IS NULL
            AND status IN ('pending', 'newapi_applied', 'completed', 'reversed', 'failed', 'reversal_pending'))
        OR
        (kind = 'reversal' AND original_payout_id IS NOT NULL
            AND status IN ('pending', 'newapi_applied', 'completed', 'failed'))
    ) NOT VALID;
ALTER TABLE invitation_rebate_payouts
    ADD CONSTRAINT fk_invitation_rebate_payouts_record
    FOREIGN KEY (rebate_record_id) REFERENCES rebate_records(id) ON DELETE RESTRICT NOT VALID;
ALTER TABLE invitation_rebate_payouts
    ADD CONSTRAINT fk_invitation_rebate_payouts_request
    FOREIGN KEY (rebate_request_id) REFERENCES rebate_requests(id) ON DELETE RESTRICT NOT VALID;
ALTER TABLE invitation_rebate_payouts
    ADD CONSTRAINT fk_invitation_rebate_payouts_original
    FOREIGN KEY (original_payout_id) REFERENCES invitation_rebate_payouts(id) ON DELETE RESTRICT NOT VALID;
ALTER TABLE invitation_rebate_payout_attempts
    ADD CONSTRAINT fk_invitation_rebate_payout_attempts_record
    FOREIGN KEY (rebate_record_id) REFERENCES rebate_records(id) ON DELETE RESTRICT NOT VALID;
ALTER TABLE invitation_rebate_payouts
    VALIDATE CONSTRAINT ck_invitation_rebate_payouts_amount_nonnegative;
ALTER TABLE invitation_rebate_payouts
    VALIDATE CONSTRAINT ck_invitation_rebate_payouts_delta_nonzero;
ALTER TABLE invitation_rebate_payouts
    VALIDATE CONSTRAINT ck_invitation_rebate_payouts_kind;
ALTER TABLE invitation_rebate_payouts
    VALIDATE CONSTRAINT ck_invitation_rebate_payouts_status;
ALTER TABLE invitation_rebate_payouts
    VALIDATE CONSTRAINT ck_invitation_rebate_payouts_kind_status_original;
ALTER TABLE invitation_rebate_payouts
    VALIDATE CONSTRAINT fk_invitation_rebate_payouts_record;
ALTER TABLE invitation_rebate_payouts
    VALIDATE CONSTRAINT fk_invitation_rebate_payouts_request;
ALTER TABLE invitation_rebate_payouts
    VALIDATE CONSTRAINT fk_invitation_rebate_payouts_original;
ALTER TABLE invitation_rebate_payout_attempts
    VALIDATE CONSTRAINT fk_invitation_rebate_payout_attempts_record;

CREATE TABLE IF NOT EXISTS sync_log (
    id BIGSERIAL PRIMARY KEY,
    sync_type VARCHAR(64) NOT NULL,
    last_sync_id BIGINT NOT NULL,
    last_sync_time BIGINT NOT NULL,
    created_at BIGINT NOT NULL
);

ALTER TABLE sync_log ALTER COLUMN sync_type TYPE VARCHAR(64);

CREATE TABLE IF NOT EXISTS system_config (
    key VARCHAR(64) PRIMARY KEY,
    value TEXT NOT NULL,
    description TEXT,
    updated_at BIGINT NOT NULL
);

INSERT INTO system_config (key, value, description, updated_at)
VALUES
    ('min_rebate_request_amount', '1000000', 'Minimum rebate request in quota units', EXTRACT(EPOCH FROM NOW())::BIGINT),
    ('rebate_request_frequency_days', '7', 'Minimum days between rebate requests', EXTRACT(EPOCH FROM NOW())::BIGINT),
    ('user_invitation_rebate_enabled', 'true', 'User invitation rebate interface switch', EXTRACT(EPOCH FROM NOW())::BIGINT),
    ('order_rebate_enabled', 'true', 'Order rebate switch', EXTRACT(EPOCH FROM NOW())::BIGINT),
    ('invitation_signup_reward_enabled', 'false', 'Invitation signup reward switch', EXTRACT(EPOCH FROM NOW())::BIGINT),
    ('invitation_signup_reward_amount', '1000000', 'Legacy signup reward quota', EXTRACT(EPOCH FROM NOW())::BIGINT),
    ('invitation_signup_inviter_reward_amount', '1000000', 'Inviter signup reward quota', EXTRACT(EPOCH FROM NOW())::BIGINT),
    ('invitation_signup_invitee_reward_amount', '1000000', 'Invitee signup reward quota', EXTRACT(EPOCH FROM NOW())::BIGINT),
    ('invitation_signup_reward_review_required', 'true', 'Signup reward review switch', EXTRACT(EPOCH FROM NOW())::BIGINT),
    ('invitation_signup_inviter_reward_requires_paid_order', 'false', 'Inviter reward paid-order switch', EXTRACT(EPOCH FROM NOW())::BIGINT),
    ('invitation_signup_invitee_reward_requires_paid_order', 'false', 'Invitee reward paid-order switch', EXTRACT(EPOCH FROM NOW())::BIGINT),
    ('rebate_to_balance_enabled', 'true', 'Rebate-to-balance interface switch', EXTRACT(EPOCH FROM NOW())::BIGINT)
ON CONFLICT (key) DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_rebate_rules_group ON rebate_rules(user_group);
CREATE UNIQUE INDEX IF NOT EXISTS uq_rebate_rules_group_type
    ON rebate_rules(user_group, rule_type);
CREATE UNIQUE INDEX IF NOT EXISTS uq_rebate_records_order ON rebate_records(order_type, order_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_rebate_records_ulid ON rebate_records(ulid);
CREATE INDEX IF NOT EXISTS idx_rebate_records_inviter ON rebate_records(inviter_id);
CREATE INDEX IF NOT EXISTS idx_rebate_records_status ON rebate_records(status);
CREATE INDEX IF NOT EXISTS idx_rebate_records_initialization ON rebate_records(status, initialization_ends_at);
CREATE INDEX IF NOT EXISTS idx_rebate_records_effective ON rebate_records(inviter_id, status, rebate_effective_at);
CREATE INDEX IF NOT EXISTS idx_rebate_records_inviter_status ON rebate_records(inviter_id, status);
CREATE INDEX IF NOT EXISTS idx_rebate_records_signup_unlock ON rebate_records(order_type, unlock_required, unlocked_at, invitee_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_invitation_registrations_invitee ON invitation_registrations(invitee_id);
CREATE INDEX IF NOT EXISTS idx_invitation_registrations_inviter ON invitation_registrations(inviter_id);
CREATE INDEX IF NOT EXISTS idx_invitation_registrations_invited_at ON invitation_registrations(invited_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_rebate_requests_user ON rebate_requests(user_id);
CREATE INDEX IF NOT EXISTS idx_rebate_requests_status ON rebate_requests(status);
CREATE UNIQUE INDEX IF NOT EXISTS uq_invitation_rebate_payouts_record_pay
    ON invitation_rebate_payouts(rebate_record_id) WHERE kind = 'payout';
CREATE UNIQUE INDEX IF NOT EXISTS uq_invitation_rebate_payouts_original_reverse
    ON invitation_rebate_payouts(original_payout_id) WHERE kind = 'reversal';
CREATE UNIQUE INDEX IF NOT EXISTS uq_invitation_rebate_payouts_request_id
    ON invitation_rebate_payouts(request_id);
CREATE INDEX IF NOT EXISTS idx_invitation_rebate_payouts_status
    ON invitation_rebate_payouts(status, updated_at);
CREATE INDEX IF NOT EXISTS idx_invitation_rebate_payout_attempts_record
    ON invitation_rebate_payout_attempts(rebate_record_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_sync_log_type_id ON sync_log(sync_type, last_sync_id);
