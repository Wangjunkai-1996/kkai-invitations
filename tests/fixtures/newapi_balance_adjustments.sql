CREATE TABLE kkai_internal_balance_adjustments (
    id BIGSERIAL PRIMARY KEY,
    operation_id VARCHAR(128) NOT NULL UNIQUE,
    user_id INTEGER NOT NULL,
    delta BIGINT NOT NULL,
    reason VARCHAR(64) NOT NULL,
    metadata TEXT NOT NULL,
    payload_sha256 CHAR(64) NOT NULL,
    original_operation_id VARCHAR(128) UNIQUE,
    balance_before BIGINT NOT NULL,
    balance_after BIGINT NOT NULL,
    created_at BIGINT NOT NULL
);

CREATE INDEX idx_kkai_balance_user
    ON kkai_internal_balance_adjustments(user_id);
CREATE INDEX idx_kkai_balance_created
    ON kkai_internal_balance_adjustments(created_at);
