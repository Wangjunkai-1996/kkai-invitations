CREATE TABLE balance_operations (
    id BIGSERIAL PRIMARY KEY,
    operation_ulid CHAR(26) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'kkai-invitations',
    business_operation_key VARCHAR(160) NOT NULL,
    operation_kind VARCHAR(16) NOT NULL,
    rebate_record_id BIGINT NOT NULL,
    rebate_request_id BIGINT,
    original_operation_ulid CHAR(26),
    user_id BIGINT NOT NULL,
    delta_quota BIGINT NOT NULL,
    idempotency_key VARCHAR(64) NOT NULL,
    payload_sha256 CHAR(64) NOT NULL,
    operation_state VARCHAR(32) NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at BIGINT,
    lease_token VARCHAR(128),
    lease_expires_at BIGINT,
    last_http_status INTEGER,
    last_error_code VARCHAR(128),
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    completed_at BIGINT,
    CONSTRAINT uq_balance_operations_operation_ulid UNIQUE (operation_ulid),
    CONSTRAINT uq_balance_operations_idempotency_key UNIQUE (idempotency_key),
    CONSTRAINT uq_balance_operations_business_identity UNIQUE (source, business_operation_key),
    CONSTRAINT fk_balance_operation_record
        FOREIGN KEY (rebate_record_id) REFERENCES rebate_records(id) ON DELETE RESTRICT,
    CONSTRAINT fk_balance_operation_request
        FOREIGN KEY (rebate_request_id) REFERENCES rebate_requests(id) ON DELETE RESTRICT,
    CONSTRAINT fk_balance_operation_original
        FOREIGN KEY (original_operation_ulid) REFERENCES balance_operations(operation_ulid) ON DELETE RESTRICT,
    CONSTRAINT ck_balance_operations_kind CHECK (operation_kind IN ('credit', 'reversal')),
    CONSTRAINT ck_balance_operations_state CHECK (operation_state IN (
        'queued', 'dispatching', 'external_pending', 'applied', 'completed',
        'retry_wait', 'blocked', 'needs_review'
    )),
    CONSTRAINT ck_balance_operations_delta_nonzero CHECK (delta_quota <> 0),
    CONSTRAINT ck_balance_operations_attempt_count CHECK (attempt_count >= 0),
    CONSTRAINT ck_balance_operations_ulid_format CHECK (
        operation_ulid ~ '^[0-9A-HJKMNP-TV-Z]{26}$'
    ),
    CONSTRAINT ck_balance_operations_payload_hash_format CHECK (
        payload_sha256 ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT ck_balance_operations_direction CHECK (
        (operation_kind = 'credit' AND delta_quota > 0 AND original_operation_ulid IS NULL)
        OR (operation_kind = 'reversal' AND delta_quota < 0 AND original_operation_ulid IS NOT NULL)
    ),
    CONSTRAINT ck_balance_operations_user_id_positive CHECK (user_id > 0)
);

CREATE INDEX idx_balance_operations_dispatch
    ON balance_operations(operation_state, next_attempt_at, id);
CREATE INDEX idx_balance_operations_record
    ON balance_operations(rebate_record_id, created_at DESC);
CREATE UNIQUE INDEX uq_balance_operations_record_credit
    ON balance_operations(rebate_record_id)
    WHERE operation_kind = 'credit';
CREATE UNIQUE INDEX uq_balance_operations_original_reversal
    ON balance_operations(original_operation_ulid)
    WHERE operation_kind = 'reversal';

CREATE OR REPLACE FUNCTION validate_balance_operation_reversal()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    original_kind VARCHAR(16);
    original_user_id BIGINT;
    original_record_id BIGINT;
    original_delta BIGINT;
BEGIN
    IF NEW.operation_kind = 'credit' THEN
        RETURN NEW;
    END IF;

    SELECT operation_kind, user_id, rebate_record_id, delta_quota
    INTO original_kind, original_user_id, original_record_id, original_delta
    FROM balance_operations
    WHERE operation_ulid = NEW.original_operation_ulid;

    IF NOT FOUND
        OR original_kind <> 'credit'
        OR original_user_id <> NEW.user_id
        OR original_record_id <> NEW.rebate_record_id
        OR original_delta::NUMERIC + NEW.delta_quota::NUMERIC <> 0
    THEN
        RAISE EXCEPTION 'balance reversal must exactly reference its original credit';
    END IF;

    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION enforce_balance_operation_financial_identity_immutable()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.operation_ulid IS DISTINCT FROM OLD.operation_ulid
        OR NEW.source IS DISTINCT FROM OLD.source
        OR NEW.business_operation_key IS DISTINCT FROM OLD.business_operation_key
        OR NEW.operation_kind IS DISTINCT FROM OLD.operation_kind
        OR NEW.rebate_record_id IS DISTINCT FROM OLD.rebate_record_id
        OR NEW.rebate_request_id IS DISTINCT FROM OLD.rebate_request_id
        OR NEW.original_operation_ulid IS DISTINCT FROM OLD.original_operation_ulid
        OR NEW.user_id IS DISTINCT FROM OLD.user_id
        OR NEW.delta_quota IS DISTINCT FROM OLD.delta_quota
        OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
        OR NEW.payload_sha256 IS DISTINCT FROM OLD.payload_sha256
    THEN
        RAISE EXCEPTION 'balance operation financial identity is immutable';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER trg_balance_operation_identity_immutable
BEFORE UPDATE OF operation_ulid, source, business_operation_key, operation_kind,
    rebate_record_id, rebate_request_id, original_operation_ulid, user_id,
    delta_quota, idempotency_key, payload_sha256 ON balance_operations
FOR EACH ROW EXECUTE FUNCTION enforce_balance_operation_financial_identity_immutable();

CREATE TRIGGER trg_balance_operation_reversal_guard
BEFORE INSERT OR UPDATE OF operation_kind, original_operation_ulid, user_id,
    rebate_record_id, delta_quota ON balance_operations
FOR EACH ROW EXECUTE FUNCTION validate_balance_operation_reversal();

CREATE TABLE balance_operation_outbox (
    id BIGSERIAL PRIMARY KEY,
    operation_id BIGINT NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    available_at BIGINT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    lease_token VARCHAR(128),
    lease_expires_at BIGINT,
    dispatched_at BIGINT,
    created_at BIGINT NOT NULL,
    CONSTRAINT uq_balance_operation_outbox_event UNIQUE (operation_id, event_type),
    CONSTRAINT fk_balance_operation_outbox_operation
        FOREIGN KEY (operation_id) REFERENCES balance_operations(id) ON DELETE RESTRICT,
    CONSTRAINT ck_balance_operation_outbox_attempt_count CHECK (attempt_count >= 0)
);

CREATE INDEX idx_balance_operation_outbox_ready
    ON balance_operation_outbox(available_at, id)
    WHERE dispatched_at IS NULL;

CREATE TABLE balance_operation_events (
    id BIGSERIAL PRIMARY KEY,
    operation_id BIGINT NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    state_from VARCHAR(32),
    state_to VARCHAR(32),
    error_code VARCHAR(128),
    occurred_at BIGINT NOT NULL,
    CONSTRAINT fk_balance_operation_event_operation
        FOREIGN KEY (operation_id) REFERENCES balance_operations(id) ON DELETE RESTRICT
);

CREATE INDEX idx_balance_operation_events_operation
    ON balance_operation_events(operation_id, occurred_at, id);

CREATE TABLE rebate_reconciliation_cases (
    id BIGSERIAL PRIMARY KEY,
    issue_code VARCHAR(128) NOT NULL,
    rebate_record_id BIGINT,
    rebate_request_id BIGINT,
    operation_id BIGINT,
    case_state VARCHAR(32) NOT NULL,
    evidence_sha256 CHAR(64) NOT NULL,
    approved_by BIGINT,
    approved_at BIGINT,
    resolved_at BIGINT,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    CONSTRAINT fk_reconciliation_record
        FOREIGN KEY (rebate_record_id) REFERENCES rebate_records(id) ON DELETE RESTRICT,
    CONSTRAINT fk_reconciliation_request
        FOREIGN KEY (rebate_request_id) REFERENCES rebate_requests(id) ON DELETE RESTRICT,
    CONSTRAINT fk_reconciliation_operation
        FOREIGN KEY (operation_id) REFERENCES balance_operations(id) ON DELETE RESTRICT,
    CONSTRAINT ck_rebate_reconciliation_case_state CHECK (
        case_state IN ('open', 'approved', 'rejected', 'resolved')
    )
);

CREATE INDEX idx_rebate_reconciliation_cases_open
    ON rebate_reconciliation_cases(case_state, issue_code, created_at);
