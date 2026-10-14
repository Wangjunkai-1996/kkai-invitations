SET LOCAL lock_timeout = '15s';
SET LOCAL statement_timeout = '5min';

CREATE TABLE sales_accounts (
    user_id BIGINT PRIMARY KEY,
    active BOOLEAN NOT NULL,
    note TEXT NOT NULL DEFAULT '',
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    updated_by BIGINT NOT NULL,
    CONSTRAINT ck_sales_accounts_identity CHECK (user_id > 0 AND updated_by > 0)
);

CREATE TABLE sales_account_events (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    active BOOLEAN NOT NULL,
    note TEXT NOT NULL,
    admin_id BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    CONSTRAINT fk_sales_account_events_account FOREIGN KEY (user_id)
        REFERENCES sales_accounts(user_id) ON DELETE RESTRICT
);
CREATE INDEX idx_sales_account_events_user ON sales_account_events(user_id, id);

ALTER TABLE rebate_requests ADD COLUMN request_kind VARCHAR(16) NOT NULL DEFAULT 'ordinary';
ALTER TABLE rebate_requests ADD CONSTRAINT ck_rebate_requests_kind
    CHECK (request_kind IN ('ordinary', 'sales'));
CREATE INDEX idx_rebate_requests_kind_status ON rebate_requests(request_kind, status, created_at);

CREATE TABLE sales_disbursements (
    request_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    amount_quota BIGINT NOT NULL,
    credit_currency VARCHAR(24) NOT NULL,
    record_amounts JSONB NOT NULL,
    confirmed_by BIGINT NOT NULL,
    confirmed_at BIGINT NOT NULL,
    note TEXT NOT NULL,
    CONSTRAINT fk_sales_disbursements_request FOREIGN KEY (request_id)
        REFERENCES rebate_requests(id) ON DELETE RESTRICT,
    CONSTRAINT ck_sales_disbursements_values CHECK (
        user_id > 0 AND amount_quota > 0 AND confirmed_by > 0 AND confirmed_at > 0
        AND length(btrim(note)) BETWEEN 1 AND 500
        AND credit_currency IN ('legacy_075', 'usd_credit_v1')
        AND jsonb_typeof(record_amounts)='object'
    )
);

CREATE FUNCTION guard_sales_request_identity() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.request_kind IS DISTINCT FROM OLD.request_kind THEN
        RAISE EXCEPTION 'rebate request kind is immutable';
    END IF;
    IF EXISTS (SELECT 1 FROM sales_disbursements WHERE request_id = OLD.id)
       AND (NEW.status IS DISTINCT FROM OLD.status
         OR NEW.user_id IS DISTINCT FROM OLD.user_id
         OR (NEW.total_amount IS DISTINCT FROM OLD.total_amount
             AND NEW.total_amount<>(SELECT COALESCE(SUM(amount_quota_snapshot),0)
                                    FROM rebate_request_items WHERE request_id=NEW.id AND membership_state='active'))
         OR NEW.rebate_record_ids IS DISTINCT FROM OLD.rebate_record_ids) THEN
        RAISE EXCEPTION 'externally confirmed sales request is immutable';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER trg_sales_request_identity BEFORE UPDATE OF
    request_kind, status, user_id, total_amount, rebate_record_ids ON rebate_requests
    FOR EACH ROW EXECUTE FUNCTION guard_sales_request_identity();

CREATE FUNCTION guard_sales_disbursement() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    request rebate_requests%ROWTYPE;
BEGIN
    SELECT * INTO request FROM rebate_requests WHERE id = NEW.request_id FOR UPDATE;
    IF NOT FOUND OR request.request_kind <> 'sales' OR request.status <> 'approved'
       OR request.user_id <> NEW.user_id OR request.total_amount <> NEW.amount_quota
       OR NEW.credit_currency <> (CASE WHEN EXISTS(SELECT 1 FROM system_config WHERE key='kkai_usd_credit_migration' AND value::JSONB->>'state'='applied') THEN 'usd_credit_v1' ELSE 'legacy_075' END)
       OR NEW.record_amounts IS DISTINCT FROM (SELECT jsonb_object_agg(rebate_record_id::TEXT,amount_quota_snapshot)
           FROM rebate_request_items WHERE request_id=NEW.request_id AND membership_state='active') THEN
        RAISE EXCEPTION 'sales disbursement must match an approved sales request';
    END IF;
    PERFORM id FROM rebate_records WHERE id = ANY(request.rebate_record_ids) ORDER BY id FOR UPDATE;
    IF cardinality(request.rebate_record_ids) = 0 OR EXISTS (
        SELECT 1 FROM unnest(request.rebate_record_ids) AS item(record_id)
        LEFT JOIN rebate_records record ON record.id = item.record_id
        LEFT JOIN rebate_record_request_ownership owner
          ON owner.rebate_record_id = item.record_id AND owner.ownership_state = 'active'
        WHERE record.id IS NULL OR record.status <> 'approved'
          OR record.inviter_id <> request.user_id
          OR owner.request_id IS DISTINCT FROM request.id
    ) OR (SELECT COALESCE(SUM(amount_quota_snapshot), 0) FROM rebate_request_items
          WHERE request_id = request.id AND membership_state = 'active') <> NEW.amount_quota
       OR EXISTS (SELECT 1 FROM invitation_rebate_payouts
                  WHERE rebate_record_id = ANY(request.rebate_record_ids))
       OR EXISTS (SELECT 1 FROM balance_operations
                  WHERE rebate_record_id = ANY(request.rebate_record_ids)) THEN
        RAISE EXCEPTION 'sales disbursement conflicts with rebate ownership or settlement';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER trg_sales_disbursement_insert BEFORE INSERT ON sales_disbursements
    FOR EACH ROW EXECUTE FUNCTION guard_sales_disbursement();

CREATE FUNCTION forbid_sales_audit_mutation() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'sales audit evidence is immutable';
END
$$;
CREATE TRIGGER trg_sales_disbursement_immutable BEFORE UPDATE OR DELETE ON sales_disbursements
    FOR EACH ROW EXECUTE FUNCTION forbid_sales_audit_mutation();
CREATE TRIGGER trg_sales_disbursement_truncate BEFORE TRUNCATE ON sales_disbursements
    FOR EACH STATEMENT EXECUTE FUNCTION forbid_sales_audit_mutation();
CREATE TRIGGER trg_sales_account_event_immutable BEFORE UPDATE OR DELETE ON sales_account_events
    FOR EACH ROW EXECUTE FUNCTION forbid_sales_audit_mutation();
CREATE TRIGGER trg_sales_account_event_truncate BEFORE TRUNCATE ON sales_account_events
    FOR EACH STATEMENT EXECUTE FUNCTION forbid_sales_audit_mutation();

CREATE FUNCTION forbid_sales_balance_payout() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM rebate_record_request_ownership owner
        JOIN rebate_requests request ON request.id = owner.request_id
        WHERE owner.rebate_record_id = NEW.rebate_record_id
          AND owner.ownership_state = 'active' AND request.request_kind = 'sales'
    ) THEN
        RAISE EXCEPTION 'sales requests cannot use balance payouts';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER trg_sales_balance_payout_guard BEFORE INSERT OR UPDATE OF rebate_record_id
    ON invitation_rebate_payouts FOR EACH ROW EXECUTE FUNCTION forbid_sales_balance_payout();
CREATE TRIGGER trg_sales_balance_operation_guard BEFORE INSERT OR UPDATE OF rebate_record_id
    ON balance_operations FOR EACH ROW EXECUTE FUNCTION forbid_sales_balance_payout();

INSERT INTO system_config(key, value, description, updated_at)
VALUES ('order_rebate_seven_day_cutover_at', EXTRACT(EPOCH FROM NOW())::BIGINT::TEXT,
        'Immutable start of seven-day order rebate cooling policy', EXTRACT(EPOCH FROM NOW())::BIGINT)
ON CONFLICT (key) DO NOTHING;
