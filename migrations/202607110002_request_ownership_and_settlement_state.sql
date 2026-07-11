SET LOCAL lock_timeout = '15s';
SET LOCAL statement_timeout = '5min';
LOCK TABLE rebate_requests, rebate_records, invitation_rebate_payouts
    IN SHARE ROW EXCLUSIVE MODE;

CREATE TABLE rebate_request_items (
    request_id BIGINT NOT NULL,
    rebate_record_id BIGINT NOT NULL,
    ordinal INTEGER,
    membership_state VARCHAR(16) NOT NULL,
    amount_quota_snapshot BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    removed_at BIGINT,
    PRIMARY KEY (request_id, rebate_record_id),
    CONSTRAINT fk_rebate_request_items_request
        FOREIGN KEY (request_id) REFERENCES rebate_requests(id) ON DELETE RESTRICT,
    CONSTRAINT fk_rebate_request_items_record
        FOREIGN KEY (rebate_record_id) REFERENCES rebate_records(id) ON DELETE RESTRICT,
    CONSTRAINT ck_rebate_request_items_membership_state
        CHECK (membership_state IN ('active', 'removed')),
    CONSTRAINT ck_rebate_request_items_amount_nonnegative
        CHECK (amount_quota_snapshot >= 0),
    CONSTRAINT ck_rebate_request_items_state_consistency CHECK (
        (membership_state = 'active' AND ordinal IS NOT NULL AND ordinal >= 0 AND removed_at IS NULL)
        OR (membership_state = 'removed' AND ordinal IS NULL AND removed_at IS NOT NULL)
    )
);

CREATE UNIQUE INDEX uq_rebate_request_items_active_ordinal
    ON rebate_request_items(request_id, ordinal)
    WHERE membership_state = 'active';

CREATE TABLE rebate_record_request_ownership (
    id BIGSERIAL PRIMARY KEY,
    rebate_record_id BIGINT NOT NULL,
    request_id BIGINT NOT NULL,
    ownership_state VARCHAR(16) NOT NULL,
    acquired_at BIGINT NOT NULL,
    released_at BIGINT,
    CONSTRAINT uq_rebate_request_ownership_request_record
        UNIQUE (request_id, rebate_record_id),
    CONSTRAINT fk_rebate_request_ownership_record
        FOREIGN KEY (rebate_record_id) REFERENCES rebate_records(id) ON DELETE RESTRICT,
    CONSTRAINT fk_rebate_request_ownership_request
        FOREIGN KEY (request_id) REFERENCES rebate_requests(id) ON DELETE RESTRICT,
    CONSTRAINT ck_rebate_request_ownership_state
        CHECK (ownership_state IN ('active', 'released')),
    CONSTRAINT ck_rebate_request_ownership_state_consistency CHECK (
        (ownership_state = 'active' AND released_at IS NULL)
        OR (ownership_state = 'released' AND released_at IS NOT NULL)
    )
);

CREATE UNIQUE INDEX uq_rebate_record_request_ownership_active
    ON rebate_record_request_ownership(rebate_record_id)
    WHERE ownership_state = 'active';
CREATE INDEX idx_rebate_record_request_ownership_request
    ON rebate_record_request_ownership(request_id, ownership_state);

INSERT INTO rebate_request_items (
    request_id, rebate_record_id, ordinal, membership_state,
    amount_quota_snapshot, created_at
)
SELECT request.id, item.rebate_record_id, (item.ordinality - 1)::INTEGER, 'active',
       record.rebate_amount, request.created_at
FROM rebate_requests AS request
CROSS JOIN LATERAL unnest(request.rebate_record_ids)
    WITH ORDINALITY AS item(rebate_record_id, ordinality)
JOIN rebate_records AS record ON record.id = item.rebate_record_id;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM rebate_requests request
        WHERE cardinality(request.rebate_record_ids) <> (
            SELECT COUNT(*)
            FROM rebate_request_items item
            WHERE item.request_id = request.id AND item.membership_state = 'active'
        )
    ) THEN
        RAISE EXCEPTION 'rebate request normalization lost or duplicated legacy items';
    END IF;
END
$$;

INSERT INTO rebate_record_request_ownership (
    rebate_record_id, request_id, ownership_state, acquired_at, released_at
)
SELECT item.rebate_record_id, request.id,
       CASE WHEN request.status = 'rejected' THEN 'released' ELSE 'active' END,
       request.created_at,
       CASE WHEN request.status = 'rejected' THEN COALESCE(request.rejected_at, request.updated_at) END
FROM rebate_requests AS request
JOIN rebate_request_items AS item
  ON item.request_id = request.id AND item.membership_state = 'active';

CREATE OR REPLACE FUNCTION sync_rebate_request_items()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE rebate_request_items
    SET membership_state = 'removed',
        ordinal = NULL,
        removed_at = NEW.updated_at
    WHERE request_id = NEW.id AND membership_state = 'active';

    UPDATE rebate_request_items existing
    SET membership_state = 'active',
        ordinal = (incoming.ordinality - 1)::INTEGER,
        removed_at = NULL
    FROM unnest(NEW.rebate_record_ids)
        WITH ORDINALITY AS incoming(rebate_record_id, ordinality)
    WHERE existing.request_id = NEW.id
      AND existing.rebate_record_id = incoming.rebate_record_id;

    INSERT INTO rebate_request_items (
        request_id, rebate_record_id, ordinal, membership_state,
        amount_quota_snapshot, created_at
    )
    SELECT NEW.id, incoming.rebate_record_id, (incoming.ordinality - 1)::INTEGER,
           'active', record.rebate_amount, NEW.created_at
    FROM unnest(NEW.rebate_record_ids)
        WITH ORDINALITY AS incoming(rebate_record_id, ordinality)
    JOIN rebate_records record ON record.id = incoming.rebate_record_id
    WHERE NOT EXISTS (
        SELECT 1 FROM rebate_request_items existing
        WHERE existing.request_id = NEW.id
          AND existing.rebate_record_id = incoming.rebate_record_id
    );

    IF (SELECT COUNT(*) FROM rebate_request_items
        WHERE request_id = NEW.id AND membership_state = 'active')
        <> cardinality(NEW.rebate_record_ids)
    THEN
        RAISE EXCEPTION 'rebate request % references missing or duplicate records', NEW.id;
    END IF;

    IF NEW.status <> 'rejected' AND cardinality(NEW.rebate_record_ids) = 0 THEN
        RAISE EXCEPTION 'active rebate request % cannot be empty', NEW.id;
    END IF;

    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION enforce_rebate_request_item_snapshot_immutable()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.amount_quota_snapshot IS DISTINCT FROM OLD.amount_quota_snapshot THEN
        RAISE EXCEPTION 'rebate request item amount snapshot is immutable';
    END IF;
    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION sync_rebate_request_ownership()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE rebate_record_request_ownership
    SET ownership_state = 'released',
        released_at = COALESCE(NEW.rejected_at, NEW.updated_at)
    WHERE request_id = NEW.id AND ownership_state = 'active';

    INSERT INTO rebate_record_request_ownership (
        rebate_record_id, request_id, ownership_state, acquired_at, released_at
    )
    SELECT item.rebate_record_id, NEW.id,
           CASE WHEN NEW.status = 'rejected' THEN 'released' ELSE 'active' END,
           NEW.created_at,
           CASE WHEN NEW.status = 'rejected' THEN COALESCE(NEW.rejected_at, NEW.updated_at) END
    FROM rebate_request_items item
    WHERE item.request_id = NEW.id AND item.membership_state = 'active'
    ON CONFLICT (request_id, rebate_record_id) DO UPDATE
    SET ownership_state = EXCLUDED.ownership_state,
        released_at = EXCLUDED.released_at;

    RETURN NEW;
END
$$;

CREATE TRIGGER trg_10_rebate_request_items
AFTER INSERT OR UPDATE OF rebate_record_ids ON rebate_requests
FOR EACH ROW EXECUTE FUNCTION sync_rebate_request_items();

CREATE TRIGGER trg_20_rebate_request_ownership
AFTER INSERT OR UPDATE OF rebate_record_ids, status, rejected_at ON rebate_requests
FOR EACH ROW EXECUTE FUNCTION sync_rebate_request_ownership();

CREATE TRIGGER trg_rebate_request_item_snapshot_immutable
BEFORE UPDATE OF amount_quota_snapshot ON rebate_request_items
FOR EACH ROW EXECUTE FUNCTION enforce_rebate_request_item_snapshot_immutable();

CREATE TABLE rebate_record_settlements (
    rebate_record_id BIGINT PRIMARY KEY,
    rebate_request_id BIGINT,
    settlement_state VARCHAR(32) NOT NULL,
    state_version BIGINT NOT NULL DEFAULT 0,
    last_operation_ulid CHAR(26),
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    CONSTRAINT fk_rebate_record_settlements_record
        FOREIGN KEY (rebate_record_id) REFERENCES rebate_records(id) ON DELETE RESTRICT,
    CONSTRAINT fk_rebate_record_settlements_request
        FOREIGN KEY (rebate_request_id) REFERENCES rebate_requests(id) ON DELETE RESTRICT,
    CONSTRAINT ck_rebate_record_settlements_state CHECK (settlement_state IN (
        'not_applicable', 'unsettled', 'payout_pending', 'paid',
        'reversal_pending', 'reversed', 'needs_review'
    )),
    CONSTRAINT ck_rebate_record_settlements_version CHECK (state_version >= 0)
);

INSERT INTO rebate_record_settlements (
    rebate_record_id, rebate_request_id, settlement_state, created_at, updated_at
)
SELECT record.id,
       ownership.request_id,
       CASE
           WHEN EXISTS (
               SELECT 1 FROM invitation_rebate_payouts payout
               WHERE payout.rebate_record_id = record.id
                 AND payout.kind = 'payout' AND payout.status = 'reversed'
           ) THEN 'reversed'
           WHEN EXISTS (
               SELECT 1 FROM invitation_rebate_payouts payout
               WHERE payout.rebate_record_id = record.id
                 AND payout.kind = 'payout' AND payout.status = 'completed'
           ) THEN 'paid'
           WHEN record.status = 'approved' THEN 'unsettled'
           WHEN EXISTS (
               SELECT 1 FROM invitation_rebate_payouts payout
               WHERE payout.rebate_record_id = record.id
           ) THEN 'needs_review'
           ELSE 'not_applicable'
       END,
       record.created_at,
       record.updated_at
FROM rebate_records record
LEFT JOIN rebate_record_request_ownership ownership
    ON ownership.rebate_record_id = record.id
   AND ownership.ownership_state = 'active';

CREATE OR REPLACE FUNCTION ensure_rebate_record_settlement_coverage()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO rebate_record_settlements (
        rebate_record_id, settlement_state, created_at, updated_at
    ) VALUES (
        NEW.id,
        CASE WHEN NEW.status = 'approved' THEN 'unsettled' ELSE 'not_applicable' END,
        NEW.created_at,
        NEW.updated_at
    );
    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION prevent_rebate_record_settlement_delete()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'rebate record settlement coverage cannot be deleted';
END
$$;

CREATE TRIGGER trg_rebate_record_settlement_insert
AFTER INSERT ON rebate_records
FOR EACH ROW EXECUTE FUNCTION ensure_rebate_record_settlement_coverage();

CREATE TRIGGER trg_rebate_record_settlement_delete_guard
BEFORE DELETE ON rebate_record_settlements
FOR EACH ROW EXECUTE FUNCTION prevent_rebate_record_settlement_delete();

CREATE TRIGGER trg_rebate_record_settlement_identity_guard
BEFORE UPDATE OF rebate_record_id ON rebate_record_settlements
FOR EACH ROW EXECUTE FUNCTION prevent_rebate_record_settlement_delete();

CREATE TRIGGER trg_rebate_record_settlement_truncate_guard
BEFORE TRUNCATE ON rebate_record_settlements
FOR EACH STATEMENT EXECUTE FUNCTION prevent_rebate_record_settlement_delete();

CREATE INDEX idx_rebate_record_settlements_state
    ON rebate_record_settlements(settlement_state, updated_at);
