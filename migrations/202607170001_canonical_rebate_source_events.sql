SET LOCAL lock_timeout = '15s';
SET LOCAL statement_timeout = '5min';

ALTER TABLE rebate_records
    ADD COLUMN canonical_event_key VARCHAR(160),
    ADD COLUMN source_payload_sha256 CHAR(64),
    ADD COLUMN source_schema_version INTEGER,
    ADD COLUMN order_amount_cents BIGINT,
    ADD COLUMN credited_quota BIGINT,
    ADD COLUMN superseded_by_record_id BIGINT;

ALTER TABLE rebate_calculation_snapshots
    ADD COLUMN credited_quota BIGINT;

ALTER TABLE rebate_reconciliation_cases
    ADD COLUMN source_event_key VARCHAR(160),
    ADD COLUMN source_order_id BIGINT;

CREATE INDEX idx_rebate_reconciliation_cases_source_order
    ON rebate_reconciliation_cases(case_state, source_order_id)
    WHERE source_event_key IS NOT NULL;

CREATE TABLE rebate_source_event_receipts (
    id BIGSERIAL PRIMARY KEY,
    event_key VARCHAR(160) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    source_order_id BIGINT NOT NULL,
    source_payload_sha256 CHAR(64) NOT NULL,
    source_schema_version INTEGER NOT NULL,
    outcome VARCHAR(32) NOT NULL,
    rebate_record_id BIGINT,
    received_at BIGINT NOT NULL,
    CONSTRAINT fk_rebate_source_event_receipts_record
        FOREIGN KEY (rebate_record_id) REFERENCES rebate_records(id) ON DELETE RESTRICT,
    CONSTRAINT ck_rebate_source_event_receipts_outcome CHECK (
        (outcome = 'created' AND rebate_record_id IS NOT NULL)
        OR (outcome = 'ignored_no_inviter' AND rebate_record_id IS NULL)
    )
);

CREATE UNIQUE INDEX uq_rebate_source_event_receipts_event_key
    ON rebate_source_event_receipts(event_key);

CREATE INDEX idx_rebate_source_event_receipts_received
    ON rebate_source_event_receipts(received_at, id);

ALTER TABLE rebate_records
    ADD CONSTRAINT fk_rebate_records_superseded_by
    FOREIGN KEY (superseded_by_record_id)
    REFERENCES rebate_records(id)
    ON DELETE RESTRICT
    NOT VALID;

DO $$
DECLARE
    legacy_master_count BIGINT;
    legacy_duplicate_count BIGINT;
BEGIN
    SELECT COUNT(*)
    INTO legacy_master_count
    FROM rebate_records
    WHERE order_type = 'topup' AND order_id < 0;

    SELECT COUNT(*)
    INTO legacy_duplicate_count
    FROM rebate_records duplicate
    JOIN rebate_records master
      ON master.order_type = 'topup'
     AND master.order_id = -duplicate.order_id
    WHERE duplicate.order_type = 'redemption'
      AND duplicate.order_id > 0;

    IF legacy_master_count = 0 AND legacy_duplicate_count = 0 THEN
        RETURN;
    END IF;
    IF legacy_master_count <> 641 THEN
        RAISE EXCEPTION
            'legacy redemption master count drifted: expected 641, got %',
            legacy_master_count;
    END IF;
    IF legacy_duplicate_count <> 3 THEN
        RAISE EXCEPTION
            'legacy redemption duplicate count drifted: expected 3, got %',
            legacy_duplicate_count;
    END IF;
END
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM rebate_records duplicate
        JOIN rebate_records master
          ON master.order_type = 'topup'
         AND master.order_id = -duplicate.order_id
        WHERE duplicate.order_type = 'redemption'
          AND duplicate.order_id > 0
          AND duplicate.status <> 'initializing'
    ) THEN
        RAISE EXCEPTION 'legacy redemption duplicate is not in initializing state';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM rebate_records duplicate
        JOIN rebate_records master
          ON master.order_type = 'topup'
         AND master.order_id = -duplicate.order_id
        WHERE duplicate.order_type = 'redemption'
          AND duplicate.order_id > 0
          AND (
              duplicate.admin_adjusted
              OR duplicate.rebate_amount <> 0
              OR duplicate.rebate_rate IS NOT NULL
              OR duplicate.inviter_id <> master.inviter_id
              OR duplicate.invitee_id <> master.invitee_id
              OR duplicate.order_amount <> master.order_amount
              OR duplicate.order_time <> master.order_time
          )
    ) THEN
        RAISE EXCEPTION 'legacy redemption duplicate has nonzero or mismatched accounting fields';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM rebate_records duplicate
        JOIN rebate_records master
          ON master.order_type = 'topup'
         AND master.order_id = -duplicate.order_id
        LEFT JOIN rebate_record_settlements settlement
          ON settlement.rebate_record_id = duplicate.id
        WHERE duplicate.order_type = 'redemption'
          AND duplicate.order_id > 0
          AND (
              settlement.rebate_record_id IS NULL
              OR settlement.rebate_request_id IS NOT NULL
              OR settlement.settlement_state <> 'not_applicable'
              OR settlement.state_version <> 0
              OR settlement.last_operation_ulid IS NOT NULL
          )
    ) THEN
        RAISE EXCEPTION 'legacy redemption duplicate has non-initial settlement state';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM rebate_records duplicate
        JOIN rebate_records master
          ON master.order_type = 'topup'
         AND master.order_id = -duplicate.order_id
        WHERE duplicate.order_type = 'redemption'
          AND duplicate.order_id > 0
          AND (
              EXISTS (
                  SELECT 1 FROM rebate_request_items item
                  WHERE item.rebate_record_id = duplicate.id
              )
              OR EXISTS (
                  SELECT 1 FROM rebate_record_request_ownership ownership
                  WHERE ownership.rebate_record_id = duplicate.id
              )
          )
    ) THEN
        RAISE EXCEPTION 'legacy redemption duplicate has rebate request ownership history';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM rebate_records duplicate
        JOIN rebate_records master
          ON master.order_type = 'topup'
         AND master.order_id = -duplicate.order_id
        JOIN invitation_rebate_payouts payout
          ON payout.rebate_record_id = duplicate.id
        WHERE duplicate.order_type = 'redemption'
          AND duplicate.order_id > 0
    ) THEN
        RAISE EXCEPTION 'legacy redemption duplicate already has payout history';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM rebate_records duplicate
        JOIN rebate_records master
          ON master.order_type = 'topup'
         AND master.order_id = -duplicate.order_id
        WHERE duplicate.order_type = 'redemption'
          AND duplicate.order_id > 0
          AND (
              EXISTS (
                  SELECT 1 FROM invitation_rebate_payout_attempts attempt
                  WHERE attempt.rebate_record_id = duplicate.id
              )
              OR EXISTS (
                  SELECT 1 FROM rebate_calculation_snapshots snapshot
                  WHERE snapshot.rebate_record_id = duplicate.id
              )
              OR EXISTS (
                  SELECT 1 FROM balance_operations operation
                  WHERE operation.rebate_record_id = duplicate.id
              )
          )
    ) THEN
        RAISE EXCEPTION 'legacy redemption duplicate has related economic history';
    END IF;
END
$$;

WITH duplicate_pairs AS (
    SELECT duplicate.id AS duplicate_id, master.id AS master_id
    FROM rebate_records duplicate
    JOIN rebate_records master
      ON master.order_type = 'topup'
     AND master.order_id = -duplicate.order_id
     AND master.inviter_id = duplicate.inviter_id
     AND master.invitee_id = duplicate.invitee_id
     AND master.order_amount = duplicate.order_amount
     AND master.order_time = duplicate.order_time
    WHERE duplicate.order_type = 'redemption'
      AND duplicate.order_id > 0
      AND duplicate.status = 'initializing'
)
UPDATE rebate_records duplicate
SET status = 'closed',
    closed_at = COALESCE(duplicate.closed_at, EXTRACT(EPOCH FROM clock_timestamp())::BIGINT),
    superseded_by_record_id = pairs.master_id,
    updated_at = GREATEST(
        duplicate.updated_at,
        EXTRACT(EPOCH FROM clock_timestamp())::BIGINT
    )
FROM duplicate_pairs pairs
WHERE duplicate.id = pairs.duplicate_id;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM rebate_records duplicate
        JOIN rebate_records master
          ON master.order_type = 'topup'
         AND master.order_id = -duplicate.order_id
        WHERE duplicate.order_type = 'redemption'
          AND duplicate.order_id > 0
          AND duplicate.superseded_by_record_id IS DISTINCT FROM master.id
    ) THEN
        RAISE EXCEPTION 'legacy redemption duplicate could not be safely superseded';
    END IF;
END
$$;

INSERT INTO rebate_reconciliation_cases (
    issue_code,
    rebate_record_id,
    case_state,
    evidence_sha256,
    resolved_at,
    created_at,
    updated_at
)
SELECT
    'legacy_redemption_duplicate_superseded',
    duplicate.id,
    'resolved',
    encode(
        sha256(
            convert_to(
                'legacy-redemption-duplicate:' || duplicate.id::TEXT || ':' || master.id::TEXT,
                'UTF8'
            )
        ),
        'hex'
    ),
    duplicate.closed_at,
    duplicate.closed_at,
    duplicate.closed_at
FROM rebate_records duplicate
JOIN rebate_records master ON master.id = duplicate.superseded_by_record_id
WHERE duplicate.order_type = 'redemption'
  AND duplicate.status = 'closed'
  AND duplicate.superseded_by_record_id IS NOT NULL;

UPDATE rebate_records
SET canonical_event_key = 'newapi:redemption:' || substring(order_id::TEXT FROM 2)
WHERE order_type = 'topup'
  AND order_id < 0
  AND canonical_event_key IS NULL;

DO $$
DECLARE
    legacy_master_count BIGINT;
    canonical_master_count BIGINT;
    superseded_duplicate_count BIGINT;
BEGIN
    SELECT COUNT(*)
    INTO legacy_master_count
    FROM rebate_records
    WHERE order_type = 'topup' AND order_id < 0;

    IF legacy_master_count = 0 THEN
        RETURN;
    END IF;

    SELECT COUNT(*)
    INTO canonical_master_count
    FROM rebate_records
    WHERE order_type = 'topup'
      AND order_id < 0
      AND canonical_event_key = 'newapi:redemption:' || substring(order_id::TEXT FROM 2);

    SELECT COUNT(*)
    INTO superseded_duplicate_count
    FROM rebate_records duplicate
    JOIN rebate_records master ON master.id = duplicate.superseded_by_record_id
    WHERE duplicate.order_type = 'redemption'
      AND duplicate.order_id > 0
      AND duplicate.status = 'closed'
      AND master.canonical_event_key = 'newapi:redemption:' || duplicate.order_id::TEXT;

    IF canonical_master_count <> 641 THEN
        RAISE EXCEPTION
            'canonical redemption master count drifted: expected 641, got %',
            canonical_master_count;
    END IF;
    IF superseded_duplicate_count <> 3 THEN
        RAISE EXCEPTION
            'superseded redemption duplicate count drifted: expected 3, got %',
            superseded_duplicate_count;
    END IF;
END
$$;

CREATE UNIQUE INDEX uq_rebate_records_canonical_event_key
    ON rebate_records(canonical_event_key)
    WHERE canonical_event_key IS NOT NULL;

CREATE OR REPLACE FUNCTION enforce_rebate_record_supersession()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'UPDATE'
       AND OLD.superseded_by_record_id IS NOT NULL
       AND (
           NEW.superseded_by_record_id IS DISTINCT FROM OLD.superseded_by_record_id
           OR NEW.status <> 'closed'
       ) THEN
        RAISE EXCEPTION 'superseded rebate record is permanently closed';
    END IF;
    IF NEW.superseded_by_record_id IS NOT NULL AND NEW.status <> 'closed' THEN
        RAISE EXCEPTION 'superseded rebate record must be closed';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER trg_rebate_record_supersession_guard
BEFORE INSERT OR UPDATE OF status, superseded_by_record_id ON rebate_records
FOR EACH ROW EXECUTE FUNCTION enforce_rebate_record_supersession();

ALTER TABLE rebate_records
    VALIDATE CONSTRAINT fk_rebate_records_superseded_by;
