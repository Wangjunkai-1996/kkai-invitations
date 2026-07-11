ALTER TABLE balance_operations
    ADD COLUMN balance_before BIGINT,
    ADD COLUMN balance_after BIGINT,
    ADD COLUMN external_created_at BIGINT;

ALTER TABLE balance_operation_outbox
    ADD COLUMN last_error_code VARCHAR(128),
    ADD COLUMN last_http_status INTEGER;

-- Partial leases are not ownership proof. Release them while migrations hold the
-- maintenance lock so existing rows remain safely claimable after the upgrade.
UPDATE balance_operation_outbox
SET lease_token = NULL,
    lease_expires_at = NULL
WHERE (lease_token IS NULL) <> (lease_expires_at IS NULL)
   OR dispatched_at IS NOT NULL;

ALTER TABLE balance_operation_outbox
    ADD CONSTRAINT ck_balance_operation_outbox_lease_consistency
    CHECK (
        (lease_token IS NULL) = (lease_expires_at IS NULL)
        AND (dispatched_at IS NULL OR lease_token IS NULL)
    );

DROP INDEX idx_balance_operation_outbox_ready;

CREATE INDEX idx_balance_operation_outbox_ready
    ON balance_operation_outbox(available_at, id)
    WHERE dispatched_at IS NULL AND lease_expires_at IS NULL;

CREATE INDEX idx_balance_operation_outbox_expired_lease
    ON balance_operation_outbox(lease_expires_at, available_at, id)
    WHERE dispatched_at IS NULL AND lease_expires_at IS NOT NULL;

ALTER TABLE invitation_rebate_payouts
    DROP CONSTRAINT ck_invitation_rebate_payouts_status,
    DROP CONSTRAINT ck_invitation_rebate_payouts_kind_status_original;

ALTER TABLE invitation_rebate_payouts
    ADD CONSTRAINT ck_invitation_rebate_payouts_status
    CHECK (status IN (
        'pending', 'newapi_applied', 'completed', 'reversed', 'failed',
        'reversal_pending', 'needs_review'
    )),
    ADD CONSTRAINT ck_invitation_rebate_payouts_kind_status_original
    CHECK (
        (kind = 'payout' AND original_payout_id IS NULL
            AND status IN (
                'pending', 'newapi_applied', 'completed', 'reversed', 'failed',
                'reversal_pending', 'needs_review'
            ))
        OR
        (kind = 'reversal' AND original_payout_id IS NOT NULL
            AND status IN (
                'pending', 'newapi_applied', 'completed', 'failed', 'needs_review'
            ))
    );
