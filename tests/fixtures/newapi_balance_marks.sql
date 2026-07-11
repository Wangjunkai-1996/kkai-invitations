CREATE TABLE kkai_invitation_rebate_balance_marks (
    id BIGSERIAL PRIMARY KEY,
    kind VARCHAR(32) NOT NULL,
    rebate_record_id BIGINT NOT NULL,
    payout_id BIGINT NOT NULL,
    original_payout_id BIGINT,
    user_id BIGINT NOT NULL,
    quota_delta BIGINT NOT NULL,
    amount_cents BIGINT NOT NULL,
    log_id BIGINT,
    request_id VARCHAR(128) NOT NULL,
    created_at BIGINT NOT NULL
);

CREATE UNIQUE INDEX uq_kkai_invitation_rebate_balance_marks_record_pay
    ON kkai_invitation_rebate_balance_marks(rebate_record_id)
    WHERE kind = 'payout';
CREATE UNIQUE INDEX uq_kkai_invitation_rebate_balance_marks_original_reverse
    ON kkai_invitation_rebate_balance_marks(original_payout_id)
    WHERE kind = 'reversal';
CREATE UNIQUE INDEX uq_kkai_invitation_rebate_balance_marks_request
    ON kkai_invitation_rebate_balance_marks(request_id);
