CREATE TABLE rebate_calculation_snapshots (
    rebate_record_id BIGINT PRIMARY KEY,
    inviter_group VARCHAR(64) NOT NULL,
    rule_id BIGINT,
    rule_type VARCHAR(20) NOT NULL,
    rebate_rate DECIMAL(9,8) NOT NULL,
    quota_per_unit BIGINT NOT NULL,
    order_amount_minor BIGINT NOT NULL,
    rebate_quota BIGINT NOT NULL,
    source_revision VARCHAR(128) NOT NULL,
    snapshotted_at BIGINT NOT NULL,
    CONSTRAINT fk_rebate_calculation_snapshot_record
        FOREIGN KEY (rebate_record_id) REFERENCES rebate_records(id) ON DELETE RESTRICT,
    CONSTRAINT fk_rebate_calculation_snapshot_rule
        FOREIGN KEY (rule_id) REFERENCES rebate_rules(id) ON DELETE SET NULL,
    CONSTRAINT ck_rebate_calculation_snapshot_rate
        CHECK (rebate_rate >= 0 AND rebate_rate <= 1),
    CONSTRAINT ck_rebate_calculation_snapshot_quota_per_unit
        CHECK (quota_per_unit > 0 AND quota_per_unit % 100 = 0),
    CONSTRAINT ck_rebate_calculation_snapshot_order_amount
        CHECK (order_amount_minor >= 0),
    CONSTRAINT ck_rebate_calculation_snapshot_rebate_quota
        CHECK (rebate_quota >= 0)
);

CREATE INDEX idx_rebate_calculation_snapshots_rule
    ON rebate_calculation_snapshots(rule_id, snapshotted_at);

CREATE TABLE rebate_request_money_snapshots (
    rebate_request_id BIGINT PRIMARY KEY,
    total_quota BIGINT NOT NULL,
    quota_per_unit BIGINT NOT NULL,
    item_count INTEGER NOT NULL,
    source_revision VARCHAR(128) NOT NULL,
    snapshotted_at BIGINT NOT NULL,
    CONSTRAINT fk_rebate_request_money_snapshot_request
        FOREIGN KEY (rebate_request_id) REFERENCES rebate_requests(id) ON DELETE RESTRICT,
    CONSTRAINT ck_rebate_request_money_snapshot_total CHECK (total_quota >= 0),
    CONSTRAINT ck_rebate_request_money_snapshot_quota_per_unit
        CHECK (quota_per_unit > 0 AND quota_per_unit % 100 = 0),
    CONSTRAINT ck_rebate_request_money_snapshot_item_count CHECK (item_count > 0)
);
