use std::borrow::Cow;
use std::sync::LazyLock;

use sqlx::postgres::{PgConnection, PgPool, PgPoolOptions};
use sqlx::{Acquire, Executor, Row};
use sqlx_core::migrate::{MigrateError, Migration, MigrationType, Migrator};
use thiserror::Error;

use crate::preflight::{self, PreflightMode, PreflightReport};

const READONLY_CONNECTION_SETUP_SQL: &str = "SET default_transaction_read_only = on";
const EXPECTED_MIGRATIONS: [i64; 7] = [
    202607110001,
    202607110002,
    202607110003,
    202607110004,
    202607110005,
    202607110006,
    202607170001,
];
#[derive(Clone, Copy)]
struct IndexSpec {
    name: &'static str,
    table: &'static str,
    columns: &'static [&'static str],
    predicate: Option<&'static str>,
    unique: bool,
}

const CRITICAL_INDEXES: &[IndexSpec] = &[
    IndexSpec {
        name: "uq_rebate_rules_group_type",
        table: "rebate_rules",
        columns: &["user_group", "rule_type"],
        predicate: None,
        unique: true,
    },
    IndexSpec {
        name: "idx_rebate_rules_group",
        table: "rebate_rules",
        columns: &["user_group"],
        predicate: None,
        unique: false,
    },
    IndexSpec {
        name: "uq_rebate_records_order",
        table: "rebate_records",
        columns: &["order_type", "order_id"],
        predicate: None,
        unique: true,
    },
    IndexSpec {
        name: "uq_rebate_records_canonical_event_key",
        table: "rebate_records",
        columns: &["canonical_event_key"],
        predicate: Some("canonical_event_key IS NOT NULL"),
        unique: true,
    },
    IndexSpec {
        name: "uq_rebate_source_event_receipts_event_key",
        table: "rebate_source_event_receipts",
        columns: &["event_key"],
        predicate: None,
        unique: true,
    },
    IndexSpec {
        name: "idx_rebate_source_event_receipts_received",
        table: "rebate_source_event_receipts",
        columns: &["received_at", "id"],
        predicate: None,
        unique: false,
    },
    IndexSpec {
        name: "idx_rebate_records_ulid",
        table: "rebate_records",
        columns: &["ulid"],
        predicate: None,
        unique: true,
    },
    IndexSpec {
        name: "uq_invitation_registrations_invitee",
        table: "invitation_registrations",
        columns: &["invitee_id"],
        predicate: None,
        unique: true,
    },
    IndexSpec {
        name: "idx_rebate_records_inviter",
        table: "rebate_records",
        columns: &["inviter_id"],
        predicate: None,
        unique: false,
    },
    IndexSpec {
        name: "idx_rebate_records_status",
        table: "rebate_records",
        columns: &["status"],
        predicate: None,
        unique: false,
    },
    IndexSpec {
        name: "idx_rebate_records_initialization",
        table: "rebate_records",
        columns: &["status", "initialization_ends_at"],
        predicate: None,
        unique: false,
    },
    IndexSpec {
        name: "idx_rebate_records_effective",
        table: "rebate_records",
        columns: &["inviter_id", "status", "rebate_effective_at"],
        predicate: None,
        unique: false,
    },
    IndexSpec {
        name: "idx_rebate_records_inviter_status",
        table: "rebate_records",
        columns: &["inviter_id", "status"],
        predicate: None,
        unique: false,
    },
    IndexSpec {
        name: "idx_rebate_records_signup_unlock",
        table: "rebate_records",
        columns: &["order_type", "unlock_required", "unlocked_at", "invitee_id"],
        predicate: None,
        unique: false,
    },
    IndexSpec {
        name: "idx_invitation_registrations_inviter",
        table: "invitation_registrations",
        columns: &["inviter_id"],
        predicate: None,
        unique: false,
    },
    IndexSpec {
        name: "idx_invitation_registrations_invited_at",
        table: "invitation_registrations",
        columns: &["invited_at", "id"],
        predicate: None,
        unique: false,
    },
    IndexSpec {
        name: "idx_rebate_requests_user",
        table: "rebate_requests",
        columns: &["user_id"],
        predicate: None,
        unique: false,
    },
    IndexSpec {
        name: "idx_rebate_requests_status",
        table: "rebate_requests",
        columns: &["status"],
        predicate: None,
        unique: false,
    },
    IndexSpec {
        name: "uq_invitation_rebate_payouts_record_pay",
        table: "invitation_rebate_payouts",
        columns: &["rebate_record_id"],
        predicate: Some("kind='payout'"),
        unique: true,
    },
    IndexSpec {
        name: "uq_invitation_rebate_payouts_original_reverse",
        table: "invitation_rebate_payouts",
        columns: &["original_payout_id"],
        predicate: Some("kind='reversal'"),
        unique: true,
    },
    IndexSpec {
        name: "uq_invitation_rebate_payouts_request_id",
        table: "invitation_rebate_payouts",
        columns: &["request_id"],
        predicate: None,
        unique: true,
    },
    IndexSpec {
        name: "idx_invitation_rebate_payouts_status",
        table: "invitation_rebate_payouts",
        columns: &["status", "updated_at"],
        predicate: None,
        unique: false,
    },
    IndexSpec {
        name: "idx_invitation_rebate_payout_attempts_record",
        table: "invitation_rebate_payout_attempts",
        columns: &["rebate_record_id", "created_at"],
        predicate: None,
        unique: false,
    },
    IndexSpec {
        name: "idx_sync_log_type_id",
        table: "sync_log",
        columns: &["sync_type", "last_sync_id"],
        predicate: None,
        unique: false,
    },
    IndexSpec {
        name: "uq_rebate_request_items_active_ordinal",
        table: "rebate_request_items",
        columns: &["request_id", "ordinal"],
        predicate: Some("membership_state='active'"),
        unique: true,
    },
    IndexSpec {
        name: "uq_rebate_record_request_ownership_active",
        table: "rebate_record_request_ownership",
        columns: &["rebate_record_id"],
        predicate: Some("ownership_state='active'"),
        unique: true,
    },
    IndexSpec {
        name: "idx_rebate_record_request_ownership_request",
        table: "rebate_record_request_ownership",
        columns: &["request_id", "ownership_state"],
        predicate: None,
        unique: false,
    },
    IndexSpec {
        name: "idx_rebate_record_settlements_state",
        table: "rebate_record_settlements",
        columns: &["settlement_state", "updated_at"],
        predicate: None,
        unique: false,
    },
    IndexSpec {
        name: "idx_rebate_calculation_snapshots_rule",
        table: "rebate_calculation_snapshots",
        columns: &["rule_id", "snapshotted_at"],
        predicate: None,
        unique: false,
    },
    IndexSpec {
        name: "uq_balance_operations_operation_ulid",
        table: "balance_operations",
        columns: &["operation_ulid"],
        predicate: None,
        unique: true,
    },
    IndexSpec {
        name: "uq_balance_operations_idempotency_key",
        table: "balance_operations",
        columns: &["idempotency_key"],
        predicate: None,
        unique: true,
    },
    IndexSpec {
        name: "uq_balance_operations_business_identity",
        table: "balance_operations",
        columns: &["source", "business_operation_key"],
        predicate: None,
        unique: true,
    },
    IndexSpec {
        name: "uq_balance_operation_outbox_event",
        table: "balance_operation_outbox",
        columns: &["operation_id", "event_type"],
        predicate: None,
        unique: true,
    },
    IndexSpec {
        name: "uq_balance_operations_record_credit",
        table: "balance_operations",
        columns: &["rebate_record_id"],
        predicate: Some("operation_kind='credit'"),
        unique: true,
    },
    IndexSpec {
        name: "uq_balance_operations_original_reversal",
        table: "balance_operations",
        columns: &["original_operation_ulid"],
        predicate: Some("operation_kind='reversal'"),
        unique: true,
    },
    IndexSpec {
        name: "idx_balance_operations_dispatch",
        table: "balance_operations",
        columns: &["operation_state", "next_attempt_at", "id"],
        predicate: None,
        unique: false,
    },
    IndexSpec {
        name: "idx_balance_operations_record",
        table: "balance_operations",
        columns: &["rebate_record_id", "created_at"],
        predicate: None,
        unique: false,
    },
    IndexSpec {
        name: "idx_balance_operation_outbox_ready",
        table: "balance_operation_outbox",
        columns: &["available_at", "id"],
        predicate: Some("dispatched_at IS NULL AND lease_expires_at IS NULL"),
        unique: false,
    },
    IndexSpec {
        name: "idx_balance_operation_outbox_expired_lease",
        table: "balance_operation_outbox",
        columns: &["lease_expires_at", "available_at", "id"],
        predicate: Some("dispatched_at IS NULL AND lease_expires_at IS NOT NULL"),
        unique: false,
    },
    IndexSpec {
        name: "idx_balance_operation_events_operation",
        table: "balance_operation_events",
        columns: &["operation_id", "occurred_at", "id"],
        predicate: None,
        unique: false,
    },
    IndexSpec {
        name: "idx_rebate_reconciliation_cases_open",
        table: "rebate_reconciliation_cases",
        columns: &["case_state", "issue_code", "created_at"],
        predicate: None,
        unique: false,
    },
    IndexSpec {
        name: "idx_rebate_reconciliation_cases_source_order",
        table: "rebate_reconciliation_cases",
        columns: &["case_state", "source_order_id"],
        predicate: Some("source_event_key IS NOT NULL"),
        unique: false,
    },
    IndexSpec {
        name: "idx_rebate_background_jobs_runnable",
        table: "rebate_background_jobs",
        columns: &["job_state", "run_after", "id"],
        predicate: Some("job_state=ANY(ARRAY['queued'::varchar,'retry_wait'::varchar])"),
        unique: false,
    },
];

#[derive(Clone, Copy)]
struct KeyConstraintSpec {
    name: &'static str,
    table: &'static str,
    constraint_type: char,
    columns: &'static [&'static str],
}

const CRITICAL_KEY_CONSTRAINTS: &[KeyConstraintSpec] = &[
    KeyConstraintSpec {
        name: "rebate_rules_pkey",
        table: "rebate_rules",
        constraint_type: 'p',
        columns: &["id"],
    },
    KeyConstraintSpec {
        name: "rebate_records_pkey",
        table: "rebate_records",
        constraint_type: 'p',
        columns: &["id"],
    },
    KeyConstraintSpec {
        name: "rebate_source_event_receipts_pkey",
        table: "rebate_source_event_receipts",
        constraint_type: 'p',
        columns: &["id"],
    },
    KeyConstraintSpec {
        name: "invitation_registrations_pkey",
        table: "invitation_registrations",
        constraint_type: 'p',
        columns: &["id"],
    },
    KeyConstraintSpec {
        name: "rebate_requests_pkey",
        table: "rebate_requests",
        constraint_type: 'p',
        columns: &["id"],
    },
    KeyConstraintSpec {
        name: "invitation_rebate_payouts_pkey",
        table: "invitation_rebate_payouts",
        constraint_type: 'p',
        columns: &["id"],
    },
    KeyConstraintSpec {
        name: "invitation_rebate_payout_attempts_pkey",
        table: "invitation_rebate_payout_attempts",
        constraint_type: 'p',
        columns: &["id"],
    },
    KeyConstraintSpec {
        name: "sync_log_pkey",
        table: "sync_log",
        constraint_type: 'p',
        columns: &["id"],
    },
    KeyConstraintSpec {
        name: "system_config_pkey",
        table: "system_config",
        constraint_type: 'p',
        columns: &["key"],
    },
    KeyConstraintSpec {
        name: "rebate_request_items_pkey",
        table: "rebate_request_items",
        constraint_type: 'p',
        columns: &["request_id", "rebate_record_id"],
    },
    KeyConstraintSpec {
        name: "rebate_record_request_ownership_pkey",
        table: "rebate_record_request_ownership",
        constraint_type: 'p',
        columns: &["id"],
    },
    KeyConstraintSpec {
        name: "uq_rebate_request_ownership_request_record",
        table: "rebate_record_request_ownership",
        constraint_type: 'u',
        columns: &["request_id", "rebate_record_id"],
    },
    KeyConstraintSpec {
        name: "rebate_record_settlements_pkey",
        table: "rebate_record_settlements",
        constraint_type: 'p',
        columns: &["rebate_record_id"],
    },
    KeyConstraintSpec {
        name: "rebate_calculation_snapshots_pkey",
        table: "rebate_calculation_snapshots",
        constraint_type: 'p',
        columns: &["rebate_record_id"],
    },
    KeyConstraintSpec {
        name: "rebate_request_money_snapshots_pkey",
        table: "rebate_request_money_snapshots",
        constraint_type: 'p',
        columns: &["rebate_request_id"],
    },
    KeyConstraintSpec {
        name: "balance_operations_pkey",
        table: "balance_operations",
        constraint_type: 'p',
        columns: &["id"],
    },
    KeyConstraintSpec {
        name: "uq_balance_operations_operation_ulid",
        table: "balance_operations",
        constraint_type: 'u',
        columns: &["operation_ulid"],
    },
    KeyConstraintSpec {
        name: "uq_balance_operations_idempotency_key",
        table: "balance_operations",
        constraint_type: 'u',
        columns: &["idempotency_key"],
    },
    KeyConstraintSpec {
        name: "uq_balance_operations_business_identity",
        table: "balance_operations",
        constraint_type: 'u',
        columns: &["source", "business_operation_key"],
    },
    KeyConstraintSpec {
        name: "balance_operation_outbox_pkey",
        table: "balance_operation_outbox",
        constraint_type: 'p',
        columns: &["id"],
    },
    KeyConstraintSpec {
        name: "uq_balance_operation_outbox_event",
        table: "balance_operation_outbox",
        constraint_type: 'u',
        columns: &["operation_id", "event_type"],
    },
    KeyConstraintSpec {
        name: "balance_operation_events_pkey",
        table: "balance_operation_events",
        constraint_type: 'p',
        columns: &["id"],
    },
    KeyConstraintSpec {
        name: "rebate_reconciliation_cases_pkey",
        table: "rebate_reconciliation_cases",
        constraint_type: 'p',
        columns: &["id"],
    },
    KeyConstraintSpec {
        name: "rebate_sync_checkpoints_pkey",
        table: "rebate_sync_checkpoints",
        constraint_type: 'p',
        columns: &["sync_type", "source_namespace"],
    },
    KeyConstraintSpec {
        name: "rebate_background_jobs_pkey",
        table: "rebate_background_jobs",
        constraint_type: 'p',
        columns: &["id"],
    },
    KeyConstraintSpec {
        name: "uq_rebate_background_jobs_identity",
        table: "rebate_background_jobs",
        constraint_type: 'u',
        columns: &["job_type", "partition_key"],
    },
    KeyConstraintSpec {
        name: "_sqlx_migrations_pkey",
        table: "_sqlx_migrations",
        constraint_type: 'p',
        columns: &["version"],
    },
];

#[derive(Clone, Copy)]
struct CheckSpec {
    name: &'static str,
    table: &'static str,
    definition_md5: &'static str,
}

const CRITICAL_CHECKS: &[CheckSpec] = &[
    CheckSpec {
        name: "ck_rebate_source_event_receipts_outcome",
        table: "rebate_source_event_receipts",
        definition_md5: "0a8fea3694b03a83540fe2437e0e219c",
    },
    CheckSpec {
        name: "ck_invitation_rebate_payouts_amount_nonnegative",
        table: "invitation_rebate_payouts",
        definition_md5: "49f5752d3872fa83d7dfef372d0ce14f",
    },
    CheckSpec {
        name: "ck_invitation_rebate_payouts_delta_nonzero",
        table: "invitation_rebate_payouts",
        definition_md5: "5b73d49bc22be26e7bb4163ae4a78a06",
    },
    CheckSpec {
        name: "ck_invitation_rebate_payouts_kind",
        table: "invitation_rebate_payouts",
        definition_md5: "018552a8cff4d559b5968de2634686e1",
    },
    CheckSpec {
        name: "ck_invitation_rebate_payouts_status",
        table: "invitation_rebate_payouts",
        definition_md5: "e309b67674db46ef902a1b43691f7317",
    },
    CheckSpec {
        name: "ck_invitation_rebate_payouts_kind_status_original",
        table: "invitation_rebate_payouts",
        definition_md5: "63aa74dbc1dda7eb384b279e6cf6f145",
    },
    CheckSpec {
        name: "ck_balance_operations_kind",
        table: "balance_operations",
        definition_md5: "9839599a1d1c520684bf9333405b27f7",
    },
    CheckSpec {
        name: "ck_balance_operations_state",
        table: "balance_operations",
        definition_md5: "0e9e3630efad4bc7dbccaaa61dfd027e",
    },
    CheckSpec {
        name: "ck_balance_operations_delta_nonzero",
        table: "balance_operations",
        definition_md5: "78448298a2b7d80f32e76aeac18e1f21",
    },
    CheckSpec {
        name: "ck_balance_operations_attempt_count",
        table: "balance_operations",
        definition_md5: "1eccb251ce5d9eae5cc567adab24bd33",
    },
    CheckSpec {
        name: "ck_balance_operations_ulid_format",
        table: "balance_operations",
        definition_md5: "ae12b1cbf2ad6392a068198c8b75a680",
    },
    CheckSpec {
        name: "ck_balance_operations_payload_hash_format",
        table: "balance_operations",
        definition_md5: "8902c21ae3d765f256a8b9e162d3088e",
    },
    CheckSpec {
        name: "ck_balance_operations_direction",
        table: "balance_operations",
        definition_md5: "9988bf0b493ddea36b42b01f3cfbe92a",
    },
    CheckSpec {
        name: "ck_rebate_request_items_membership_state",
        table: "rebate_request_items",
        definition_md5: "419396e994ae87e5fbc0f938697025a7",
    },
    CheckSpec {
        name: "ck_rebate_request_items_amount_nonnegative",
        table: "rebate_request_items",
        definition_md5: "8fece32e601ff5359fd178b21e3c5931",
    },
    CheckSpec {
        name: "ck_rebate_request_items_state_consistency",
        table: "rebate_request_items",
        definition_md5: "865e28ae4b3591d7db2881489b4b0c6d",
    },
    CheckSpec {
        name: "ck_rebate_request_ownership_state",
        table: "rebate_record_request_ownership",
        definition_md5: "2aa48dd95b5f047aa482cf984252a8eb",
    },
    CheckSpec {
        name: "ck_rebate_request_ownership_state_consistency",
        table: "rebate_record_request_ownership",
        definition_md5: "83300393acf02040a9472b0c4e860262",
    },
    CheckSpec {
        name: "ck_rebate_record_settlements_state",
        table: "rebate_record_settlements",
        definition_md5: "c713d471b64337da6a5219415c0aacc7",
    },
    CheckSpec {
        name: "ck_rebate_record_settlements_version",
        table: "rebate_record_settlements",
        definition_md5: "165e3705cd69898a22ebe56540b430b8",
    },
    CheckSpec {
        name: "ck_rebate_calculation_snapshot_rate",
        table: "rebate_calculation_snapshots",
        definition_md5: "afa6c4daf51f71494145d7a4c9d6ca0a",
    },
    CheckSpec {
        name: "ck_rebate_calculation_snapshot_quota_per_unit",
        table: "rebate_calculation_snapshots",
        definition_md5: "becd77852fa8f38e4e18751295350f82",
    },
    CheckSpec {
        name: "ck_rebate_calculation_snapshot_order_amount",
        table: "rebate_calculation_snapshots",
        definition_md5: "2886d5d6db82e3b896dab02a55ee6c13",
    },
    CheckSpec {
        name: "ck_rebate_calculation_snapshot_rebate_quota",
        table: "rebate_calculation_snapshots",
        definition_md5: "dbfae43a187c04b587665df0a9759461",
    },
    CheckSpec {
        name: "ck_rebate_request_money_snapshot_total",
        table: "rebate_request_money_snapshots",
        definition_md5: "a20c393354ef48deb7df03d34bf25cfd",
    },
    CheckSpec {
        name: "ck_rebate_request_money_snapshot_quota_per_unit",
        table: "rebate_request_money_snapshots",
        definition_md5: "becd77852fa8f38e4e18751295350f82",
    },
    CheckSpec {
        name: "ck_rebate_request_money_snapshot_item_count",
        table: "rebate_request_money_snapshots",
        definition_md5: "3288868615be074cd1ed65354dcffa3f",
    },
    CheckSpec {
        name: "ck_balance_operations_user_id_positive",
        table: "balance_operations",
        definition_md5: "b748a84ccc55d7a5fff8d98a421077a4",
    },
    CheckSpec {
        name: "ck_balance_operation_outbox_attempt_count",
        table: "balance_operation_outbox",
        definition_md5: "1eccb251ce5d9eae5cc567adab24bd33",
    },
    CheckSpec {
        name: "ck_balance_operation_outbox_lease_consistency",
        table: "balance_operation_outbox",
        definition_md5: "b540d7e3f032cb3c12de012b2242ef9c",
    },
    CheckSpec {
        name: "ck_rebate_reconciliation_case_state",
        table: "rebate_reconciliation_cases",
        definition_md5: "03eb71123f220122096f58298c9f3b66",
    },
    CheckSpec {
        name: "ck_rebate_sync_checkpoints_cursor_time",
        table: "rebate_sync_checkpoints",
        definition_md5: "0b23f6ead04280a8fb85419b3b51fa0d",
    },
    CheckSpec {
        name: "ck_rebate_sync_checkpoints_cursor_id",
        table: "rebate_sync_checkpoints",
        definition_md5: "a4a3927b7f2d7f624e482d7389ed19c0",
    },
    CheckSpec {
        name: "ck_rebate_sync_checkpoints_lookback",
        table: "rebate_sync_checkpoints",
        definition_md5: "5aaa4b5d4f864befacbd47826f47ded5",
    },
    CheckSpec {
        name: "ck_rebate_background_jobs_state",
        table: "rebate_background_jobs",
        definition_md5: "3246b4460d802abd827d81a67c7120b7",
    },
    CheckSpec {
        name: "ck_rebate_background_jobs_cursor_time",
        table: "rebate_background_jobs",
        definition_md5: "0b23f6ead04280a8fb85419b3b51fa0d",
    },
    CheckSpec {
        name: "ck_rebate_background_jobs_cursor_id",
        table: "rebate_background_jobs",
        definition_md5: "a4a3927b7f2d7f624e482d7389ed19c0",
    },
    CheckSpec {
        name: "ck_rebate_background_jobs_attempt_count",
        table: "rebate_background_jobs",
        definition_md5: "1eccb251ce5d9eae5cc567adab24bd33",
    },
];

#[derive(Clone, Copy)]
struct ForeignKeySpec {
    name: &'static str,
    table: &'static str,
    columns: &'static [&'static str],
    referenced_table: &'static str,
    referenced_columns: &'static [&'static str],
    delete_action: char,
}

const CRITICAL_FOREIGN_KEYS: &[ForeignKeySpec] = &[
    ForeignKeySpec {
        name: "fk_invitation_rebate_payouts_record",
        table: "invitation_rebate_payouts",
        columns: &["rebate_record_id"],
        referenced_table: "rebate_records",
        referenced_columns: &["id"],
        delete_action: 'r',
    },
    ForeignKeySpec {
        name: "fk_rebate_records_superseded_by",
        table: "rebate_records",
        columns: &["superseded_by_record_id"],
        referenced_table: "rebate_records",
        referenced_columns: &["id"],
        delete_action: 'r',
    },
    ForeignKeySpec {
        name: "fk_rebate_source_event_receipts_record",
        table: "rebate_source_event_receipts",
        columns: &["rebate_record_id"],
        referenced_table: "rebate_records",
        referenced_columns: &["id"],
        delete_action: 'r',
    },
    ForeignKeySpec {
        name: "fk_invitation_rebate_payouts_request",
        table: "invitation_rebate_payouts",
        columns: &["rebate_request_id"],
        referenced_table: "rebate_requests",
        referenced_columns: &["id"],
        delete_action: 'r',
    },
    ForeignKeySpec {
        name: "fk_invitation_rebate_payouts_original",
        table: "invitation_rebate_payouts",
        columns: &["original_payout_id"],
        referenced_table: "invitation_rebate_payouts",
        referenced_columns: &["id"],
        delete_action: 'r',
    },
    ForeignKeySpec {
        name: "fk_invitation_rebate_payout_attempts_record",
        table: "invitation_rebate_payout_attempts",
        columns: &["rebate_record_id"],
        referenced_table: "rebate_records",
        referenced_columns: &["id"],
        delete_action: 'r',
    },
    ForeignKeySpec {
        name: "fk_rebate_request_items_request",
        table: "rebate_request_items",
        columns: &["request_id"],
        referenced_table: "rebate_requests",
        referenced_columns: &["id"],
        delete_action: 'r',
    },
    ForeignKeySpec {
        name: "fk_rebate_request_items_record",
        table: "rebate_request_items",
        columns: &["rebate_record_id"],
        referenced_table: "rebate_records",
        referenced_columns: &["id"],
        delete_action: 'r',
    },
    ForeignKeySpec {
        name: "fk_rebate_request_ownership_record",
        table: "rebate_record_request_ownership",
        columns: &["rebate_record_id"],
        referenced_table: "rebate_records",
        referenced_columns: &["id"],
        delete_action: 'r',
    },
    ForeignKeySpec {
        name: "fk_rebate_request_ownership_request",
        table: "rebate_record_request_ownership",
        columns: &["request_id"],
        referenced_table: "rebate_requests",
        referenced_columns: &["id"],
        delete_action: 'r',
    },
    ForeignKeySpec {
        name: "fk_rebate_record_settlements_record",
        table: "rebate_record_settlements",
        columns: &["rebate_record_id"],
        referenced_table: "rebate_records",
        referenced_columns: &["id"],
        delete_action: 'r',
    },
    ForeignKeySpec {
        name: "fk_rebate_record_settlements_request",
        table: "rebate_record_settlements",
        columns: &["rebate_request_id"],
        referenced_table: "rebate_requests",
        referenced_columns: &["id"],
        delete_action: 'r',
    },
    ForeignKeySpec {
        name: "fk_rebate_calculation_snapshot_record",
        table: "rebate_calculation_snapshots",
        columns: &["rebate_record_id"],
        referenced_table: "rebate_records",
        referenced_columns: &["id"],
        delete_action: 'r',
    },
    ForeignKeySpec {
        name: "fk_rebate_calculation_snapshot_rule",
        table: "rebate_calculation_snapshots",
        columns: &["rule_id"],
        referenced_table: "rebate_rules",
        referenced_columns: &["id"],
        delete_action: 'n',
    },
    ForeignKeySpec {
        name: "fk_rebate_request_money_snapshot_request",
        table: "rebate_request_money_snapshots",
        columns: &["rebate_request_id"],
        referenced_table: "rebate_requests",
        referenced_columns: &["id"],
        delete_action: 'r',
    },
    ForeignKeySpec {
        name: "fk_balance_operation_record",
        table: "balance_operations",
        columns: &["rebate_record_id"],
        referenced_table: "rebate_records",
        referenced_columns: &["id"],
        delete_action: 'r',
    },
    ForeignKeySpec {
        name: "fk_balance_operation_request",
        table: "balance_operations",
        columns: &["rebate_request_id"],
        referenced_table: "rebate_requests",
        referenced_columns: &["id"],
        delete_action: 'r',
    },
    ForeignKeySpec {
        name: "fk_balance_operation_original",
        table: "balance_operations",
        columns: &["original_operation_ulid"],
        referenced_table: "balance_operations",
        referenced_columns: &["operation_ulid"],
        delete_action: 'r',
    },
    ForeignKeySpec {
        name: "fk_balance_operation_outbox_operation",
        table: "balance_operation_outbox",
        columns: &["operation_id"],
        referenced_table: "balance_operations",
        referenced_columns: &["id"],
        delete_action: 'r',
    },
    ForeignKeySpec {
        name: "fk_balance_operation_event_operation",
        table: "balance_operation_events",
        columns: &["operation_id"],
        referenced_table: "balance_operations",
        referenced_columns: &["id"],
        delete_action: 'r',
    },
    ForeignKeySpec {
        name: "fk_reconciliation_record",
        table: "rebate_reconciliation_cases",
        columns: &["rebate_record_id"],
        referenced_table: "rebate_records",
        referenced_columns: &["id"],
        delete_action: 'r',
    },
    ForeignKeySpec {
        name: "fk_reconciliation_request",
        table: "rebate_reconciliation_cases",
        columns: &["rebate_request_id"],
        referenced_table: "rebate_requests",
        referenced_columns: &["id"],
        delete_action: 'r',
    },
    ForeignKeySpec {
        name: "fk_reconciliation_operation",
        table: "rebate_reconciliation_cases",
        columns: &["operation_id"],
        referenced_table: "balance_operations",
        referenced_columns: &["id"],
        delete_action: 'r',
    },
];

#[derive(Clone, Copy)]
struct TriggerSpec {
    name: &'static str,
    table: &'static str,
    trigger_type: i16,
    update_columns: &'static [&'static str],
    function: &'static str,
    source_md5: &'static str,
}

const CRITICAL_TRIGGERS: &[TriggerSpec] = &[
    TriggerSpec {
        name: "trg_rebate_record_supersession_guard",
        table: "rebate_records",
        trigger_type: 23,
        update_columns: &["status", "superseded_by_record_id"],
        function: "enforce_rebate_record_supersession",
        source_md5: "34c6359091448d33fdee333bb23172bf",
    },
    TriggerSpec {
        name: "trg_10_rebate_request_items",
        table: "rebate_requests",
        trigger_type: 21,
        update_columns: &["rebate_record_ids"],
        function: "sync_rebate_request_items",
        source_md5: "089ea8416bd942fd30a6601fbaa19ae5",
    },
    TriggerSpec {
        name: "trg_20_rebate_request_ownership",
        table: "rebate_requests",
        trigger_type: 21,
        update_columns: &["rebate_record_ids", "rejected_at", "status"],
        function: "sync_rebate_request_ownership",
        source_md5: "bcd91b03acfec042be9d9c0359125730",
    },
    TriggerSpec {
        name: "trg_rebate_request_item_snapshot_immutable",
        table: "rebate_request_items",
        trigger_type: 19,
        update_columns: &["amount_quota_snapshot"],
        function: "enforce_rebate_request_item_snapshot_immutable",
        source_md5: "b61d225e11e9c37a3ef5c128d4ec9ba0",
    },
    TriggerSpec {
        name: "trg_balance_operation_reversal_guard",
        table: "balance_operations",
        trigger_type: 23,
        update_columns: &[
            "delta_quota",
            "operation_kind",
            "original_operation_ulid",
            "rebate_record_id",
            "user_id",
        ],
        function: "validate_balance_operation_reversal",
        source_md5: "199b2cb381b65e3e40434382804a1fb7",
    },
    TriggerSpec {
        name: "trg_balance_operation_identity_immutable",
        table: "balance_operations",
        trigger_type: 19,
        update_columns: &[
            "business_operation_key",
            "delta_quota",
            "idempotency_key",
            "operation_kind",
            "operation_ulid",
            "original_operation_ulid",
            "payload_sha256",
            "rebate_record_id",
            "rebate_request_id",
            "source",
            "user_id",
        ],
        function: "enforce_balance_operation_financial_identity_immutable",
        source_md5: "04cc209bd7a1f4ff4db6806e774eb556",
    },
    TriggerSpec {
        name: "trg_rebate_record_settlement_insert",
        table: "rebate_records",
        trigger_type: 5,
        update_columns: &[],
        function: "ensure_rebate_record_settlement_coverage",
        source_md5: "35876047af5dfc836ed7d8a2248f3ed2",
    },
    TriggerSpec {
        name: "trg_rebate_record_settlement_delete_guard",
        table: "rebate_record_settlements",
        trigger_type: 11,
        update_columns: &[],
        function: "prevent_rebate_record_settlement_delete",
        source_md5: "8792ac445cbce1e008f526e76adff255",
    },
    TriggerSpec {
        name: "trg_rebate_record_settlement_identity_guard",
        table: "rebate_record_settlements",
        trigger_type: 19,
        update_columns: &["rebate_record_id"],
        function: "prevent_rebate_record_settlement_delete",
        source_md5: "8792ac445cbce1e008f526e76adff255",
    },
    TriggerSpec {
        name: "trg_rebate_record_settlement_truncate_guard",
        table: "rebate_record_settlements",
        trigger_type: 34,
        update_columns: &[],
        function: "prevent_rebate_record_settlement_delete",
        source_md5: "8792ac445cbce1e008f526e76adff255",
    },
];

const REQUIRED_NOT_NULL_COLUMNS: &[(&str, &[&str])] = &[
    (
        "invitation_rebate_payouts",
        &[
            "id",
            "rebate_record_id",
            "inviter_user_id",
            "invitee_user_id",
            "order_id",
            "order_type",
            "amount_cents",
            "quota_delta",
            "kind",
            "status",
            "request_id",
            "created_at",
            "updated_at",
        ],
    ),
    (
        "rebate_request_items",
        &[
            "request_id",
            "rebate_record_id",
            "membership_state",
            "amount_quota_snapshot",
            "created_at",
        ],
    ),
    (
        "rebate_record_request_ownership",
        &[
            "id",
            "rebate_record_id",
            "request_id",
            "ownership_state",
            "acquired_at",
        ],
    ),
    (
        "rebate_record_settlements",
        &[
            "rebate_record_id",
            "settlement_state",
            "state_version",
            "created_at",
            "updated_at",
        ],
    ),
    (
        "balance_operations",
        &[
            "id",
            "operation_ulid",
            "source",
            "business_operation_key",
            "operation_kind",
            "rebate_record_id",
            "user_id",
            "delta_quota",
            "idempotency_key",
            "payload_sha256",
            "operation_state",
            "attempt_count",
            "created_at",
            "updated_at",
        ],
    ),
    (
        "balance_operation_outbox",
        &[
            "id",
            "operation_id",
            "event_type",
            "available_at",
            "attempt_count",
            "created_at",
        ],
    ),
    (
        "balance_operation_events",
        &["id", "operation_id", "event_type", "occurred_at"],
    ),
    (
        "rebate_reconciliation_cases",
        &[
            "id",
            "issue_code",
            "case_state",
            "evidence_sha256",
            "created_at",
            "updated_at",
        ],
    ),
];

static MIGRATOR: LazyLock<Migrator> = LazyLock::new(|| Migrator {
    migrations: Cow::Owned(vec![
        Migration::new(
            202607110001,
            Cow::Borrowed("legacy baseline"),
            MigrationType::Simple,
            Cow::Borrowed(include_str!(
                "../migrations/202607110001_legacy_baseline.sql"
            )),
            false,
        ),
        Migration::new(
            202607110002,
            Cow::Borrowed("request ownership and settlement state"),
            MigrationType::Simple,
            Cow::Borrowed(include_str!(
                "../migrations/202607110002_request_ownership_and_settlement_state.sql"
            )),
            false,
        ),
        Migration::new(
            202607110003,
            Cow::Borrowed("rule and money snapshots"),
            MigrationType::Simple,
            Cow::Borrowed(include_str!(
                "../migrations/202607110003_rule_and_money_snapshots.sql"
            )),
            false,
        ),
        Migration::new(
            202607110004,
            Cow::Borrowed("balance operation outbox"),
            MigrationType::Simple,
            Cow::Borrowed(include_str!(
                "../migrations/202607110004_balance_operation_outbox.sql"
            )),
            false,
        ),
        Migration::new(
            202607110005,
            Cow::Borrowed("sync checkpoints and jobs"),
            MigrationType::Simple,
            Cow::Borrowed(include_str!(
                "../migrations/202607110005_sync_checkpoints_and_jobs.sql"
            )),
            false,
        ),
        Migration::new(
            202607110006,
            Cow::Borrowed("dispatcher leases and responses"),
            MigrationType::Simple,
            Cow::Borrowed(include_str!(
                "../migrations/202607110006_dispatcher_leases_and_responses.sql"
            )),
            false,
        ),
        Migration::new(
            202607170001,
            Cow::Borrowed("canonical rebate source events"),
            MigrationType::Simple,
            Cow::Borrowed(include_str!(
                "../migrations/202607170001_canonical_rebate_source_events.sql"
            )),
            false,
        ),
    ]),
    ..Migrator::DEFAULT
});

#[derive(Debug, Error)]
pub enum DatabaseStartupError {
    #[error("failed to connect to invitations database: {0}")]
    Connect(#[source] sqlx::Error),
    #[error("invitations database preflight query failed: {0}")]
    PreflightQuery(#[source] sqlx::Error),
    #[error("invitations database preflight blocked migration with {blocking_issue_count} issue categories")]
    PreflightBlocked {
        blocking_issue_count: usize,
        report: PreflightReport,
    },
    #[error("invitations database migration failed: {0}")]
    Migration(#[source] MigrateError),
    #[error("invitations database schema verification failed: {0}")]
    Verification(String),
}

impl DatabaseStartupError {
    pub fn preflight_report(&self) -> Option<&PreflightReport> {
        match self {
            Self::PreflightBlocked { report, .. } => Some(report),
            _ => None,
        }
    }
}

pub async fn init_pool(
    database_url: &str,
    newapi_pool: &PgPool,
) -> Result<PgPool, DatabaseStartupError> {
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(database_url)
        .await
        .map_err(DatabaseStartupError::Connect)?;

    tracing::info!("Invitations database pool connected");
    prepare_database(&pool, newapi_pool).await?;
    Ok(pool)
}

pub async fn init_readonly_pool(database_url: &str) -> Result<PgPool, sqlx::Error> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                conn.execute(READONLY_CONNECTION_SETUP_SQL).await?;
                Ok(())
            })
        })
        .connect(database_url)
        .await?;

    tracing::info!("new-api readonly database pool connected");
    Ok(pool)
}

pub async fn preflight_database(
    database_url: &str,
    newapi_database_url: &str,
) -> Result<PreflightReport, sqlx::Error> {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(database_url)
        .await?;
    let newapi_pool = PgPoolOptions::new()
        .max_connections(2)
        .after_connect(|connection, _meta| {
            Box::pin(async move {
                connection.execute(READONLY_CONNECTION_SETUP_SQL).await?;
                Ok(())
            })
        })
        .connect(newapi_database_url)
        .await?;
    let mut connection = pool.acquire().await?;
    let mut external_connection = newapi_pool.acquire().await?;
    let report = preflight::run(
        &mut connection,
        &mut external_connection,
        PreflightMode::ReportOnly,
    )
    .await;
    drop(connection);
    drop(external_connection);
    pool.close().await;
    newapi_pool.close().await;
    report
}

async fn prepare_database(pool: &PgPool, newapi_pool: &PgPool) -> Result<(), DatabaseStartupError> {
    tracing::info!("Running invitations database preflight");
    let mut connection = pool
        .acquire()
        .await
        .map_err(DatabaseStartupError::Connect)?;
    let mut external_connection = newapi_pool
        .acquire()
        .await
        .map_err(DatabaseStartupError::Connect)?;
    let mut transaction = connection
        .begin()
        .await
        .map_err(DatabaseStartupError::Connect)?;
    let mut external_transaction = external_connection
        .begin()
        .await
        .map_err(DatabaseStartupError::Connect)?;

    // Maintenance locks make every later READ COMMITTED statement observe all writers
    // that completed before the lock was granted, without pinning a stale snapshot.
    sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        .execute(&mut *transaction)
        .await
        .map_err(DatabaseStartupError::PreflightQuery)?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *external_transaction)
        .await
        .map_err(DatabaseStartupError::PreflightQuery)?;
    sqlx::query("SET LOCAL lock_timeout = '15s'")
        .execute(&mut *transaction)
        .await
        .map_err(DatabaseStartupError::PreflightQuery)?;
    sqlx::query("SET LOCAL statement_timeout = '5min'")
        .execute(&mut *transaction)
        .await
        .map_err(DatabaseStartupError::PreflightQuery)?;
    sqlx::query("SET LOCAL statement_timeout = '5min'")
        .execute(&mut *external_transaction)
        .await
        .map_err(DatabaseStartupError::PreflightQuery)?;
    sqlx::query("SELECT pg_advisory_xact_lock(1263358281, 20260711)")
        .execute(&mut *transaction)
        .await
        .map_err(DatabaseStartupError::PreflightQuery)?;
    lock_maintenance_tables(&mut transaction).await?;

    let report = preflight::run(
        &mut transaction,
        &mut external_transaction,
        PreflightMode::MaintenanceLocked,
    )
    .await
    .map_err(DatabaseStartupError::PreflightQuery)?;

    for issue in &report.issues {
        tracing::warn!(
            issue_code = issue.code,
            severity = ?issue.severity,
            count = issue.count,
            "Invitations database preflight finding"
        );
    }

    if !report.safe_to_migrate {
        return Err(DatabaseStartupError::PreflightBlocked {
            blocking_issue_count: report.blocking_issue_count(),
            report,
        });
    }

    tracing::info!("Applying versioned invitations database migrations");
    MIGRATOR
        .run(&mut *transaction)
        .await
        .map_err(DatabaseStartupError::Migration)?;
    verify_schema(&mut transaction).await?;
    transaction
        .commit()
        .await
        .map_err(DatabaseStartupError::Connect)?;
    external_transaction
        .commit()
        .await
        .map_err(DatabaseStartupError::Connect)?;
    tracing::info!(
        schema_version = EXPECTED_MIGRATIONS[EXPECTED_MIGRATIONS.len() - 1],
        "Invitations database is ready"
    );
    Ok(())
}

async fn lock_maintenance_tables(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), DatabaseStartupError> {
    const TABLES: &[&str] = &[
        "balance_operation_events",
        "_sqlx_migrations",
        "balance_operation_outbox",
        "balance_operations",
        "invitation_rebate_payout_attempts",
        "invitation_rebate_payouts",
        "invitation_registrations",
        "rebate_background_jobs",
        "rebate_calculation_snapshots",
        "rebate_record_request_ownership",
        "rebate_record_settlements",
        "rebate_records",
        "rebate_reconciliation_cases",
        "rebate_request_items",
        "rebate_request_money_snapshots",
        "rebate_requests",
        "rebate_rules",
        "rebate_sync_checkpoints",
        "sync_log",
        "system_config",
        "withdrawal_requests",
    ];
    for table in TABLES {
        let exists: bool = sqlx::query_scalar(
            "SELECT to_regclass(format('%I.%I', current_schema(), $1)) IS NOT NULL",
        )
        .bind(table)
        .fetch_one(&mut **transaction)
        .await
        .map_err(DatabaseStartupError::PreflightQuery)?;
        if exists {
            let statement = format!("LOCK TABLE \"{table}\" IN SHARE ROW EXCLUSIVE MODE");
            sqlx::query(&statement)
                .execute(&mut **transaction)
                .await
                .map_err(DatabaseStartupError::PreflightQuery)?;
        }
    }
    Ok(())
}

async fn verify_schema(connection: &mut PgConnection) -> Result<(), DatabaseStartupError> {
    let applied: Vec<i64> = sqlx::query_scalar(
        "SELECT version FROM _sqlx_migrations WHERE success = TRUE ORDER BY version",
    )
    .fetch_all(&mut *connection)
    .await
    .map_err(|error| DatabaseStartupError::Verification(error.to_string()))?;
    let missing_migrations: Vec<i64> = EXPECTED_MIGRATIONS
        .into_iter()
        .filter(|version| !applied.contains(version))
        .collect();
    if !missing_migrations.is_empty() {
        return Err(DatabaseStartupError::Verification(format!(
            "missing migration versions: {missing_migrations:?}"
        )));
    }

    let required_tables = [
        "rebate_request_items",
        "rebate_record_request_ownership",
        "rebate_record_settlements",
        "rebate_calculation_snapshots",
        "rebate_request_money_snapshots",
        "balance_operations",
        "balance_operation_outbox",
        "balance_operation_events",
        "rebate_reconciliation_cases",
        "rebate_sync_checkpoints",
        "rebate_background_jobs",
    ];
    for table in required_tables {
        let exists: bool = sqlx::query_scalar(
            "SELECT to_regclass(format('%I.%I', current_schema(), $1)) IS NOT NULL",
        )
        .bind(table)
        .fetch_one(&mut *connection)
        .await
        .map_err(|error| DatabaseStartupError::Verification(error.to_string()))?;
        if !exists {
            return Err(DatabaseStartupError::Verification(format!(
                "required table is missing: {table}"
            )));
        }
    }

    for expected in CRITICAL_INDEXES {
        let row = sqlx::query(
            r#"
            SELECT table_class.relname AS table_name,
                   pg_index.indisvalid AS is_valid,
                   pg_index.indisunique AS is_unique,
                   pg_get_expr(pg_index.indpred, pg_index.indrelid) AS predicate,
                   ARRAY(
                       SELECT attribute.attname::TEXT
                       FROM unnest(pg_index.indkey::SMALLINT[])
                            WITH ORDINALITY AS key(attnum, position)
                       JOIN pg_attribute attribute
                         ON attribute.attrelid = pg_index.indrelid
                        AND attribute.attnum = key.attnum
                       WHERE key.position <= pg_index.indnkeyatts
                       ORDER BY key.position
                   )::TEXT[] AS columns
            FROM pg_class index_class
            JOIN pg_namespace namespace ON namespace.oid = index_class.relnamespace
            JOIN pg_index ON pg_index.indexrelid = index_class.oid
            JOIN pg_class table_class ON table_class.oid = pg_index.indrelid
            WHERE namespace.nspname = current_schema() AND index_class.relname = $1
            "#,
        )
        .bind(expected.name)
        .fetch_optional(&mut *connection)
        .await
        .map_err(|error| DatabaseStartupError::Verification(error.to_string()))?
        .ok_or_else(|| {
            DatabaseStartupError::Verification(format!(
                "critical index is missing: {}",
                expected.name
            ))
        })?;
        let table_name: String = row
            .try_get("table_name")
            .map_err(|error| DatabaseStartupError::Verification(error.to_string()))?;
        let is_valid: bool = row
            .try_get("is_valid")
            .map_err(|error| DatabaseStartupError::Verification(error.to_string()))?;
        let is_unique: bool = row
            .try_get("is_unique")
            .map_err(|error| DatabaseStartupError::Verification(error.to_string()))?;
        let columns: Vec<String> = row
            .try_get("columns")
            .map_err(|error| DatabaseStartupError::Verification(error.to_string()))?;
        let predicate: Option<String> = row
            .try_get("predicate")
            .map_err(|error| DatabaseStartupError::Verification(error.to_string()))?;
        let expected_columns: Vec<String> = expected
            .columns
            .iter()
            .map(|column| (*column).to_string())
            .collect();
        if !is_valid
            || is_unique != expected.unique
            || table_name != expected.table
            || columns != expected_columns
            || predicate.as_deref().map(normalize_catalog_expression)
                != expected.predicate.map(normalize_catalog_expression)
        {
            return Err(DatabaseStartupError::Verification(format!(
                "critical index definition drifted: {}",
                expected.name
            )));
        }
    }

    for expected in CRITICAL_KEY_CONSTRAINTS {
        verify_key_constraint(connection, *expected).await?;
    }

    for expected in CRITICAL_CHECKS {
        let row = sqlx::query(
            r#"
            SELECT table_class.relname AS table_name,
                   pg_constraint.contype::TEXT AS constraint_type,
                   pg_constraint.convalidated AS is_valid,
                   md5(pg_get_constraintdef(pg_constraint.oid, TRUE)) AS definition_md5
            FROM pg_constraint
            JOIN pg_namespace namespace ON namespace.oid = pg_constraint.connamespace
            JOIN pg_class table_class ON table_class.oid = pg_constraint.conrelid
            WHERE namespace.nspname = current_schema() AND pg_constraint.conname = $1
            "#,
        )
        .bind(expected.name)
        .fetch_optional(&mut *connection)
        .await
        .map_err(|error| DatabaseStartupError::Verification(error.to_string()))?
        .ok_or_else(|| {
            DatabaseStartupError::Verification(format!(
                "critical check is missing: {}",
                expected.name
            ))
        })?;
        let table_name: String = row.try_get("table_name").map_err(verification_error)?;
        let constraint_type: String = row.try_get("constraint_type").map_err(verification_error)?;
        let is_valid: bool = row.try_get("is_valid").map_err(verification_error)?;
        let definition_md5: String = row.try_get("definition_md5").map_err(verification_error)?;
        if table_name != expected.table
            || constraint_type != "c"
            || !is_valid
            || !is_supported_check_definition(expected, &definition_md5)
        {
            return Err(DatabaseStartupError::Verification(format!(
                "critical check definition drifted: {} (actual md5 {definition_md5})",
                expected.name
            )));
        }
    }

    for expected in CRITICAL_FOREIGN_KEYS {
        verify_foreign_key(connection, *expected).await?;
    }
    for (table, columns) in REQUIRED_NOT_NULL_COLUMNS {
        for column in *columns {
            let is_not_null: Option<bool> = sqlx::query_scalar(
                r#"
                SELECT attribute.attnotnull
                FROM pg_attribute attribute
                JOIN pg_class table_class ON table_class.oid = attribute.attrelid
                JOIN pg_namespace namespace ON namespace.oid = table_class.relnamespace
                WHERE namespace.nspname = current_schema()
                  AND table_class.relname = $1 AND attribute.attname = $2
                  AND NOT attribute.attisdropped
                "#,
            )
            .bind(table)
            .bind(column)
            .fetch_optional(&mut *connection)
            .await
            .map_err(verification_error)?;
            if is_not_null != Some(true) {
                return Err(DatabaseStartupError::Verification(format!(
                    "required NOT NULL column drifted: {table}.{column}"
                )));
            }
        }
    }
    verify_column_shapes(
        connection,
        "rebate_records",
        &[
            ("canonical_event_key", "varchar", true),
            ("source_payload_sha256", "bpchar", true),
            ("source_schema_version", "int4", true),
            ("order_amount_cents", "int8", true),
            ("credited_quota", "int8", true),
            ("superseded_by_record_id", "int8", true),
        ],
    )
    .await?;
    verify_column_shapes(
        connection,
        "rebate_calculation_snapshots",
        &[("credited_quota", "int8", true)],
    )
    .await?;
    verify_column_shapes(
        connection,
        "rebate_reconciliation_cases",
        &[
            ("source_event_key", "varchar", true),
            ("source_order_id", "int8", true),
        ],
    )
    .await?;
    verify_column_shapes(
        connection,
        "balance_operations",
        &[
            ("balance_before", "int8", true),
            ("balance_after", "int8", true),
            ("external_created_at", "int8", true),
        ],
    )
    .await?;
    verify_column_shapes(
        connection,
        "balance_operation_outbox",
        &[
            ("last_error_code", "varchar", true),
            ("last_http_status", "int4", true),
        ],
    )
    .await?;
    for expected in CRITICAL_TRIGGERS {
        verify_trigger(connection, *expected).await?;
    }

    let missing_settlement_coverage: Option<i64> = sqlx::query_scalar(
        r#"
        SELECT record.id
        FROM rebate_records record
        LEFT JOIN rebate_record_settlements settlement
          ON settlement.rebate_record_id = record.id
        WHERE settlement.rebate_record_id IS NULL
        ORDER BY record.id
        LIMIT 1
        "#,
    )
    .fetch_optional(&mut *connection)
    .await
    .map_err(verification_error)?;
    if let Some(record_id) = missing_settlement_coverage {
        return Err(DatabaseStartupError::Verification(format!(
            "normalized settlement coverage is missing for rebate record {record_id}"
        )));
    }

    Ok(())
}

fn is_supported_check_definition(expected: &CheckSpec, actual_md5: &str) -> bool {
    if actual_md5 == expected.definition_md5 {
        return true;
    }
    matches!(
        (expected.name, actual_md5),
        (
            "ck_invitation_rebate_payouts_kind",
            "22cc0e52a6d8cfeef0bc543b79ef005c"
        ) | (
            "ck_invitation_rebate_payouts_status",
            "24892e3a20ec7d78cad00d2b36e9d537"
        ) | (
            "ck_invitation_rebate_payouts_kind_status_original",
            "16f7edeb150cd90d95802c69c9e215e6"
        ) | (
            "ck_balance_operations_kind",
            "5cb137469d1f3b09af93da532af5a393"
        ) | (
            "ck_balance_operations_state",
            "8cb55a86cb4953c2c98902beaa34b9d9"
        ) | (
            "ck_rebate_request_items_membership_state",
            "0067984024457714135c3c710b2653c9"
        ) | (
            "ck_rebate_request_ownership_state",
            "be7eb51a5e5f7972313ec2a55e854f3b"
        ) | (
            "ck_rebate_record_settlements_state",
            "4238e100522ee179442d860cbcd82b95"
        ) | (
            "ck_rebate_reconciliation_case_state",
            "19e8df2ec6da3d22e8602127dd0d2e52"
        ) | (
            "ck_rebate_background_jobs_state",
            "ab468d08bc9cab14b20196f65c383832"
        )
    )
}

async fn verify_column_shapes(
    connection: &mut PgConnection,
    table: &str,
    columns: &[(&str, &str, bool)],
) -> Result<(), DatabaseStartupError> {
    for (column, expected_type, expected_nullable) in columns {
        let row = sqlx::query(
            r#"
            SELECT udt_name, is_nullable
            FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = $1 AND column_name = $2
            "#,
        )
        .bind(table)
        .bind(column)
        .fetch_optional(&mut *connection)
        .await
        .map_err(verification_error)?;
        let Some(row) = row else {
            return Err(DatabaseStartupError::Verification(format!(
                "required dispatcher column is missing: {table}.{column}"
            )));
        };
        let actual_type: String = row.try_get("udt_name").map_err(verification_error)?;
        let actual_nullable: String = row.try_get("is_nullable").map_err(verification_error)?;
        if actual_type != *expected_type || (actual_nullable == "YES") != *expected_nullable {
            return Err(DatabaseStartupError::Verification(format!(
                "required dispatcher column drifted: {table}.{column}"
            )));
        }
    }
    Ok(())
}

async fn verify_key_constraint(
    connection: &mut PgConnection,
    expected: KeyConstraintSpec,
) -> Result<(), DatabaseStartupError> {
    let row = sqlx::query(
        r#"
        SELECT table_class.relname AS table_name,
               constraint_meta.contype::TEXT AS constraint_type,
               constraint_meta.convalidated AS is_valid,
               ARRAY(
                   SELECT attribute.attname::TEXT
                   FROM unnest(constraint_meta.conkey) WITH ORDINALITY AS key(attnum, position)
                   JOIN pg_attribute attribute
                     ON attribute.attrelid = constraint_meta.conrelid
                    AND attribute.attnum = key.attnum
                   ORDER BY key.position
               )::TEXT[] AS columns
        FROM pg_constraint constraint_meta
        JOIN pg_class table_class ON table_class.oid = constraint_meta.conrelid
        JOIN pg_namespace namespace ON namespace.oid = table_class.relnamespace
        WHERE namespace.nspname = current_schema() AND constraint_meta.conname = $1
        "#,
    )
    .bind(expected.name)
    .fetch_optional(&mut *connection)
    .await
    .map_err(verification_error)?
    .ok_or_else(|| {
        DatabaseStartupError::Verification(format!(
            "critical constraint is missing: {}",
            expected.name
        ))
    })?;
    let table_name: String = row.try_get("table_name").map_err(verification_error)?;
    let constraint_type: String = row.try_get("constraint_type").map_err(verification_error)?;
    let is_valid: bool = row.try_get("is_valid").map_err(verification_error)?;
    let columns: Vec<String> = row.try_get("columns").map_err(verification_error)?;
    let expected_columns: Vec<String> = expected
        .columns
        .iter()
        .map(|column| (*column).to_string())
        .collect();
    if table_name != expected.table
        || constraint_type != expected.constraint_type.to_string()
        || !is_valid
        || columns != expected_columns
    {
        return Err(DatabaseStartupError::Verification(format!(
            "critical constraint definition drifted: {}",
            expected.name
        )));
    }
    Ok(())
}

async fn verify_foreign_key(
    connection: &mut PgConnection,
    expected: ForeignKeySpec,
) -> Result<(), DatabaseStartupError> {
    let row = sqlx::query(
        r#"
        SELECT source_table.relname AS table_name,
               referenced_table.relname AS referenced_table_name,
               constraint_meta.convalidated AS is_valid,
               constraint_meta.confdeltype::TEXT AS delete_action,
               ARRAY(
                   SELECT attribute.attname::TEXT
                   FROM unnest(constraint_meta.conkey) WITH ORDINALITY AS key(attnum, position)
                   JOIN pg_attribute attribute
                     ON attribute.attrelid = constraint_meta.conrelid
                    AND attribute.attnum = key.attnum
                   ORDER BY key.position
               )::TEXT[] AS columns,
               ARRAY(
                   SELECT attribute.attname::TEXT
                   FROM unnest(constraint_meta.confkey) WITH ORDINALITY AS key(attnum, position)
                   JOIN pg_attribute attribute
                     ON attribute.attrelid = constraint_meta.confrelid
                    AND attribute.attnum = key.attnum
                   ORDER BY key.position
               )::TEXT[] AS referenced_columns
        FROM pg_constraint constraint_meta
        JOIN pg_namespace namespace ON namespace.oid = constraint_meta.connamespace
        JOIN pg_class source_table ON source_table.oid = constraint_meta.conrelid
        JOIN pg_class referenced_table ON referenced_table.oid = constraint_meta.confrelid
        WHERE namespace.nspname = current_schema()
          AND constraint_meta.conname = $1 AND constraint_meta.contype = 'f'
        "#,
    )
    .bind(expected.name)
    .fetch_optional(&mut *connection)
    .await
    .map_err(verification_error)?
    .ok_or_else(|| {
        DatabaseStartupError::Verification(format!(
            "critical foreign key is missing: {}",
            expected.name
        ))
    })?;

    let table_name: String = row.try_get("table_name").map_err(verification_error)?;
    let referenced_table: String = row
        .try_get("referenced_table_name")
        .map_err(verification_error)?;
    let is_valid: bool = row.try_get("is_valid").map_err(verification_error)?;
    let delete_action: String = row.try_get("delete_action").map_err(verification_error)?;
    let columns: Vec<String> = row.try_get("columns").map_err(verification_error)?;
    let referenced_columns: Vec<String> = row
        .try_get("referenced_columns")
        .map_err(verification_error)?;
    let expected_columns: Vec<String> = expected
        .columns
        .iter()
        .map(|value| (*value).to_string())
        .collect();
    let expected_referenced_columns: Vec<String> = expected
        .referenced_columns
        .iter()
        .map(|value| (*value).to_string())
        .collect();
    if table_name != expected.table
        || referenced_table != expected.referenced_table
        || !is_valid
        || delete_action != expected.delete_action.to_string()
        || columns != expected_columns
        || referenced_columns != expected_referenced_columns
    {
        return Err(DatabaseStartupError::Verification(format!(
            "critical foreign key definition drifted: {}",
            expected.name
        )));
    }
    Ok(())
}

async fn verify_trigger(
    connection: &mut PgConnection,
    expected: TriggerSpec,
) -> Result<(), DatabaseStartupError> {
    let row = sqlx::query(
        r#"
        SELECT table_class.relname AS table_name,
               trigger_meta.tgenabled::TEXT AS enabled,
               trigger_meta.tgtype::SMALLINT AS trigger_type,
               function_meta.proname AS function_name,
               md5(function_meta.prosrc) AS source_md5,
               ARRAY(
                   SELECT attribute.attname::TEXT
                   FROM unnest(trigger_meta.tgattr::SMALLINT[]) AS key(attnum)
                   JOIN pg_attribute attribute
                     ON attribute.attrelid = trigger_meta.tgrelid
                    AND attribute.attnum = key.attnum
                   ORDER BY attribute.attname
               )::TEXT[] AS update_columns
        FROM pg_trigger trigger_meta
        JOIN pg_class table_class ON table_class.oid = trigger_meta.tgrelid
        JOIN pg_namespace namespace ON namespace.oid = table_class.relnamespace
        JOIN pg_proc function_meta ON function_meta.oid = trigger_meta.tgfoid
        WHERE namespace.nspname = current_schema()
          AND trigger_meta.tgname = $1 AND NOT trigger_meta.tgisinternal
        "#,
    )
    .bind(expected.name)
    .fetch_optional(&mut *connection)
    .await
    .map_err(verification_error)?
    .ok_or_else(|| {
        DatabaseStartupError::Verification(format!(
            "critical trigger is missing: {}",
            expected.name
        ))
    })?;
    let table_name: String = row.try_get("table_name").map_err(verification_error)?;
    let enabled: String = row.try_get("enabled").map_err(verification_error)?;
    let trigger_type: i16 = row.try_get("trigger_type").map_err(verification_error)?;
    let function_name: String = row.try_get("function_name").map_err(verification_error)?;
    let source_md5: String = row.try_get("source_md5").map_err(verification_error)?;
    let update_columns: Vec<String> = row.try_get("update_columns").map_err(verification_error)?;
    let expected_columns: Vec<String> = expected
        .update_columns
        .iter()
        .map(|value| (*value).to_string())
        .collect();
    if table_name != expected.table
        || enabled != "O"
        || trigger_type != expected.trigger_type
        || function_name != expected.function
        || update_columns != expected_columns
        || source_md5 != expected.source_md5
    {
        return Err(DatabaseStartupError::Verification(format!(
            "critical trigger definition drifted: {} (actual source md5 {source_md5})",
            expected.name
        )));
    }
    Ok(())
}

fn normalize_catalog_expression(value: &str) -> String {
    value
        .to_ascii_lowercase()
        .replace("::character varying", "")
        .replace("::varchar", "")
        .replace("::text[]", "")
        .replace("::text", "")
        .replace("array[", "[")
        .replace([' ', '\n', '\t', '(', ')', '"'], "")
}

fn verification_error(error: sqlx::Error) -> DatabaseStartupError {
    DatabaseStartupError::Verification(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::env;
    use std::time::Duration;

    use sqlx::postgres::PgPoolOptions;
    use sqlx::Executor;
    use tower::ServiceExt;

    use super::*;
    use crate::balance_dispatcher::{
        claim_operation_ids_for_test, enqueue_operation, mark_needs_review_for_test,
        persist_applied_for_test, EnqueueOperation, OperationKind,
    };
    use crate::config::{Config, SettlementMode};
    use crate::middleware::AdminUser;
    use crate::rebate_lifecycle::{finalize_ready_topup_event_records, INITIALIZATION_SECONDS};
    use crate::rebate_source_events::{
        ingest_rebate_source_event, receive_rebate_source_event, IngestError, IngestOutcome,
        RebateSourceEvent,
    };
    use crate::reconciliation_cli::{apply_for_test, plan, verify_for_test};
    use crate::routes::admin::{
        fetch_local_rebate_record_id_for_test, fetch_newapi_topup_rebate_order_ids_for_test,
        reopen_rebate_order_records, update_rebate_order_records, RebateOrderRecordIdsRequest,
        UpdateRebateOrderRecordsRequest,
    };
    use crate::routes::system::run_topup_reconciliation;
    use crate::session::SessionUser;
    use crate::AppState;

    const LATEST_LEGACY_FIXTURE: &str = include_str!("../tests/fixtures/latest_legacy.sql");
    const NEWAPI_ADJUSTMENT_FIXTURE: &str =
        include_str!("../tests/fixtures/newapi_balance_adjustments.sql");
    const NEWAPI_MARK_FIXTURE: &str = include_str!("../tests/fixtures/newapi_balance_marks.sql");

    #[test]
    fn check_verifier_accepts_known_equivalent_postgres_catalog_renderings() {
        let expected = CRITICAL_CHECKS
            .iter()
            .find(|check| check.name == "ck_invitation_rebate_payouts_kind")
            .unwrap();
        assert!(is_supported_check_definition(
            expected,
            expected.definition_md5
        ));
        assert!(is_supported_check_definition(
            expected,
            "22cc0e52a6d8cfeef0bc543b79ef005c"
        ));
        assert!(!is_supported_check_definition(
            expected,
            "00000000000000000000000000000000"
        ));
    }

    struct TestDatabase {
        admin_pool: PgPool,
        pool: PgPool,
        external_pool: PgPool,
        schema: String,
        external_schema: String,
    }

    impl TestDatabase {
        async fn create() -> Self {
            let database_url = env::var("TEST_DATABASE_URL")
                .expect("ignored migration tests require TEST_DATABASE_URL");
            let admin_pool = PgPoolOptions::new()
                .max_connections(2)
                .connect(&database_url)
                .await
                .expect("connect TEST_DATABASE_URL");
            let schema = format!(
                "invitations_test_{}",
                ulid::Ulid::new().to_string().to_lowercase()
            );
            let external_schema = format!(
                "newapi_test_{}",
                ulid::Ulid::new().to_string().to_lowercase()
            );
            admin_pool
                .execute(format!("CREATE SCHEMA {schema}").as_str())
                .await
                .expect("create test schema");
            admin_pool
                .execute(format!("CREATE SCHEMA {external_schema}").as_str())
                .await
                .expect("create external test schema");

            let search_path = schema.clone();
            let pool = PgPoolOptions::new()
                .max_connections(4)
                .after_connect(move |connection, _| {
                    let statement = format!("SET search_path TO {search_path}");
                    Box::pin(async move {
                        connection.execute(statement.as_str()).await?;
                        Ok(())
                    })
                })
                .connect(&database_url)
                .await
                .expect("connect isolated test schema");
            let external_search_path = external_schema.clone();
            let external_pool = PgPoolOptions::new()
                .max_connections(4)
                .after_connect(move |connection, _| {
                    let statement = format!("SET search_path TO {external_search_path}");
                    Box::pin(async move {
                        connection.execute(statement.as_str()).await?;
                        Ok(())
                    })
                })
                .connect(&database_url)
                .await
                .expect("connect isolated external test schema");
            Self {
                admin_pool,
                pool,
                external_pool,
                schema,
                external_schema,
            }
        }

        async fn preflight(&self) -> Result<PreflightReport, sqlx::Error> {
            let mut connection = self.pool.acquire().await?;
            let mut external_connection = self.external_pool.acquire().await?;
            preflight::run(
                &mut connection,
                &mut external_connection,
                PreflightMode::ReportOnly,
            )
            .await
        }

        async fn prepare(&self) -> Result<(), DatabaseStartupError> {
            prepare_database(&self.pool, &self.external_pool).await
        }

        async fn verify(&self) -> Result<(), DatabaseStartupError> {
            let mut connection = self.pool.acquire().await.unwrap();
            verify_schema(&mut connection).await
        }

        async fn create_external_mark_table(&self) {
            sqlx::raw_sql(NEWAPI_MARK_FIXTURE)
                .execute(&self.external_pool)
                .await
                .unwrap();
        }

        async fn create_external_adjustment_table(&self) {
            sqlx::raw_sql(NEWAPI_ADJUSTMENT_FIXTURE)
                .execute(&self.external_pool)
                .await
                .unwrap();
        }

        async fn cleanup(self) {
            self.pool.close().await;
            self.external_pool.close().await;
            self.admin_pool
                .execute(format!("DROP SCHEMA {} CASCADE", self.schema).as_str())
                .await
                .expect("drop test schema");
            self.admin_pool
                .execute(format!("DROP SCHEMA {} CASCADE", self.external_schema).as_str())
                .await
                .expect("drop external test schema");
            self.admin_pool.close().await;
        }
    }

    fn migrator_through_v6() -> Migrator {
        Migrator {
            migrations: Cow::Owned(
                MIGRATOR
                    .iter()
                    .filter(|migration| migration.version <= 202607110006)
                    .cloned()
                    .collect(),
            ),
            ..Migrator::DEFAULT
        }
    }

    async fn seed_legacy_redemption_history(pool: &PgPool) {
        sqlx::query(
            r#"
            INSERT INTO rebate_records (
                inviter_id, invitee_id, user_group, order_type, order_id,
                order_amount, order_time, rebate_effective_at, rebate_amount, rebate_rate,
                status, scan_started_at, initialization_ends_at, created_at, updated_at
            )
            SELECT 11, 10000 + redemption_id, 'default', 'topup', -redemption_id,
                   10.00, 1700000000 + redemption_id, 1700600000 + redemption_id,
                   250000, 0.0500, 'pending', 1700000000, 1700172800,
                   1700000000, 1700000000
            FROM generate_series(4000, 4639) AS redemption_id
            "#,
        )
        .execute(pool)
        .await
        .unwrap();

        for redemption_id in [4432_i64, 4494, 4865] {
            if redemption_id == 4865 {
                sqlx::query(
                    r#"
                    INSERT INTO rebate_records (
                        inviter_id, invitee_id, user_group, order_type, order_id,
                        order_amount, order_time, rebate_effective_at, rebate_amount, rebate_rate,
                        status, scan_started_at, initialization_ends_at, created_at, updated_at
                    ) VALUES (
                        11, $1, 'default', 'topup', $2, 10.00, $3, $4,
                        250000, 0.0500, 'pending', 1700000000, 1700172800,
                        1700000000, 1700000000
                    )
                    "#,
                )
                .bind(10000 + redemption_id)
                .bind(-redemption_id)
                .bind(1700000000 + redemption_id)
                .bind(1700600000 + redemption_id)
                .execute(pool)
                .await
                .unwrap();
            }

            sqlx::query(
                r#"
                INSERT INTO rebate_records (
                    inviter_id, invitee_id, user_group, order_type, order_id,
                    order_amount, order_time, rebate_effective_at, rebate_amount,
                    status, scan_started_at, initialization_ends_at, created_at, updated_at
                ) VALUES (
                    11, $1, 'default', 'redemption', $2, 10.00, $3, $4, 0,
                    'initializing', 1700000100, 1700172900, 1700000100, 1700000100
                )
                "#,
            )
            .bind(10000 + redemption_id)
            .bind(redemption_id)
            .bind(1700000000 + redemption_id)
            .bind(1700600000 + redemption_id)
            .execute(pool)
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn empty_database_migrates_and_reruns_idempotently() {
        let database = TestDatabase::create().await;
        let report = database.preflight().await.unwrap();
        assert!(report.safe_to_migrate);

        database.prepare().await.unwrap();
        database.prepare().await.unwrap();
        database.verify().await.unwrap();

        let migration_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations WHERE success = TRUE")
                .fetch_one(&database.pool)
                .await
                .unwrap();
        assert_eq!(migration_count, 7);
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn canonical_event_migration_preserves_legacy_redemptions_and_supersedes_duplicates() {
        let database = TestDatabase::create().await;
        migrator_through_v6().run(&database.pool).await.unwrap();
        seed_legacy_redemption_history(&database.pool).await;

        database.prepare().await.unwrap();
        database.verify().await.unwrap();

        let canonical_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM rebate_records WHERE canonical_event_key LIKE 'newapi:redemption:%'",
        )
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert_eq!(canonical_count, 641);

        let superseded: Vec<(i64, String, i64)> = sqlx::query_as(
            r#"
            SELECT duplicate.order_id, duplicate.status, master.order_id
            FROM rebate_records duplicate
            JOIN rebate_records master ON master.id = duplicate.superseded_by_record_id
            WHERE duplicate.order_type = 'redemption'
            ORDER BY duplicate.order_id
            "#,
        )
        .fetch_all(&database.pool)
        .await
        .unwrap();
        assert_eq!(
            superseded,
            vec![
                (4432, "closed".to_string(), -4432),
                (4494, "closed".to_string(), -4494),
                (4865, "closed".to_string(), -4865),
            ]
        );

        let resolved_cases: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM rebate_reconciliation_cases WHERE issue_code = 'legacy_redemption_duplicate_superseded' AND case_state = 'resolved'",
        )
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert_eq!(resolved_cases, 3);

        let master_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM rebate_records WHERE order_type = 'topup' AND order_id < 0",
        )
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert_eq!(master_count, 641);

        let superseded_id: i64 = sqlx::query_scalar(
            "SELECT id FROM rebate_records WHERE order_type = 'redemption' AND order_id = 4432",
        )
        .fetch_one(&database.pool)
        .await
        .unwrap();
        let master_id: i64 = sqlx::query_scalar(
            "SELECT id FROM rebate_records WHERE canonical_event_key = 'newapi:redemption:4432'",
        )
        .fetch_one(&database.pool)
        .await
        .unwrap();
        let state = AppState {
            config: Config {
                listen_addr: "127.0.0.1:0".to_string(),
                base_path: String::new(),
                database_url: "postgresql://test/invitations".to_string(),
                newapi_database_url: "postgresql://test/newapi".to_string(),
                session_secret: "test-session-secret".to_string(),
                rebate_event_ingest_secret: "test-rebate-event-ingest-secret".to_string(),
                sync_secret: None,
                auto_sync_interval_seconds: 0,
                auto_sync_initial_delay_seconds: 0,
                topup_reconciliation_interval_seconds: 0,
                topup_reconciliation_initial_delay_seconds: 0,
                topup_rebate_active_from_id: 1,
                settlement_mode: SettlementMode::ReadOnly,
                quota_per_unit: 500_000,
                balance_api: None,
                balance_dispatch_interval_milliseconds: 1_000,
            },
            pool: database.pool.clone(),
            newapi_pool: database.external_pool.clone(),
            balance_client: None,
        };
        assert_eq!(
            fetch_local_rebate_record_id_for_test(&state, "redemption", 4432)
                .await
                .unwrap(),
            Some(master_id)
        );
        let positive_legacy_id: i64 = sqlx::query_scalar(
            r#"
            INSERT INTO rebate_records (
                inviter_id, invitee_id, user_group, order_type, order_id,
                order_amount, order_time, rebate_effective_at, rebate_amount,
                status, scan_started_at, initialization_ends_at, created_at, updated_at
            ) VALUES (
                11, 19000, 'default', 'redemption', 9000,
                10.00, 1700009000, 1700609000, 0,
                'initializing', 1700009000, 1700181800, 1700009000, 1700009000
            )
            RETURNING id
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert_eq!(
            fetch_local_rebate_record_id_for_test(&state, "redemption", 9000)
                .await
                .unwrap(),
            Some(positive_legacy_id)
        );
        assert!(
            sqlx::query("UPDATE rebate_records SET status = 'pending' WHERE id = $1")
                .bind(superseded_id)
                .execute(&database.pool)
                .await
                .is_err()
        );
        assert!(sqlx::query(
            "UPDATE rebate_records SET superseded_by_record_id = NULL WHERE id = $1"
        )
        .bind(superseded_id)
        .execute(&database.pool)
        .await
        .is_err());

        database.prepare().await.unwrap();
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn canonical_event_migration_rejects_partial_legacy_redemption_history() {
        let database = TestDatabase::create().await;
        let migrator_v6 = Migrator {
            migrations: Cow::Owned(
                MIGRATOR
                    .iter()
                    .filter(|migration| migration.version <= 202607110006)
                    .cloned()
                    .collect(),
            ),
            ..Migrator::DEFAULT
        };
        migrator_v6.run(&database.pool).await.unwrap();
        sqlx::query(
            r#"
            INSERT INTO rebate_records (
                inviter_id, invitee_id, user_group, order_type, order_id,
                order_amount, order_time, rebate_effective_at, rebate_amount, rebate_rate,
                status, scan_started_at, initialization_ends_at, created_at, updated_at
            ) VALUES (
                11, 12, 'default', 'topup', -4000,
                10.00, 1700000000, 1700600000, 250000, 0.0500,
                'pending', 1700000000, 1700172800, 1700000000, 1700000000
            )
            "#,
        )
        .execute(&database.pool)
        .await
        .unwrap();

        let error = database.prepare().await.unwrap_err().to_string();
        assert!(error.contains("legacy redemption master count"));
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn canonical_event_migration_rejects_nonzero_duplicate_economic_state() {
        for drift in [
            "admin_adjusted",
            "rebate_amount",
            "rebate_rate",
            "request_ownership",
            "settlement",
            "calculation_snapshot",
            "balance_operation",
            "payout_attempt",
        ] {
            let database = TestDatabase::create().await;
            migrator_through_v6().run(&database.pool).await.unwrap();
            seed_legacy_redemption_history(&database.pool).await;
            let duplicate_id: i64 = sqlx::query_scalar(
                "SELECT id FROM rebate_records WHERE order_type = 'redemption' AND order_id = 4432",
            )
            .fetch_one(&database.pool)
            .await
            .unwrap();

            match drift {
                "admin_adjusted" => {
                    sqlx::query("UPDATE rebate_records SET admin_adjusted = TRUE WHERE id = $1")
                        .bind(duplicate_id)
                        .execute(&database.pool)
                        .await
                        .unwrap();
                }
                "rebate_amount" => {
                    sqlx::query("UPDATE rebate_records SET rebate_amount = 1 WHERE id = $1")
                        .bind(duplicate_id)
                        .execute(&database.pool)
                        .await
                        .unwrap();
                }
                "rebate_rate" => {
                    sqlx::query("UPDATE rebate_records SET rebate_rate = 0.0500 WHERE id = $1")
                        .bind(duplicate_id)
                        .execute(&database.pool)
                        .await
                        .unwrap();
                }
                "request_ownership" => {
                    sqlx::query(
                        r#"
                        INSERT INTO rebate_requests (
                            user_id, rebate_record_ids, total_amount, status, created_at, updated_at
                        ) VALUES (11, ARRAY[$1]::BIGINT[], 0, 'pending', 1, 1)
                        "#,
                    )
                    .bind(duplicate_id)
                    .execute(&database.pool)
                    .await
                    .unwrap();
                }
                "settlement" => {
                    sqlx::query(
                        r#"
                        UPDATE rebate_record_settlements
                        SET settlement_state = 'unsettled', state_version = 1
                        WHERE rebate_record_id = $1
                        "#,
                    )
                    .bind(duplicate_id)
                    .execute(&database.pool)
                    .await
                    .unwrap();
                }
                "calculation_snapshot" => {
                    sqlx::query(
                        r#"
                        INSERT INTO rebate_calculation_snapshots (
                            rebate_record_id, inviter_group, rule_id, rule_type, rebate_rate,
                            quota_per_unit, order_amount_minor, rebate_quota,
                            source_revision, snapshotted_at
                        ) VALUES ($1, 'default', NULL, 'redemption', 0, 500000, 1000, 0,
                                  'test-drift', 1)
                        "#,
                    )
                    .bind(duplicate_id)
                    .execute(&database.pool)
                    .await
                    .unwrap();
                }
                "balance_operation" => {
                    sqlx::query(
                        r#"
                        INSERT INTO balance_operations (
                            operation_ulid, source, business_operation_key, operation_kind,
                            rebate_record_id, user_id, delta_quota, idempotency_key,
                            payload_sha256, operation_state, created_at, updated_at
                        ) VALUES (
                            '01J00000000000000000000099', 'test', 'test:duplicate:4432', 'credit',
                            $1, 11, 1, 'test-duplicate-4432', repeat('a', 64), 'queued', 1, 1
                        )
                        "#,
                    )
                    .bind(duplicate_id)
                    .execute(&database.pool)
                    .await
                    .unwrap();
                }
                "payout_attempt" => {
                    sqlx::query(
                        r#"
                        INSERT INTO invitation_rebate_payout_attempts (
                            rebate_record_id, action, request_id, status, http_status, created_at
                        ) VALUES ($1, 'payout', 'test-duplicate-4432', 'failed', 500, 1)
                        "#,
                    )
                    .bind(duplicate_id)
                    .execute(&database.pool)
                    .await
                    .unwrap();
                }
                _ => unreachable!(),
            }

            let error = MIGRATOR.run(&database.pool).await.unwrap_err().to_string();
            assert!(
                error.contains("legacy redemption duplicate"),
                "unexpected migration result for {drift}: {error}"
            );
            database.cleanup().await;
        }
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn rebate_source_event_ingest_is_transactional_idempotent_and_conflict_detecting() {
        let database = TestDatabase::create().await;
        database.prepare().await.unwrap();
        sqlx::query(
            r#"
            INSERT INTO rebate_rules (
                user_group, rule_type, rebate_rate, effective_time, created_at, updated_at
            ) VALUES ('vip', 'topup', 0.0500, 1, 1, 1)
            "#,
        )
        .execute(&database.pool)
        .await
        .unwrap();
        let event = RebateSourceEvent {
            schema_version: 1,
            event_key: "newapi:topup:842".to_string(),
            event_type: "topup.completed".to_string(),
            source_order_id: 842,
            invitee_id: 3418,
            inviter_id: Some(1001),
            inviter_group: "vip".to_string(),
            rebate_base_amount_cents: Some(7500),
            credited_quota: None,
            completed_at: 1_784_211_072,
            payment_provider: "epay".to_string(),
        };

        let (first, concurrent_replay) = tokio::join!(
            ingest_rebate_source_event(&database.pool, event.clone(), 500_000, 1),
            ingest_rebate_source_event(&database.pool, event.clone(), 500_000, 1),
        );
        let outcomes = [first.unwrap(), concurrent_replay.unwrap()];
        assert!(outcomes.contains(&IngestOutcome::Created));
        assert!(outcomes.contains(&IngestOutcome::Replay));
        assert_eq!(
            ingest_rebate_source_event(&database.pool, event.clone(), 500_000, 1)
                .await
                .unwrap(),
            IngestOutcome::Replay
        );

        let record: (String, i64, i64, i64, i64, String) = sqlx::query_as(
            r#"
            SELECT canonical_event_key, order_amount_cents, order_time,
                   initialization_ends_at, rebate_amount, rebate_rate::TEXT
            FROM rebate_records
            WHERE canonical_event_key = 'newapi:topup:842'
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert_eq!(record.0, "newapi:topup:842");
        assert_eq!(record.1, 7500);
        assert_eq!(record.2, event.completed_at);
        assert_eq!(record.3, event.completed_at + 48 * 60 * 60);
        assert_eq!(record.4, 1_875_000);
        assert_eq!(record.5, "0.0500");

        let snapshot: (String, i64, i64) = sqlx::query_as(
            r#"
            SELECT inviter_group, order_amount_minor, rebate_quota
            FROM rebate_calculation_snapshots
            WHERE rebate_record_id = (
                SELECT id FROM rebate_records WHERE canonical_event_key = 'newapi:topup:842'
            )
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert_eq!(snapshot, ("vip".to_string(), 7500, 1_875_000));

        sqlx::query(
            r#"
            INSERT INTO rebate_records (
                inviter_id, invitee_id, user_group, order_type, order_id,
                order_amount, order_time, rebate_effective_at, rebate_amount, rebate_rate,
                status, scan_started_at, initialization_ends_at, created_at, updated_at
            ) VALUES (
                1001, 3419, 'vip', 'topup', 9999,
                10.00, $1, $2, 250000, 0.0500,
                'initializing', $1, $3, $1, $1
            )
            "#,
        )
        .bind(event.completed_at)
        .bind(event.completed_at + 7 * 24 * 60 * 60)
        .bind(event.completed_at + INITIALIZATION_SECONDS)
        .execute(&database.pool)
        .await
        .unwrap();

        assert_eq!(
            finalize_ready_topup_event_records(
                &database.pool,
                event.completed_at + INITIALIZATION_SECONDS
            )
            .await
            .unwrap(),
            1
        );
        let lifecycle_status: String = sqlx::query_scalar(
            "SELECT status FROM rebate_records WHERE canonical_event_key = 'newapi:topup:842'",
        )
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert_eq!(lifecycle_status, "pending");
        let legacy_status: String =
            sqlx::query_scalar("SELECT status FROM rebate_records WHERE order_id = 9999")
                .fetch_one(&database.pool)
                .await
                .unwrap();
        assert_eq!(legacy_status, "initializing");

        sqlx::query("DELETE FROM rebate_source_event_receipts WHERE event_key = $1")
            .bind(&event.event_key)
            .execute(&database.pool)
            .await
            .unwrap();
        sqlx::query("UPDATE rebate_rules SET rebate_rate = 0.9000 WHERE user_group = 'vip'")
            .execute(&database.pool)
            .await
            .unwrap();
        assert_eq!(
            ingest_rebate_source_event(&database.pool, event.clone(), 500_000, 1)
                .await
                .unwrap(),
            IngestOutcome::Replay
        );

        sqlx::query("DELETE FROM rebate_source_event_receipts WHERE event_key = $1")
            .bind(&event.event_key)
            .execute(&database.pool)
            .await
            .unwrap();
        sqlx::query(
            r#"
            UPDATE rebate_calculation_snapshots
            SET rebate_quota = rebate_quota + 1
            WHERE rebate_record_id = (
                SELECT id FROM rebate_records WHERE canonical_event_key = 'newapi:topup:842'
            )
            "#,
        )
        .execute(&database.pool)
        .await
        .unwrap();
        assert!(matches!(
            ingest_rebate_source_event(&database.pool, event.clone(), 500_000, 1).await,
            Err(IngestError::Conflict(_))
        ));
        let missing_receipt: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM rebate_source_event_receipts WHERE event_key = $1",
        )
        .bind(&event.event_key)
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert_eq!(missing_receipt, 0);
        sqlx::query(
            r#"
            UPDATE rebate_calculation_snapshots
            SET rebate_quota = 1875000
            WHERE rebate_record_id = (
                SELECT id FROM rebate_records WHERE canonical_event_key = 'newapi:topup:842'
            )
            "#,
        )
        .execute(&database.pool)
        .await
        .unwrap();
        assert_eq!(
            ingest_rebate_source_event(&database.pool, event.clone(), 500_000, 1)
                .await
                .unwrap(),
            IngestOutcome::Replay
        );

        let mut conflicting = event.clone();
        conflicting.rebate_base_amount_cents = Some(7600);
        assert!(matches!(
            ingest_rebate_source_event(&database.pool, conflicting, 500_000, 1).await,
            Err(IngestError::Conflict(_))
        ));

        let mut invalid_rule_event = event;
        invalid_rule_event.source_order_id = 843;
        invalid_rule_event.event_key = "newapi:topup:843".to_string();
        sqlx::query("UPDATE rebate_rules SET rebate_rate = 1.5000 WHERE user_group = 'vip'")
            .execute(&database.pool)
            .await
            .unwrap();
        assert!(matches!(
            ingest_rebate_source_event(&database.pool, invalid_rule_event, 500_000, 1).await,
            Err(IngestError::Invalid(_))
        ));
        let rolled_back: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM rebate_records WHERE canonical_event_key = 'newapi:topup:843'",
        )
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert_eq!(rolled_back, 0);
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn online_ingest_reuses_only_unadjusted_matching_initializing_records() {
        let database = TestDatabase::create().await;
        database.prepare().await.unwrap();
        sqlx::query(
            r#"
            INSERT INTO rebate_rules (
                user_group, rule_type, rebate_rate, effective_time, created_at, updated_at
            ) VALUES ('vip', 'topup', 0.0500, 1, 1, 1)
            "#,
        )
        .execute(&database.pool)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO rebate_records (
                inviter_id, invitee_id, user_group, order_type, order_id,
                order_amount, order_time, rebate_effective_at, rebate_amount, rebate_rate,
                status, scan_started_at, initialization_ends_at, finalized_at,
                admin_adjusted, closed_at, created_at, updated_at,
                order_amount_cents, credited_quota
            ) VALUES
                (1001, 3418, 'vip', 'topup', 850, 75.00, 1784211072, 1784815872,
                 1875000, 0.0500, 'initializing', 1784211072, 1784383872, NULL,
                 FALSE, NULL, 1784211072, 1784211072, 7500, 37500000),
                (1001, 3418, 'vip', 'topup', 851, 75.00, 1784211072, 1784815872,
                 1875000, 0.0500, 'initializing', 1784211072, 1784383872, NULL,
                 TRUE, NULL, 1784211072, 1784211072, 7500, 37500000),
                (1001, 3418, 'vip', 'topup', 852, 75.00, 1784211072, 1784815872,
                 1875000, 0.0500, 'pending', 1784211072, 1784383872, 1784383872,
                 FALSE, NULL, 1784211072, 1784211072, 7500, 37500000),
                (1001, 3418, 'vip', 'topup', 853, 75.00, 1784211072, 1784815872,
                 1875000, 0.0500, 'closed', 1784211072, 1784383872, NULL,
                 FALSE, 1784212000, 1784211072, 1784211072, 7500, 37500000)
            "#,
        )
        .execute(&database.pool)
        .await
        .unwrap();

        let event = |source_order_id| RebateSourceEvent {
            schema_version: 1,
            event_key: format!("newapi:topup:{source_order_id}"),
            event_type: "topup.completed".to_string(),
            source_order_id,
            invitee_id: 3418,
            inviter_id: Some(1001),
            inviter_group: "vip".to_string(),
            rebate_base_amount_cents: Some(7500),
            credited_quota: None,
            completed_at: 1_784_211_072,
            payment_provider: "epay".to_string(),
        };

        assert_eq!(
            ingest_rebate_source_event(&database.pool, event(850), 500_000, 1)
                .await
                .unwrap(),
            IngestOutcome::Created
        );
        for source_order_id in [851_i64, 852, 853] {
            assert!(matches!(
                ingest_rebate_source_event(&database.pool, event(source_order_id), 500_000, 1)
                    .await,
                Err(IngestError::Conflict(_))
            ));
        }

        let attached: Vec<(i64, Option<String>, i64, i64)> = sqlx::query_as(
            r#"
            SELECT record.order_id, record.canonical_event_key,
                   COUNT(receipt.id)::BIGINT, COUNT(snapshot.rebate_record_id)::BIGINT
            FROM rebate_records record
            LEFT JOIN rebate_source_event_receipts receipt
              ON receipt.rebate_record_id = record.id
            LEFT JOIN rebate_calculation_snapshots snapshot
              ON snapshot.rebate_record_id = record.id
            WHERE record.order_id BETWEEN 850 AND 853
            GROUP BY record.id, record.order_id, record.canonical_event_key
            ORDER BY record.order_id
            "#,
        )
        .fetch_all(&database.pool)
        .await
        .unwrap();
        assert_eq!(
            attached,
            vec![
                (850, Some("newapi:topup:850".to_string()), 1, 1),
                (851, None, 0, 0),
                (852, None, 0, 0),
                (853, None, 0, 0),
            ]
        );
        let v2_event = RebateSourceEvent {
            schema_version: 2,
            event_key: "newapi:topup:854".to_string(),
            event_type: "topup.completed".to_string(),
            source_order_id: 854,
            invitee_id: 3418,
            inviter_id: Some(1001),
            inviter_group: "vip".to_string(),
            rebate_base_amount_cents: None,
            credited_quota: Some(37_500_000),
            completed_at: 1_784_211_072,
            payment_provider: "epay".to_string(),
        };
        assert_eq!(
            ingest_rebate_source_event(&database.pool, v2_event, 500_000, 1)
                .await
                .unwrap(),
            IngestOutcome::Created
        );
        let v2_accounting: (Option<i64>, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT record.order_amount_cents, record.credited_quota,
                   record.rebate_amount, snapshot.credited_quota
            FROM rebate_records record
            JOIN rebate_calculation_snapshots snapshot ON snapshot.rebate_record_id = record.id
            WHERE record.canonical_event_key = 'newapi:topup:854'
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert_eq!(v2_accounting, (None, 37_500_000, 1_875_000, 37_500_000));
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn rebate_source_event_http_contract_classifies_replays_and_permanent_errors() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use axum::routing::post;
        use axum::Router;

        let database = TestDatabase::create().await;
        database.prepare().await.unwrap();
        sqlx::query(
            r#"
            INSERT INTO rebate_rules (
                user_group, rule_type, rebate_rate, effective_time, created_at, updated_at
            ) VALUES ('default', 'topup', 0.0500, 1, 1, 1)
            "#,
        )
        .execute(&database.pool)
        .await
        .unwrap();
        let secret = "0123456789abcdef0123456789abcdef";
        let state = AppState {
            config: Config {
                listen_addr: "127.0.0.1:0".to_string(),
                base_path: String::new(),
                database_url: "postgresql://test/invitations".to_string(),
                newapi_database_url: "postgresql://test/newapi".to_string(),
                session_secret: "test-session-secret".to_string(),
                rebate_event_ingest_secret: secret.to_string(),
                sync_secret: None,
                auto_sync_interval_seconds: 0,
                auto_sync_initial_delay_seconds: 0,
                topup_reconciliation_interval_seconds: 0,
                topup_reconciliation_initial_delay_seconds: 0,
                topup_rebate_active_from_id: 9001,
                settlement_mode: SettlementMode::ReadOnly,
                quota_per_unit: 500_000,
                balance_api: None,
                balance_dispatch_interval_milliseconds: 1_000,
            },
            pool: database.pool.clone(),
            newapi_pool: database.external_pool.clone(),
            balance_client: None,
        };
        let app = Router::new()
            .route(
                "/api/internal/rebate-source-events",
                post(receive_rebate_source_event),
            )
            .with_state(state);
        let event = serde_json::json!({
            "schema_version": 1,
            "event_key": "newapi:topup:9001",
            "event_type": "topup.completed",
            "source_order_id": 9001,
            "invitee_id": 3418,
            "inviter_id": 1001,
            "inviter_group": "default",
            "rebate_base_amount_cents": 7500,
            "completed_at": 1784211072_i64,
            "payment_provider": "epay"
        });
        let request = |payload: serde_json::Value, authorization: Option<&str>| {
            let mut builder = Request::builder()
                .method("POST")
                .uri("/api/internal/rebate-source-events")
                .header("content-type", "application/json");
            if let Some(authorization) = authorization {
                builder = builder.header("authorization", authorization);
            }
            builder.body(Body::from(payload.to_string())).unwrap()
        };
        let bearer = format!("Bearer {secret}");

        assert_eq!(
            app.clone()
                .oneshot(request(event.clone(), None))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        let mut before_active_boundary = event.clone();
        before_active_boundary["source_order_id"] = serde_json::json!(9000);
        before_active_boundary["event_key"] = serde_json::json!("newapi:topup:9000");
        assert_eq!(
            app.clone()
                .oneshot(request(before_active_boundary, Some(&bearer)))
                .await
                .unwrap()
                .status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(
            app.clone()
                .oneshot(request(event.clone(), Some(&bearer)))
                .await
                .unwrap()
                .status(),
            StatusCode::CREATED
        );
        let mut replay_with_unknown_field = event.clone();
        replay_with_unknown_field["future_field"] = serde_json::json!("accepted");
        assert_eq!(
            app.clone()
                .oneshot(request(replay_with_unknown_field, Some(&bearer)))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        let v2_event = serde_json::json!({
            "schema_version": 2,
            "event_key": "newapi:topup:9010",
            "event_type": "topup.completed",
            "source_order_id": 9010,
            "invitee_id": 3418,
            "inviter_id": 1001,
            "inviter_group": "default",
            "credited_quota": 37500000_i64,
            "completed_at": 1784211072_i64,
            "payment_provider": "epay"
        });
        assert_eq!(
            app.clone()
                .oneshot(request(v2_event.clone(), Some(&bearer)))
                .await
                .unwrap()
                .status(),
            StatusCode::CREATED
        );
        let mut invalid_v2 = v2_event;
        invalid_v2["rebate_base_amount_cents"] = serde_json::json!(7500);
        assert_eq!(
            app.clone()
                .oneshot(request(invalid_v2, Some(&bearer)))
                .await
                .unwrap()
                .status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        let mut conflict = event.clone();
        conflict["rebate_base_amount_cents"] = serde_json::json!(7600);
        assert_eq!(
            app.clone()
                .oneshot(request(conflict, Some(&bearer)))
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );
        let mut no_inviter = event.clone();
        no_inviter["source_order_id"] = serde_json::json!(9002);
        no_inviter["event_key"] = serde_json::json!("newapi:topup:9002");
        no_inviter["inviter_id"] = serde_json::Value::Null;
        assert_eq!(
            app.clone()
                .oneshot(request(no_inviter.clone(), Some(&bearer)))
                .await
                .unwrap()
                .status(),
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            app.clone()
                .oneshot(request(no_inviter.clone(), Some(&bearer)))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        no_inviter["inviter_id"] = serde_json::json!(1001);
        assert_eq!(
            app.clone()
                .oneshot(request(no_inviter, Some(&bearer)))
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );
        let mut invalid = event;
        invalid["source_order_id"] = serde_json::json!(-1);
        assert_eq!(
            app.oneshot(request(invalid, Some(&bearer)))
                .await
                .unwrap()
                .status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn reconciliation_manifest_apply_is_repeatable_and_verify_is_read_only() {
        let database = TestDatabase::create().await;
        database.prepare().await.unwrap();
        sqlx::query(
            r#"
            INSERT INTO rebate_rules (
                user_group, rule_type, rebate_rate, effective_time, created_at, updated_at
            ) VALUES ('vip', 'topup', 0.0500, 1, 1, 1)
            "#,
        )
        .execute(&database.pool)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO rebate_records (
                inviter_id, invitee_id, user_group, order_type, order_id,
                order_amount, order_time, rebate_effective_at, rebate_amount, rebate_rate,
                status, scan_started_at, initialization_ends_at, closed_at, created_at, updated_at
            ) VALUES
                (1001, 3418, 'vip', 'topup', 426,
                 75.00, 1779659545, 1780264345, 0, NULL,
                 'closed', 1779659545, 1779832345, 1780711715, 1779659545, 1780711715),
                (1001, 3418, 'vip', 'topup', 842,
                 75.00, 1784211000, 1784815800, 1875000, 0.0500,
                 'initializing', 1784211000, 1784383800, NULL, 1784211000, 1784211000)
            "#,
        )
        .execute(&database.pool)
        .await
        .unwrap();
        sqlx::raw_sql(
            r#"
            CREATE TABLE users (
                id BIGINT PRIMARY KEY,
                inviter_id BIGINT,
                "group" VARCHAR(64),
                display_name VARCHAR(255),
                username VARCHAR(255)
            );
            CREATE TABLE top_ups (
                id BIGINT PRIMARY KEY,
                user_id BIGINT NOT NULL,
                money DOUBLE PRECISION NOT NULL,
                amount DOUBLE PRECISION NOT NULL,
                complete_time BIGINT NOT NULL,
                create_time BIGINT NOT NULL,
                payment_provider VARCHAR(50),
                payment_method VARCHAR(50),
                trade_no VARCHAR(255) NOT NULL,
                status VARCHAR(20) NOT NULL
            );
            CREATE TABLE subscription_orders (
                id BIGINT PRIMARY KEY,
                user_id BIGINT NOT NULL,
                money DOUBLE PRECISION NOT NULL,
                complete_time BIGINT NOT NULL,
                create_time BIGINT NOT NULL,
                trade_no VARCHAR(255) NOT NULL,
                status VARCHAR(20) NOT NULL
            );
            CREATE TABLE redemptions (
                id BIGINT PRIMARY KEY,
                used_user_id BIGINT NOT NULL,
                quota BIGINT NOT NULL,
                redeemed_time BIGINT NOT NULL,
                created_time BIGINT NOT NULL,
                status INTEGER NOT NULL
            );
            INSERT INTO users (id, inviter_id, "group", display_name, username) VALUES
                (1001, NULL, 'vip', 'Inviter', 'inviter'),
                (3418, 1001, 'default', 'Invitee', 'invitee');
            INSERT INTO top_ups (
                id, user_id, money, amount, complete_time, create_time,
                payment_provider, payment_method, trade_no, status
            ) VALUES
                (426, 3418, 75.00, 75.00, 0, 1779659545,
                 'epay', 'alipay', 'trade-426', 'success'),
                (842, 3418, 75.00, 75.00, 1784211072, 1784211000,
                 'epay', 'alipay', 'trade-842', 'success');
            "#,
        )
        .execute(&database.external_pool)
        .await
        .unwrap();

        let active_scope_state = AppState {
            config: Config {
                listen_addr: "127.0.0.1:0".to_string(),
                base_path: String::new(),
                database_url: "postgresql://test/invitations".to_string(),
                newapi_database_url: "postgresql://test/newapi".to_string(),
                session_secret: "test-session-secret".to_string(),
                rebate_event_ingest_secret: "test-rebate-event-ingest-secret".to_string(),
                sync_secret: None,
                auto_sync_interval_seconds: 0,
                auto_sync_initial_delay_seconds: 0,
                topup_reconciliation_interval_seconds: 0,
                topup_reconciliation_initial_delay_seconds: 0,
                topup_rebate_active_from_id: 842,
                settlement_mode: SettlementMode::ReadOnly,
                quota_per_unit: 500_000,
                balance_api: None,
                balance_dispatch_interval_milliseconds: 1_000,
            },
            pool: database.pool.clone(),
            newapi_pool: database.external_pool.clone(),
            balance_client: None,
        };
        assert_eq!(
            fetch_newapi_topup_rebate_order_ids_for_test(&active_scope_state)
                .await
                .unwrap(),
            (1, vec![842])
        );
        let historical_record_id: i64 = sqlx::query_scalar(
            "SELECT id FROM rebate_records WHERE order_type = 'topup' AND order_id = 426",
        )
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert!(reopen_rebate_order_records(
            axum::extract::State(active_scope_state.clone()),
            AdminUser(SessionUser {
                id: 1,
                username: "admin".to_string(),
                role: 10,
                status: 1,
                group: "default".to_string(),
            }),
            axum::Json(RebateOrderRecordIdsRequest {
                record_ids: vec![historical_record_id],
            }),
        )
        .await
        .is_err());
        let historical_status: String =
            sqlx::query_scalar("SELECT status FROM rebate_records WHERE id = $1")
                .bind(historical_record_id)
                .fetch_one(&database.pool)
                .await
                .unwrap();
        assert_eq!(historical_status, "closed");

        sqlx::query("UPDATE rebate_records SET status = 'pending' WHERE order_id = 842")
            .execute(&database.pool)
            .await
            .unwrap();
        assert!(
            plan(&database.pool, &database.external_pool, 500_000, 842, 842)
                .await
                .unwrap_err()
                .to_string()
                .contains("not initializing")
        );
        sqlx::query(
            "UPDATE rebate_records SET status = 'initializing', admin_adjusted = TRUE WHERE order_id = 842",
        )
        .execute(&database.pool)
        .await
        .unwrap();
        assert!(
            plan(&database.pool, &database.external_pool, 500_000, 842, 842)
                .await
                .unwrap_err()
                .to_string()
                .contains("admin-adjusted")
        );
        sqlx::query("UPDATE rebate_records SET admin_adjusted = FALSE WHERE order_id = 842")
            .execute(&database.pool)
            .await
            .unwrap();

        let manifest = plan(&database.pool, &database.external_pool, 500_000, 842, 842)
            .await
            .unwrap();
        assert_eq!(manifest.orders.len(), 1);
        assert_eq!(manifest.orders[0].inviter_group, "vip");
        assert_eq!(manifest.orders[0].order_amount_cents, None);
        assert_eq!(manifest.orders[0].credited_quota, 37_500_000);
        assert_eq!(manifest.orders[0].expected_rebate_quota, 1_875_000);
        assert_eq!(
            manifest.orders[0].existing_record,
            Some(crate::reconciliation_cli::ManifestRecordState {
                record_id: manifest.orders[0]
                    .existing_record
                    .as_ref()
                    .unwrap()
                    .record_id,
                status: "initializing".to_string(),
                canonical_event_key: None,
                credited_quota: None,
                rebate_amount: 1_875_000,
                rebate_rate: Some("0.0500".to_string()),
                order_time: 1_784_211_000,
                initialization_ends_at: 1_784_383_800,
                rebate_effective_at: 1_784_815_800,
                admin_adjusted: false,
            })
        );

        sqlx::query("UPDATE rebate_rules SET rebate_rate = 0.0600 WHERE user_group = 'vip'")
            .execute(&database.pool)
            .await
            .unwrap();
        assert!(
            apply_for_test(&database.pool, &database.external_pool, manifest.clone())
                .await
                .unwrap_err()
                .to_string()
                .contains("source data changed")
        );
        let unapplied: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM rebate_records WHERE canonical_event_key = 'newapi:topup:842'",
        )
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert_eq!(unapplied, 0);
        sqlx::query("UPDATE rebate_rules SET rebate_rate = 0.0500 WHERE user_group = 'vip'")
            .execute(&database.pool)
            .await
            .unwrap();

        let accounting_drifts = [
            (
                "UPDATE rebate_records SET credited_quota = 1 WHERE order_id = 842",
                "UPDATE rebate_records SET credited_quota = NULL WHERE order_id = 842",
            ),
            (
                "UPDATE rebate_records SET rebate_amount = rebate_amount + 1 WHERE order_id = 842",
                "UPDATE rebate_records SET rebate_amount = 1875000 WHERE order_id = 842",
            ),
            (
                "UPDATE rebate_records SET rebate_rate = 0.0600 WHERE order_id = 842",
                "UPDATE rebate_records SET rebate_rate = 0.0500 WHERE order_id = 842",
            ),
            (
                "UPDATE rebate_records SET order_time = order_time + 1 WHERE order_id = 842",
                "UPDATE rebate_records SET order_time = 1784211000 WHERE order_id = 842",
            ),
            (
                "UPDATE rebate_records SET initialization_ends_at = initialization_ends_at + 1 WHERE order_id = 842",
                "UPDATE rebate_records SET initialization_ends_at = 1784383800 WHERE order_id = 842",
            ),
            (
                "UPDATE rebate_records SET rebate_effective_at = rebate_effective_at + 1 WHERE order_id = 842",
                "UPDATE rebate_records SET rebate_effective_at = 1784815800 WHERE order_id = 842",
            ),
            (
                "UPDATE rebate_records SET admin_adjusted = TRUE WHERE order_id = 842",
                "UPDATE rebate_records SET admin_adjusted = FALSE WHERE order_id = 842",
            ),
            (
                "UPDATE rebate_records SET status = 'pending' WHERE order_id = 842",
                "UPDATE rebate_records SET status = 'initializing' WHERE order_id = 842",
            ),
        ];
        for (drift, restore) in accounting_drifts {
            sqlx::query(drift).execute(&database.pool).await.unwrap();
            assert!(
                apply_for_test(&database.pool, &database.external_pool, manifest.clone())
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("source data changed")
            );
            sqlx::query(restore).execute(&database.pool).await.unwrap();
        }

        assert_eq!(
            apply_for_test(&database.pool, &database.external_pool, manifest.clone())
                .await
                .unwrap(),
            (1, 0)
        );
        let repaired_times: (i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT order_time, initialization_ends_at, rebate_effective_at
            FROM rebate_records
            WHERE canonical_event_key = 'newapi:topup:842'
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert_eq!(
            repaired_times,
            (
                1_784_211_072,
                1_784_211_072 + 48 * 60 * 60,
                1_784_211_072 + 7 * 24 * 60 * 60,
            )
        );
        sqlx::query(
            "UPDATE rebate_records SET rebate_amount = rebate_amount + 1 WHERE canonical_event_key = 'newapi:topup:842'",
        )
        .execute(&database.pool)
        .await
        .unwrap();
        assert!(
            apply_for_test(&database.pool, &database.external_pool, manifest.clone())
                .await
                .unwrap_err()
                .to_string()
                .contains("source data changed")
        );
        assert!(
            verify_for_test(&database.pool, &database.external_pool, manifest.clone())
                .await
                .unwrap_err()
                .to_string()
                .contains("verification failed")
        );
        sqlx::query(
            "UPDATE rebate_records SET rebate_amount = 1875000 WHERE canonical_event_key = 'newapi:topup:842'",
        )
        .execute(&database.pool)
        .await
        .unwrap();
        sqlx::query(
            r#"
            UPDATE rebate_calculation_snapshots
            SET rebate_quota = rebate_quota + 1
            WHERE rebate_record_id = (
                SELECT id FROM rebate_records WHERE canonical_event_key = 'newapi:topup:842'
            )
            "#,
        )
        .execute(&database.pool)
        .await
        .unwrap();
        assert!(
            apply_for_test(&database.pool, &database.external_pool, manifest.clone())
                .await
                .unwrap_err()
                .to_string()
                .contains("source data changed")
        );
        assert!(
            verify_for_test(&database.pool, &database.external_pool, manifest.clone())
                .await
                .unwrap_err()
                .to_string()
                .contains("verification failed")
        );
        sqlx::query(
            r#"
            UPDATE rebate_calculation_snapshots
            SET rebate_quota = 1875000
            WHERE rebate_record_id = (
                SELECT id FROM rebate_records WHERE canonical_event_key = 'newapi:topup:842'
            )
            "#,
        )
        .execute(&database.pool)
        .await
        .unwrap();
        assert_eq!(
            apply_for_test(&database.pool, &database.external_pool, manifest.clone())
                .await
                .unwrap(),
            (0, 1)
        );
        assert_eq!(
            verify_for_test(&database.pool, &database.external_pool, manifest.clone())
                .await
                .unwrap(),
            1
        );

        sqlx::query("UPDATE top_ups SET money = 76.00 WHERE id = 842")
            .execute(&database.external_pool)
            .await
            .unwrap();
        assert_eq!(
            apply_for_test(&database.pool, &database.external_pool, manifest.clone())
                .await
                .unwrap(),
            (0, 1)
        );
        sqlx::query("UPDATE top_ups SET amount = 76.00 WHERE id = 842")
            .execute(&database.external_pool)
            .await
            .unwrap();
        assert!(
            apply_for_test(&database.pool, &database.external_pool, manifest)
                .await
                .unwrap_err()
                .to_string()
                .contains("source data changed")
        );
        let record_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM rebate_records WHERE canonical_event_key = 'newapi:topup:842'",
        )
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert_eq!(record_count, 1);

        sqlx::query("UPDATE top_ups SET complete_time = 0 WHERE id = 842")
            .execute(&database.external_pool)
            .await
            .unwrap();
        let completion_error = plan(&database.pool, &database.external_pool, 500_000, 842, 842)
            .await
            .unwrap_err();
        assert_eq!(completion_error.code(), "completion_evidence_required");
        assert!(completion_error.to_string().contains("topup_id=842"));
        assert!(completion_error.to_string().contains("trade-842"));
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn admin_rate_adjustment_uses_v2_credited_quota_and_legacy_cents() {
        let database = TestDatabase::create().await;
        database.prepare().await.unwrap();
        let record_ids: Vec<i64> = sqlx::query_scalar(
            r#"
            INSERT INTO rebate_records (
                inviter_id, invitee_id, user_group, order_type, order_id,
                order_amount, order_time, rebate_effective_at, rebate_amount, rebate_rate,
                status, scan_started_at, initialization_ends_at, created_at, updated_at,
                source_schema_version, credited_quota
            ) VALUES
                (1001, 3418, 'default', 'topup', 850, 0, 1, 1, 0, 0.01,
                 'pending', 1, 1, 1, 1, 2, 10),
                (1001, 3419, 'default', 'topup', 851, 75, 1, 1, 0, 0.01,
                 'pending', 1, 1, 1, 1, NULL, NULL)
            RETURNING id
            "#,
        )
        .fetch_all(&database.pool)
        .await
        .unwrap();
        let state = AppState {
            config: Config {
                listen_addr: "127.0.0.1:0".to_string(),
                base_path: String::new(),
                database_url: "postgresql://test/invitations".to_string(),
                newapi_database_url: "postgresql://test/newapi".to_string(),
                session_secret: "test-session-secret".to_string(),
                rebate_event_ingest_secret: "test-rebate-event-ingest-secret".to_string(),
                sync_secret: None,
                auto_sync_interval_seconds: 0,
                auto_sync_initial_delay_seconds: 0,
                topup_reconciliation_interval_seconds: 0,
                topup_reconciliation_initial_delay_seconds: 0,
                topup_rebate_active_from_id: 1,
                settlement_mode: SettlementMode::ReadOnly,
                quota_per_unit: 500_000,
                balance_api: None,
                balance_dispatch_interval_milliseconds: 1_000,
            },
            pool: database.pool.clone(),
            newapi_pool: database.external_pool.clone(),
            balance_client: None,
        };
        let _response = update_rebate_order_records(
            axum::extract::State(state),
            AdminUser(SessionUser {
                id: 1,
                username: "admin".to_string(),
                role: 10,
                status: 1,
                group: "default".to_string(),
            }),
            axum::Json(UpdateRebateOrderRecordsRequest {
                record_ids: record_ids.clone(),
                rebate_amount: None,
                rebate_ratio: Some(0.05),
            }),
        )
        .await
        .unwrap();

        let adjusted: Vec<(i64, i64, bool)> = sqlx::query_as(
            r#"
            SELECT order_id, rebate_amount, admin_adjusted
            FROM rebate_records
            WHERE id = ANY($1)
            ORDER BY order_id
            "#,
        )
        .bind(&record_ids)
        .fetch_all(&database.pool)
        .await
        .unwrap();
        assert_eq!(adjusted, vec![(850, 1, true), (851, 1_875_000, true)]);
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn topup_reconciliation_uses_receipt_outcomes_not_current_inviter_relationships() {
        let database = TestDatabase::create().await;
        database.prepare().await.unwrap();
        sqlx::query(
            r#"
            INSERT INTO rebate_rules (
                user_group, rule_type, rebate_rate, effective_time, created_at, updated_at
            ) VALUES ('default', 'topup', 0.0500, 1, 1, 1)
            "#,
        )
        .execute(&database.pool)
        .await
        .unwrap();
        let completed_at = chrono::Utc::now().timestamp() - 120;
        let event = |source_order_id, invitee_id, inviter_id| RebateSourceEvent {
            schema_version: 1,
            event_key: format!("newapi:topup:{source_order_id}"),
            event_type: "topup.completed".to_string(),
            source_order_id,
            invitee_id,
            inviter_id,
            inviter_group: "default".to_string(),
            rebate_base_amount_cents: Some(7500),
            credited_quota: None,
            completed_at,
            payment_provider: "epay".to_string(),
        };
        assert_eq!(
            ingest_rebate_source_event(&database.pool, event(9001, 3418, Some(1001)), 500_000, 1,)
                .await
                .unwrap(),
            IngestOutcome::Created
        );
        assert_eq!(
            ingest_rebate_source_event(&database.pool, event(9002, 3419, None), 500_000, 1,)
                .await
                .unwrap(),
            IngestOutcome::IgnoredNoInviter
        );

        sqlx::raw_sql(
            r#"
            CREATE TABLE users (
                id BIGINT PRIMARY KEY,
                inviter_id BIGINT
            );
            CREATE TABLE top_ups (
                id BIGINT PRIMARY KEY,
                user_id BIGINT NOT NULL,
                complete_time BIGINT NOT NULL,
                create_time BIGINT NOT NULL,
                trade_no VARCHAR(255) NOT NULL,
                status VARCHAR(20) NOT NULL
            );
            CREATE TABLE subscription_orders (trade_no VARCHAR(255));
            CREATE TABLE kkai_outbox (
                event_key VARCHAR(191) NOT NULL,
                topic VARCHAR(128) NOT NULL,
                status VARCHAR(20) NOT NULL
            );
            INSERT INTO users (id, inviter_id) VALUES
                (3418, NULL),
                (3419, 1001),
                (3420, NULL);
            "#,
        )
        .execute(&database.external_pool)
        .await
        .unwrap();
        for (order_id, user_id) in [
            (9000_i64, 3420_i64),
            (9001, 3418),
            (9002, 3419),
            (9003, 3420),
        ] {
            sqlx::query(
                r#"
                INSERT INTO top_ups (
                    id, user_id, complete_time, create_time, trade_no, status
                ) VALUES ($1, $2, $3, $3, $4, 'success')
                "#,
            )
            .bind(order_id)
            .bind(user_id)
            .bind(completed_at)
            .bind(format!("trade-{order_id}"))
            .execute(&database.external_pool)
            .await
            .unwrap();
            if !matches!(order_id, 9000 | 9002) {
                sqlx::query(
                    r#"
                    INSERT INTO kkai_outbox (event_key, topic, status)
                    VALUES ($1, 'kkai.billing.topup.completed', 'delivered')
                    "#,
                )
                .bind(format!("newapi:topup:{order_id}"))
                .execute(&database.external_pool)
                .await
                .unwrap();
            }
        }

        let state = AppState {
            config: Config {
                listen_addr: "127.0.0.1:0".to_string(),
                base_path: String::new(),
                database_url: "postgresql://test/invitations".to_string(),
                newapi_database_url: "postgresql://test/newapi".to_string(),
                session_secret: "test-session-secret".to_string(),
                rebate_event_ingest_secret: "test-rebate-event-ingest-secret".to_string(),
                sync_secret: None,
                auto_sync_interval_seconds: 0,
                auto_sync_initial_delay_seconds: 0,
                topup_reconciliation_interval_seconds: 300,
                topup_reconciliation_initial_delay_seconds: 0,
                topup_rebate_active_from_id: 9001,
                settlement_mode: SettlementMode::ReadOnly,
                quota_per_unit: 500_000,
                balance_api: None,
                balance_dispatch_interval_milliseconds: 1_000,
            },
            pool: database.pool.clone(),
            newapi_pool: database.external_pool.clone(),
            balance_client: None,
        };
        let first = run_topup_reconciliation(&state).await.unwrap();
        assert_eq!(first.created_cases, 1);
        let open_issue_codes: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT issue_code
            FROM rebate_reconciliation_cases
            WHERE case_state = 'open'
            ORDER BY issue_code
            "#,
        )
        .fetch_all(&database.pool)
        .await
        .unwrap();
        assert_eq!(
            open_issue_codes,
            vec!["topup_event_receipt_missing".to_string()]
        );

        sqlx::query(
            r#"
            UPDATE users
            SET inviter_id = CASE id
                WHEN 3418 THEN 1001
                WHEN 3419 THEN NULL
                WHEN 3420 THEN 1001
            END
            "#,
        )
        .execute(&database.external_pool)
        .await
        .unwrap();
        sqlx::query("UPDATE top_ups SET complete_time = $1, create_time = $1 WHERE id = 9003")
            .bind(chrono::Utc::now().timestamp() - 2 * 24 * 60 * 60)
            .execute(&database.external_pool)
            .await
            .unwrap();
        assert_eq!(
            ingest_rebate_source_event(&database.pool, event(9003, 3420, None), 500_000, 1,)
                .await
                .unwrap(),
            IngestOutcome::IgnoredNoInviter
        );
        let second = run_topup_reconciliation(&state).await.unwrap();
        assert_eq!(second.created_cases, 0);
        let open_case_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM rebate_reconciliation_cases WHERE case_state = 'open'",
        )
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert_eq!(open_case_count, 0);

        sqlx::query(
            r#"
            UPDATE rebate_calculation_snapshots
            SET rebate_quota = rebate_quota + 1
            WHERE rebate_record_id = (
                SELECT id FROM rebate_records WHERE canonical_event_key = 'newapi:topup:9001'
            )
            "#,
        )
        .execute(&database.pool)
        .await
        .unwrap();
        assert_eq!(
            run_topup_reconciliation(&state)
                .await
                .unwrap()
                .created_cases,
            1
        );
        let drift_issue: String = sqlx::query_scalar(
            "SELECT issue_code FROM rebate_reconciliation_cases WHERE case_state = 'open'",
        )
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert_eq!(drift_issue, "topup_receipt_integrity_drift");
        sqlx::query(
            r#"
            UPDATE rebate_calculation_snapshots
            SET rebate_quota = 1875000
            WHERE rebate_record_id = (
                SELECT id FROM rebate_records WHERE canonical_event_key = 'newapi:topup:9001'
            )
            "#,
        )
        .execute(&database.pool)
        .await
        .unwrap();
        assert_eq!(
            run_topup_reconciliation(&state)
                .await
                .unwrap()
                .created_cases,
            0
        );

        let conflicting_ignored_record: i64 = sqlx::query_scalar(
            r#"
            INSERT INTO rebate_records (
                inviter_id, invitee_id, user_group, order_type, order_id,
                order_amount, order_time, rebate_effective_at, rebate_amount,
                status, scan_started_at, initialization_ends_at, created_at, updated_at,
                canonical_event_key
            ) VALUES (
                1001, 3419, 'default', 'topup', 9002,
                75.00, $1, $2, 0,
                'initializing', $1, $3, $1, $1,
                'newapi:topup:9002'
            )
            RETURNING id
            "#,
        )
        .bind(completed_at)
        .bind(completed_at + 7 * 24 * 60 * 60)
        .bind(completed_at + 48 * 60 * 60)
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert_eq!(
            run_topup_reconciliation(&state)
                .await
                .unwrap()
                .created_cases,
            1
        );
        sqlx::query("UPDATE rebate_records SET canonical_event_key = NULL WHERE id = $1")
            .bind(conflicting_ignored_record)
            .execute(&database.pool)
            .await
            .unwrap();
        assert_eq!(
            run_topup_reconciliation(&state)
                .await
                .unwrap()
                .created_cases,
            0
        );
        let unresolved: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM rebate_reconciliation_cases WHERE case_state IN ('open', 'approved')",
        )
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert_eq!(unresolved, 0);
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn dispatcher_schema_supports_leased_claims_and_response_evidence() {
        let database = TestDatabase::create().await;
        sqlx::raw_sql(LATEST_LEGACY_FIXTURE)
            .execute(&database.pool)
            .await
            .unwrap();
        database.prepare().await.unwrap();

        sqlx::query(
            r#"
            INSERT INTO balance_operations (
                operation_ulid, business_operation_key, operation_kind,
                rebate_record_id, rebate_request_id, user_id, delta_quota,
                idempotency_key, payload_sha256, operation_state, created_at, updated_at,
                balance_before, balance_after, external_created_at
            ) VALUES (
                '01J00000000000000000000006', 'credit:dispatcher:101', 'credit',
                101, 201, 11, 5000000, 'ki1-dispatcher-credit-101', repeat('a', 64),
                'queued', 1700000000, 1700000000, 10, 5000010, 1700000001
            )
            "#,
        )
        .execute(&database.pool)
        .await
        .unwrap();
        let operation_id: i64 = sqlx::query_scalar(
            "SELECT id FROM balance_operations WHERE business_operation_key = 'credit:dispatcher:101'",
        )
        .fetch_one(&database.pool)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO balance_operation_outbox (
                operation_id, event_type, available_at, lease_token, lease_expires_at,
                last_error_code, last_http_status, created_at
            ) VALUES ($1, 'balance_adjustment_requested', 1700000000, 'lease-1', 1700000060,
                      'request_timeout', 503, 1700000000)
            "#,
        )
        .bind(operation_id)
        .execute(&database.pool)
        .await
        .unwrap();

        assert!(sqlx::query(
            r#"
            INSERT INTO balance_operation_outbox (
                operation_id, event_type, available_at, lease_token, created_at
            ) VALUES ($1, 'invalid_partial_lease', 1700000000, 'lease-2', 1700000000)
            "#,
        )
        .bind(operation_id)
        .execute(&database.pool)
        .await
        .is_err());

        database.verify().await.unwrap();
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn concurrent_dispatchers_claim_each_outbox_row_once() {
        let database = TestDatabase::create().await;
        database.prepare().await.unwrap();
        for id in 1..=2_i64 {
            sqlx::query(
                r#"
                INSERT INTO rebate_records(
                    id, ulid, inviter_id, invitee_id, user_group, order_type, order_id,
                    order_amount, order_time, rebate_effective_at, rebate_amount, status,
                    scan_started_at, initialization_ends_at, created_at, updated_at
                ) VALUES ($1, $2, 11, 12, 'test', 'topup', $3, 1, 1, 1, 5000,
                          'approved', 1, 1, 1, 1)
                "#,
            )
            .bind(id)
            .bind(format!("01J000000000000000000000{:02}", id + 10))
            .bind(1000 + id)
            .execute(&database.pool)
            .await
            .unwrap();
            let payout_id: i64 = sqlx::query_scalar(
                r#"
                INSERT INTO invitation_rebate_payouts(
                    rebate_record_id, inviter_user_id, invitee_user_id, order_id, order_type,
                    amount_cents, quota_delta, kind, status, request_id, created_at, updated_at
                ) VALUES ($1, 11, 12, $2, 'topup', 1, 5000, 'payout', 'pending', $3, 1, 1)
                RETURNING id
                "#,
            )
            .bind(id)
            .bind(1000 + id)
            .bind(format!("test-{id}"))
            .fetch_one(&database.pool)
            .await
            .unwrap();
            let operation_id: i64 = sqlx::query_scalar(
                r#"
                INSERT INTO balance_operations(
                    operation_ulid, business_operation_key, operation_kind, rebate_record_id,
                    user_id, delta_quota, idempotency_key, payload_sha256, operation_state,
                    next_attempt_at, created_at, updated_at
                ) VALUES ($1, $2, 'credit', $3, 11, 5000, $1, repeat('a', 64),
                          'queued', 1, 1, 1)
                RETURNING id
                "#,
            )
            .bind(format!("01J000000000000000000001{:02}", id))
            .bind(format!("test-credit-{payout_id}"))
            .bind(id)
            .fetch_one(&database.pool)
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO balance_operation_outbox(operation_id, event_type, available_at, created_at) VALUES ($1, 'dispatch_balance_adjustment', 1, 1)",
            )
            .bind(operation_id)
            .execute(&database.pool)
            .await
            .unwrap();
        }

        let (first, second) = tokio::join!(
            claim_operation_ids_for_test(&database.pool, 2, 1),
            claim_operation_ids_for_test(&database.pool, 2, 1),
        );
        let mut claimed = [first.unwrap(), second.unwrap()].concat();
        claimed.sort();
        claimed.dedup();
        assert_eq!(claimed.len(), 2);
        let attempts: Vec<i32> =
            sqlx::query_scalar("SELECT attempt_count FROM balance_operation_outbox ORDER BY id")
                .fetch_all(&database.pool)
                .await
                .unwrap();
        assert_eq!(attempts, vec![1, 1]);
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn enqueue_is_transactional_and_reuses_stable_operation_ulid() {
        let database = TestDatabase::create().await;
        sqlx::raw_sql(LATEST_LEGACY_FIXTURE)
            .execute(&database.pool)
            .await
            .unwrap();
        database.prepare().await.unwrap();
        let payout_id: i64 = sqlx::query_scalar(
            r#"
            INSERT INTO invitation_rebate_payouts(
                rebate_record_id, rebate_request_id, inviter_user_id, invitee_user_id,
                order_id, order_type, amount_cents, quota_delta, kind, status,
                request_id, created_at, updated_at
            ) VALUES (101, 201, 11, 12, 501, 'topup', 1000, 5000000, 'payout',
                      'pending', 'enqueue-test', 1700000000, 1700000000)
            RETURNING id
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .unwrap();
        let input = || EnqueueOperation {
            kind: OperationKind::Credit,
            rebate_record_id: 101,
            rebate_request_id: Some(201),
            original_operation_ulid: None,
            user_id: 11,
            payout_id,
            delta_quota: 5_000_000,
            now: 1_700_000_001,
        };

        let mut transaction = database.pool.begin().await.unwrap();
        let rolled_back = enqueue_operation(&mut transaction, input()).await.unwrap();
        transaction.rollback().await.unwrap();
        let operation_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM balance_operations")
            .fetch_one(&database.pool)
            .await
            .unwrap();
        assert_eq!(operation_count, 0);

        let mut transaction = database.pool.begin().await.unwrap();
        let first = enqueue_operation(&mut transaction, input()).await.unwrap();
        transaction.commit().await.unwrap();
        assert_ne!(first.operation_ulid, rolled_back.operation_ulid);
        let mut transaction = database.pool.begin().await.unwrap();
        let replay = enqueue_operation(&mut transaction, input()).await.unwrap();
        transaction.commit().await.unwrap();
        assert_eq!(replay.operation_ulid, first.operation_ulid);
        let counts: (i64, i64) = sqlx::query_as(
            "SELECT (SELECT COUNT(*) FROM balance_operations), (SELECT COUNT(*) FROM balance_operation_outbox)",
        )
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert_eq!(counts, (1, 1));
        let settlement: (String, String) = sqlx::query_as(
            "SELECT settlement_state, last_operation_ulid::TEXT FROM rebate_record_settlements WHERE rebate_record_id = 101",
        )
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert_eq!(
            settlement,
            ("payout_pending".to_string(), first.operation_ulid)
        );
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn successful_dispatch_atomically_completes_local_settlement_state() {
        let database = TestDatabase::create().await;
        sqlx::raw_sql(LATEST_LEGACY_FIXTURE)
            .execute(&database.pool)
            .await
            .unwrap();
        database.prepare().await.unwrap();
        let payout_id: i64 = sqlx::query_scalar(
            r#"
            INSERT INTO invitation_rebate_payouts(
                rebate_record_id, rebate_request_id, inviter_user_id, invitee_user_id,
                order_id, order_type, amount_cents, quota_delta, kind, status,
                request_id, created_at, updated_at
            ) VALUES (101, 201, 11, 12, 501, 'topup', 1000, 5000000, 'payout',
                      'pending', 'dispatch-success-test', 1700000000, 1700000000)
            RETURNING id
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .unwrap();
        let mut transaction = database.pool.begin().await.unwrap();
        let operation = enqueue_operation(
            &mut transaction,
            EnqueueOperation {
                kind: OperationKind::Credit,
                rebate_record_id: 101,
                rebate_request_id: Some(201),
                original_operation_ulid: None,
                user_id: 11,
                payout_id,
                delta_quota: 5_000_000,
                now: 1_700_000_001,
            },
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();
        assert_eq!(
            claim_operation_ids_for_test(&database.pool, 1_700_000_002, 1)
                .await
                .unwrap(),
            vec![operation.operation_ulid.clone()]
        );
        persist_applied_for_test(&database.pool, &operation.operation_ulid, 1_700_000_003)
            .await
            .unwrap();

        let operation_state: (String, Option<i64>, Option<i64>, Option<i64>) = sqlx::query_as(
            "SELECT operation_state, balance_before, balance_after, completed_at FROM balance_operations WHERE operation_ulid = $1",
        )
        .bind(&operation.operation_ulid)
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert_eq!(
            operation_state,
            (
                "completed".to_string(),
                Some(10_000_000),
                Some(15_000_000),
                Some(1_700_000_003)
            )
        );
        let states: (String, String, String, String, Option<i64>) = sqlx::query_as(
            r#"
            SELECT payout.status, record.status, settlement.settlement_state,
                   request.status, outbox.dispatched_at
            FROM invitation_rebate_payouts payout
            JOIN rebate_records record ON record.id = payout.rebate_record_id
            JOIN rebate_record_settlements settlement ON settlement.rebate_record_id = record.id
            JOIN rebate_requests request ON request.id = payout.rebate_request_id
            JOIN balance_operations operation ON operation.rebate_record_id = record.id
            JOIN balance_operation_outbox outbox ON outbox.operation_id = operation.id
            WHERE payout.id = $1
            "#,
        )
        .bind(payout_id)
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert_eq!(
            states,
            (
                "completed".to_string(),
                "completed".to_string(),
                "paid".to_string(),
                "completed".to_string(),
                Some(1_700_000_003),
            )
        );
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn manual_retry_requeues_same_operation_ulid_after_review() {
        let database = TestDatabase::create().await;
        sqlx::raw_sql(LATEST_LEGACY_FIXTURE)
            .execute(&database.pool)
            .await
            .unwrap();
        database.prepare().await.unwrap();
        let payout_id: i64 = sqlx::query_scalar(
            r#"
            INSERT INTO invitation_rebate_payouts(
                rebate_record_id, rebate_request_id, inviter_user_id, invitee_user_id,
                order_id, order_type, amount_cents, quota_delta, kind, status,
                request_id, created_at, updated_at
            ) VALUES (101, 201, 11, 12, 501, 'topup', 1000, 5000000, 'payout',
                      'pending', 'manual-retry-test', 1700000000, 1700000000)
            RETURNING id
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .unwrap();
        let input = |now| EnqueueOperation {
            kind: OperationKind::Credit,
            rebate_record_id: 101,
            rebate_request_id: Some(201),
            original_operation_ulid: None,
            user_id: 11,
            payout_id,
            delta_quota: 5_000_000,
            now,
        };
        let mut transaction = database.pool.begin().await.unwrap();
        let operation = enqueue_operation(&mut transaction, input(1_700_000_001))
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        mark_needs_review_for_test(&database.pool, &operation.operation_ulid, 1_700_000_002)
            .await
            .unwrap();
        sqlx::query("UPDATE invitation_rebate_payouts SET status = 'failed' WHERE id = $1")
            .bind(payout_id)
            .execute(&database.pool)
            .await
            .unwrap();

        let mut transaction = database.pool.begin().await.unwrap();
        let retried = enqueue_operation(&mut transaction, input(1_700_000_003))
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        assert_eq!(retried.operation_ulid, operation.operation_ulid);
        assert_eq!(retried.operation_state, "queued");
        let state: (String, Option<i64>, String, Option<String>) = sqlx::query_as(
            r#"
            SELECT operation.operation_state, outbox.dispatched_at,
                   payout.status, operation.last_error_code
            FROM balance_operations operation
            JOIN balance_operation_outbox outbox ON outbox.operation_id = operation.id
            JOIN invitation_rebate_payouts payout ON payout.id = $2
            WHERE operation.operation_ulid = $1
            "#,
        )
        .bind(&operation.operation_ulid)
        .bind(payout_id)
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert_eq!(
            state,
            ("queued".to_string(), None, "pending".to_string(), None)
        );
        let operation_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM balance_operations")
            .fetch_one(&database.pool)
            .await
            .unwrap();
        assert_eq!(operation_count, 1);
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn dispatcher_migration_releases_ambiguous_existing_leases() {
        let database = TestDatabase::create().await;
        sqlx::raw_sql(LATEST_LEGACY_FIXTURE)
            .execute(&database.pool)
            .await
            .unwrap();
        let migrator_v5 = Migrator {
            migrations: Cow::Owned(
                MIGRATOR
                    .iter()
                    .filter(|migration| migration.version <= 202607110005)
                    .cloned()
                    .collect(),
            ),
            ..Migrator::DEFAULT
        };
        migrator_v5.run(&database.pool).await.unwrap();
        let operation_id: i64 = sqlx::query_scalar(
            r#"
            INSERT INTO balance_operations (
                operation_ulid, business_operation_key, operation_kind,
                rebate_record_id, rebate_request_id, user_id, delta_quota,
                idempotency_key, payload_sha256, operation_state, created_at, updated_at
            ) VALUES (
                '01J00000000000000000000007', 'credit:stale-lease:101', 'credit',
                101, 201, 11, 5000000, 'ki1-stale-lease-credit-101', repeat('b', 64),
                'queued', 1700000000, 1700000000
            ) RETURNING id
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO balance_operation_outbox (
                operation_id, event_type, available_at, lease_token, created_at
            ) VALUES ($1, 'balance_adjustment_requested', 1700000000, 'ambiguous-lease', 1700000000)
            "#,
        )
        .bind(operation_id)
        .execute(&database.pool)
        .await
        .unwrap();

        database.prepare().await.unwrap();
        let lease: (Option<String>, Option<i64>) = sqlx::query_as(
            "SELECT lease_token, lease_expires_at FROM balance_operation_outbox WHERE operation_id = $1",
        )
        .bind(operation_id)
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert_eq!(lease, (None, None));
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn dispatcher_schema_readiness_rejects_index_and_column_drift() {
        let database = TestDatabase::create().await;
        database.prepare().await.unwrap();

        sqlx::query("DROP INDEX idx_balance_operation_outbox_expired_lease")
            .execute(&database.pool)
            .await
            .unwrap();
        assert!(database
            .verify()
            .await
            .unwrap_err()
            .to_string()
            .contains("critical index is missing"));
        sqlx::query(
            r#"
            CREATE INDEX idx_balance_operation_outbox_expired_lease
            ON balance_operation_outbox(lease_expires_at, available_at, id)
            WHERE dispatched_at IS NULL AND lease_expires_at IS NOT NULL
            "#,
        )
        .execute(&database.pool)
        .await
        .unwrap();

        sqlx::query("ALTER TABLE balance_operations ALTER COLUMN balance_before SET NOT NULL")
            .execute(&database.pool)
            .await
            .unwrap();
        assert!(database
            .verify()
            .await
            .unwrap_err()
            .to_string()
            .contains("required dispatcher column drifted"));
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn preflight_accepts_version_five_and_requires_version_six_shape_after_upgrade() {
        let database = TestDatabase::create().await;
        let migrator_v5 = Migrator {
            migrations: Cow::Owned(
                MIGRATOR
                    .iter()
                    .filter(|migration| migration.version <= 202607110005)
                    .cloned()
                    .collect(),
            ),
            ..Migrator::DEFAULT
        };
        migrator_v5
            .run(&database.pool)
            .await
            .expect("apply migrations through version five");
        let before_upgrade = database.preflight().await.unwrap();
        assert!(
            before_upgrade.schema_compatible_before_migration,
            "unexpected issues: {:?}",
            before_upgrade.issues
        );

        database.prepare().await.unwrap();
        sqlx::query("ALTER TABLE balance_operations ALTER COLUMN balance_after SET NOT NULL")
            .execute(&database.pool)
            .await
            .unwrap();
        let after_upgrade = database.preflight().await.unwrap();
        assert!(!after_upgrade.schema_compatible_before_migration);
        assert!(after_upgrade.issues.iter().any(|issue| {
            issue.code == "unsupported_table_shape"
                && issue
                    .sample_ids
                    .iter()
                    .any(|detail| detail.contains("balance_operations.balance_after"))
        }));
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn preflight_accepts_pre_canonical_schema_and_rejects_canonical_column_drift() {
        let database = TestDatabase::create().await;
        let pre_canonical_migrator = Migrator {
            migrations: Cow::Owned(
                MIGRATOR
                    .iter()
                    .filter(|migration| migration.version < 202607170001)
                    .cloned()
                    .collect(),
            ),
            ..Migrator::DEFAULT
        };
        pre_canonical_migrator
            .run(&database.pool)
            .await
            .expect("apply migrations before canonical source events");
        let before_upgrade = database.preflight().await.unwrap();
        assert!(
            before_upgrade.schema_compatible_before_migration,
            "unexpected issues: {:?}",
            before_upgrade.issues
        );
        database.cleanup().await;

        for (table, column) in [
            ("rebate_records", "credited_quota"),
            ("rebate_calculation_snapshots", "credited_quota"),
            ("rebate_reconciliation_cases", "source_event_key"),
            ("rebate_reconciliation_cases", "source_order_id"),
        ] {
            let database = TestDatabase::create().await;
            database.prepare().await.unwrap();
            sqlx::query(&format!(
                "ALTER TABLE {table} ALTER COLUMN {column} SET NOT NULL"
            ))
            .execute(&database.pool)
            .await
            .unwrap();

            let report = database.preflight().await.unwrap();
            assert!(!report.schema_compatible_before_migration);
            assert!(report.issues.iter().any(|issue| {
                issue.code == "unsupported_table_shape"
                    && issue
                        .sample_ids
                        .iter()
                        .any(|detail| detail.contains(&format!("{table}.{column}")))
            }));
            database.cleanup().await;
        }
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn latest_legacy_fixture_is_adopted_without_data_loss() {
        let database = TestDatabase::create().await;
        sqlx::raw_sql(LATEST_LEGACY_FIXTURE)
            .execute(&database.pool)
            .await
            .unwrap();
        assert!(database.preflight().await.unwrap().safe_to_migrate);

        database.prepare().await.unwrap();

        let item: (i64, i64) = sqlx::query_as(
            "SELECT request_id, rebate_record_id FROM rebate_request_items WHERE request_id = 201",
        )
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert_eq!(item, (201, 101));
        let settlement_state: String = sqlx::query_scalar(
            "SELECT settlement_state FROM rebate_record_settlements WHERE rebate_record_id = 101",
        )
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert_eq!(settlement_state, "unsettled");
        let checkpoint: (i64, i64) = sqlx::query_as(
            "SELECT cursor_time, cursor_id FROM rebate_sync_checkpoints WHERE sync_type = 'topup'",
        )
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert_eq!(checkpoint, (1_700_000_001, 501));
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn status_changes_preserve_snapshots_and_audit_rows_restrict_deletion() {
        let database = TestDatabase::create().await;
        sqlx::raw_sql(LATEST_LEGACY_FIXTURE)
            .execute(&database.pool)
            .await
            .unwrap();
        database.prepare().await.unwrap();

        let missing = sqlx::query(
            "UPDATE rebate_requests SET rebate_record_ids = ARRAY[999]::BIGINT[] WHERE id = 201",
        )
        .execute(&database.pool)
        .await;
        assert!(missing.is_err());

        sqlx::query("UPDATE rebate_requests SET status = 'rejected', rejected_at = 1700000002 WHERE id = 201")
        .execute(&database.pool)
        .await
        .unwrap();
        sqlx::query("UPDATE rebate_records SET rebate_amount = 6000000 WHERE id = 101")
            .execute(&database.pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE rebate_requests SET status = 'pending', rejected_at = NULL WHERE id = 201",
        )
        .execute(&database.pool)
        .await
        .unwrap();

        let snapshot: i64 = sqlx::query_scalar(
            "SELECT amount_quota_snapshot FROM rebate_request_items WHERE request_id = 201 AND rebate_record_id = 101",
        )
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert_eq!(snapshot, 5_000_000);
        assert!(sqlx::query(
            "UPDATE rebate_request_items SET amount_quota_snapshot = 6000000 WHERE request_id = 201 AND rebate_record_id = 101",
        )
        .execute(&database.pool)
        .await
        .is_err());
        let ownership_state: String = sqlx::query_scalar(
            "SELECT ownership_state FROM rebate_record_request_ownership WHERE request_id = 201 AND rebate_record_id = 101",
        )
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert_eq!(ownership_state, "active");
        assert!(sqlx::query("DELETE FROM rebate_records WHERE id = 101")
            .execute(&database.pool)
            .await
            .is_err());
        assert!(sqlx::query("DELETE FROM rebate_requests WHERE id = 201")
            .execute(&database.pool)
            .await
            .is_err());
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn duplicate_active_ownership_blocks_before_migration() {
        let database = TestDatabase::create().await;
        sqlx::raw_sql(LATEST_LEGACY_FIXTURE)
            .execute(&database.pool)
            .await
            .unwrap();
        sqlx::query(
            r#"
            INSERT INTO rebate_requests (
                id, user_id, rebate_record_ids, total_amount, status, created_at, updated_at
            ) VALUES (202, 11, ARRAY[101]::BIGINT[], 5000000, 'pending', 1700000001, 1700000001)
            "#,
        )
        .execute(&database.pool)
        .await
        .unwrap();

        let report = database.preflight().await.unwrap();
        assert!(!report.safe_to_migrate);
        assert!(report
            .issues
            .iter()
            .any(|issue| issue.code == "duplicate_active_record_ownership"));
        assert!(matches!(
            database.prepare().await,
            Err(DatabaseStartupError::PreflightBlocked { .. })
        ));
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn missing_request_record_blocks_before_migration() {
        let database = TestDatabase::create().await;
        sqlx::raw_sql(LATEST_LEGACY_FIXTURE)
            .execute(&database.pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE rebate_requests SET rebate_record_ids = ARRAY[999]::BIGINT[] WHERE id = 201",
        )
        .execute(&database.pool)
        .await
        .unwrap();

        let report = database.preflight().await.unwrap();
        assert!(!report.safe_to_migrate);
        assert!(report
            .issues
            .iter()
            .any(|issue| issue.code == "request_references_missing_record"));
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn payout_and_completion_ambiguities_are_reported_without_correction() {
        let database = TestDatabase::create().await;
        sqlx::raw_sql(LATEST_LEGACY_FIXTURE)
            .execute(&database.pool)
            .await
            .unwrap();
        sqlx::query("UPDATE rebate_records SET status = 'completed' WHERE id = 101")
            .execute(&database.pool)
            .await
            .unwrap();
        sqlx::query(
            r#"
            INSERT INTO rebate_records (
                id, ulid, inviter_id, invitee_id, user_group, order_type, order_id,
                order_amount, order_time, rebate_effective_at, rebate_amount, rebate_rate,
                status, scan_started_at, initialization_ends_at, created_at, updated_at
            ) VALUES (
                102, '01J00000000000000000000001', 11, 13, 'legacy-group', 'topup', 502,
                50.00, 1700000000, 1700000000, 2500000, 0.0500,
                'completed', 1700000000, 1700000000, 1700000000, 1700000000
            )
            "#,
        )
        .execute(&database.pool)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO invitation_rebate_payouts (
                id, rebate_record_id, inviter_user_id, invitee_user_id, order_id, order_type,
                amount_cents, quota_delta, kind, status, request_id, created_at, updated_at
            ) VALUES
                (301, 101, 11, 12, 501, 'topup', 1000, 5000000, 'payout', 'completed', 'pay-301', 1700000000, 1700000000),
                (302, 101, 11, 12, 501, 'topup', 0, 0, 'payout', 'pending', 'pay-302', 1700000001, 1700000001),
                (303, 101, 11, 12, 501, 'topup', 1000, -5000000, 'reversal', 'pending', 'reverse-303', 1700000002, 1700000002),
                (304, 101, 11, 12, 501, 'topup', 1000, -5000000, 'reversal', 'reversed', 'reverse-304', 1700000003, 1700000003)
            "#,
        )
        .execute(&database.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE invitation_rebate_payouts SET original_payout_id = 301 WHERE id IN (303, 304)",
        )
        .execute(&database.pool)
        .await
        .unwrap();

        let report = database.preflight().await.unwrap();
        assert!(!report.safe_to_migrate);
        for expected in [
            "duplicate_payout_for_record",
            "duplicate_reversal_for_payout",
            "illegal_payout_state",
            "invalid_payout_amount",
            "inflight_payout_requires_reconciliation",
            "external_mark_state_mismatch",
            "completed_record_without_ledger_evidence",
        ] {
            assert!(
                report.issues.iter().any(|issue| issue.code == expected),
                "missing preflight issue {expected}"
            );
        }
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn external_marks_are_verified_bidirectionally_by_exact_identity() {
        let database = TestDatabase::create().await;
        sqlx::raw_sql(LATEST_LEGACY_FIXTURE)
            .execute(&database.pool)
            .await
            .unwrap();
        database.create_external_mark_table().await;
        sqlx::query("UPDATE rebate_records SET status = 'completed' WHERE id = 101")
            .execute(&database.pool)
            .await
            .unwrap();
        sqlx::query(
            r#"
            INSERT INTO invitation_rebate_payouts (
                id, rebate_record_id, rebate_request_id, inviter_user_id, invitee_user_id,
                order_id, order_type, amount_cents, quota_delta, kind, status,
                request_id, newapi_mark_id, created_at, updated_at, completed_at
            ) VALUES (
                301, 101, 201, 11, 12, 501, 'topup', 1000, 5000000,
                'payout', 'completed', 'pay-301', 401, 1700000000, 1700000000, 1700000000
            )
            "#,
        )
        .execute(&database.pool)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO kkai_invitation_rebate_balance_marks (
                id, kind, rebate_record_id, payout_id, user_id, quota_delta,
                amount_cents, request_id, created_at
            ) VALUES (401, 'payout', 101, 301, 11, 5000000, 1000, 'pay-301', 1700000000)
            "#,
        )
        .execute(&database.external_pool)
        .await
        .unwrap();

        let exact = database.preflight().await.unwrap();
        assert!(
            exact.safe_to_migrate,
            "unexpected issues: {:?}",
            exact.issues
        );
        assert!(exact.external_evidence_checked);

        sqlx::query("UPDATE kkai_invitation_rebate_balance_marks SET user_id = 99 WHERE id = 401")
            .execute(&database.external_pool)
            .await
            .unwrap();
        let mismatched = database.preflight().await.unwrap();
        assert!(!mismatched.safe_to_migrate);
        assert!(mismatched
            .issues
            .iter()
            .any(|issue| issue.code == "external_mark_identity_mismatch"));

        sqlx::query("UPDATE kkai_invitation_rebate_balance_marks SET user_id = 11 WHERE id = 401")
            .execute(&database.external_pool)
            .await
            .unwrap();
        sqlx::query(
            r#"
            INSERT INTO kkai_invitation_rebate_balance_marks (
                id, kind, rebate_record_id, payout_id, user_id, quota_delta,
                amount_cents, request_id, created_at
            ) VALUES (402, 'payout', 999, 999, 99, 1, 1, 'orphan-402', 1700000001)
            "#,
        )
        .execute(&database.external_pool)
        .await
        .unwrap();
        let orphaned = database.preflight().await.unwrap();
        assert!(!orphaned.safe_to_migrate);
        assert!(orphaned
            .issues
            .iter()
            .any(|issue| issue.code == "external_orphan_balance_mark"));
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn preflight_accepts_legacy_marks_and_current_adjustments_with_overlapping_ids() {
        let database = TestDatabase::create().await;
        sqlx::raw_sql(LATEST_LEGACY_FIXTURE)
            .execute(&database.pool)
            .await
            .unwrap();
        database.prepare().await.unwrap();
        database.create_external_mark_table().await;
        database.create_external_adjustment_table().await;

        sqlx::query(
            r#"
            INSERT INTO invitation_rebate_payouts (
                id, rebate_record_id, rebate_request_id, inviter_user_id, invitee_user_id,
                order_id, order_type, amount_cents, quota_delta, kind, status,
                request_id, newapi_mark_id, original_payout_id,
                created_at, updated_at, completed_at, reversed_at
            ) VALUES (
                301, 101, 201, 11, 12, 501, 'topup', 1000, 5000000,
                'payout', 'reversed', 'pay-301', NULL, NULL,
                1700000000, 1700000002, 1700000001, 1700000002
            ), (
                302, 101, 201, 11, 12, 501, 'topup', 1000, -5000000,
                'reversal', 'completed', 'reverse-302', 401, 301,
                1700000002, 1700000002, 1700000002, NULL
            )
            "#,
        )
        .execute(&database.pool)
        .await
        .unwrap();
        sqlx::query("UPDATE rebate_record_settlements SET settlement_state = 'reversed' WHERE rebate_record_id = 101")
            .execute(&database.pool)
            .await
            .unwrap();
        sqlx::query(
            r#"
            INSERT INTO balance_operations (
                operation_ulid, business_operation_key, operation_kind,
                rebate_record_id, rebate_request_id, user_id, delta_quota,
                idempotency_key, payload_sha256, operation_state,
                created_at, updated_at, completed_at
            ) VALUES (
                '01J00000000000000000000301', 'credit:mixed:301', 'credit',
                101, 201, 11, 5000000, 'ki1-mixed-credit-301', repeat('a', 64),
                'completed', 1700000000, 1700000001, 1700000001
            )
            "#,
        )
        .execute(&database.pool)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO balance_operations (
                operation_ulid, business_operation_key, operation_kind,
                rebate_record_id, rebate_request_id, original_operation_ulid,
                user_id, delta_quota, idempotency_key, payload_sha256,
                operation_state, created_at, updated_at, completed_at
            ) VALUES (
                '01J00000000000000000000302', 'reversal:mixed:302', 'reversal',
                101, 201, '01J00000000000000000000301', 11, -5000000,
                'ki1-mixed-reversal-302', repeat('b', 64),
                'completed', 1700000002, 1700000002, 1700000002
            )
            "#,
        )
        .execute(&database.pool)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO kkai_invitation_rebate_balance_marks (
                id, kind, rebate_record_id, payout_id, original_payout_id,
                user_id, quota_delta, amount_cents, request_id, created_at
            ) VALUES (
                401, 'reversal', 101, 302, 301, 11, -5000000,
                1000, 'reverse-302', 1700000002
            )
            "#,
        )
        .execute(&database.external_pool)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO kkai_internal_balance_adjustments (
                id, operation_id, user_id, delta, reason, metadata,
                payload_sha256, original_operation_id,
                balance_before, balance_after, created_at
            ) VALUES (
                401, '01J00000000000000000000301', 11, 5000000,
                'invitation_reward',
                '{"rebate_record_id":101,"payout_id":301}',
                repeat('a', 64), NULL,
                4000000, 9000000, 1700000001
            )
            "#,
        )
        .execute(&database.external_pool)
        .await
        .unwrap();

        let exact = database.preflight().await.unwrap();
        assert!(
            exact.safe_to_migrate,
            "unexpected issues: {:?}",
            exact.issues
        );

        sqlx::query(
            r#"
            INSERT INTO kkai_invitation_rebate_balance_marks (
                id, kind, rebate_record_id, payout_id, user_id, quota_delta,
                amount_cents, request_id, created_at
            ) VALUES (
                402, 'payout', 101, 301, 11, 5000000,
                1000, 'pay-301', 1700000001
            )
            "#,
        )
        .execute(&database.external_pool)
        .await
        .unwrap();
        sqlx::query("UPDATE invitation_rebate_payouts SET newapi_mark_id = 402 WHERE id = 301")
            .execute(&database.pool)
            .await
            .unwrap();
        let duplicate_evidence = database.preflight().await.unwrap();
        assert!(!duplicate_evidence.safe_to_migrate);
        assert!(duplicate_evidence
            .issues
            .iter()
            .any(|issue| issue.code == "external_mark_identity_mismatch"));

        sqlx::query("DELETE FROM kkai_invitation_rebate_balance_marks WHERE id = 402")
            .execute(&database.external_pool)
            .await
            .unwrap();
        sqlx::query("UPDATE invitation_rebate_payouts SET newapi_mark_id = NULL WHERE id = 301")
            .execute(&database.pool)
            .await
            .unwrap();
        sqlx::query("UPDATE kkai_internal_balance_adjustments SET delta = 4999999 WHERE id = 401")
            .execute(&database.external_pool)
            .await
            .unwrap();
        let mismatched = database.preflight().await.unwrap();
        assert!(!mismatched.safe_to_migrate);
        assert!(mismatched
            .issues
            .iter()
            .any(|issue| issue.code == "external_mark_identity_mismatch"));
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn external_evidence_requires_primary_and_business_unique_keys() {
        let database = TestDatabase::create().await;
        database.create_external_mark_table().await;
        sqlx::query(
            "ALTER TABLE kkai_invitation_rebate_balance_marks DROP CONSTRAINT kkai_invitation_rebate_balance_marks_pkey",
        )
        .execute(&database.external_pool)
        .await
        .unwrap();

        let missing_primary_key = database.preflight().await.unwrap();
        assert!(!missing_primary_key.schema_compatible_before_migration);
        assert!(missing_primary_key.issues.iter().any(|issue| {
            issue.code == "external_evidence_schema_incompatible"
                && issue
                    .sample_ids
                    .iter()
                    .any(|detail| detail.contains("primary_key"))
        }));

        sqlx::query("ALTER TABLE kkai_invitation_rebate_balance_marks ADD PRIMARY KEY (id)")
            .execute(&database.external_pool)
            .await
            .unwrap();
        sqlx::query("DROP INDEX uq_kkai_invitation_rebate_balance_marks_request")
            .execute(&database.external_pool)
            .await
            .unwrap();
        let missing_business_identity = database.preflight().await.unwrap();
        assert!(!missing_business_identity.schema_compatible_before_migration);
        assert!(missing_business_identity.issues.iter().any(|issue| {
            issue.code == "external_evidence_schema_incompatible"
                && issue
                    .sample_ids
                    .iter()
                    .any(|detail| detail.contains("unique_key:request_id"))
        }));
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn unsupported_empty_table_shape_is_a_blocker() {
        let database = TestDatabase::create().await;
        sqlx::query("CREATE TABLE rebate_records (id TEXT PRIMARY KEY)")
            .execute(&database.pool)
            .await
            .unwrap();

        let report = database.preflight().await.unwrap();
        assert!(!report.schema_compatible_before_migration);
        assert!(!report.safe_to_migrate);
        assert!(report
            .issues
            .iter()
            .any(|issue| issue.code == "unsupported_table_shape"));
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn balance_operation_constraints_enforce_single_exact_reversal() {
        let database = TestDatabase::create().await;
        sqlx::raw_sql(LATEST_LEGACY_FIXTURE)
            .execute(&database.pool)
            .await
            .unwrap();
        database.prepare().await.unwrap();

        sqlx::query(
            r#"
            INSERT INTO balance_operations (
                operation_ulid, business_operation_key, operation_kind,
                rebate_record_id, rebate_request_id, user_id, delta_quota,
                idempotency_key, payload_sha256, operation_state, created_at, updated_at
            ) VALUES (
                '01J00000000000000000000010', 'credit:101', 'credit',
                101, 201, 11, 5000000, 'ki1-credit-101', repeat('a', 64),
                'queued', 1700000000, 1700000000
            )
            "#,
        )
        .execute(&database.pool)
        .await
        .unwrap();
        assert!(sqlx::query(
            r#"
            INSERT INTO balance_operations (
                operation_ulid, business_operation_key, operation_kind,
                rebate_record_id, rebate_request_id, user_id, delta_quota,
                idempotency_key, payload_sha256, operation_state, created_at, updated_at
            ) VALUES (
                '01J00000000000000000000011', 'credit:101:duplicate', 'credit',
                101, 201, 11, 5000000, 'ki1-credit-101-duplicate', repeat('b', 64),
                'queued', 1700000001, 1700000001
            )
            "#,
        )
        .execute(&database.pool)
        .await
        .is_err());
        assert!(sqlx::query(
            r#"
            INSERT INTO balance_operations (
                operation_ulid, business_operation_key, operation_kind,
                rebate_record_id, rebate_request_id, original_operation_ulid,
                user_id, delta_quota, idempotency_key, payload_sha256,
                operation_state, created_at, updated_at
            ) VALUES (
                '01J00000000000000000000012', 'reversal:101:wrong', 'reversal',
                101, 201, '01J00000000000000000000010', 99, -5000000,
                'ki1-reversal-101-wrong', repeat('c', 64), 'queued', 1700000002, 1700000002
            )
            "#,
        )
        .execute(&database.pool)
        .await
        .is_err());
        sqlx::query(
            r#"
            INSERT INTO balance_operations (
                operation_ulid, business_operation_key, operation_kind,
                rebate_record_id, rebate_request_id, original_operation_ulid,
                user_id, delta_quota, idempotency_key, payload_sha256,
                operation_state, created_at, updated_at
            ) VALUES (
                '01J00000000000000000000013', 'reversal:101', 'reversal',
                101, 201, '01J00000000000000000000010', 11, -5000000,
                'ki1-reversal-101', repeat('d', 64), 'queued', 1700000003, 1700000003
            )
            "#,
        )
        .execute(&database.pool)
        .await
        .unwrap();
        assert!(sqlx::query(
            r#"
            INSERT INTO balance_operations (
                operation_ulid, business_operation_key, operation_kind,
                rebate_record_id, rebate_request_id, original_operation_ulid,
                user_id, delta_quota, idempotency_key, payload_sha256,
                operation_state, created_at, updated_at
            ) VALUES (
                '01J00000000000000000000014', 'reversal:101:duplicate', 'reversal',
                101, 201, '01J00000000000000000000010', 11, -5000000,
                'ki1-reversal-101-duplicate', repeat('e', 64), 'queued', 1700000004, 1700000004
            )
            "#,
        )
        .execute(&database.pool)
        .await
        .is_err());
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn preflight_blocks_historical_balance_operation_conflicts() {
        let database = TestDatabase::create().await;
        sqlx::raw_sql(LATEST_LEGACY_FIXTURE)
            .execute(&database.pool)
            .await
            .unwrap();
        database.prepare().await.unwrap();
        sqlx::query(
            r#"
            INSERT INTO balance_operations (
                operation_ulid, business_operation_key, operation_kind,
                rebate_record_id, rebate_request_id, user_id, delta_quota,
                idempotency_key, payload_sha256, operation_state, created_at, updated_at
            ) VALUES (
                '01J00000000000000000000020', 'credit:history:1', 'credit',
                101, 201, 11, 5000000, 'ki1-history-credit-1', repeat('1', 64),
                'queued', 1700000000, 1700000000
            )
            "#,
        )
        .execute(&database.pool)
        .await
        .unwrap();
        sqlx::query("DROP INDEX uq_balance_operations_record_credit")
            .execute(&database.pool)
            .await
            .unwrap();
        sqlx::query("DROP INDEX uq_balance_operations_original_reversal")
            .execute(&database.pool)
            .await
            .unwrap();
        sqlx::query(
            "ALTER TABLE balance_operations DISABLE TRIGGER trg_balance_operation_reversal_guard",
        )
        .execute(&database.pool)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO balance_operations (
                operation_ulid, business_operation_key, operation_kind,
                rebate_record_id, rebate_request_id, user_id, delta_quota,
                idempotency_key, payload_sha256, operation_state, created_at, updated_at
            ) VALUES (
                '01J00000000000000000000021', 'credit:history:2', 'credit',
                101, 201, 11, 5000000, 'ki1-history-credit-2', repeat('2', 64),
                'queued', 1700000001, 1700000001
            )
            "#,
        )
        .execute(&database.pool)
        .await
        .unwrap();
        for (ulid, key, user_id) in [
            ("01J00000000000000000000022", "reversal:history:1", 99_i64),
            ("01J00000000000000000000023", "reversal:history:2", 11_i64),
        ] {
            sqlx::query(
                r#"
                INSERT INTO balance_operations (
                    operation_ulid, business_operation_key, operation_kind,
                    rebate_record_id, rebate_request_id, original_operation_ulid,
                    user_id, delta_quota, idempotency_key, payload_sha256,
                    operation_state, created_at, updated_at
                ) VALUES ($1, $2, 'reversal', 101, 201,
                    '01J00000000000000000000020', $3, -5000000,
                    $2 || ':idempotency', repeat('3', 64), 'queued', 1700000002, 1700000002)
                "#,
            )
            .bind(ulid)
            .bind(key)
            .bind(user_id)
            .execute(&database.pool)
            .await
            .unwrap();
        }

        let report = database.preflight().await.unwrap();
        for expected in [
            "duplicate_balance_credit_for_record",
            "duplicate_balance_reversal_for_credit",
            "balance_reversal_identity_mismatch",
        ] {
            assert!(
                report.issues.iter().any(|issue| issue.code == expected),
                "missing preflight issue {expected}"
            );
        }
        assert!(!report.safe_to_migrate);
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn readiness_rejects_same_name_wrong_index_and_disabled_trigger() {
        let database = TestDatabase::create().await;
        database.prepare().await.unwrap();
        sqlx::query("DROP INDEX uq_invitation_rebate_payouts_record_pay")
            .execute(&database.pool)
            .await
            .unwrap();
        sqlx::query(
            "CREATE UNIQUE INDEX uq_invitation_rebate_payouts_record_pay ON rebate_records(inviter_id)",
        )
        .execute(&database.pool)
        .await
        .unwrap();
        assert!(database
            .verify()
            .await
            .unwrap_err()
            .to_string()
            .contains("index definition drifted"));

        sqlx::query("DROP INDEX uq_invitation_rebate_payouts_record_pay")
            .execute(&database.pool)
            .await
            .unwrap();
        sqlx::query(
            "CREATE UNIQUE INDEX uq_invitation_rebate_payouts_record_pay ON invitation_rebate_payouts(rebate_record_id) WHERE kind = 'payout'",
        )
        .execute(&database.pool)
        .await
        .unwrap();
        sqlx::query("ALTER TABLE rebate_requests DISABLE TRIGGER trg_20_rebate_request_ownership")
            .execute(&database.pool)
            .await
            .unwrap();
        assert!(database
            .verify()
            .await
            .unwrap_err()
            .to_string()
            .contains("trigger definition drifted"));
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn maintenance_lock_blocks_concurrent_business_writes() {
        let database = TestDatabase::create().await;
        sqlx::raw_sql(LATEST_LEGACY_FIXTURE)
            .execute(&database.pool)
            .await
            .unwrap();
        let mut connection = database.pool.acquire().await.unwrap();
        let mut transaction = connection.begin().await.unwrap();
        lock_maintenance_tables(&mut transaction).await.unwrap();

        let mut writer = database.pool.acquire().await.unwrap();
        sqlx::query("SET lock_timeout = '200ms'")
            .execute(&mut *writer)
            .await
            .unwrap();
        let write =
            sqlx::query("UPDATE rebate_records SET updated_at = updated_at + 1 WHERE id = 101")
                .execute(&mut *writer)
                .await;
        assert!(write.is_err());
        transaction.rollback().await.unwrap();
        drop(writer);
        drop(connection);
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn missing_critical_index_refuses_schema_readiness() {
        let database = TestDatabase::create().await;
        database.prepare().await.unwrap();
        sqlx::query("DROP INDEX uq_invitation_rebate_payouts_record_pay")
            .execute(&database.pool)
            .await
            .unwrap();

        let error = database.verify().await.unwrap_err();
        assert!(error.to_string().contains("critical index is missing"));
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn full_prepare_observes_a_write_committed_while_waiting_for_maintenance_lock() {
        let database = TestDatabase::create().await;
        sqlx::raw_sql(LATEST_LEGACY_FIXTURE)
            .execute(&database.pool)
            .await
            .unwrap();

        let mut writer = database.pool.acquire().await.unwrap();
        let mut writer_transaction = writer.begin().await.unwrap();
        sqlx::query("UPDATE rebate_records SET status = 'illegal_state' WHERE id = 101")
            .execute(&mut *writer_transaction)
            .await
            .unwrap();

        let prepare = prepare_database(&database.pool, &database.external_pool);
        let release_writer = async {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let waiting: bool = sqlx::query_scalar(
                        r#"
                        SELECT EXISTS (
                            SELECT 1
                            FROM pg_locks lock
                            JOIN pg_class relation ON relation.oid = lock.relation
                            JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
                            WHERE namespace.nspname = $1
                              AND relation.relname = 'rebate_records'
                              AND NOT lock.granted
                        )
                        "#,
                    )
                    .bind(&database.schema)
                    .fetch_one(&database.admin_pool)
                    .await
                    .unwrap();
                    if waiting {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("prepare_database did not wait for the legacy writer lock");
            writer_transaction.commit().await.unwrap();
        };
        let (prepare_result, ()) = tokio::join!(prepare, release_writer);
        drop(writer);
        let error = prepare_result.expect_err(
            "the post-lock snapshot must observe and block the committed invalid state",
        );
        let report = error.preflight_report().expect("preflight blocked report");
        assert!(report
            .issues
            .iter()
            .any(|issue| issue.code == "illegal_rebate_record_state"));
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn settlement_coverage_is_created_guarded_and_verified() {
        let database = TestDatabase::create().await;
        sqlx::raw_sql(LATEST_LEGACY_FIXTURE)
            .execute(&database.pool)
            .await
            .unwrap();
        database.prepare().await.unwrap();

        let covered: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM rebate_record_settlements WHERE rebate_record_id = 101",
        )
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert_eq!(covered, 1);
        assert!(
            sqlx::query("DELETE FROM rebate_record_settlements WHERE rebate_record_id = 101",)
                .execute(&database.pool)
                .await
                .is_err()
        );
        assert!(sqlx::query("UPDATE rebate_record_settlements SET rebate_record_id = 999 WHERE rebate_record_id = 101")
            .execute(&database.pool)
            .await
            .is_err());
        assert!(sqlx::query("TRUNCATE rebate_record_settlements")
            .execute(&database.pool)
            .await
            .is_err());

        sqlx::query(
            "ALTER TABLE rebate_record_settlements DISABLE TRIGGER trg_rebate_record_settlement_delete_guard",
        )
        .execute(&database.pool)
        .await
        .unwrap();
        sqlx::query("DELETE FROM rebate_record_settlements WHERE rebate_record_id = 101")
            .execute(&database.pool)
            .await
            .unwrap();
        sqlx::query(
            "ALTER TABLE rebate_record_settlements ENABLE TRIGGER trg_rebate_record_settlement_delete_guard",
        )
        .execute(&database.pool)
        .await
        .unwrap();

        let report = database.preflight().await.unwrap();
        assert!(!report.safe_to_migrate);
        assert!(report
            .issues
            .iter()
            .any(|issue| issue.code == "normalized_settlement_coverage_missing"));
        assert!(database
            .verify()
            .await
            .unwrap_err()
            .to_string()
            .contains("settlement coverage"));
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn readiness_manifest_covers_every_migration_catalog_object() {
        let database = TestDatabase::create().await;
        database.prepare().await.unwrap();

        let constraint_names: BTreeSet<String> = sqlx::query_scalar(
            r#"
            SELECT constraint_meta.conname::TEXT
            FROM pg_constraint constraint_meta
            JOIN pg_class table_class ON table_class.oid = constraint_meta.conrelid
            JOIN pg_namespace namespace ON namespace.oid = table_class.relnamespace
            WHERE namespace.nspname = current_schema()
              AND constraint_meta.contype IN ('p', 'u', 'c', 'f')
            ORDER BY constraint_meta.conname
            "#,
        )
        .fetch_all(&database.pool)
        .await
        .unwrap()
        .into_iter()
        .collect();
        let expected_constraints: BTreeSet<String> = CRITICAL_KEY_CONSTRAINTS
            .iter()
            .map(|spec| spec.name.to_string())
            .chain(CRITICAL_CHECKS.iter().map(|spec| spec.name.to_string()))
            .chain(
                CRITICAL_FOREIGN_KEYS
                    .iter()
                    .map(|spec| spec.name.to_string()),
            )
            .collect();
        assert_eq!(constraint_names, expected_constraints);

        let standalone_index_names: BTreeSet<String> = sqlx::query_scalar(
            r#"
            SELECT index_class.relname::TEXT
            FROM pg_index index_meta
            JOIN pg_class index_class ON index_class.oid = index_meta.indexrelid
            JOIN pg_namespace namespace ON namespace.oid = index_class.relnamespace
            WHERE namespace.nspname = current_schema()
              AND NOT EXISTS (
                  SELECT 1 FROM pg_constraint constraint_meta
                  WHERE constraint_meta.conindid = index_meta.indexrelid
              )
            ORDER BY index_class.relname
            "#,
        )
        .fetch_all(&database.pool)
        .await
        .unwrap()
        .into_iter()
        .collect();
        let expected_indexes: BTreeSet<String> = CRITICAL_INDEXES
            .iter()
            .filter(|spec| {
                !CRITICAL_KEY_CONSTRAINTS
                    .iter()
                    .any(|constraint| constraint.name == spec.name)
            })
            .map(|spec| spec.name.to_string())
            .collect();
        assert_eq!(standalone_index_names, expected_indexes);

        let trigger_names: BTreeSet<String> = sqlx::query_scalar(
            r#"
            SELECT trigger_meta.tgname::TEXT
            FROM pg_trigger trigger_meta
            JOIN pg_class table_class ON table_class.oid = trigger_meta.tgrelid
            JOIN pg_namespace namespace ON namespace.oid = table_class.relnamespace
            WHERE namespace.nspname = current_schema() AND NOT trigger_meta.tgisinternal
            ORDER BY trigger_meta.tgname
            "#,
        )
        .fetch_all(&database.pool)
        .await
        .unwrap()
        .into_iter()
        .collect();
        let expected_triggers: BTreeSet<String> = CRITICAL_TRIGGERS
            .iter()
            .map(|spec| spec.name.to_string())
            .collect();
        assert_eq!(trigger_names, expected_triggers);
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn balance_operation_financial_identity_is_immutable_after_insert() {
        let database = TestDatabase::create().await;
        sqlx::raw_sql(LATEST_LEGACY_FIXTURE)
            .execute(&database.pool)
            .await
            .unwrap();
        database.prepare().await.unwrap();
        sqlx::query(
            r#"
            INSERT INTO balance_operations (
                operation_ulid, business_operation_key, operation_kind,
                rebate_record_id, rebate_request_id, user_id, delta_quota,
                idempotency_key, payload_sha256, operation_state, created_at, updated_at
            ) VALUES (
                '01J00000000000000000000030', 'credit:immutable:101', 'credit',
                101, 201, 11, 5000000, 'ki1-immutable-credit-101', repeat('a', 64),
                'queued', 1700000000, 1700000000
            )
            "#,
        )
        .execute(&database.pool)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO balance_operations (
                operation_ulid, business_operation_key, operation_kind,
                rebate_record_id, rebate_request_id, original_operation_ulid,
                user_id, delta_quota, idempotency_key, payload_sha256,
                operation_state, created_at, updated_at
            ) VALUES (
                '01J00000000000000000000031', 'reversal:immutable:101', 'reversal',
                101, 201, '01J00000000000000000000030', 11, -5000000,
                'ki1-immutable-reversal-101', repeat('b', 64), 'queued', 1700000001, 1700000001
            )
            "#,
        )
        .execute(&database.pool)
        .await
        .unwrap();

        for update in [
            "UPDATE balance_operations SET operation_ulid = '01J00000000000000000000039' WHERE operation_kind = 'credit'",
            "UPDATE balance_operations SET source = 'changed-source' WHERE operation_kind = 'credit'",
            "UPDATE balance_operations SET business_operation_key = 'changed-key' WHERE operation_kind = 'credit'",
            "UPDATE balance_operations SET operation_kind = 'reversal' WHERE operation_kind = 'credit'",
            "UPDATE balance_operations SET rebate_record_id = 999 WHERE operation_kind = 'credit'",
            "UPDATE balance_operations SET rebate_request_id = NULL WHERE operation_kind = 'credit'",
            "UPDATE balance_operations SET user_id = 99 WHERE operation_kind = 'credit'",
            "UPDATE balance_operations SET delta_quota = 6000000 WHERE operation_kind = 'credit'",
            "UPDATE balance_operations SET idempotency_key = 'changed-idempotency' WHERE operation_kind = 'credit'",
            "UPDATE balance_operations SET payload_sha256 = repeat('c', 64) WHERE operation_kind = 'credit'",
            "UPDATE balance_operations SET original_operation_ulid = NULL WHERE operation_kind = 'reversal'",
        ] {
            let mut transaction = database.pool.begin().await.unwrap();
            let error = sqlx::query(update)
                .execute(&mut *transaction)
                .await
                .expect_err(update);
            assert!(
                error.to_string().contains("financial identity is immutable"),
                "unexpected update error for {update}: {error}"
            );
            transaction.rollback().await.unwrap();
        }
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn readiness_rejects_checkpoint_primary_key_drift() {
        let database = TestDatabase::create().await;
        database.prepare().await.unwrap();
        sqlx::query(
            "ALTER TABLE rebate_sync_checkpoints DROP CONSTRAINT rebate_sync_checkpoints_pkey",
        )
        .execute(&database.pool)
        .await
        .unwrap();

        assert!(database
            .verify()
            .await
            .unwrap_err()
            .to_string()
            .contains("critical constraint is missing"));
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn supported_legacy_columns_without_primary_key_are_rejected() {
        let database = TestDatabase::create().await;
        sqlx::raw_sql(LATEST_LEGACY_FIXTURE)
            .execute(&database.pool)
            .await
            .unwrap();
        sqlx::query("ALTER TABLE rebate_records DROP CONSTRAINT rebate_records_pkey")
            .execute(&database.pool)
            .await
            .unwrap();

        let report = database.preflight().await.unwrap();
        assert!(!report.schema_compatible_before_migration);
        assert!(report.issues.iter().any(|issue| {
            issue.code == "unsupported_table_shape"
                && issue
                    .sample_ids
                    .iter()
                    .any(|item| item.contains("primary_key"))
        }));
        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
    async fn legacy_relation_kind_is_part_of_supported_shape() {
        let database = TestDatabase::create().await;
        sqlx::raw_sql(LATEST_LEGACY_FIXTURE)
            .execute(&database.pool)
            .await
            .unwrap();
        sqlx::query("ALTER TABLE rebate_records RENAME TO rebate_records_backing")
            .execute(&database.pool)
            .await
            .unwrap();
        sqlx::query("CREATE VIEW rebate_records AS SELECT * FROM rebate_records_backing")
            .execute(&database.pool)
            .await
            .unwrap();

        let report = database.preflight().await.unwrap();
        assert!(!report.schema_compatible_before_migration);
        assert!(report.issues.iter().any(|issue| {
            issue.code == "unsupported_table_shape"
                && issue
                    .sample_ids
                    .iter()
                    .any(|item| item == "rebate_records.relkind:v")
        }));
        database.cleanup().await;
    }
}
