use std::collections::HashMap;

use serde::Serialize;
use sqlx::{PgConnection, Row};

const SAMPLE_LIMIT: i64 = 50;
const DISPATCHER_SCHEMA_MIGRATION: i64 = 202607110006;
pub const ROLLBACK_BOUNDARY: &str = "Schema changes are expand-only. Application rollback is allowed only while settlement remains disabled and no balance_operations rows exist; never down-migrate or roll back to a binary that writes NewAPI quota directly.";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PreflightMode {
    ReportOnly,
    MaintenanceLocked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IssueSeverity {
    Blocking,
    Warning,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PreflightIssue {
    pub code: &'static str,
    pub severity: IssueSeverity,
    pub count: i64,
    pub sample_ids: Vec<String>,
    pub message: &'static str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PreflightReport {
    pub safe_to_migrate: bool,
    pub schema_compatible_before_migration: bool,
    pub execution_mode: PreflightMode,
    pub postgres_version_num: i32,
    pub external_postgres_version_num: i32,
    pub external_evidence_checked: bool,
    pub rollback_boundary: &'static str,
    pub inspected_tables: Vec<String>,
    pub issues: Vec<PreflightIssue>,
}

impl PreflightReport {
    fn new(mode: PreflightMode) -> Self {
        Self {
            safe_to_migrate: true,
            schema_compatible_before_migration: true,
            execution_mode: mode,
            postgres_version_num: 0,
            external_postgres_version_num: 0,
            external_evidence_checked: false,
            rollback_boundary: ROLLBACK_BOUNDARY,
            inspected_tables: Vec::new(),
            issues: Vec::new(),
        }
    }

    fn finish(mut self) -> Self {
        self.safe_to_migrate = self.schema_compatible_before_migration
            && !self
                .issues
                .iter()
                .any(|issue| issue.severity == IssueSeverity::Blocking);
        self
    }

    pub fn blocking_issue_count(&self) -> usize {
        self.issues
            .iter()
            .filter(|issue| issue.severity == IssueSeverity::Blocking)
            .count()
    }
}

pub async fn run(
    connection: &mut PgConnection,
    external_connection: &mut PgConnection,
    mode: PreflightMode,
) -> Result<PreflightReport, sqlx::Error> {
    let mut report = PreflightReport::new(mode);
    if mode == PreflightMode::ReportOnly {
        report.issues.push(PreflightIssue {
            code: "report_only_not_migration_authority",
            severity: IssueSeverity::Warning,
            count: 0,
            sample_ids: Vec::new(),
            message: "This report is read-only; only maintenance-locked startup preflight authorizes migration.",
        });
    }
    let known_tables = [
        "rebate_rules",
        "rebate_records",
        "invitation_registrations",
        "rebate_requests",
        "withdrawal_requests",
        "invitation_rebate_payouts",
        "invitation_rebate_payout_attempts",
        "sync_log",
        "system_config",
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
        "_sqlx_migrations",
    ];

    for table in known_tables {
        if table_exists(connection, table).await? {
            report.inspected_tables.push(table.to_string());
        }
    }

    report.postgres_version_num = postgres_version_num(connection).await?;
    report.external_postgres_version_num = postgres_version_num(external_connection).await?;
    inspect_postgres_versions(&mut report);
    inspect_schema_shapes(connection, &mut report).await?;
    inspect_external_schema_shape(external_connection, &mut report).await?;

    if !report.schema_compatible_before_migration {
        return Ok(report.finish());
    }

    inspect_legacy_shape(connection, &mut report).await?;
    inspect_rebate_records(connection, &mut report).await?;
    inspect_requests(connection, &mut report).await?;
    inspect_payouts(connection, &mut report).await?;
    inspect_registrations(connection, &mut report).await?;
    inspect_normalized_state(connection, &mut report).await?;
    inspect_balance_operations(connection, &mut report).await?;
    inspect_migration_history(connection, &mut report).await?;
    inspect_external_evidence(connection, external_connection, &mut report).await?;

    Ok(report.finish())
}

#[derive(Clone, Copy)]
struct ColumnSpec {
    name: &'static str,
    udt_name: &'static str,
    nullable: bool,
}

#[derive(Clone, Copy)]
struct TableShape {
    name: &'static str,
    columns: &'static [ColumnSpec],
}

const fn column(name: &'static str, udt_name: &'static str, nullable: bool) -> ColumnSpec {
    ColumnSpec {
        name,
        udt_name,
        nullable,
    }
}

const LOCAL_TABLE_SHAPES: &[TableShape] = &[
    TableShape {
        name: "rebate_rules",
        columns: &[
            column("id", "int8", false),
            column("user_group", "varchar", false),
            column("rule_type", "varchar", false),
            column("rebate_rate", "numeric", false),
            column("effective_time", "int8", false),
        ],
    },
    TableShape {
        name: "rebate_records",
        columns: &[
            column("id", "int8", false),
            column("ulid", "varchar", true),
            column("inviter_id", "int8", false),
            column("invitee_id", "int8", false),
            column("user_group", "varchar", false),
            column("order_type", "varchar", false),
            column("order_id", "int8", false),
            column("order_amount", "numeric", false),
            column("order_time", "int8", false),
            column("rebate_effective_at", "int8", false),
            column("rebate_amount", "int8", false),
            column("rebate_rate", "numeric", true),
            column("status", "varchar", false),
            column("scan_started_at", "int8", false),
            column("initialization_ends_at", "int8", false),
            column("admin_adjusted", "bool", false),
            column("unlock_required", "bool", false),
            column("created_at", "int8", false),
            column("updated_at", "int8", false),
        ],
    },
    TableShape {
        name: "invitation_registrations",
        columns: &[
            column("id", "int8", false),
            column("inviter_id", "int8", false),
            column("invitee_id", "int8", false),
            column("user_group", "varchar", false),
            column("invited_at", "int8", false),
        ],
    },
    TableShape {
        name: "rebate_requests",
        columns: &[
            column("id", "int8", false),
            column("user_id", "int8", false),
            column("rebate_record_ids", "_int8", false),
            column("total_amount", "int8", false),
            column("status", "varchar", false),
            column("created_at", "int8", false),
            column("updated_at", "int8", false),
            column("rejected_at", "int8", true),
        ],
    },
    TableShape {
        name: "invitation_rebate_payouts",
        columns: &[
            column("id", "int8", false),
            column("rebate_record_id", "int8", false),
            column("rebate_request_id", "int8", true),
            column("inviter_user_id", "int8", false),
            column("invitee_user_id", "int8", false),
            column("order_id", "int8", false),
            column("order_type", "varchar", false),
            column("amount_cents", "int8", false),
            column("quota_delta", "int8", false),
            column("kind", "varchar", false),
            column("status", "varchar", false),
            column("request_id", "varchar", false),
            column("newapi_mark_id", "int8", true),
            column("newapi_log_id", "int8", true),
            column("original_payout_id", "int8", true),
            column("created_at", "int8", false),
            column("updated_at", "int8", false),
        ],
    },
    TableShape {
        name: "invitation_rebate_payout_attempts",
        columns: &[
            column("id", "int8", false),
            column("rebate_record_id", "int8", true),
            column("action", "varchar", false),
            column("request_id", "varchar", false),
            column("status", "varchar", false),
        ],
    },
    TableShape {
        name: "sync_log",
        columns: &[
            column("id", "int8", false),
            column("sync_type", "varchar", false),
            column("last_sync_id", "int8", false),
            column("last_sync_time", "int8", false),
        ],
    },
    TableShape {
        name: "system_config",
        columns: &[
            column("key", "varchar", false),
            column("value", "text", false),
            column("updated_at", "int8", false),
        ],
    },
    TableShape {
        name: "rebate_request_items",
        columns: &[
            column("request_id", "int8", false),
            column("rebate_record_id", "int8", false),
            column("ordinal", "int4", true),
            column("membership_state", "varchar", false),
            column("amount_quota_snapshot", "int8", false),
            column("created_at", "int8", false),
            column("removed_at", "int8", true),
        ],
    },
    TableShape {
        name: "rebate_record_request_ownership",
        columns: &[
            column("id", "int8", false),
            column("rebate_record_id", "int8", false),
            column("request_id", "int8", false),
            column("ownership_state", "varchar", false),
            column("acquired_at", "int8", false),
            column("released_at", "int8", true),
        ],
    },
    TableShape {
        name: "rebate_record_settlements",
        columns: &[
            column("rebate_record_id", "int8", false),
            column("rebate_request_id", "int8", true),
            column("settlement_state", "varchar", false),
            column("state_version", "int8", false),
        ],
    },
    TableShape {
        name: "rebate_calculation_snapshots",
        columns: &[
            column("rebate_record_id", "int8", false),
            column("inviter_group", "varchar", false),
            column("quota_per_unit", "int8", false),
            column("rebate_quota", "int8", false),
        ],
    },
    TableShape {
        name: "rebate_request_money_snapshots",
        columns: &[
            column("rebate_request_id", "int8", false),
            column("total_quota", "int8", false),
            column("quota_per_unit", "int8", false),
        ],
    },
    TableShape {
        name: "balance_operations",
        columns: &[
            column("id", "int8", false),
            column("operation_ulid", "bpchar", false),
            column("source", "varchar", false),
            column("business_operation_key", "varchar", false),
            column("operation_kind", "varchar", false),
            column("rebate_record_id", "int8", false),
            column("original_operation_ulid", "bpchar", true),
            column("user_id", "int8", false),
            column("delta_quota", "int8", false),
            column("idempotency_key", "varchar", false),
            column("payload_sha256", "bpchar", false),
            column("operation_state", "varchar", false),
            column("attempt_count", "int4", false),
            column("next_attempt_at", "int8", true),
            column("lease_token", "varchar", true),
            column("lease_expires_at", "int8", true),
            column("last_http_status", "int4", true),
            column("last_error_code", "varchar", true),
            column("balance_before", "int8", true),
            column("balance_after", "int8", true),
            column("external_created_at", "int8", true),
        ],
    },
    TableShape {
        name: "balance_operation_outbox",
        columns: &[
            column("id", "int8", false),
            column("operation_id", "int8", false),
            column("event_type", "varchar", false),
            column("available_at", "int8", false),
            column("attempt_count", "int4", false),
            column("lease_token", "varchar", true),
            column("lease_expires_at", "int8", true),
            column("last_error_code", "varchar", true),
            column("last_http_status", "int4", true),
            column("dispatched_at", "int8", true),
        ],
    },
    TableShape {
        name: "balance_operation_events",
        columns: &[
            column("id", "int8", false),
            column("operation_id", "int8", false),
            column("event_type", "varchar", false),
        ],
    },
    TableShape {
        name: "rebate_reconciliation_cases",
        columns: &[
            column("id", "int8", false),
            column("issue_code", "varchar", false),
            column("case_state", "varchar", false),
            column("evidence_sha256", "bpchar", false),
        ],
    },
    TableShape {
        name: "rebate_sync_checkpoints",
        columns: &[
            column("sync_type", "varchar", false),
            column("source_namespace", "varchar", false),
            column("cursor_time", "int8", false),
            column("cursor_id", "int8", false),
        ],
    },
    TableShape {
        name: "rebate_background_jobs",
        columns: &[
            column("id", "int8", false),
            column("job_type", "varchar", false),
            column("partition_key", "varchar", false),
            column("job_state", "varchar", false),
        ],
    },
    TableShape {
        name: "_sqlx_migrations",
        columns: &[
            column("version", "int8", false),
            column("success", "bool", false),
            column("checksum", "bytea", false),
        ],
    },
];

const EXTERNAL_MARK_SHAPE: TableShape = TableShape {
    name: "kkai_invitation_rebate_balance_marks",
    columns: &[
        column("id", "int8", false),
        column("kind", "varchar", false),
        column("rebate_record_id", "int8", false),
        column("payout_id", "int8", false),
        column("original_payout_id", "int8", true),
        column("user_id", "int8", false),
        column("quota_delta", "int8", false),
        column("amount_cents", "int8", false),
        column("request_id", "varchar", false),
    ],
};

const PRE_DISPATCHER_OUTBOX_COLUMNS: &[ColumnSpec] = &[
    column("id", "int8", false),
    column("operation_id", "int8", false),
    column("event_type", "varchar", false),
    column("available_at", "int8", false),
    column("attempt_count", "int4", false),
    column("lease_token", "varchar", true),
    column("lease_expires_at", "int8", true),
    column("dispatched_at", "int8", true),
];

async fn inspect_schema_shapes(
    connection: &mut PgConnection,
    report: &mut PreflightReport,
) -> Result<(), sqlx::Error> {
    if table_exists(connection, "withdrawal_requests").await? {
        block_unsupported_shape(
            report,
            "withdrawal_requests",
            &["unsupported_legacy_table".to_string()],
        );
    }

    let dispatcher_schema_applied =
        migration_applied(connection, DISPATCHER_SCHEMA_MIGRATION).await?;
    for shape in LOCAL_TABLE_SHAPES {
        if !table_exists(connection, shape.name).await? {
            continue;
        }
        let shape = dispatcher_compatible_shape(*shape, dispatcher_schema_applied);
        let mismatches = table_shape_mismatches(connection, shape).await?;
        if !mismatches.is_empty() {
            block_unsupported_shape(report, shape.name, &mismatches);
        }
    }

    for (child, parents) in [
        (
            "invitation_rebate_payouts",
            &["rebate_records", "rebate_requests"] as &[&str],
        ),
        (
            "rebate_request_items",
            &["rebate_records", "rebate_requests"] as &[&str],
        ),
        (
            "balance_operations",
            &["rebate_records", "rebate_requests"] as &[&str],
        ),
    ] {
        if table_exists(connection, child).await? {
            for parent in parents {
                if !table_exists(connection, parent).await? {
                    block_unsupported_shape(
                        report,
                        child,
                        &[format!("{child}:missing_parent:{parent}")],
                    );
                }
            }
        }
    }
    Ok(())
}

fn dispatcher_compatible_shape(shape: TableShape, dispatcher_schema_applied: bool) -> TableShape {
    if dispatcher_schema_applied {
        return shape;
    }

    match shape.name {
        "balance_operations" => TableShape {
            name: shape.name,
            columns: &shape.columns[..shape.columns.len() - 3],
        },
        "balance_operation_outbox" => TableShape {
            name: shape.name,
            columns: PRE_DISPATCHER_OUTBOX_COLUMNS,
        },
        _ => shape,
    }
}

async fn migration_applied(
    connection: &mut PgConnection,
    version: i64,
) -> Result<bool, sqlx::Error> {
    if !has_columns(connection, "_sqlx_migrations", &["version", "success"]).await? {
        return Ok(false);
    }
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM _sqlx_migrations WHERE version = $1 AND success = TRUE)",
    )
    .bind(version)
    .fetch_one(&mut *connection)
    .await
}

async fn inspect_external_schema_shape(
    connection: &mut PgConnection,
    report: &mut PreflightReport,
) -> Result<(), sqlx::Error> {
    if !table_exists(connection, EXTERNAL_MARK_SHAPE.name).await? {
        return Ok(());
    }
    report
        .inspected_tables
        .push(format!("newapi.{}", EXTERNAL_MARK_SHAPE.name));
    let mismatches = table_shape_mismatches(connection, EXTERNAL_MARK_SHAPE).await?;
    if !mismatches.is_empty() {
        report.schema_compatible_before_migration = false;
        report.issues.push(PreflightIssue {
            code: "external_evidence_schema_incompatible",
            severity: IssueSeverity::Blocking,
            count: mismatches.len() as i64,
            sample_ids: mismatches.into_iter().take(SAMPLE_LIMIT as usize).collect(),
            message: "The NewAPI balance-mark table cannot provide provable settlement evidence.",
        });
    }
    Ok(())
}

async fn table_shape_mismatches(
    connection: &mut PgConnection,
    shape: TableShape,
) -> Result<Vec<String>, sqlx::Error> {
    let mut mismatches = Vec::new();
    let relation_kind: Option<String> = sqlx::query_scalar(
        r#"
        SELECT table_class.relkind::TEXT
        FROM pg_class table_class
        JOIN pg_namespace namespace ON namespace.oid = table_class.relnamespace
        WHERE namespace.nspname = current_schema() AND table_class.relname = $1
        "#,
    )
    .bind(shape.name)
    .fetch_optional(&mut *connection)
    .await?;
    if relation_kind.as_deref() != Some("r") {
        mismatches.push(format!(
            "{}.relkind:{}",
            shape.name,
            relation_kind.as_deref().unwrap_or("missing")
        ));
    }

    for expected in shape.columns {
        let actual = sqlx::query(
            r#"
            SELECT udt_name, is_nullable
            FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = $1 AND column_name = $2
            "#,
        )
        .bind(shape.name)
        .bind(expected.name)
        .fetch_optional(&mut *connection)
        .await?;
        match actual {
            Some(row) => {
                let udt_name: String = row.try_get("udt_name")?;
                let is_nullable: String = row.try_get("is_nullable")?;
                if udt_name != expected.udt_name || (is_nullable == "YES") != expected.nullable {
                    mismatches.push(format!(
                        "{}.{}:{}:{}",
                        shape.name, expected.name, udt_name, is_nullable
                    ));
                }
            }
            None => mismatches.push(format!("{}.{}:missing", shape.name, expected.name)),
        }
    }

    if let Some(expected_primary_key) = expected_primary_key(shape.name) {
        let actual_primary_key: Option<Vec<String>> = sqlx::query_scalar(
            r#"
            SELECT ARRAY(
                SELECT attribute.attname::TEXT
                FROM unnest(constraint_meta.conkey) WITH ORDINALITY AS key(attnum, position)
                JOIN pg_attribute attribute
                  ON attribute.attrelid = constraint_meta.conrelid
                 AND attribute.attnum = key.attnum
                ORDER BY key.position
            )::TEXT[]
            FROM pg_constraint constraint_meta
            JOIN pg_class table_class ON table_class.oid = constraint_meta.conrelid
            JOIN pg_namespace namespace ON namespace.oid = table_class.relnamespace
            WHERE namespace.nspname = current_schema()
              AND table_class.relname = $1
              AND constraint_meta.contype = 'p'
            "#,
        )
        .bind(shape.name)
        .fetch_optional(&mut *connection)
        .await?;
        let expected: Vec<String> = expected_primary_key
            .iter()
            .map(|column| (*column).to_string())
            .collect();
        if actual_primary_key.as_ref() != Some(&expected) {
            mismatches.push(format!(
                "{}.primary_key:{}",
                shape.name,
                actual_primary_key
                    .map(|columns| columns.join(","))
                    .unwrap_or_else(|| "missing".to_string())
            ));
        }
    }

    for expected_unique_key in expected_unique_keys(shape.name) {
        let expected: Vec<String> = expected_unique_key
            .iter()
            .map(|column| (*column).to_string())
            .collect();
        let exists: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM pg_index index_meta
                JOIN pg_class table_class ON table_class.oid = index_meta.indrelid
                JOIN pg_namespace namespace ON namespace.oid = table_class.relnamespace
                WHERE namespace.nspname = current_schema()
                  AND table_class.relname = $1
                  AND index_meta.indisunique
                  AND index_meta.indisvalid
                  AND index_meta.indisready
                  AND index_meta.indpred IS NULL
                  AND index_meta.indexprs IS NULL
                  AND ARRAY(
                      SELECT attribute.attname::TEXT
                      FROM unnest(index_meta.indkey::SMALLINT[])
                           WITH ORDINALITY AS key(attnum, position)
                      JOIN pg_attribute attribute
                        ON attribute.attrelid = index_meta.indrelid
                       AND attribute.attnum = key.attnum
                      WHERE key.position <= index_meta.indnkeyatts
                      ORDER BY key.position
                  )::TEXT[] = $2::TEXT[]
            )
            "#,
        )
        .bind(shape.name)
        .bind(&expected)
        .fetch_one(&mut *connection)
        .await?;
        if !exists {
            mismatches.push(format!("{}.unique_key:{}", shape.name, expected.join(",")));
        }
    }
    Ok(mismatches)
}

fn expected_primary_key(table: &str) -> Option<&'static [&'static str]> {
    match table {
        "rebate_rules"
        | "rebate_records"
        | "invitation_registrations"
        | "rebate_requests"
        | "invitation_rebate_payouts"
        | "invitation_rebate_payout_attempts"
        | "sync_log"
        | "rebate_record_request_ownership"
        | "balance_operations"
        | "balance_operation_outbox"
        | "balance_operation_events"
        | "rebate_reconciliation_cases"
        | "rebate_background_jobs" => Some(&["id"]),
        "system_config" => Some(&["key"]),
        "rebate_request_items" => Some(&["request_id", "rebate_record_id"]),
        "rebate_record_settlements" | "rebate_calculation_snapshots" => Some(&["rebate_record_id"]),
        "rebate_request_money_snapshots" => Some(&["rebate_request_id"]),
        "rebate_sync_checkpoints" => Some(&["sync_type", "source_namespace"]),
        "_sqlx_migrations" => Some(&["version"]),
        "kkai_invitation_rebate_balance_marks" => Some(&["id"]),
        _ => None,
    }
}

fn expected_unique_keys(table: &str) -> &'static [&'static [&'static str]] {
    match table {
        "rebate_rules" => &[&["user_group", "rule_type"]],
        "rebate_records" => &[&["order_type", "order_id"]],
        "invitation_registrations" => &[&["invitee_id"]],
        "rebate_record_request_ownership" => &[&["request_id", "rebate_record_id"]],
        "balance_operations" => &[
            &["operation_ulid"],
            &["idempotency_key"],
            &["source", "business_operation_key"],
        ],
        "balance_operation_outbox" => &[&["operation_id", "event_type"]],
        "rebate_background_jobs" => &[&["job_type", "partition_key"]],
        "kkai_invitation_rebate_balance_marks" => &[&["request_id"]],
        _ => &[],
    }
}

fn block_unsupported_shape(report: &mut PreflightReport, table: &str, details: &[String]) {
    report.schema_compatible_before_migration = false;
    let mut ids = if details.is_empty() {
        vec![table.to_string()]
    } else {
        details.to_vec()
    };
    ids.truncate(SAMPLE_LIMIT as usize);
    report.issues.push(PreflightIssue {
        code: "unsupported_table_shape",
        severity: IssueSeverity::Blocking,
        count: details.len().max(1) as i64,
        sample_ids: ids,
        message: "An existing table does not match an explicitly supported schema shape, even if it is empty.",
    });
}

async fn postgres_version_num(connection: &mut PgConnection) -> Result<i32, sqlx::Error> {
    let version: String = sqlx::query_scalar("SHOW server_version_num")
        .fetch_one(&mut *connection)
        .await?;
    version.parse::<i32>().map_err(|error| {
        sqlx::Error::Protocol(format!("invalid PostgreSQL server_version_num: {error}"))
    })
}

fn inspect_postgres_versions(report: &mut PreflightReport) {
    for (scope, version) in [
        ("invitations", report.postgres_version_num),
        ("newapi", report.external_postgres_version_num),
    ] {
        if !(140_000..170_000).contains(&version) {
            report.schema_compatible_before_migration = false;
            report.issues.push(PreflightIssue {
                code: "unsupported_postgres_version",
                severity: IssueSeverity::Blocking,
                count: 1,
                sample_ids: vec![format!("{scope}:{version}")],
                message: "PostgreSQL 14 through 16 is required for this migration protocol.",
            });
        }
    }
}

#[derive(Debug)]
struct LocalPayoutEvidence {
    id: i64,
    rebate_record_id: i64,
    inviter_user_id: i64,
    amount_cents: i64,
    quota_delta: i64,
    kind: String,
    status: String,
    request_id: String,
    newapi_mark_id: Option<i64>,
    original_payout_id: Option<i64>,
}

#[derive(Debug)]
struct ExternalMarkEvidence {
    id: i64,
    kind: String,
    rebate_record_id: i64,
    payout_id: i64,
    original_payout_id: Option<i64>,
    user_id: i64,
    quota_delta: i64,
    amount_cents: i64,
    request_id: String,
}

async fn inspect_external_evidence(
    connection: &mut PgConnection,
    external_connection: &mut PgConnection,
    report: &mut PreflightReport,
) -> Result<(), sqlx::Error> {
    let local_rows = if table_exists(connection, "invitation_rebate_payouts").await? {
        sqlx::query(
            r#"
            SELECT id, rebate_record_id, inviter_user_id,
                   amount_cents, quota_delta, kind, status, request_id,
                   newapi_mark_id, original_payout_id
            FROM invitation_rebate_payouts
            ORDER BY id
            "#,
        )
        .fetch_all(&mut *connection)
        .await?
        .iter()
        .map(local_payout_evidence_from_row)
        .collect::<Result<Vec<_>, _>>()?
    } else {
        Vec::new()
    };

    let external_table_exists = table_exists(external_connection, EXTERNAL_MARK_SHAPE.name).await?;
    if !external_table_exists {
        if !local_rows.is_empty() {
            push_memory_issue(
                report,
                "external_evidence_table_missing",
                "Local payout rows exist but NewAPI has no verifiable balance-mark table.",
                local_rows.iter().map(|row| row.id.to_string()).collect(),
            );
        }
        report.external_evidence_checked = true;
        return Ok(());
    }

    let external_rows = sqlx::query(
        r#"
        SELECT id, kind, rebate_record_id, payout_id, original_payout_id,
               user_id, quota_delta, amount_cents, request_id
        FROM kkai_invitation_rebate_balance_marks
        ORDER BY id
        "#,
    )
    .fetch_all(&mut *external_connection)
    .await?
    .iter()
    .map(external_mark_evidence_from_row)
    .collect::<Result<Vec<_>, _>>()?;

    let mut marks_by_id: HashMap<i64, Vec<&ExternalMarkEvidence>> = HashMap::new();
    for mark in &external_rows {
        marks_by_id.entry(mark.id).or_default().push(mark);
    }
    let duplicate_mark_ids: Vec<String> = marks_by_id
        .iter()
        .filter(|(_, rows)| rows.len() > 1)
        .map(|(id, _)| id.to_string())
        .collect();
    if !duplicate_mark_ids.is_empty() {
        push_memory_issue(
            report,
            "duplicate_external_mark_id",
            "NewAPI contains duplicate balance-mark IDs and cannot prove identity.",
            duplicate_mark_ids,
        );
    }

    let mut local_by_id: HashMap<i64, Vec<&LocalPayoutEvidence>> = HashMap::new();
    for payout in &local_rows {
        local_by_id.entry(payout.id).or_default().push(payout);
    }

    let mut missing_mark_ids = Vec::new();
    let mut mismatched_mark_ids = Vec::new();
    for payout in &local_rows {
        if matches!(payout.status.as_str(), "completed" | "reversed")
            && payout.newapi_mark_id.is_none()
        {
            missing_mark_ids.push(payout.id.to_string());
            continue;
        }
        let Some(mark_id) = payout.newapi_mark_id else {
            continue;
        };
        let Some(candidates) = marks_by_id.get(&mark_id) else {
            missing_mark_ids.push(payout.id.to_string());
            continue;
        };
        if candidates.len() != 1 || !external_mark_matches(payout, candidates[0]) {
            mismatched_mark_ids.push(format!("{}:{mark_id}", payout.id));
        }
    }
    if !missing_mark_ids.is_empty() {
        push_memory_issue(
            report,
            "external_mark_missing",
            "A locally final or marked payout has no matching NewAPI evidence row.",
            missing_mark_ids,
        );
    }
    if !mismatched_mark_ids.is_empty() {
        push_memory_issue(
            report,
            "external_mark_identity_mismatch",
            "A NewAPI mark disagrees with local payout, record, user, kind, amount, delta, request, or original identity.",
            mismatched_mark_ids,
        );
    }

    let mut orphan_marks = Vec::new();
    for mark in &external_rows {
        let Some(candidates) = local_by_id.get(&mark.payout_id) else {
            orphan_marks.push(mark.id.to_string());
            continue;
        };
        if candidates.len() != 1
            || candidates[0].newapi_mark_id != Some(mark.id)
            || !external_mark_matches(candidates[0], mark)
        {
            orphan_marks.push(mark.id.to_string());
        }
    }
    if !orphan_marks.is_empty() {
        push_memory_issue(
            report,
            "external_orphan_balance_mark",
            "A NewAPI balance mark has no exact local payout owner.",
            orphan_marks,
        );
    }

    report.external_evidence_checked = true;
    Ok(())
}

fn external_mark_matches(payout: &LocalPayoutEvidence, mark: &ExternalMarkEvidence) -> bool {
    mark.payout_id == payout.id
        && mark.rebate_record_id == payout.rebate_record_id
        && mark.user_id == payout.inviter_user_id
        && mark.kind == payout.kind
        && mark.quota_delta == payout.quota_delta
        && mark.amount_cents == payout.amount_cents
        && mark.request_id == payout.request_id
        && mark.original_payout_id == payout.original_payout_id
}

fn local_payout_evidence_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<LocalPayoutEvidence, sqlx::Error> {
    Ok(LocalPayoutEvidence {
        id: row.try_get("id")?,
        rebate_record_id: row.try_get("rebate_record_id")?,
        inviter_user_id: row.try_get("inviter_user_id")?,
        amount_cents: row.try_get("amount_cents")?,
        quota_delta: row.try_get("quota_delta")?,
        kind: row.try_get("kind")?,
        status: row.try_get("status")?,
        request_id: row.try_get("request_id")?,
        newapi_mark_id: row.try_get("newapi_mark_id")?,
        original_payout_id: row.try_get("original_payout_id")?,
    })
}

fn external_mark_evidence_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<ExternalMarkEvidence, sqlx::Error> {
    Ok(ExternalMarkEvidence {
        id: row.try_get("id")?,
        kind: row.try_get("kind")?,
        rebate_record_id: row.try_get("rebate_record_id")?,
        payout_id: row.try_get("payout_id")?,
        original_payout_id: row.try_get("original_payout_id")?,
        user_id: row.try_get("user_id")?,
        quota_delta: row.try_get("quota_delta")?,
        amount_cents: row.try_get("amount_cents")?,
        request_id: row.try_get("request_id")?,
    })
}

fn push_memory_issue(
    report: &mut PreflightReport,
    code: &'static str,
    message: &'static str,
    mut ids: Vec<String>,
) {
    if ids.is_empty() {
        return;
    }
    let count = ids.len() as i64;
    ids.sort();
    ids.truncate(SAMPLE_LIMIT as usize);
    report.issues.push(PreflightIssue {
        code,
        severity: IssueSeverity::Blocking,
        count,
        sample_ids: ids,
        message,
    });
}

async fn inspect_legacy_shape(
    connection: &mut PgConnection,
    report: &mut PreflightReport,
) -> Result<(), sqlx::Error> {
    if table_exists(connection, "rebate_records").await? {
        let required = [
            "id",
            "user_group",
            "order_time",
            "rebate_effective_at",
            "scan_started_at",
            "initialization_ends_at",
        ];
        let missing = missing_columns(connection, "rebate_records", &required).await?;
        if !missing.is_empty() && table_has_rows(connection, "rebate_records").await? {
            let message =
                "Populated legacy rebate records lack fields that cannot be inferred safely.";
            add_table_id_issue(
                connection,
                report,
                "rebate_records",
                "legacy_rebate_records_require_manual_enrichment",
                message,
            )
            .await?;
        }

        if missing.is_empty() {
            add_query_issue(
                connection,
                report,
                IssueSpec {
                    code: "legacy_rebate_records_have_null_required_fields",
                    severity: IssueSeverity::Blocking,
                    message: "Legacy rebate records contain null fields required by the current read model.",
                },
                r#"
                SELECT id::TEXT AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count
                FROM rebate_records
                WHERE user_group IS NULL OR order_time IS NULL OR rebate_effective_at IS NULL
                   OR scan_started_at IS NULL OR initialization_ends_at IS NULL
                ORDER BY id LIMIT $1
                "#,
            )
            .await?;
        }
    }

    if table_exists(connection, "rebate_requests").await? {
        let required = [
            "id",
            "user_id",
            "rebate_record_ids",
            "total_amount",
            "status",
            "created_at",
            "updated_at",
        ];
        if !missing_columns(connection, "rebate_requests", &required)
            .await?
            .is_empty()
            && table_has_rows(connection, "rebate_requests").await?
        {
            add_table_id_issue(
                connection,
                report,
                "rebate_requests",
                "legacy_rebate_requests_have_incompatible_shape",
                "Populated legacy rebate requests lack columns required for safe normalization.",
            )
            .await?;
        }
    }

    if table_exists(connection, "invitation_rebate_payouts").await? {
        let required = [
            "id",
            "rebate_record_id",
            "kind",
            "status",
            "request_id",
            "original_payout_id",
            "amount_cents",
            "quota_delta",
            "newapi_mark_id",
        ];
        if !missing_columns(connection, "invitation_rebate_payouts", &required)
            .await?
            .is_empty()
            && table_has_rows(connection, "invitation_rebate_payouts").await?
        {
            add_table_id_issue(
                connection,
                report,
                "invitation_rebate_payouts",
                "legacy_payout_ledger_has_incompatible_shape",
                "Populated legacy payout rows lack fields required to prove settlement state.",
            )
            .await?;
        }
    }

    Ok(())
}

async fn inspect_rebate_records(
    connection: &mut PgConnection,
    report: &mut PreflightReport,
) -> Result<(), sqlx::Error> {
    if !has_columns(
        connection,
        "rebate_records",
        &[
            "id",
            "status",
            "order_type",
            "order_id",
            "order_amount",
            "rebate_amount",
            "rebate_rate",
        ],
    )
    .await?
    {
        return Ok(());
    }

    add_query_issue(
        connection,
        report,
        IssueSpec {
            code: "duplicate_rebate_order_identity",
            severity: IssueSeverity::Blocking,
            message: "Multiple rebate records use the same order type and order ID.",
        },
        r#"
        SELECT (order_type || ':' || order_id::TEXT) AS entity_id,
               COUNT(*) OVER ()::BIGINT AS total_count
        FROM (
            SELECT order_type, order_id
            FROM rebate_records
            GROUP BY order_type, order_id
            HAVING COUNT(*) > 1
        ) duplicates
        ORDER BY order_type, order_id LIMIT $1
        "#,
    )
    .await?;

    add_query_issue(
        connection,
        report,
        IssueSpec {
            code: "illegal_rebate_record_state",
            severity: IssueSeverity::Blocking,
            message: "Rebate records contain states outside the reviewed legacy state set.",
        },
        r#"
        SELECT id::TEXT AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count
        FROM rebate_records
        WHERE status IS NULL
           OR status NOT IN ('initializing', 'pending', 'requested', 'approved', 'completed', 'closed')
        ORDER BY id LIMIT $1
        "#,
    )
    .await?;

    add_query_issue(
        connection,
        report,
        IssueSpec {
            code: "invalid_rebate_record_amount",
            severity: IssueSeverity::Blocking,
            message: "Rebate records contain negative values, invalid rates, or values unsafe for integer minor-unit conversion.",
        },
        r#"
        SELECT id::TEXT AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count
        FROM rebate_records
        WHERE order_amount IS NULL OR rebate_amount IS NULL
           OR order_amount < 0
           OR order_amount * 100 > 9223372036854775807::NUMERIC
           OR rebate_amount < 0
           OR (rebate_rate IS NOT NULL AND (rebate_rate < 0 OR rebate_rate > 1))
        ORDER BY id LIMIT $1
        "#,
    )
    .await?;

    Ok(())
}

async fn inspect_requests(
    connection: &mut PgConnection,
    report: &mut PreflightReport,
) -> Result<(), sqlx::Error> {
    if !has_columns(
        connection,
        "rebate_requests",
        &[
            "id",
            "user_id",
            "rebate_record_ids",
            "total_amount",
            "status",
        ],
    )
    .await?
    {
        return Ok(());
    }

    add_query_issue(
        connection,
        report,
        IssueSpec {
            code: "illegal_rebate_request_state",
            severity: IssueSeverity::Blocking,
            message: "Rebate requests contain states outside the reviewed legacy state set.",
        },
        r#"
        SELECT id::TEXT AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count
        FROM rebate_requests
        WHERE status IS NULL OR rebate_record_ids IS NULL OR total_amount IS NULL
           OR status NOT IN ('pending', 'approved', 'rejected', 'completed')
        ORDER BY id LIMIT $1
        "#,
    )
    .await?;

    add_query_issue(
        connection,
        report,
        IssueSpec {
            code: "empty_active_rebate_request",
            severity: IssueSeverity::Blocking,
            message: "An active rebate request has no rebate records.",
        },
        r#"
        SELECT id::TEXT AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count
        FROM rebate_requests
        WHERE status <> 'rejected' AND cardinality(rebate_record_ids) = 0
        ORDER BY id LIMIT $1
        "#,
    )
    .await?;

    add_query_issue(
        connection,
        report,
        IssueSpec {
            code: "duplicate_record_within_request",
            severity: IssueSeverity::Blocking,
            message: "A rebate request references the same record more than once.",
        },
        r#"
        SELECT (request_id::TEXT || ':' || rebate_record_id::TEXT) AS entity_id,
               COUNT(*) OVER ()::BIGINT AS total_count
        FROM (
            SELECT request.id AS request_id, item.rebate_record_id
            FROM rebate_requests request
            CROSS JOIN LATERAL unnest(request.rebate_record_ids) AS item(rebate_record_id)
            GROUP BY request.id, item.rebate_record_id
            HAVING COUNT(*) > 1
        ) duplicates
        ORDER BY request_id, rebate_record_id LIMIT $1
        "#,
    )
    .await?;

    if table_exists(connection, "rebate_records").await? {
        add_query_issue(
            connection,
            report,
            IssueSpec {
                code: "request_references_missing_record",
                severity: IssueSeverity::Blocking,
                message: "A rebate request array references a rebate record that does not exist.",
            },
            r#"
            SELECT (request_id::TEXT || ':' || rebate_record_id::TEXT) AS entity_id,
                   COUNT(*) OVER ()::BIGINT AS total_count
            FROM (
                SELECT request.id AS request_id, item.rebate_record_id
                FROM rebate_requests request
                CROSS JOIN LATERAL unnest(request.rebate_record_ids) AS item(rebate_record_id)
                LEFT JOIN rebate_records record ON record.id = item.rebate_record_id
                WHERE record.id IS NULL
            ) missing
            ORDER BY request_id, rebate_record_id LIMIT $1
            "#,
        )
        .await?;

        add_query_issue(
            connection,
            report,
            IssueSpec {
                code: "duplicate_active_record_ownership",
                severity: IssueSeverity::Blocking,
                message: "A rebate record is owned by more than one non-rejected request.",
            },
            r#"
            SELECT rebate_record_id::TEXT AS entity_id,
                   COUNT(*) OVER ()::BIGINT AS total_count
            FROM (
                SELECT item.rebate_record_id
                FROM rebate_requests request
                CROSS JOIN LATERAL unnest(request.rebate_record_ids) AS item(rebate_record_id)
                WHERE request.status <> 'rejected'
                GROUP BY item.rebate_record_id
                HAVING COUNT(DISTINCT request.id) > 1
            ) duplicates
            ORDER BY rebate_record_id LIMIT $1
            "#,
        )
        .await?;

        if has_columns(
            connection,
            "rebate_records",
            &["inviter_id", "rebate_amount"],
        )
        .await?
        {
            add_query_issue(
                connection,
                report,
                IssueSpec {
                    code: "request_record_owner_mismatch",
                    severity: IssueSeverity::Blocking,
                    message: "A request contains a rebate record belonging to a different inviter.",
                },
                r#"
                SELECT (request.id::TEXT || ':' || record.id::TEXT) AS entity_id,
                       COUNT(*) OVER ()::BIGINT AS total_count
                FROM rebate_requests request
                CROSS JOIN LATERAL unnest(request.rebate_record_ids) AS item(rebate_record_id)
                JOIN rebate_records record ON record.id = item.rebate_record_id
                WHERE request.user_id <> record.inviter_id
                ORDER BY request.id, record.id LIMIT $1
                "#,
            )
            .await?;

            let total_sql = if table_exists(connection, "rebate_request_items").await? {
                r#"
                SELECT request.id::TEXT AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count
                FROM rebate_requests request
                JOIN LATERAL (
                    SELECT COALESCE(SUM(item.amount_quota_snapshot::NUMERIC), 0) AS exact_total
                    FROM rebate_request_items item
                    WHERE item.request_id = request.id AND item.membership_state = 'active'
                ) totals ON TRUE
                WHERE request.total_amount IS NULL OR request.total_amount < 0
                   OR totals.exact_total > 9223372036854775807::NUMERIC
                   OR request.total_amount::NUMERIC <> totals.exact_total
                ORDER BY request.id LIMIT $1
                "#
            } else {
                r#"
                SELECT request.id::TEXT AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count
                FROM rebate_requests request
                JOIN LATERAL (
                    SELECT COALESCE(SUM(record.rebate_amount::NUMERIC), 0) AS exact_total
                    FROM unnest(request.rebate_record_ids) AS item(rebate_record_id)
                    JOIN rebate_records record ON record.id = item.rebate_record_id
                ) totals ON TRUE
                WHERE request.total_amount IS NULL OR request.total_amount < 0
                   OR totals.exact_total > 9223372036854775807::NUMERIC
                   OR request.total_amount::NUMERIC <> totals.exact_total
                ORDER BY request.id LIMIT $1
                "#
            };
            add_query_issue(
                connection,
                report,
                IssueSpec {
                    code: "request_total_mismatch",
                    severity: IssueSeverity::Blocking,
                    message: "A request total differs from the exact sum of its referenced rebate records.",
                },
                total_sql,
            )
            .await?;
        }
    }

    Ok(())
}

async fn inspect_payouts(
    connection: &mut PgConnection,
    report: &mut PreflightReport,
) -> Result<(), sqlx::Error> {
    if !has_columns(
        connection,
        "invitation_rebate_payouts",
        &[
            "id",
            "rebate_record_id",
            "kind",
            "status",
            "request_id",
            "original_payout_id",
            "amount_cents",
            "quota_delta",
            "newapi_mark_id",
        ],
    )
    .await?
    {
        inspect_completed_without_payout_table(connection, report).await?;
        return Ok(());
    }

    add_query_issue(
        connection,
        report,
        IssueSpec {
            code: "duplicate_payout_for_record",
            severity: IssueSeverity::Blocking,
            message: "A rebate record has more than one payout ledger row.",
        },
        r#"
        SELECT rebate_record_id::TEXT AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count
        FROM (
            SELECT rebate_record_id
            FROM invitation_rebate_payouts
            WHERE kind = 'payout' AND rebate_record_id IS NOT NULL
            GROUP BY rebate_record_id HAVING COUNT(*) > 1
        ) duplicates
        ORDER BY rebate_record_id LIMIT $1
        "#,
    )
    .await?;

    add_query_issue(
        connection,
        report,
        IssueSpec {
            code: "duplicate_reversal_for_payout",
            severity: IssueSeverity::Blocking,
            message: "A payout has more than one reversal ledger row.",
        },
        r#"
        SELECT original_payout_id::TEXT AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count
        FROM (
            SELECT original_payout_id
            FROM invitation_rebate_payouts
            WHERE kind = 'reversal' AND original_payout_id IS NOT NULL
            GROUP BY original_payout_id HAVING COUNT(*) > 1
        ) duplicates
        ORDER BY original_payout_id LIMIT $1
        "#,
    )
    .await?;

    add_query_issue(
        connection,
        report,
        IssueSpec {
            code: "duplicate_payout_request_id",
            severity: IssueSeverity::Blocking,
            message: "Multiple payout ledger rows share the same request ID.",
        },
        r#"
        SELECT request_id AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count
        FROM (
            SELECT request_id
            FROM invitation_rebate_payouts
            WHERE request_id IS NOT NULL
            GROUP BY request_id HAVING COUNT(*) > 1
        ) duplicates
        ORDER BY request_id LIMIT $1
        "#,
    )
    .await?;

    add_query_issue(
        connection,
        report,
        IssueSpec {
            code: "illegal_payout_state",
            severity: IssueSeverity::Blocking,
            message: "Payout ledger rows contain an unknown kind or state.",
        },
        r#"
        SELECT id::TEXT AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count
        FROM invitation_rebate_payouts
        WHERE kind IS NULL OR status IS NULL OR request_id IS NULL OR rebate_record_id IS NULL
           OR kind NOT IN ('payout', 'reversal')
           OR status NOT IN ('pending', 'newapi_applied', 'completed', 'reversed', 'failed', 'reversal_pending')
           OR (kind = 'payout' AND original_payout_id IS NOT NULL)
           OR (kind = 'reversal' AND (
                original_payout_id IS NULL
                OR status NOT IN ('pending', 'newapi_applied', 'completed', 'failed')
           ))
        ORDER BY id LIMIT $1
        "#,
    )
    .await?;

    add_query_issue(
        connection,
        report,
        IssueSpec {
            code: "invalid_payout_amount",
            severity: IssueSeverity::Blocking,
            message: "A payout ledger row has an invalid sign, amount, or non-reversible minimum integer value.",
        },
        r#"
        SELECT id::TEXT AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count
        FROM invitation_rebate_payouts
        WHERE amount_cents IS NULL OR quota_delta IS NULL
           OR amount_cents < 0 OR quota_delta = 0
           OR (kind = 'payout' AND quota_delta < 0)
           OR (kind = 'reversal' AND quota_delta > 0)
           OR quota_delta = (-9223372036854775807::BIGINT - 1)
        ORDER BY id LIMIT $1
        "#,
    )
    .await?;

    add_query_issue(
        connection,
        report,
        IssueSpec {
            code: "inflight_payout_requires_reconciliation",
            severity: IssueSeverity::Blocking,
            message: "A payout or reversal is unfinished or failed and must be reconciled before migration.",
        },
        r#"
        SELECT id::TEXT AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count
        FROM invitation_rebate_payouts
        WHERE status IN ('pending', 'newapi_applied', 'failed', 'reversal_pending', 'needs_review')
        ORDER BY id LIMIT $1
        "#,
    )
    .await?;

    add_query_issue(
        connection,
        report,
        IssueSpec {
            code: "invalid_reversal_linkage",
            severity: IssueSeverity::Blocking,
            message: "A reversal is missing its original payout or does not exactly reverse it.",
        },
        r#"
        SELECT reversal.id::TEXT AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count
        FROM invitation_rebate_payouts reversal
        LEFT JOIN invitation_rebate_payouts original ON original.id = reversal.original_payout_id
        WHERE reversal.kind = 'reversal'
          AND (
              original.id IS NULL OR original.kind <> 'payout'
              OR original.rebate_record_id <> reversal.rebate_record_id
              OR original.rebate_request_id IS DISTINCT FROM reversal.rebate_request_id
              OR original.inviter_user_id <> reversal.inviter_user_id
              OR original.invitee_user_id <> reversal.invitee_user_id
              OR original.order_id <> reversal.order_id
              OR original.order_type <> reversal.order_type
              OR original.amount_cents <> reversal.amount_cents
              OR original.quota_delta::NUMERIC + reversal.quota_delta::NUMERIC <> 0
          )
        ORDER BY reversal.id LIMIT $1
        "#,
    )
    .await?;

    add_query_issue(
        connection,
        report,
        IssueSpec {
            code: "external_mark_state_mismatch",
            severity: IssueSeverity::Blocking,
            message: "A locally final payout state lacks its external mark, or a reversed payout lacks a completed reversal.",
        },
        r#"
        SELECT id::TEXT AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count
        FROM invitation_rebate_payouts payout
        WHERE (payout.status IN ('completed', 'reversed') AND payout.newapi_mark_id IS NULL)
           OR (payout.kind = 'payout' AND payout.status = 'reversed' AND NOT EXISTS (
                SELECT 1 FROM invitation_rebate_payouts reversal
                WHERE reversal.original_payout_id = payout.id
                  AND reversal.kind = 'reversal' AND reversal.status = 'completed'
           ))
        ORDER BY id LIMIT $1
        "#,
    )
    .await?;

    if table_exists(connection, "rebate_records").await? {
        add_query_issue(
            connection,
            report,
            IssueSpec {
                code: "payout_references_missing_record",
                severity: IssueSeverity::Blocking,
                message: "A payout ledger row references a missing rebate record.",
            },
            r#"
            SELECT payout.id::TEXT AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count
            FROM invitation_rebate_payouts payout
            LEFT JOIN rebate_records record ON record.id = payout.rebate_record_id
            WHERE record.id IS NULL
            ORDER BY payout.id LIMIT $1
            "#,
        )
        .await?;

        add_query_issue(
            connection,
            report,
            IssueSpec {
                code: "payout_business_identity_mismatch",
                severity: IssueSeverity::Blocking,
                message: "A payout or reversal disagrees with its record/request/order/user identity or exact rebate quota.",
            },
            r#"
            SELECT payout.id::TEXT AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count
            FROM invitation_rebate_payouts payout
            LEFT JOIN rebate_records record ON record.id = payout.rebate_record_id
            LEFT JOIN rebate_requests request ON request.id = payout.rebate_request_id
            WHERE record.id IS NULL
               OR payout.inviter_user_id <> record.inviter_id
               OR payout.invitee_user_id <> record.invitee_id
               OR payout.order_id <> record.order_id
               OR payout.order_type <> record.order_type
               OR (payout.kind = 'payout' AND payout.quota_delta <> record.rebate_amount)
               OR (payout.kind = 'reversal'
                   AND payout.quota_delta::NUMERIC + record.rebate_amount::NUMERIC <> 0)
               OR (payout.rebate_request_id IS NOT NULL AND (
                    request.id IS NULL
                    OR request.user_id <> payout.inviter_user_id
                    OR NOT (payout.rebate_record_id = ANY(request.rebate_record_ids))
               ))
               OR (payout.rebate_request_id IS NULL AND EXISTS (
                    SELECT 1 FROM rebate_requests owner
                    WHERE payout.rebate_record_id = ANY(owner.rebate_record_ids)
                      AND owner.status <> 'rejected'
               ))
            ORDER BY payout.id LIMIT $1
            "#,
        )
        .await?;

        if table_exists(connection, "rebate_request_items").await? {
            add_query_issue(
                connection,
                report,
                IssueSpec {
                    code: "payout_snapshot_amount_mismatch",
                    severity: IssueSeverity::Blocking,
                    message: "A request-linked payout does not exactly match its immutable amount snapshot.",
                },
                r#"
                SELECT payout.id::TEXT AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count
                FROM invitation_rebate_payouts payout
                LEFT JOIN rebate_request_items item
                  ON item.request_id = payout.rebate_request_id
                 AND item.rebate_record_id = payout.rebate_record_id
                WHERE payout.rebate_request_id IS NOT NULL
                  AND (
                      item.request_id IS NULL
                      OR (payout.kind = 'payout'
                          AND payout.quota_delta <> item.amount_quota_snapshot)
                      OR (payout.kind = 'reversal'
                          AND payout.quota_delta::NUMERIC
                              + item.amount_quota_snapshot::NUMERIC <> 0)
                  )
                ORDER BY payout.id LIMIT $1
                "#,
            )
            .await?;
        }

        add_query_issue(
            connection,
            report,
            IssueSpec {
                code: "payout_record_state_mismatch",
                severity: IssueSeverity::Blocking,
                message: "A final payout state disagrees with the locally stored rebate record state.",
            },
            r#"
            SELECT payout.id::TEXT AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count
            FROM invitation_rebate_payouts payout
            JOIN rebate_records record ON record.id = payout.rebate_record_id
            WHERE (payout.kind = 'payout' AND payout.status = 'completed' AND record.status <> 'completed')
               OR (payout.kind = 'payout' AND payout.status = 'reversed' AND record.status = 'completed')
               OR (payout.kind = 'reversal' AND payout.status = 'completed' AND NOT EXISTS (
                    SELECT 1 FROM invitation_rebate_payouts original
                    WHERE original.id = payout.original_payout_id
                      AND original.kind = 'payout' AND original.status = 'reversed'
               ))
            ORDER BY payout.id LIMIT $1
            "#,
        )
        .await?;

        inspect_completed_without_payout_table(connection, report).await?;
    }

    if table_exists(connection, "invitation_rebate_payout_attempts").await? {
        add_query_issue(
            connection,
            report,
            IssueSpec {
                code: "payout_attempt_references_missing_record",
                severity: IssueSeverity::Blocking,
                message: "A payout audit attempt references a missing rebate record.",
            },
            r#"
            SELECT attempt.id::TEXT AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count
            FROM invitation_rebate_payout_attempts attempt
            LEFT JOIN rebate_records record ON record.id = attempt.rebate_record_id
            WHERE attempt.rebate_record_id IS NOT NULL AND record.id IS NULL
            ORDER BY attempt.id LIMIT $1
            "#,
        )
        .await?;
    }

    Ok(())
}

async fn inspect_completed_without_payout_table(
    connection: &mut PgConnection,
    report: &mut PreflightReport,
) -> Result<(), sqlx::Error> {
    if !has_columns(connection, "rebate_records", &["id", "status"]).await? {
        return Ok(());
    }

    let payout_table = has_columns(
        connection,
        "invitation_rebate_payouts",
        &["rebate_record_id", "kind", "status"],
    )
    .await?;
    let sql = if payout_table {
        r#"
        SELECT record.id::TEXT AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count
        FROM rebate_records record
        WHERE record.status = 'completed'
          AND NOT EXISTS (
              SELECT 1 FROM invitation_rebate_payouts payout
              WHERE payout.rebate_record_id = record.id
                AND payout.kind = 'payout' AND payout.status IN ('completed', 'reversed')
          )
        ORDER BY record.id LIMIT $1
        "#
    } else {
        r#"
        SELECT id::TEXT AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count
        FROM rebate_records WHERE status = 'completed'
        ORDER BY id LIMIT $1
        "#
    };

    add_query_issue(
        connection,
        report,
        IssueSpec {
            code: "completed_record_without_ledger_evidence",
            severity: IssueSeverity::Blocking,
            message: "A completed rebate record has no locally provable payout ledger evidence.",
        },
        sql,
    )
    .await?;

    if payout_table
        && has_columns(
            connection,
            "rebate_requests",
            &["id", "status", "rebate_record_ids"],
        )
        .await?
    {
        add_query_issue(
            connection,
            report,
            IssueSpec {
                code: "completed_request_without_ledger_evidence",
                severity: IssueSeverity::Blocking,
                message: "A completed request contains at least one record without completed payout evidence.",
            },
            r#"
            SELECT request.id::TEXT AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count
            FROM rebate_requests request
            WHERE request.status = 'completed'
              AND EXISTS (
                  SELECT 1
                  FROM unnest(request.rebate_record_ids) AS item(rebate_record_id)
                  WHERE NOT EXISTS (
                      SELECT 1 FROM invitation_rebate_payouts payout
                      WHERE payout.rebate_record_id = item.rebate_record_id
                        AND payout.kind = 'payout' AND payout.status IN ('completed', 'reversed')
                  )
              )
            ORDER BY request.id LIMIT $1
            "#,
        )
        .await?;
    }

    Ok(())
}

async fn inspect_registrations(
    connection: &mut PgConnection,
    report: &mut PreflightReport,
) -> Result<(), sqlx::Error> {
    if !has_columns(
        connection,
        "invitation_registrations",
        &["id", "invitee_id"],
    )
    .await?
    {
        return Ok(());
    }

    add_query_issue(
        connection,
        report,
        IssueSpec {
            code: "duplicate_invitation_registration",
            severity: IssueSeverity::Blocking,
            message: "An invitee has more than one invitation registration.",
        },
        r#"
        SELECT invitee_id::TEXT AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count
        FROM (
            SELECT invitee_id FROM invitation_registrations
            WHERE invitee_id IS NOT NULL
            GROUP BY invitee_id HAVING COUNT(*) > 1
        ) duplicates
        ORDER BY invitee_id LIMIT $1
        "#,
    )
    .await?;
    Ok(())
}

async fn inspect_normalized_state(
    connection: &mut PgConnection,
    report: &mut PreflightReport,
) -> Result<(), sqlx::Error> {
    if table_exists(connection, "rebate_records").await?
        && table_exists(connection, "rebate_record_settlements").await?
        && has_columns(connection, "rebate_records", &["id"]).await?
        && has_columns(
            connection,
            "rebate_record_settlements",
            &["rebate_record_id"],
        )
        .await?
    {
        add_query_issue(
            connection,
            report,
            IssueSpec {
                code: "normalized_settlement_coverage_missing",
                severity: IssueSeverity::Blocking,
                message: "A rebate record has no normalized settlement state row.",
            },
            r#"
            SELECT record.id::TEXT AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count
            FROM rebate_records record
            LEFT JOIN rebate_record_settlements settlement
              ON settlement.rebate_record_id = record.id
            WHERE settlement.rebate_record_id IS NULL
            ORDER BY record.id LIMIT $1
            "#,
        )
        .await?;
    }

    if !table_exists(connection, "rebate_request_items").await?
        || !table_exists(connection, "rebate_requests").await?
    {
        return Ok(());
    }

    add_query_issue(
        connection,
        report,
        IssueSpec {
            code: "normalized_request_items_diverged",
            severity: IssueSeverity::Blocking,
            message: "Normalized request items differ from the legacy compatibility array.",
        },
        r#"
        SELECT request.id::TEXT AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count
        FROM rebate_requests request
        WHERE ARRAY(
            SELECT item.rebate_record_id
            FROM rebate_request_items item
            WHERE item.request_id = request.id AND item.membership_state = 'active'
            ORDER BY item.ordinal
        )::BIGINT[] IS DISTINCT FROM request.rebate_record_ids
        ORDER BY request.id LIMIT $1
        "#,
    )
    .await?;

    if table_exists(connection, "rebate_record_request_ownership").await? {
        add_query_issue(
            connection,
            report,
            IssueSpec {
                code: "normalized_active_ownership_diverged",
                severity: IssueSeverity::Blocking,
                message:
                    "Normalized active ownership differs from non-rejected request membership.",
            },
            r#"
            SELECT item.rebate_record_id::TEXT AS entity_id,
                   COUNT(*) OVER ()::BIGINT AS total_count
            FROM rebate_request_items item
            JOIN rebate_requests request ON request.id = item.request_id
            LEFT JOIN rebate_record_request_ownership ownership
              ON ownership.request_id = item.request_id
             AND ownership.rebate_record_id = item.rebate_record_id
             AND ownership.ownership_state = 'active'
            WHERE item.membership_state = 'active'
              AND request.status <> 'rejected' AND ownership.id IS NULL
            ORDER BY item.rebate_record_id LIMIT $1
            "#,
        )
        .await?;

        add_query_issue(
            connection,
            report,
            IssueSpec {
                code: "normalized_stale_active_ownership",
                severity: IssueSeverity::Blocking,
                message:
                    "An active ownership row points to removed membership or a rejected request.",
            },
            r#"
            SELECT ownership.id::TEXT AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count
            FROM rebate_record_request_ownership ownership
            LEFT JOIN rebate_request_items item
              ON item.request_id = ownership.request_id
             AND item.rebate_record_id = ownership.rebate_record_id
            LEFT JOIN rebate_requests request ON request.id = ownership.request_id
            WHERE ownership.ownership_state = 'active'
              AND (
                  item.request_id IS NULL OR item.membership_state <> 'active'
                  OR request.id IS NULL OR request.status = 'rejected'
              )
            ORDER BY ownership.id LIMIT $1
            "#,
        )
        .await?;
    }

    Ok(())
}

async fn inspect_balance_operations(
    connection: &mut PgConnection,
    report: &mut PreflightReport,
) -> Result<(), sqlx::Error> {
    if !has_columns(
        connection,
        "balance_operations",
        &[
            "id",
            "operation_kind",
            "operation_state",
            "delta_quota",
            "original_operation_ulid",
        ],
    )
    .await?
    {
        return Ok(());
    }

    add_query_issue(
        connection,
        report,
        IssueSpec {
            code: "illegal_balance_operation",
            severity: IssueSeverity::Blocking,
            message: "A balance operation has an illegal state, sign, or reversal linkage.",
        },
        r#"
        SELECT id::TEXT AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count
        FROM balance_operations
        WHERE operation_state NOT IN (
            'queued', 'dispatching', 'external_pending', 'applied', 'completed',
            'retry_wait', 'blocked', 'needs_review'
        )
           OR (operation_kind = 'credit' AND (delta_quota <= 0 OR original_operation_ulid IS NOT NULL))
           OR (operation_kind = 'reversal' AND (delta_quota >= 0 OR original_operation_ulid IS NULL))
           OR operation_kind NOT IN ('credit', 'reversal')
        ORDER BY id LIMIT $1
        "#,
    )
    .await?;

    add_query_issue(
        connection,
        report,
        IssueSpec {
            code: "duplicate_balance_credit_for_record",
            severity: IssueSeverity::Blocking,
            message: "A rebate record has more than one balance credit operation.",
        },
        r#"
        SELECT rebate_record_id::TEXT AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count
        FROM (
            SELECT rebate_record_id FROM balance_operations
            WHERE operation_kind = 'credit'
            GROUP BY rebate_record_id HAVING COUNT(*) > 1
        ) duplicates
        ORDER BY rebate_record_id LIMIT $1
        "#,
    )
    .await?;

    add_query_issue(
        connection,
        report,
        IssueSpec {
            code: "duplicate_balance_reversal_for_credit",
            severity: IssueSeverity::Blocking,
            message: "A credit operation has more than one reversal operation.",
        },
        r#"
        SELECT original_operation_ulid::TEXT AS entity_id,
               COUNT(*) OVER ()::BIGINT AS total_count
        FROM (
            SELECT original_operation_ulid FROM balance_operations
            WHERE operation_kind = 'reversal' AND original_operation_ulid IS NOT NULL
            GROUP BY original_operation_ulid HAVING COUNT(*) > 1
        ) duplicates
        ORDER BY original_operation_ulid LIMIT $1
        "#,
    )
    .await?;

    add_query_issue(
        connection,
        report,
        IssueSpec {
            code: "balance_reversal_identity_mismatch",
            severity: IssueSeverity::Blocking,
            message: "A reversal does not exactly match one original credit for the same user and record.",
        },
        r#"
        SELECT reversal.id::TEXT AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count
        FROM balance_operations reversal
        LEFT JOIN balance_operations credit
          ON credit.operation_ulid = reversal.original_operation_ulid
        WHERE reversal.operation_kind = 'reversal'
          AND (
              credit.id IS NULL OR credit.operation_kind <> 'credit'
              OR credit.user_id <> reversal.user_id
              OR credit.rebate_record_id <> reversal.rebate_record_id
              OR credit.delta_quota::NUMERIC + reversal.delta_quota::NUMERIC <> 0
          )
        ORDER BY reversal.id LIMIT $1
        "#,
    )
    .await?;

    add_query_issue(
        connection,
        report,
        IssueSpec {
            code: "balance_operation_orphan_identity",
            severity: IssueSeverity::Blocking,
            message: "A balance operation references a missing record or request identity.",
        },
        r#"
        SELECT operation.id::TEXT AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count
        FROM balance_operations operation
        LEFT JOIN rebate_records record ON record.id = operation.rebate_record_id
        LEFT JOIN rebate_requests request ON request.id = operation.rebate_request_id
        WHERE record.id IS NULL
           OR (operation.rebate_request_id IS NOT NULL AND request.id IS NULL)
        ORDER BY operation.id LIMIT $1
        "#,
    )
    .await?;

    Ok(())
}

async fn inspect_migration_history(
    connection: &mut PgConnection,
    report: &mut PreflightReport,
) -> Result<(), sqlx::Error> {
    if !has_columns(connection, "_sqlx_migrations", &["version", "success"]).await? {
        return Ok(());
    }

    add_query_issue(
        connection,
        report,
        IssueSpec {
            code: "failed_sqlx_migration_record",
            severity: IssueSeverity::Blocking,
            message: "SQLx records a failed migration; startup must not continue.",
        },
        r#"
        SELECT version::TEXT AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count
        FROM _sqlx_migrations WHERE success = FALSE
        ORDER BY version LIMIT $1
        "#,
    )
    .await?;
    Ok(())
}

#[derive(Clone, Copy)]
struct IssueSpec {
    code: &'static str,
    severity: IssueSeverity,
    message: &'static str,
}

async fn add_query_issue(
    connection: &mut PgConnection,
    report: &mut PreflightReport,
    spec: IssueSpec,
    sql: &str,
) -> Result<(), sqlx::Error> {
    let rows = sqlx::query(sql)
        .bind(SAMPLE_LIMIT)
        .fetch_all(&mut *connection)
        .await?;
    if rows.is_empty() {
        return Ok(());
    }

    let count = rows[0].try_get::<i64, _>("total_count")?;
    let sample_ids = rows
        .iter()
        .map(|row| row.try_get::<String, _>("entity_id"))
        .collect::<Result<Vec<_>, _>>()?;
    report.issues.push(PreflightIssue {
        code: spec.code,
        severity: spec.severity,
        count,
        sample_ids,
        message: spec.message,
    });
    Ok(())
}

async fn add_table_id_issue(
    connection: &mut PgConnection,
    report: &mut PreflightReport,
    table: &str,
    code: &'static str,
    message: &'static str,
) -> Result<(), sqlx::Error> {
    let sql = format!(
        "SELECT id::TEXT AS entity_id, COUNT(*) OVER ()::BIGINT AS total_count FROM {table} ORDER BY id LIMIT $1"
    );
    add_query_issue(
        connection,
        report,
        IssueSpec {
            code,
            severity: IssueSeverity::Blocking,
            message,
        },
        &sql,
    )
    .await
}

async fn table_exists(connection: &mut PgConnection, table: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, bool>(
        "SELECT to_regclass(format('%I.%I', current_schema(), $1)) IS NOT NULL",
    )
    .bind(table)
    .fetch_one(&mut *connection)
    .await
}

async fn table_has_rows(connection: &mut PgConnection, table: &str) -> Result<bool, sqlx::Error> {
    let sql = format!("SELECT EXISTS (SELECT 1 FROM {table} LIMIT 1)");
    sqlx::query_scalar(&sql).fetch_one(&mut *connection).await
}

async fn has_columns(
    connection: &mut PgConnection,
    table: &str,
    columns: &[&str],
) -> Result<bool, sqlx::Error> {
    if !table_exists(connection, table).await? {
        return Ok(false);
    }
    Ok(missing_columns(connection, table, columns)
        .await?
        .is_empty())
}

async fn missing_columns(
    connection: &mut PgConnection,
    table: &str,
    columns: &[&str],
) -> Result<Vec<String>, sqlx::Error> {
    let existing: Vec<String> = sqlx::query_scalar(
        r#"
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = $1
        "#,
    )
    .bind(table)
    .fetch_all(&mut *connection)
    .await?;

    Ok(columns
        .iter()
        .filter(|column| !existing.iter().any(|candidate| candidate == **column))
        .map(|column| (*column).to_string())
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_is_blocked_by_any_blocking_issue() {
        let mut report = PreflightReport::new(PreflightMode::ReportOnly);
        report.issues.push(PreflightIssue {
            code: "test_warning",
            severity: IssueSeverity::Warning,
            count: 1,
            sample_ids: vec!["1".to_string()],
            message: "warning",
        });
        assert!(report.finish().safe_to_migrate);

        let mut report = PreflightReport::new(PreflightMode::MaintenanceLocked);
        report.issues.push(PreflightIssue {
            code: "test_blocker",
            severity: IssueSeverity::Blocking,
            count: 1,
            sample_ids: vec!["1".to_string()],
            message: "blocked",
        });
        assert!(!report.finish().safe_to_migrate);
    }
}
