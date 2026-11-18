use std::collections::BTreeMap;

use axum::body::{to_bytes, Body};
use axum::extract::State;
use axum::http::{HeaderMap, Request, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{json, Value};

use super::*;
use crate::config::{BalanceApiConfig, NodeRole};
use crate::rebate_lifecycle::selected_rebate_rule;
use crate::routes::user;

const AS_OF: i64 = 1_800_000_000;
const ORDINARY: i64 = 101;
const SELLER: i64 = 102;
const OTHER: i64 = 103;

async fn execute(pool: &PgPool, statements: &str) {
    sqlx::raw_sql(statements).execute(pool).await.unwrap();
}

async fn fixture() -> (TestDatabase, AppState) {
    let database = TestDatabase::create().await;
    database.prepare().await.unwrap();
    execute(
        &database.external_pool,
        r#"
        CREATE TABLE users (id BIGINT PRIMARY KEY, aff_code TEXT, "group" TEXT,
            inviter_id BIGINT, deleted_at TIMESTAMP, quota BIGINT DEFAULT 123456);
        INSERT INTO users (id, aff_code, "group") VALUES
            (101,'QA-A',' qa-main '), (102,'QA-S','qa-main'), (103,'QA-B','QA-MAIN');
    "#,
    )
    .await;
    execute(&database.pool, "
        INSERT INTO rebate_rules (user_group,rule_type,rebate_rate,effective_time,created_at,updated_at)
            VALUES ('__sales__','topup',0.15,1,1,1), ('qa-main','topup',0.075,1,1,1),
                   ('__all__','topup',0.03,1,1,1);
        INSERT INTO sales_accounts (user_id,active,created_at,updated_at,updated_by)
            VALUES (102,true,1,1,1);
        INSERT INTO sales_account_events (user_id,active,note,admin_id,created_at)
            VALUES (102,true,'',1,1);
    ").await;
    let state = AppState {
        pool: database.pool.clone(),
        newapi_pool: database.external_pool.clone(),
        ..crate::node_role_tests::state(NodeRole::Serving)
    };
    (database, state)
}

fn session(id: i64) -> SessionUser {
    SessionUser {
        id,
        username: "fixture".into(),
        role: 1,
        status: 1,
        group: "stale-session-group".into(),
    }
}

async fn summary_at(state: &AppState, id: i64, as_of: i64) -> Value {
    let result = user::my_code_at(state, session(id), as_of).await.unwrap();
    serde_json::to_value(result.0.data).unwrap()
}

async fn assert_selection(
    state: &AppState,
    id: i64,
    group: &str,
    as_of: i64,
    expected: Option<f64>,
) {
    let selected = selected_rebate_rule(&state.pool, group, "topup", id, as_of)
        .await
        .unwrap()
        .map(|(_, rate)| rate.parse::<f64>().unwrap());
    assert_eq!(selected, expected, "settlement, user {id} at {as_of}");
    let policy = match selected {
        Some(ratio) => json!({"state":"available", "ratio":ratio}),
        None => json!({"state":"not_configured"}),
    };
    assert_eq!(
        summary_at(state, id, as_of).await["topupRebatePolicy"],
        policy,
        "my-code, user {id} at {as_of}"
    );
}

async fn http_get(
    app: &Router,
    endpoint: &str,
    credential: Option<&str>,
    status: StatusCode,
) -> Value {
    let mut request = Request::get(format!("/invitations/api/invitations/{endpoint}"));
    if let Some(credential) = credential {
        request = request.header("authorization", credential);
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), status, "GET {endpoint}");
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap();
    assert_eq!(body["success"], status.is_success());
    if !status.is_success() {
        assert!(body.get("data").is_none(), "{body}");
    }
    body
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
async fn my_code_topup_policy_http_session_isolation_access_and_base_errors() {
    let (database, mut state) = fixture().await;
    let console = Router::new()
        .route(
            "/api/status",
            get(|| async {
                Json(json!({"success":true,"data":{"console_contract":{"api_contracts":[2]}}}))
            }),
        )
        .route(
            "/api/user/self",
            get(|headers: HeaderMap| async move {
                let id = match headers
                    .get("authorization")
                    .and_then(|value| value.to_str().ok())
                {
                    Some("Bearer QA-A") => ORDINARY,
                    Some("Bearer QA-S") => SELLER,
                    Some("Bearer QA-B") => OTHER,
                    _ => return Json(json!({"success":false})),
                };
                Json(
                    json!({"success":true,"data":{"id":id,"username":"qa","role":1,
                "status":1,"auth_method":"session","group":"stale-session-group"}}),
                )
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, console).await.unwrap() });
    state.config.balance_api = Some(BalanceApiConfig {
        base_url: format!("http://{address}"),
        secret: "qa".into(),
        connect_timeout_milliseconds: 1000,
        request_timeout_milliseconds: 1000,
    });
    let app = crate::application_router(state.clone(), tokio::sync::watch::channel(false).1);
    let mine = "my-code?inviterId=102&userId=102&group=__sales__&isSales=true&asOf=1";
    let alice = Some("Bearer QA-A");
    http_get(&app, mine, None, StatusCode::UNAUTHORIZED).await;
    let public = http_get(&app, "status", None, StatusCode::OK).await;
    assert_eq!(
        public["data"],
        json!({"available":true,"userInvitationRebateEnabled":true,
        "orderRebateEnabled":true,"invitationSignupRewardEnabled":false,"rebateToBalanceEnabled":true,
        "settlementEnabled":true,"creditCurrency":"legacy_075"})
    );
    for (id, code, group, ratio) in [
        (ORDINARY, "QA-A", "qa-main", 0.075),
        (SELLER, "QA-S", "qa-main", 0.15),
        (OTHER, "QA-B", "QA-MAIN", 0.03),
        (ORDINARY, "QA-A", "qa-main", 0.075),
    ] {
        assert_selection(&state, id, group, AS_OF, Some(ratio)).await;
        let body = http_get(&app, mine, Some(&format!("Bearer {code}")), StatusCode::OK).await;
        assert_eq!(
            body,
            json!({"success":true,"message":"","data":{
            "invitationCode":code,"isSales":id == SELLER,"hasSalesAccount":id == SELLER,
            "invitedCount":0,"totalRebate":0,"completedRebate":0,"pendingRebate":0,"confirmingRebate":0,
            "topupRebatePolicy":{"state":"available","ratio":ratio}}})
        );
    }
    let bob = Some("Bearer QA-B");
    execute(
        &state.newapi_pool,
        "UPDATE users SET inviter_id=103 WHERE id=101",
    )
    .await;
    let mut expected = http_get(&app, mine, bob, StatusCode::OK).await;
    assert_eq!(expected["data"]["invitedCount"], 1);
    for (change, policy) in [
        (
            "UPDATE rebate_rules SET rebate_rate=0 WHERE user_group='__all__'",
            json!({"state":"available","ratio":0.0}),
        ),
        (
            "UPDATE rebate_rules SET user_group='no-match' WHERE user_group='__all__'",
            json!({"state":"not_configured"}),
        ),
        (
            "ALTER TABLE rebate_rules RENAME TO missing_rules",
            json!({"state":"unavailable"}),
        ),
        (
            "UPDATE system_config SET value='false' WHERE key='order_rebate_enabled'",
            json!({"state":"disabled"}),
        ),
        (
            "ALTER TABLE missing_rules RENAME TO rebate_rules;
          UPDATE rebate_rules SET user_group='__all__',rebate_rate=0.03 WHERE user_group='no-match';
          UPDATE system_config SET value='true' WHERE key='order_rebate_enabled'",
            json!({"state":"available","ratio":0.03}),
        ),
    ] {
        execute(&state.pool, change).await;
        expected["data"]["topupRebatePolicy"] = policy;
        assert_eq!(http_get(&app, mine, bob, StatusCode::OK).await, expected);
    }
    execute(
        &state.pool,
        "UPDATE system_config SET value='false' WHERE key='user_invitation_rebate_enabled'",
    )
    .await;
    http_get(&app, mine, alice, StatusCode::FORBIDDEN).await;
    http_get(&app, mine, Some("Bearer QA-S"), StatusCode::OK).await;
    execute(
        &state.pool,
        "UPDATE system_config SET value='true' WHERE key='user_invitation_rebate_enabled'",
    )
    .await;
    execute(&state.newapi_pool, "DELETE FROM users WHERE id=103").await;
    http_get(&app, mine, Some("Bearer QA-B"), StatusCode::NOT_FOUND).await;
    execute(
        &state.pool,
        "ALTER TABLE rebate_records RENAME TO missing_records",
    )
    .await;
    http_get(&app, mine, alice, StatusCode::INTERNAL_SERVER_ERROR).await;
    execute(
        &state.pool,
        "ALTER TABLE missing_records RENAME TO rebate_records;
        UPDATE system_config SET value='invalid' WHERE key='order_rebate_enabled'",
    )
    .await;
    http_get(&app, mine, alice, StatusCode::INTERNAL_SERVER_ERROR).await;
    execute(
        &state.pool,
        "DELETE FROM system_config WHERE key='order_rebate_enabled'",
    )
    .await;
    http_get(&app, mine, alice, StatusCode::INTERNAL_SERVER_ERROR).await;
    state
        .config
        .balance_api
        .as_mut()
        .unwrap()
        .base_url
        .push_str("/offline");
    let offline = crate::application_router(state, tokio::sync::watch::channel(false).1);
    assert_eq!(
        http_get(&offline, mine, alice, StatusCode::LOCKED).await["code"],
        "funds_maintenance"
    );
    server.abort();
    database.cleanup().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
async fn my_code_topup_policy_http_read_only_keeps_ratio_and_rejects_request_write() {
    let (database, mut state) = fixture().await;
    execute(
        &state.pool,
        "UPDATE system_config SET value = CASE
            WHEN key = 'min_rebate_request_amount' THEN '0'
            WHEN key = 'rebate_request_frequency_days' THEN '0'
            ELSE value
        END",
    )
    .await;
    let record_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO rebate_records (
            inviter_id, invitee_id, user_group, order_type, order_id, order_amount,
            order_time, rebate_effective_at, rebate_amount, rebate_rate, status,
            scan_started_at, initialization_ends_at, created_at, updated_at
        )
        VALUES (101, 201, 'qa-main', 'topup', 9001, 100, 1, 1, 50000, 0.075,
                'pending', 1, 1, 1, 1)
        RETURNING id
        "#,
    )
    .fetch_one(&state.pool)
    .await
    .unwrap();

    let console = Router::new()
        .route(
            "/api/status",
            get(|| async {
                Json(json!({
                    "success": true,
                    "data": {"console_contract": {"api_contracts": [2]}}
                }))
            }),
        )
        .route(
            "/api/user/self",
            get(|headers: HeaderMap| async move {
                let success = headers
                    .get("authorization")
                    .and_then(|value| value.to_str().ok())
                    == Some("Bearer QA-A");
                Json(json!({
                    "success": success,
                    "data": {
                        "id": ORDINARY,
                        "username": "qa",
                        "role": 1,
                        "status": 1,
                        "auth_method": "session",
                        "group": "stale-session-group"
                    }
                }))
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, console).await.unwrap() });
    state.config.balance_api = Some(BalanceApiConfig {
        base_url: format!("http://{address}"),
        secret: "qa".into(),
        connect_timeout_milliseconds: 1000,
        request_timeout_milliseconds: 1000,
    });
    state.config.settlement_mode = SettlementMode::ReadOnly;
    let app = crate::application_router(state.clone(), tokio::sync::watch::channel(false).1);

    let summary = http_get(&app, "my-code", Some("Bearer QA-A"), StatusCode::OK).await;
    assert_eq!(
        summary["data"]["topupRebatePolicy"],
        json!({"state":"available","ratio":0.075})
    );

    let before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM rebate_requests")
        .fetch_one(&state.pool)
        .await
        .unwrap();
    let response = app
        .oneshot(
            Request::post("/invitations/api/invitations/rebate-requests")
                .header("authorization", "Bearer QA-A")
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"rebateRecordIds":[{record_id}]}}"#
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::LOCKED);
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap();
    assert_eq!(body["success"], false);
    assert_eq!(body["code"], "funds_maintenance");
    assert_eq!(body["message"], "资金结算维护中，请稍后重试");

    let after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM rebate_requests")
        .fetch_one(&state.pool)
        .await
        .unwrap();
    assert_eq!(after, before);
    let record_status: String =
        sqlx::query_scalar("SELECT status FROM rebate_records WHERE id = $1")
            .bind(record_id)
            .fetch_one(&state.pool)
            .await
            .unwrap();
    assert_eq!(record_status, "pending");

    server.abort();
    database.cleanup().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
async fn my_code_topup_policy_sales_rule_and_identity_boundaries() {
    let (database, state) = fixture().await;
    sqlx::query("UPDATE rebate_rules SET effective_time=CASE WHEN user_group='__sales__' THEN $1::bigint ELSE $1::bigint+10 END")
        .bind(AS_OF).execute(&state.pool).await.unwrap();
    for (time, expected) in [(AS_OF - 1, 0.075), (AS_OF, 0.075), (AS_OF + 1, 0.15)] {
        assert_selection(&state, SELLER, "qa-main", time, Some(expected)).await;
    }
    assert_selection(&state, OTHER, "QA-MAIN", AS_OF, Some(0.03)).await;
    execute(&state.pool, "UPDATE rebate_rules SET effective_time=1 WHERE user_group='__sales__';
        INSERT INTO sales_accounts (user_id,active,created_at,updated_at,updated_by) VALUES (101,true,1,1,1);
        UPDATE sales_accounts SET active=false WHERE user_id=102").await;
    sqlx::query(
        "INSERT INTO sales_account_events (user_id,active,note,admin_id,created_at)
        VALUES (101,false,'',1,$1),(101,true,'',1,$1),(102,true,'',1,$1),(102,false,'',1,$1)",
    )
    .bind(AS_OF)
    .execute(&state.pool)
    .await
    .unwrap();
    for (time, enabled, disabled) in [
        (AS_OF - 1, 0.075, 0.15),
        (AS_OF, 0.075, 0.15),
        (AS_OF + 1, 0.15, 0.075),
    ] {
        assert_selection(&state, ORDINARY, "qa-main", time, Some(enabled)).await;
        assert_selection(&state, SELLER, "qa-main", time, Some(disabled)).await;
    }
    execute(&state.pool, "INSERT INTO rebate_records (inviter_id,invitee_id,user_group,order_type,order_id,
        order_amount,order_time,rebate_effective_at,rebate_amount,rebate_rate,status,scan_started_at,
        initialization_ends_at,created_at,updated_at) VALUES (102,201,'qa-main','topup',1,1,1,1,50000,.15,'pending',1,1,1,1)").await;
    let request = Json(user::RebateRequest {
        amount: None,
        rebate_record_ids: vec![1],
    });
    let rejected = user::request_rebate(State(state.clone()), session(SELLER), request)
        .await
        .into_response();
    assert_eq!(rejected.status(), StatusCode::FORBIDDEN);
    execute(
        &state.pool,
        "UPDATE system_config SET value='false' WHERE key='user_invitation_rebate_enabled'",
    )
    .await;
    let seller = summary_at(&state, SELLER, AS_OF).await;
    assert_eq!(seller["isSales"], false);
    assert_eq!(seller["hasSalesAccount"], true);
    assert_eq!(
        seller["topupRebatePolicy"],
        json!({"state":"available","ratio":0.15})
    );
    assert_eq!(seller["pendingRebate"], 10);
    database.cleanup().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
async fn my_code_topup_policy_groups_precision_zero_and_no_match() {
    let (database, state) = fixture().await;
    execute(&state.pool, "INSERT INTO rebate_rules (user_group,rule_type,rebate_rate,effective_time,created_at,updated_at)
        VALUES ('default','topup',0.09,1,1,1),(repeat('é',32),'topup',0.19,1,1,1),
               ('qa-main','subscription',0.90,1,1,1)").await;
    let max_group = "é".repeat(32);
    for (raw, normalized, expected) in [
        (None, "default", 0.09),
        (Some(" \t\u{3000}"), "default", 0.09),
        (Some("\u{3000}qa-main\t"), "qa-main", 0.075),
        (Some("QA-MAIN"), "QA-MAIN", 0.03),
        (Some("__sales__"), "__sales__", 0.03),
        (Some(max_group.as_str()), max_group.as_str(), 0.19),
    ] {
        sqlx::query(r#"UPDATE users SET "group"=$1 WHERE id=103"#)
            .bind(raw)
            .execute(&state.newapi_pool)
            .await
            .unwrap();
        assert_selection(&state, OTHER, normalized, AS_OF, Some(expected)).await;
    }
    execute(
        &state.newapi_pool,
        r#"UPDATE users SET "group"=repeat('é',32)||'x' WHERE id=103"#,
    )
    .await;
    assert_eq!(
        summary_at(&state, OTHER, AS_OF).await["topupRebatePolicy"],
        json!({"state":"unavailable"})
    );

    for (rate, expected) in [("0.0000", 0.0), ("0.0001", 0.0001), ("1.0000", 1.0)] {
        sqlx::query("UPDATE rebate_rules SET rebate_rate=$1::text::numeric WHERE user_group='qa-main' AND rule_type='topup'")
            .bind(rate).execute(&state.pool).await.unwrap();
        assert_selection(&state, ORDINARY, "qa-main", AS_OF, Some(expected)).await;
    }
    execute(
        &state.pool,
        "ALTER TABLE rebate_rules ALTER COLUMN rebate_rate DROP NOT NULL",
    )
    .await;
    for rate in [Some("-0.01"), Some("1.01"), Some("NaN"), None] {
        sqlx::query("UPDATE rebate_rules SET rebate_rate=$1::text::numeric WHERE user_group='qa-main' AND rule_type='topup'")
            .bind(rate).execute(&state.pool).await.unwrap();
        assert_eq!(
            summary_at(&state, ORDINARY, AS_OF).await["topupRebatePolicy"],
            json!({"state":"unavailable"}),
            "stored rate {rate:?}"
        );
    }
    execute(
        &state.pool,
        "DELETE FROM rebate_rules WHERE user_group='qa-main' AND rule_type='topup'",
    )
    .await;
    assert_selection(&state, ORDINARY, "qa-main", AS_OF, Some(0.03)).await;
    execute(
        &state.pool,
        "DELETE FROM rebate_rules WHERE user_group='__all__'",
    )
    .await;
    assert_selection(&state, ORDINARY, "qa-main", AS_OF, None).await;
    database.cleanup().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
async fn my_code_topup_policy_local_failures_timeout_switches_and_recovery() {
    let (database, mut state) = fixture().await;
    execute(
        &state.newapi_pool,
        "INSERT INTO users (id,inviter_id) VALUES (201,103)",
    )
    .await;
    execute(&state.pool, "UPDATE system_config SET value='true' WHERE key='invitation_signup_reward_enabled';
        INSERT INTO rebate_records (inviter_id,invitee_id,user_group,order_type,order_id,order_amount,
            order_time,rebate_effective_at,rebate_amount,status,scan_started_at,initialization_ends_at,created_at,updated_at)
            VALUES (103,201,'QA-MAIN','invite_inviter',201,0,1,1,25000,'pending',1,1,1,1)").await;
    let available = summary_at(&state, OTHER, AS_OF).await;
    assert_eq!(available["invitationCode"], "QA-B");
    assert_eq!(available["invitedCount"], 1);
    assert_eq!(available["pendingRebate"], 5);
    assert_eq!(
        available["topupRebatePolicy"],
        json!({"state":"available","ratio":0.03})
    );
    let mut unavailable = available.clone();
    unavailable["topupRebatePolicy"] = json!({"state":"unavailable"});
    let mut disabled = available.clone();
    disabled["topupRebatePolicy"] = json!({"state":"disabled"});

    execute(
        &state.newapi_pool,
        "UPDATE users SET deleted_at=now() WHERE id=103",
    )
    .await;
    assert_eq!(summary_at(&state, OTHER, AS_OF).await, unavailable);
    execute(
        &state.newapi_pool,
        r#"UPDATE users SET deleted_at=NULL WHERE id=103;
        ALTER TABLE users RENAME COLUMN "group" TO missing_group"#,
    )
    .await;
    assert_eq!(summary_at(&state, OTHER, AS_OF).await, unavailable);
    execute(
        &state.pool,
        "UPDATE system_config SET value='false' WHERE key='order_rebate_enabled'",
    )
    .await;
    assert_eq!(summary_at(&state, OTHER, AS_OF).await, disabled);
    execute(
        &state.newapi_pool,
        r#"ALTER TABLE users RENAME COLUMN missing_group TO "group""#,
    )
    .await;
    execute(
        &state.pool,
        "UPDATE system_config SET value='true' WHERE key='order_rebate_enabled'",
    )
    .await;
    assert_eq!(summary_at(&state, OTHER, AS_OF).await, available);
    execute(
        &state.pool,
        "ALTER TABLE rebate_rules RENAME TO missing_rules",
    )
    .await;
    assert_eq!(summary_at(&state, OTHER, AS_OF).await, unavailable);
    execute(
        &state.pool,
        "ALTER TABLE missing_rules RENAME TO rebate_rules",
    )
    .await;

    let mut lock = state.pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE rebate_rules IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(3), summary_at(&state, OTHER, AS_OF))
        .await
        .expect("my-code must return while the rule lock is still held");
    assert_eq!(result, unavailable);
    lock.rollback().await.unwrap();
    assert_eq!(summary_at(&state, OTHER, AS_OF).await, available);

    execute(&state.pool, "UPDATE system_config SET value='false' WHERE key IN ('rebate_to_balance_enabled','invitation_signup_reward_enabled')").await;
    assert_selection(&state, ORDINARY, "qa-main", AS_OF, Some(0.075)).await;
    execute(
        &state.pool,
        "UPDATE system_config SET value='true' WHERE key='rebate_to_balance_enabled'",
    )
    .await;
    state.config.settlement_mode = SettlementMode::ReadOnly;
    assert_selection(&state, ORDINARY, "qa-main", AS_OF, Some(0.075)).await;
    database.cleanup().await;
}

async fn stored_rows(pool: &PgPool) -> BTreeMap<String, Vec<String>> {
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT tablename::text FROM pg_tables WHERE schemaname=current_schema()",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    let mut rows = BTreeMap::new();
    for table in tables {
        let query = format!(
            r#"SELECT xmin::text || ':' || to_jsonb(entry)::text FROM "{table}" entry ORDER BY 1"#
        );
        rows.insert(
            table,
            sqlx::query_scalar(&query).fetch_all(pool).await.unwrap(),
        );
    }
    rows
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL PostgreSQL instance"]
async fn my_code_topup_policy_preserves_historical_records_snapshots_and_balances() {
    let (database, state) = fixture().await;
    execute(
        &state.newapi_pool,
        r#"
        INSERT INTO users (id,"group",inviter_id) VALUES (201,'buyer-a',101),(202,'buyer-b',101);
        CREATE TABLE top_ups (id BIGINT, user_id BIGINT, status TEXT, money NUMERIC);
        INSERT INTO top_ups VALUES (301,201,'success',4.99),(302,202,'success',8.99);
        CREATE TABLE subscription_orders (user_id BIGINT, status TEXT);
        CREATE TABLE user_subscriptions (user_id BIGINT, status TEXT);
    "#,
    )
    .await;
    execute(
        &state.pool,
        "UPDATE system_config SET value='true' WHERE key='invitation_signup_reward_enabled';
        UPDATE rebate_rules SET rebate_rate=0.05 WHERE user_group='qa-main'",
    )
    .await;

    let app = crate::application_router(state.clone(), tokio::sync::watch::channel(false).1);
    let receive = |source_order_id: i64, invitee_id: i64, source: &str| {
        let event = json!({
            "schema_version": 2, "event_key": format!("newapi:{source}:{source_order_id}"),
            "event_type": "topup.completed", "source_order_id": source_order_id, "invitee_id": invitee_id,
            "inviter_id": ORDINARY, "inviter_group": " qa-main ", "credited_quota": 10_000_000,
            "completed_at": 1_700_000_000 + source_order_id,
            "payment_provider": if source == "redemption" { "redemption" } else { "epay" }
        });
        let request = Request::post("/invitations/api/internal/rebate-source-events")
            .header("content-type", "application/json")
            .header(
                "authorization",
                format!("Bearer {}", state.config.rebate_event_ingest_secret),
            )
            .body(Body::from(event.to_string()))
            .unwrap();
        app.clone().oneshot(request)
    };
    assert_eq!(
        receive(301, 201, "topup").await.unwrap().status(),
        StatusCode::CREATED
    );
    assert_eq!(
        finalize_ready_topup_event_records(&state.pool, AS_OF)
            .await
            .unwrap(),
        1
    );
    assert_selection(&state, ORDINARY, "qa-main", AS_OF, Some(0.05)).await;
    let old_records = fetch_rebate_records(&state.pool, Some(ORDINARY), None, false, None, 20, 0)
        .await
        .unwrap();
    assert_eq!(old_records.len(), 1);
    let frozen = serde_json::to_value(&old_records[0]).unwrap();
    assert_eq!(frozen["rebateRatio"], 0.05);
    assert_eq!(frozen["rebateAmount"], 100);
    assert_eq!(frozen["displayStatus"], "claimable");
    execute(
        &state.pool,
        "UPDATE rebate_rules SET rebate_rate=0.075 WHERE user_group='qa-main'",
    )
    .await;
    assert_selection(&state, ORDINARY, "qa-main", AS_OF, Some(0.075)).await;
    assert_eq!(
        receive(302, 202, "topup").await.unwrap().status(),
        StatusCode::CREATED
    );
    assert_eq!(
        receive(303, 201, "redemption").await.unwrap().status(),
        StatusCode::CREATED
    );

    execute(&state.pool, "
        INSERT INTO rebate_records (id,inviter_id,invitee_id,user_group,order_type,order_id,order_amount,order_time,
            rebate_effective_at,rebate_amount,rebate_rate,status,admin_adjusted,unlock_required,
            scan_started_at,initialization_ends_at,created_at,updated_at)
        SELECT id,101,buyer,grp,kind,id,99,1,1,quota,rate,status,adjusted,locked,1,1,1,1 FROM (VALUES
            (701,201,'qa-main','topup',0,0,'pending',false,false),
            (702,201,'qa-main','topup',0,NULL,'initializing',false,false),
            (703,201,'qa-main','topup',0,NULL,'pending',false,false),
            (704,201,'qa-main','topup',5000,NULL,'completed',false,false),
            (705,201,'qa-main','topup',35000,0.12,'pending',true,false),
            (706,201,'qa-main','invite_inviter',10000,NULL,'pending',false,true),
            (707,202,'qa-main','invite_invitee',15000,0,'pending',false,false),
            (708,201,'buyer-a','subscription',20000,0.04,'pending',false,false),
            (709,202,'buyer-b','subscription',45000,0.09,'pending',false,false),
            (710,201,'buyer-a','redemption',10000,0.02,'pending',false,false)
        ) AS history(id,buyer,grp,kind,quota,rate,status,adjusted,locked);
    ").await;
    let ledger = stored_rows(&state.pool).await;
    let accounts = stored_rows(&state.newapi_pool).await;
    for _ in 0..2 {
        let summary = user::my_code(State(state.clone()), session(ORDINARY))
            .await
            .unwrap()
            .0
            .data;
        let data = serde_json::to_value(summary).unwrap();
        assert_eq!(
            data["topupRebatePolicy"],
            json!({"state":"available","ratio":0.075})
        );
        assert_eq!(data["invitedCount"], 2);
        assert_eq!(data["confirmingRebate"], 300);
        assert_eq!(data["pendingRebate"], 127);
        assert_eq!(data["completedRebate"], 1);
        assert_eq!(data["totalRebate"], 428);
    }
    assert_eq!(stored_rows(&state.pool).await, ledger);
    assert_eq!(stored_rows(&state.newapi_pool).await, accounts);
    let records = fetch_rebate_records(&state.pool, Some(ORDINARY), None, false, None, 20, 0)
        .await
        .unwrap();
    assert_eq!(records.len(), 13);
    let record = |id| records.iter().find(|row| row.id == id).unwrap();
    assert_eq!(
        serde_json::to_value(record(old_records[0].id)).unwrap(),
        frozen
    );
    for (order_id, expected_rate, expected_quota, display) in [
        (301_i64, 0.05, 500_000_i64, "claimable"),
        (302, 0.075, 750_000, "confirming"),
        (-303, 0.075, 750_000, "confirming"),
    ] {
        let (id, quota, base, snapshot_rate, snapshot_quota): (i64,i64,i64,f64,i64) = sqlx::query_as(
            "SELECT r.id,r.rebate_amount,s.credited_quota,s.rebate_rate::float8,s.rebate_quota FROM rebate_records r
             JOIN rebate_calculation_snapshots s ON s.rebate_record_id=r.id WHERE r.order_id=$1 AND r.order_type='topup'",
        ).bind(order_id).fetch_one(&state.pool).await.unwrap();
        assert_eq!(
            (quota, base, snapshot_rate, snapshot_quota),
            (expected_quota, 10_000_000, expected_rate, expected_quota)
        );
        let row = record(id);
        assert_eq!(
            (
                row.order_type.as_str(),
                row.rebate_ratio,
                row.rebate_amount,
                row.display_status.as_str()
            ),
            ("topup", Some(expected_rate), expected_quota / 5000, display)
        );
    }
    for (id, kind, ratio, amount, status) in [
        (701, "topup", Some(0.0), 0, "claimable"),
        (702, "topup", None, 0, "confirming"),
        (703, "topup", None, 0, "estimated"),
        (704, "topup", None, 1, "paid"),
        (705, "topup", Some(0.12), 7, "claimable"),
        (706, "invite_inviter", None, 2, "waiting_unlock"),
        (707, "invite_invitee", Some(0.0), 3, "claimable"),
        (708, "subscription", Some(0.04), 4, "claimable"),
        (709, "subscription", Some(0.09), 9, "claimable"),
        (710, "redemption", Some(0.02), 2, "claimable"),
    ] {
        let row = record(id);
        assert_eq!(
            (
                row.order_type.as_str(),
                row.rebate_ratio,
                row.rebate_amount,
                row.display_status.as_str()
            ),
            (kind, ratio, amount, status),
            "record {id}"
        );
    }
    database.cleanup().await;
}
