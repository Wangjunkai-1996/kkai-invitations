mod balance_dispatcher;
mod config;
mod credit_currency;
mod db;
mod error;
mod invitation_signup;
mod middleware;
mod payout;
mod preflight;
mod rebate_lifecycle;
mod rebate_source_events;
mod rebate_types;
mod reconciliation_cli;
mod response;
mod routes;
mod runtime;
mod sales;
mod sales_customers;
mod sales_reprice;
mod session;
mod sync_scheduler;
mod system_config;

use axum::{
    routing::{delete, get, patch, post, put},
    Router,
};
use sqlx::PgPool;
use tower_http::cors::CorsLayer;
use tracing_subscriber::EnvFilter;

use config::{Config, NodeRole};
use std::future::IntoFuture;
use tokio::sync::watch;
use tokio::task::JoinSet;

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub pool: PgPool,
    pub newapi_pool: PgPool,
    pub balance_client: Option<reqwest::Client>,
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    let args = std::env::args().collect::<Vec<_>>();
    if args.get(1).map(String::as_str) == Some("credit-migrate") {
        match credit_currency::run(&args[2..]).await {
            Ok(result) => println!("{result}"),
            Err(error) => {
                eprintln!("credit migration failed: {error}");
                std::process::exit(2);
            }
        }
        return;
    }
    if args.get(1).map(String::as_str) == Some("reconcile") {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .init();
        match reconciliation_cli::run(&args[2..]).await {
            Ok(output) => println!("{output}"),
            Err(error) => {
                eprintln!(
                    "{}",
                    serde_json::json!({
                        "ok": false,
                        "error": {
                            "code": error.code(),
                            "message": error.to_string(),
                        }
                    })
                );
                std::process::exit(2);
            }
        }
        return;
    }
    if args.get(1).map(String::as_str) == Some("sales-reprice") {
        match sales_reprice::run(&args[2..]).await {
            Ok(output) => println!("{output}"),
            Err(error) => {
                eprintln!("sales reprice failed: {error}");
                std::process::exit(2);
            }
        }
        return;
    }

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    if args.get(1).map(String::as_str) == Some("migrate-only") {
        if config::node_role_from_env() != Ok(NodeRole::Standalone) {
            eprintln!("migrate-only requires INVITATIONS_NODE_ROLE=standalone");
            std::process::exit(2);
        }
        if !matches!(
            config::settlement_mode_from_env(),
            Ok(config::SettlementMode::ReadOnly)
        ) {
            eprintln!("migrate-only requires SETTLEMENT_MODE=read_only");
            std::process::exit(2);
        }
        let database_url = config::invitations_database_url_from_env().unwrap_or_else(|error| {
            eprintln!("invalid invitations database configuration: {error}");
            std::process::exit(2);
        });
        let newapi_database_url = config::newapi_database_url_from_env().unwrap_or_else(|error| {
            eprintln!("invalid NewAPI database configuration: {error}");
            std::process::exit(2);
        });
        let newapi_pool = db::init_readonly_pool(&newapi_database_url)
            .await
            .unwrap_or_else(|error| {
                eprintln!("failed to connect to NewAPI readonly database: {error}");
                std::process::exit(2);
            });
        let pool = match db::init_pool(&database_url, &newapi_pool).await {
            Ok(pool) => pool,
            Err(error) => {
                if let Some(report) = error.preflight_report() {
                    eprintln!(
                        "{}",
                        serde_json::to_string_pretty(report)
                            .expect("serialize blocked preflight report")
                    );
                }
                eprintln!("invitations migration failed: {error}");
                std::process::exit(2);
            }
        };
        if let Err(error) = credit_currency::verify_runtime(&pool, &newapi_pool).await {
            eprintln!("currency cutover receipts must match after migration: {error}");
            std::process::exit(2);
        }
        pool.close().await;
        newapi_pool.close().await;
        println!(
            "{}",
            serde_json::json!({"ok": true, "schemaVersion": db::latest_schema_version()})
        );
        return;
    }

    if args.get(1).map(String::as_str) == Some("--preflight") {
        let database_url = match config::invitations_database_url_from_env() {
            Ok(value) => value,
            Err(_) => exit_preflight_error(
                3,
                "invalid_invitations_database_configuration",
                "DATABASE_URL or INVITATIONS_DATABASE_URL must be set",
            ),
        };
        let newapi_database_url = match config::newapi_database_url_from_env() {
            Ok(value) => value,
            Err(_) => exit_preflight_error(
                3,
                "invalid_newapi_database_configuration",
                "NEWAPI_DATABASE_URL must be set",
            ),
        };
        let report = match db::preflight_database(&database_url, &newapi_database_url).await {
            Ok(report) => report,
            Err(_) => exit_preflight_error(
                4,
                "preflight_database_unavailable",
                "Preflight could not read both PostgreSQL databases",
            ),
        };
        match serde_json::to_string_pretty(&report) {
            Ok(json) => println!("{json}"),
            Err(_) => exit_preflight_error(
                5,
                "preflight_serialization_failed",
                "Preflight report serialization failed",
            ),
        }
        if !report.safe_to_migrate {
            std::process::exit(2);
        }
        return;
    }

    let config = Config::from_env()
        .unwrap_or_else(|error| panic!("invalid settlement configuration: {error}"));
    let newapi_limit = if config.node_role == NodeRole::Standalone {
        5
    } else {
        2
    };
    let newapi_pool = db::init_readonly_pool_with_limit(&config.newapi_database_url, newapi_limit)
        .await
        .expect("failed to connect to new-api readonly database");
    let pool_result = if config.node_role == NodeRole::Standalone {
        db::init_pool(&config.database_url, &newapi_pool).await
    } else {
        db::init_verified_pool(&config.database_url).await
    };
    let pool = match pool_result {
        Ok(pool) => pool,
        Err(error) => {
            if let Some(report) = error.preflight_report() {
                eprintln!(
                    "{}",
                    serde_json::to_string_pretty(report)
                        .expect("serialize blocked preflight report")
                );
            }
            panic!("invitations database is not ready: {error}");
        }
    };
    credit_currency::verify_runtime(&pool, &newapi_pool)
        .await
        .expect("currency cutover receipts must match before starting workers");
    let balance_client = config
        .balance_api
        .as_ref()
        .map(balance_dispatcher::build_http_client)
        .transpose()
        .expect("failed to build NewAPI balance HTTP client");
    tracing::info!(settlement_mode = ?config.settlement_mode, "Settlement mode configured");
    let listen_addr = config.listen_addr.clone();
    let base_path = config.base_path.clone();

    let state = AppState {
        config,
        pool,
        newapi_pool,
        balance_client,
    };
    let (stop, shutdown) = watch::channel(false);
    let listener = tokio::net::TcpListener::bind(&listen_addr)
        .await
        .expect("failed to bind");
    let app = application_router(state.clone(), shutdown.clone());
    let mut jobs = JoinSet::new();
    runtime::spawn_background_jobs(state.clone(), shutdown.clone(), &mut jobs);
    tracing::info!(
        node_role = state.config.node_role.as_str(),
        "invitations server listening on {} (base_path: {:?})",
        listen_addr,
        base_path
    );
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(runtime::wait_for_shutdown(shutdown))
        .into_future();
    tokio::pin!(server);
    let mut server_finished = false;
    let mut failed = false;
    tokio::select! {
        _ = runtime::shutdown_signal() => {},
        result = &mut server => {
            server_finished = true;
            if let Err(error) = result {
                tracing::error!(%error, "HTTP server failed");
                failed = true;
            }
        },
        result = jobs.join_next(), if !jobs.is_empty() => {
            tracing::error!(?result, "Background job stopped unexpectedly");
            failed = true;
        },
    }
    stop.send_replace(true);
    tracing::info!("Draining HTTP requests and background jobs");
    if !server_finished {
        if let Err(error) = server.await {
            tracing::error!(%error, "HTTP drain failed");
            failed = true;
        }
    }
    while let Some(result) = jobs.join_next().await {
        if let Err(error) = result {
            tracing::error!(%error, "Background job drain failed");
            failed = true;
        }
    }
    state.pool.close().await;
    state.newapi_pool.close().await;
    tracing::info!("HTTP and background jobs drained; database connections closed");
    if failed {
        std::process::exit(1);
    }
}

fn application_router(state: AppState, shutdown: watch::Receiver<bool>) -> Router {
    let health = runtime::health_router(state.clone(), shutdown);
    if state.config.node_role == NodeRole::Worker {
        return health.fallback(runtime::worker_unavailable);
    }
    let base_path = state.config.base_path.clone();
    let b = &base_path;
    let mut app = Router::new()
        // 用户 API
        .route(
            &format!("{b}/api/invitations/sales-profile"),
            get(sales::my_profile),
        )
        .route(
            &format!("{b}/api/invitations/sales-customers"),
            get(sales_customers::customers),
        )
        .route(
            &format!("{b}/api/invitations/sales-customers/{{userId}}/recharges"),
            get(sales_customers::recharges),
        )
        .route(
            &format!("{b}/api/admin/sales-accounts"),
            get(sales::list_accounts),
        )
        .route(
            &format!("{b}/api/admin/sales-accounts/{{userId}}"),
            put(sales::update_account),
        )
        .route(
            &format!("{b}/api/admin/rebate-requests/{{id}}/confirm-issued"),
            post(sales::confirm_issued),
        )
        .route(
            &format!("{b}/api/invitations/status"),
            get(routes::user::invitation_feature_status),
        )
        .route(
            &format!("{b}/api/invitations/my-code"),
            get(routes::user::my_code),
        )
        .route(
            &format!("{b}/api/invitations/rebate-records"),
            get(routes::user::rebate_records),
        )
        .route(
            &format!("{b}/api/invitations/available-rebates"),
            get(routes::user::available_rebates),
        )
        .route(
            &format!("{b}/api/invitations/rebate-requests"),
            get(routes::user::my_rebate_requests).post(routes::user::request_rebate),
        )
        // 管理员 API
        .route(
            &format!("{b}/api/admin/rebate-rules"),
            get(routes::admin::get_rebate_rules),
        )
        .route(
            &format!("{b}/api/admin/rebate-rules"),
            post(routes::admin::create_rebate_rule),
        )
        .route(
            &format!("{b}/api/admin/rebate-rules/{{id}}"),
            put(routes::admin::update_rebate_rule),
        )
        .route(
            &format!("{b}/api/admin/rebate-rules/{{id}}"),
            delete(routes::admin::delete_rebate_rule),
        )
        .route(
            &format!("{b}/api/admin/user-groups"),
            get(routes::admin::get_user_groups),
        )
        .route(
            &format!("{b}/api/admin/system-config"),
            get(routes::admin::get_system_config),
        )
        .route(
            &format!("{b}/api/admin/system-config"),
            post(routes::admin::update_system_config),
        )
        .route(
            &format!("{b}/api/admin/system-config"),
            put(routes::admin::update_system_config),
        )
        .route(
            &format!("{b}/api/admin/rebate-requests"),
            get(routes::admin::get_rebate_requests),
        )
        .route(
            &format!("{b}/api/admin/rebate-requests/approve-and-pay-batch"),
            post(routes::admin::batch_approve_and_pay_rebate_requests),
        )
        .route(
            &format!("{b}/api/admin/rebate-requests/{{id}}/approve"),
            post(routes::admin::approve_rebate_request),
        )
        .route(
            &format!("{b}/api/admin/rebate-requests/{{id}}/approve-and-pay"),
            post(routes::admin::approve_and_pay_rebate_request),
        )
        .route(
            &format!("{b}/api/admin/rebate-requests/{{id}}/reject"),
            post(routes::admin::reject_rebate_request),
        )
        .route(
            &format!("{b}/api/admin/rebate-requests/{{id}}/reset"),
            post(routes::admin::reset_rebate_request_review),
        )
        .route(
            &format!("{b}/api/admin/rebate-requests/{{id}}/complete"),
            post(routes::admin::complete_rebate_request),
        )
        .route(
            &format!("{b}/api/admin/rebate-requests/{{id}}/undo-complete"),
            post(routes::admin::undo_complete_rebate_request),
        )
        .route(
            &format!("{b}/api/admin/rebate-stats"),
            get(routes::admin::get_rebate_stats),
        )
        .route(
            &format!("{b}/api/admin/rebate-records"),
            get(routes::admin::get_rebate_records),
        )
        .route(
            &format!("{b}/api/admin/rebate-records/{{id}}/revoke-signup-reward"),
            post(routes::admin::revoke_signup_reward_rebate_record),
        )
        .route(
            &format!("{b}/api/admin/rebate-payouts/{{id}}/status"),
            get(payout::get_payout_status),
        )
        .route(
            &format!("{b}/api/admin/rebate-payouts/{{id}}/pay"),
            post(payout::pay_rebate_payout),
        )
        .route(
            &format!("{b}/api/admin/rebate-payouts/{{id}}/reverse"),
            post(payout::reverse_rebate_payout),
        )
        .route(
            &format!("{b}/api/admin/rebate-order-records"),
            get(routes::admin::get_rebate_order_records),
        )
        .route(
            &format!("{b}/api/admin/rebate-order-records"),
            patch(routes::admin::update_rebate_order_records),
        )
        .route(
            &format!("{b}/api/admin/rebate-order-records/close"),
            post(routes::admin::close_rebate_order_records),
        )
        .route(
            &format!("{b}/api/admin/rebate-order-records/reopen"),
            post(routes::admin::reopen_rebate_order_records),
        )
        .route(
            &format!("{b}/api/admin/rebate-order-records/end-initialization"),
            post(routes::admin::end_rebate_order_initialization),
        )
        .route(
            &format!("{b}/api/admin/rebate-order-records/extend-initialization"),
            post(routes::admin::extend_rebate_order_initialization),
        )
        .route(
            &format!("{b}/api/admin/invitation-registrations"),
            get(routes::admin::get_invitation_registrations),
        )
        .route(
            &format!("{b}/api/admin/invitation-registrations/{{id}}/inviter-reward"),
            post(routes::admin::generate_invitation_registration_inviter_reward),
        )
        .route(
            &format!("{b}/api/admin/invitation-registrations/{{id}}/inviter-reward/revoke"),
            post(routes::admin::revoke_invitation_registration_inviter_reward),
        )
        .route(
            &format!("{b}/api/admin/invitation-registrations/{{id}}/invitee-reward"),
            post(routes::admin::generate_invitation_registration_invitee_reward),
        )
        .route(
            &format!("{b}/api/admin/invitation-registrations/{{id}}/invitee-reward/revoke"),
            post(routes::admin::revoke_invitation_registration_invitee_reward),
        )
        .route(
            &format!("{b}/api/admin/user-rebates/{{user_id}}"),
            get(routes::admin::get_user_rebates),
        )
        .route(
            &format!("{b}/api/admin/users/{{user_id}}/rebate-details"),
            get(routes::admin::get_user_rebates),
        )
        // 系统 API
        .route(
            &format!("{b}/api/internal/rebate-source-events"),
            post(rebate_source_events::receive_rebate_source_event),
        )
        .route(
            &format!("{b}/api/system/sync-rebates"),
            post(routes::system::sync_rebates),
        );

    if !base_path.is_empty() {
        app = app.route(b, get(|| async { "Invitations API" }));
    }

    let settlement_guard =
        axum::middleware::from_fn_with_state(state.clone(), payout::enforce_settlement_mode);
    app.layer(settlement_guard)
        .layer(CorsLayer::permissive())
        .with_state(state)
        .merge(health)
}

fn exit_preflight_error(exit_code: i32, code: &str, message: &str) -> ! {
    let payload = serde_json::json!({
        "ok": false,
        "error": {
            "code": code,
            "message": message,
        }
    });
    println!("{payload}");
    std::process::exit(exit_code);
}

#[cfg(test)]
mod node_role_tests {
    use super::*;
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    pub(crate) fn state(role: NodeRole) -> AppState {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://localhost:1/unavailable")
            .unwrap();
        AppState {
            config: Config {
                node_role: role,
                listen_addr: "127.0.0.1:6212".into(),
                base_path: "/invitations".into(),
                database_url: String::new(),
                newapi_database_url: String::new(),
                session_secret: "test-session".into(),
                rebate_event_ingest_secret: "test-ingest".into(),
                sync_secret: None,
                auto_sync_interval_seconds: 1,
                auto_sync_initial_delay_seconds: 60,
                topup_reconciliation_interval_seconds: 1,
                topup_reconciliation_initial_delay_seconds: 60,
                topup_rebate_active_from_id: 1,
                settlement_mode: config::SettlementMode::Enabled,
                quota_per_unit: 500000,
                balance_api: None,
                balance_dispatch_interval_milliseconds: 1000,
            },
            pool: pool.clone(),
            newapi_pool: pool,
            balance_client: None,
        }
    }

    #[tokio::test]
    async fn node_role_worker_has_no_business_routes_and_health_identifies_draining() {
        let (_stop, shutdown) = watch::channel(true);
        let app = application_router(state(NodeRole::Worker), shutdown);
        for (method, path) in [
            ("GET", "/invitations/api/invitations/sales-profile"),
            ("POST", "/api/internal/rebate-source-events"),
            ("POST", "/invitations/api/admin/rebate-requests/1/approve"),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE, "{path}");
        }
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap();
        assert_eq!(body["ready"], false);
        assert_eq!(body["node_role"], "worker");
        assert_eq!(body["schema_version"], db::latest_schema_version());
        assert_eq!(body["deployment_protocol"], 1);
        assert!(body["version"].as_str().is_some());
        assert!(body["source_revision"].as_str().is_some());
    }

    #[tokio::test]
    async fn node_role_serving_retains_business_routes_without_jobs() {
        let (_stop, shutdown) = watch::channel(false);
        let state = state(NodeRole::Serving);
        let mut jobs = JoinSet::new();
        runtime::spawn_background_jobs(state.clone(), shutdown.clone(), &mut jobs);
        assert!(jobs.is_empty());
        let response = application_router(state, shutdown)
            .oneshot(
                Request::builder()
                    .uri("/invitations/api/invitations/sales-profile")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn node_role_background_jobs_stop_before_starting_another_iteration() {
        for role in [NodeRole::Worker, NodeRole::Standalone] {
            let (_stop, shutdown) = watch::channel(true);
            let mut jobs = JoinSet::new();
            runtime::spawn_background_jobs(state(role), shutdown, &mut jobs);
            assert_eq!(jobs.len(), 3);
            tokio::time::timeout(std::time::Duration::from_secs(1), async {
                while let Some(result) = jobs.join_next().await {
                    result.unwrap();
                }
            }).await.expect("already-stopped jobs must never wait for the next tick or query the unavailable DB");
        }
    }

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL"]
    async fn node_role_health_checks_both_databases_and_reports_readiness() {
        let url = std::env::var("TEST_DATABASE_URL").unwrap();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .unwrap();
        let readonly_pool = db::init_readonly_pool_with_limit(&url, 2).await.unwrap();
        let mut state = state(NodeRole::Serving);
        state.pool = pool.clone();
        state.newapi_pool = readonly_pool.clone();
        let (_stop, shutdown) = watch::channel(false);
        let app = application_router(state, shutdown);
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap();
        assert_eq!(body["ready"], true);
        assert_eq!(body["node_role"], "serving");
        readonly_pool.close().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        pool.close().await;
    }
}
