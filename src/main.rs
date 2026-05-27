mod config;
mod db;
mod error;
mod middleware;
mod response;
mod routes;
mod session;

use axum::{
    routing::{delete, get, post, put},
    Router,
};
use sqlx::PgPool;
use tower_http::cors::CorsLayer;
use tracing_subscriber::EnvFilter;

use config::Config;

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub pool: PgPool,
    pub newapi_pool: PgPool,
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let config = Config::from_env();
    let pool = db::init_pool(&config.database_url).await;
    let newapi_pool = db::init_readonly_pool(&config.newapi_database_url).await;
    let listen_addr = config.listen_addr.clone();
    let base_path = config.base_path.clone();

    let state = AppState {
        config,
        pool,
        newapi_pool,
    };

    let b = &base_path;
    let mut app = Router::new()
        // 用户 API
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
            &format!("{b}/api/invitations/request-withdrawal"),
            post(routes::user::request_withdrawal),
        )
        .route(
            &format!("{b}/api/invitations/my-requests"),
            get(routes::user::my_requests),
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
            &format!("{b}/api/admin/withdrawal-requests"),
            get(routes::admin::get_withdrawal_requests),
        )
        .route(
            &format!("{b}/api/admin/withdrawal-requests/{{id}}/approve"),
            post(routes::admin::approve_withdrawal),
        )
        .route(
            &format!("{b}/api/admin/withdrawal-requests/{{id}}/reject"),
            post(routes::admin::reject_withdrawal),
        )
        .route(
            &format!("{b}/api/admin/withdrawal-requests/{{id}}/complete"),
            post(routes::admin::complete_withdrawal),
        )
        .route(
            &format!("{b}/api/admin/rebate-stats"),
            get(routes::admin::get_rebate_stats),
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
            &format!("{b}/api/system/sync-rebates"),
            post(routes::system::sync_rebates),
        );

    if !base_path.is_empty() {
        app = app.route(b, get(|| async { "Invitations API" }));
    }

    let app = app.layer(CorsLayer::permissive()).with_state(state);

    let listener = tokio::net::TcpListener::bind(&listen_addr)
        .await
        .expect("failed to bind");

    tracing::info!(
        "invitations server listening on {} (base_path: {:?})",
        listen_addr,
        base_path
    );
    axum::serve(listener, app).await.unwrap();
}
