mod config;
mod db;
mod error;
mod invitation_signup;
mod middleware;
mod rebate_lifecycle;
mod rebate_types;
mod response;
mod routes;
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
    sync_scheduler::spawn_rebate_sync_scheduler(state.clone());

    let b = &base_path;
    let mut app = Router::new()
        // 用户 API
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
            &format!("{b}/api/admin/rebate-requests/{{id}}/approve"),
            post(routes::admin::approve_rebate_request),
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
