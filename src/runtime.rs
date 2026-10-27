use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::AppState;

#[derive(Clone)]
struct HealthState {
    app: AppState,
    shutdown: watch::Receiver<bool>,
}

pub fn health_router(state: AppState, shutdown: watch::Receiver<bool>) -> Router {
    Router::new()
        .route("/health", get(health))
        .with_state(HealthState {
            app: state,
            shutdown,
        })
}

async fn health(State(state): State<HealthState>) -> (StatusCode, Json<serde_json::Value>) {
    let ready = !*state.shutdown.borrow()
        && matches!(
            tokio::time::timeout(Duration::from_secs(2), async {
                sqlx::query("SELECT 1").execute(&state.app.pool).await?;
                sqlx::query("SELECT 1")
                    .execute(&state.app.newapi_pool)
                    .await?;
                Ok::<(), sqlx::Error>(())
            })
            .await,
            Ok(Ok(()))
        )
        && !*state.shutdown.borrow();
    (
        if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(serde_json::json!({
            "ready": ready,
            "deployment_protocol": 1,
            "node_role": state.app.config.node_role.as_str(),
            "version": option_env!("APP_VERSION").unwrap_or("development"),
            "source_revision": option_env!("SOURCE_REVISION").unwrap_or("development"),
            "schema_version": crate::db::latest_schema_version(),
        })),
    )
}

pub fn spawn_background_jobs(
    state: AppState,
    shutdown: watch::Receiver<bool>,
    jobs: &mut JoinSet<()>,
) {
    if !state.config.node_role.runs_background_jobs() {
        return;
    }
    crate::sync_scheduler::spawn_rebate_sync_scheduler(state.clone(), shutdown.clone(), jobs);
    crate::sync_scheduler::spawn_topup_reconciliation_scheduler(
        state.clone(),
        shutdown.clone(),
        jobs,
    );
    crate::balance_dispatcher::spawn_balance_dispatcher(state, shutdown, jobs);
}

pub async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    while !*shutdown.borrow_and_update() {
        if shutdown.changed().await.is_err() {
            break;
        }
    }
}

pub async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install Ctrl-C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

pub async fn worker_unavailable() -> StatusCode {
    StatusCode::SERVICE_UNAVAILABLE
}
