use std::time::Duration;

use tokio::time::{interval_at, Instant, MissedTickBehavior};

use crate::routes::system::run_rebate_sync;
use crate::AppState;

pub fn spawn_rebate_sync_scheduler(state: AppState) {
    let interval_seconds = state.config.auto_sync_interval_seconds;
    if interval_seconds == 0 {
        tracing::info!("Automatic rebate sync disabled");
        return;
    }

    let initial_delay_seconds = state.config.auto_sync_initial_delay_seconds;
    let first_tick = Instant::now() + Duration::from_secs(initial_delay_seconds);
    let interval_duration = Duration::from_secs(interval_seconds);

    tokio::spawn(async move {
        tracing::info!(
            interval_seconds,
            initial_delay_seconds,
            "Automatic rebate sync scheduler started"
        );

        let mut ticker = interval_at(first_tick, interval_duration);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            ticker.tick().await;
            match run_rebate_sync(&state).await {
                Ok(response) if response.skipped => {
                    tracing::info!("Automatic rebate sync skipped: sync already running");
                }
                Ok(response) => {
                    tracing::info!(
                        finalized = response.finalized,
                        invitation_registrations_synced = response.invitation_registrations_synced,
                        invitation_signup_synced = response.invitation_signup_synced,
                        invitation_signup_unlocked = response.invitation_signup_unlocked,
                        topup_synced = response.topup_synced,
                        subscription_synced = response.subscription_synced,
                        redemption_synced = response.redemption_synced,
                        "Automatic rebate sync completed"
                    );
                }
                Err(err) => {
                    tracing::error!(error = %err, "Automatic rebate sync failed");
                }
            }
        }
    });
}
