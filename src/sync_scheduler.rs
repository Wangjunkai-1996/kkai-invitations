use std::time::Duration;

use tokio::time::{interval_at, Instant, MissedTickBehavior};

use crate::routes::system::{run_rebate_sync, run_topup_reconciliation};
use crate::AppState;

pub fn spawn_rebate_sync_scheduler(state: AppState) {
    let interval_seconds = state.config.auto_sync_interval_seconds;
    let initial_delay_seconds = state.config.auto_sync_initial_delay_seconds;
    let Some((interval_duration, initial_delay)) =
        scheduler_timing(interval_seconds, initial_delay_seconds)
    else {
        tracing::info!("Automatic rebate sync disabled");
        return;
    };

    let first_tick = Instant::now() + initial_delay;

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

pub fn spawn_topup_reconciliation_scheduler(state: AppState) {
    let interval_seconds = state.config.topup_reconciliation_interval_seconds;
    let initial_delay_seconds = state.config.topup_reconciliation_initial_delay_seconds;
    let Some((interval_duration, initial_delay)) =
        scheduler_timing(interval_seconds, initial_delay_seconds)
    else {
        tracing::info!("Automatic top-up reconciliation disabled");
        return;
    };

    let first_tick = Instant::now() + initial_delay;
    tokio::spawn(async move {
        tracing::info!(
            interval_seconds,
            initial_delay_seconds,
            "Automatic top-up reconciliation scheduler started"
        );

        let mut ticker = interval_at(first_tick, interval_duration);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            ticker.tick().await;
            match run_topup_reconciliation(&state).await {
                Ok(result) if result.skipped => {
                    tracing::info!("Automatic top-up reconciliation skipped: sync already running");
                }
                Ok(result) => {
                    tracing::info!(
                        finalized = result.finalized,
                        created_cases = result.created_cases,
                        "Automatic top-up reconciliation completed"
                    );
                }
                Err(err) => {
                    tracing::error!(error = %err, "Automatic top-up reconciliation failed");
                }
            }
        }
    });
}

fn scheduler_timing(
    interval_seconds: u64,
    initial_delay_seconds: u64,
) -> Option<(Duration, Duration)> {
    if interval_seconds == 0 {
        return None;
    }
    Some((
        Duration::from_secs(interval_seconds),
        Duration::from_secs(initial_delay_seconds),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_scheduler_has_no_timing() {
        assert_eq!(scheduler_timing(0, 30), None);
    }

    #[test]
    fn scheduler_timing_preserves_interval_and_initial_delay() {
        assert_eq!(
            scheduler_timing(300, 30),
            Some((Duration::from_secs(300), Duration::from_secs(30)))
        );
    }
}
