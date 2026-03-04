use std::{
    sync::{
        Arc,
        atomic::Ordering as AtomicOrdering,
    },
    time::Duration,
};

use chrono::{TimeDelta, Utc};

use crate::stats::CachedStatistics;
use super::State;

pub fn spawn_stats_task(state: Arc<State>) {
    let stats_state = Arc::clone(&state);
    tokio::task::spawn(async move {
        loop {
            let all_time = match crate::stats::get_stats(&*stats_state).await {
                Ok(stats) => stats,
                Err(e) => {
                    tracing::error!("error generating stats: {:#?}", e);
                    continue;
                }
            };

            let seven_days = match crate::stats::get_stats_seven_days(&*stats_state).await {
                Ok(stats) => stats,
                Err(e) => {
                    tracing::error!("error generating stats: {:#?}", e);
                    continue;
                }
            };

            *stats_state.stats.write().await = Some(CachedStatistics {
                all_time,
                seven_days,
            });

            tokio::time::sleep(Duration::from_secs(60 * 60 * 12)).await;
        }
    });
}

pub fn spawn_fflogs_lease_sweeper_task(state: Arc<State>) {
    let lease_state = Arc::clone(&state);
    tokio::task::spawn(async move {
        let Some(lease_ttl) = TimeDelta::try_minutes(super::FFLOGS_LEASE_TTL_MINUTES) else {
            tracing::error!(
                ttl_minutes = super::FFLOGS_LEASE_TTL_MINUTES,
                "failed to initialize FFLogs lease sweeper: invalid lease ttl"
            );
            return;
        };

        loop {
            let now = Utc::now();
            let mut leases = lease_state.fflogs_job_leases.write().await;
            let before = leases.len();

            leases.retain(|_, lease| (lease.leased_at + lease_ttl) > now);

            let expired_count = before.saturating_sub(leases.len());
            drop(leases);

            if expired_count > 0 {
                tracing::debug!(
                    expired_count,
                    "fflogs lease sweeper released expired lease(s)"
                );
            }

            tokio::time::sleep(Duration::from_secs(
                super::FFLOGS_LEASE_SWEEP_INTERVAL_SECONDS,
            ))
            .await;
        }
    });
}

pub fn spawn_monitor_snapshot_task(state: Arc<State>) {
    let monitor_state = Arc::clone(&state);
    tokio::task::spawn(async move {
        let interval_seconds = monitor_state.monitor_snapshot_interval_seconds;
        if interval_seconds == 0 {
            tracing::debug!("monitor snapshot disabled (monitor_snapshot_interval_seconds=0)");
            return;
        }

        let sleep_interval = Duration::from_secs(interval_seconds.max(5));

        loop {
            let active_leases = {
                let leases = monitor_state.fflogs_job_leases.read().await;
                leases.len() as u64
            };

            let jobs_dispatched = monitor_state
                .fflogs_jobs_dispatched_total
                .load(AtomicOrdering::Relaxed);
            let results_received = monitor_state
                .fflogs_results_received_total
                .load(AtomicOrdering::Relaxed);
            let results_rejected = monitor_state
                .fflogs_results_rejected_total
                .load(AtomicOrdering::Relaxed);
            let leases_abandoned = monitor_state
                .fflogs_leases_abandoned_total
                .load(AtomicOrdering::Relaxed);
            let leases_abandon_rejected = monitor_state
                .fflogs_leases_abandon_rejected_total
                .load(AtomicOrdering::Relaxed);
            let hidden_refresh = monitor_state
                .fflogs_hidden_refresh_total
                .load(AtomicOrdering::Relaxed);
            let leader_fallback = monitor_state
                .fflogs_leader_fallback_total
                .load(AtomicOrdering::Relaxed);

            tracing::info!(
                "[MONITOR] FFLogs dispatched={} received={} rejected={} lease_active={} lease_abandoned={} lease_abandon_rejected={} hidden_refresh={} leader_fallback={}",
                jobs_dispatched,
                results_received,
                results_rejected,
                active_leases,
                leases_abandoned,
                leases_abandon_rejected,
                hidden_refresh,
                leader_fallback,
            );

            tokio::time::sleep(sleep_interval).await;
        }
    });
}

