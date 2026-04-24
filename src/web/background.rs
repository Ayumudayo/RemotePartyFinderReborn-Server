use std::{
    sync::{atomic::Ordering as AtomicOrdering, Arc},
    time::Duration,
};

use chrono::{TimeDelta, Utc};

use super::State;
use crate::stats::CachedStatistics;

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

pub fn spawn_listings_revision_publisher_task(state: Arc<State>) {
    let revision_state = Arc::clone(&state);
    tokio::task::spawn(async move {
        loop {
            revision_state.listings_revision_notify.notified().await;

            loop {
                let sleep = tokio::time::sleep(revision_state.listings_revision_coalesce_window);
                tokio::pin!(sleep);
                let notified = revision_state.listings_revision_notify.notified();
                tokio::pin!(notified);

                tokio::select! {
                    _ = &mut sleep => {
                        let had_pending = revision_state
                            .listings_revision_pending
                            .swap(false, AtomicOrdering::Relaxed);
                        if had_pending {
                            let revision = revision_state
                                .listings_revision
                                .fetch_add(1, AtomicOrdering::Relaxed)
                                + 1;
                            let _ = revision_state.listings_change_channel.send(revision);
                        }
                        break;
                    }
                    _ = &mut notified => {
                        continue;
                    }
                }
            }
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

#[cfg(test)]
mod tests {
    use super::spawn_listings_revision_publisher_task;
    use crate::web::{IngestRateLimits, IngestSecurityConfig, State};
    use mongodb::Client;
    use std::{collections::HashMap, sync::Arc, time::Duration};
    use tokio::sync::{Notify, RwLock};

    async fn test_state_with_coalesce_window(coalesce_window: Duration) -> Arc<State> {
        let mongo = Client::with_uri_str("mongodb://127.0.0.1:27017")
            .await
            .expect("construct test mongo client");
        let (change_tx, _) = tokio::sync::broadcast::channel(8);

        Arc::new(State {
            mongo,
            stats: Default::default(),
            listings_change_channel: change_tx,
            listings_revision: Default::default(),
            listings_revision_pending: Default::default(),
            listings_revision_notify: Notify::new(),
            listings_revision_coalesce_window: coalesce_window,
            listings_snapshot_cache: Default::default(),
            fflogs_jobs_limit: 1,
            fflogs_hidden_cache_ttl_hours: 24,
            listing_upsert_concurrency: 1,
            player_upsert_concurrency: 1,
            max_body_bytes_contribute: 1024,
            max_body_bytes_multiple: 1024,
            max_body_bytes_players: 1024,
            max_body_bytes_detail: 1024,
            max_body_bytes_fflogs_results: 1024,
            max_multiple_batch_size: 10,
            max_players_batch_size: 10,
            max_fflogs_results_batch_size: 10,
            max_detail_member_count: 8,
            ingest_security: IngestSecurityConfig {
                require_signature: false,
                shared_secret: "test-shared-secret".to_string(),
                clock_skew_seconds: 300,
                nonce_ttl_seconds: 300,
                require_capabilities_for_protected_endpoints: false,
                capability_secret: "test-capability-secret".to_string(),
                capability_session_ttl_seconds: 300,
                capability_detail_ttl_seconds: 300,
                rate_limits: IngestRateLimits {
                    contribute_per_minute: 60,
                    multiple_per_minute: 60,
                    players_per_minute: 60,
                    detail_per_minute: 60,
                    fflogs_jobs_per_minute: 60,
                    fflogs_results_per_minute: 60,
                },
            },
            ingest_rate_windows: RwLock::new(HashMap::new()),
            ingest_nonces: RwLock::new(HashMap::new()),
            fflogs_job_leases: RwLock::new(HashMap::new()),
            fflogs_jobs_dispatched_total: Default::default(),
            fflogs_results_received_total: Default::default(),
            fflogs_results_rejected_total: Default::default(),
            fflogs_leases_abandoned_total: Default::default(),
            fflogs_leases_abandon_rejected_total: Default::default(),
            fflogs_hidden_refresh_total: Default::default(),
            fflogs_leader_fallback_total: Default::default(),
            monitor_snapshot_interval_seconds: 0,
        })
    }

    #[tokio::test]
    async fn listings_revision_publisher_coalesces_bursts_into_one_revision() {
        let state = test_state_with_coalesce_window(Duration::from_millis(25)).await;
        spawn_listings_revision_publisher_task(Arc::clone(&state));
        let mut receiver = state.listings_change_channel.subscribe();

        let initial_revision = state.current_listings_revision();
        state.notify_listings_changed(1);
        tokio::time::sleep(Duration::from_millis(10)).await;
        state.notify_listings_changed(1);

        let revision = tokio::time::timeout(Duration::from_millis(150), receiver.recv())
            .await
            .expect("coalesced revision should arrive")
            .expect("broadcast should stay open");

        assert_eq!(revision, initial_revision + 1);
        assert!(
            tokio::time::timeout(Duration::from_millis(60), receiver.recv())
                .await
                .is_err(),
            "burst should emit only one revision"
        );
    }

    #[tokio::test]
    async fn listings_revision_publisher_emits_new_revision_after_next_quiet_window() {
        let state = test_state_with_coalesce_window(Duration::from_millis(20)).await;
        spawn_listings_revision_publisher_task(Arc::clone(&state));
        let mut receiver = state.listings_change_channel.subscribe();

        let initial_revision = state.current_listings_revision();
        state.notify_listings_changed(1);
        let first = tokio::time::timeout(Duration::from_millis(120), receiver.recv())
            .await
            .expect("first revision should arrive")
            .expect("broadcast should stay open");

        state.notify_listings_changed(1);
        let second = tokio::time::timeout(Duration::from_millis(120), receiver.recv())
            .await
            .expect("second revision should arrive")
            .expect("broadcast should stay open");

        assert_eq!(first, initial_revision + 1);
        assert_eq!(second, initial_revision + 2);
    }
}
