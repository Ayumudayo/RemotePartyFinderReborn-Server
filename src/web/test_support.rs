use std::sync::Arc;
use std::time::Duration;

use sestring::SeString;

use crate::config::ListingsSnapshotSource;
use crate::listing::{
    ConditionFlags, DutyCategory, DutyFinderSettingsFlags, DutyType, JobFlags, LootRuleFlags,
    ObjectiveFlags, PartyFinderListing, PartyFinderSlot, SearchAreaFlags,
};

use super::{IngestRateLimits, IngestSecurityConfig, State};

pub(crate) async fn state_with_signature_required() -> Arc<State> {
    let mongo = mongodb::Client::with_uri_str("mongodb://127.0.0.1:27017")
        .await
        .expect("construct test mongo client");
    let (change_tx, _) = tokio::sync::broadcast::channel(1);

    Arc::new(State {
        mongo,
        stats: Default::default(),
        listings_change_channel: change_tx,
        listings_revision: Default::default(),
        listings_revision_pending: Default::default(),
        listings_revision_notify: tokio::sync::Notify::new(),
        listings_revision_coalesce_window: Duration::from_millis(1),
        listings_snapshot_cache: Default::default(),
        listings_snapshot_source: ListingsSnapshotSource::Inline,
        listings_snapshot_collection_name: "listings_snapshots".to_string(),
        listings_snapshot_document_id: "current".to_string(),
        listing_source_state_collection_name: "listing_source_state".to_string(),
        listing_source_state_document_id: "current".to_string(),
        listing_snapshot_revision_state_collection_name: "listing_snapshot_revision_state"
            .to_string(),
        listing_snapshot_worker_lease_collection_name: "listing_snapshot_worker_leases".to_string(),
        materialized_snapshot_reconcile_interval_seconds: 30,
        snapshot_refresh_shared_secret: String::new(),
        snapshot_refresh_client_id: "listings-snapshot-worker".to_string(),
        snapshot_refresh_clock_skew_seconds: 300,
        snapshot_refresh_nonce_ttl_seconds: 300,
        fflogs_jobs_limit: 1,
        fflogs_hidden_cache_ttl_hours: 24,
        listing_upsert_concurrency: 1,
        player_upsert_concurrency: 1,
        max_body_bytes_contribute: 16 * 1024,
        max_body_bytes_multiple: 16 * 1024,
        max_body_bytes_players: 16 * 1024,
        max_body_bytes_detail: 16 * 1024,
        max_body_bytes_fflogs_results: 16 * 1024,
        max_multiple_batch_size: 10,
        max_players_batch_size: 10,
        max_fflogs_results_batch_size: 10,
        max_detail_member_count: 8,
        ingest_security: IngestSecurityConfig {
            require_signature: true,
            shared_secret: "test-shared-secret".to_string(),
            clock_skew_seconds: 300,
            nonce_ttl_seconds: 300,
            require_capabilities_for_protected_endpoints: true,
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
        ingest_rate_windows: Default::default(),
        ingest_nonces: Default::default(),
        snapshot_refresh_nonces: Default::default(),
        fflogs_job_leases: Default::default(),
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

pub(crate) fn sample_listing() -> PartyFinderListing {
    PartyFinderListing {
        id: 166_086,
        content_id_lower: 40_205_661,
        name: SeString::parse(b"Route Test").unwrap(),
        description: SeString::parse(b"Route Test Description").unwrap(),
        created_world: 44,
        home_world: 44,
        current_world: 44,
        category: DutyCategory::HighEndDuty,
        duty: 1010,
        duty_type: DutyType::Normal,
        beginners_welcome: false,
        seconds_remaining: 1667,
        min_item_level: 0,
        num_parties: 1,
        slots_available: 8,
        last_server_restart: 1_774_336_035,
        objective: ObjectiveFlags::DUTY_COMPLETION,
        conditions: ConditionFlags::DUTY_COMPLETE,
        duty_finder_settings: DutyFinderSettingsFlags::NONE,
        loot_rules: LootRuleFlags::NONE,
        search_area: SearchAreaFlags::DATA_CENTRE,
        slots: vec![PartyFinderSlot {
            accepting: JobFlags::all(),
        }],
        jobs_present: vec![0; 8],
        member_content_ids: Vec::new(),
        member_jobs: Vec::new(),
        leader_content_id: 0,
    }
}
