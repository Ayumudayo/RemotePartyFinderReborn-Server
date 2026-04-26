
use super::{build_detail_update_doc, detail_payload_is_too_large, UploadablePartyDetail};
use chrono::Utc;
use std::sync::Arc;
use warp::http::{HeaderMap, StatusCode};

async fn test_state_with_signature_required() -> Arc<crate::web::State> {
    let mongo = mongodb::Client::with_uri_str("mongodb://127.0.0.1:27017")
        .await
        .expect("construct test mongo client");
    let (change_tx, _) = tokio::sync::broadcast::channel(1);

    Arc::new(crate::web::State {
        mongo,
        stats: Default::default(),
        listings_change_channel: change_tx,
        listings_revision: Default::default(),
        listings_revision_pending: Default::default(),
        listings_revision_notify: tokio::sync::Notify::new(),
        listings_revision_coalesce_window: std::time::Duration::from_millis(1),
        listings_snapshot_cache: Default::default(),
        listings_snapshot_source: crate::config::ListingsSnapshotSource::Inline,
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
        max_body_bytes_contribute: 1024,
        max_body_bytes_multiple: 1024,
        max_body_bytes_players: 1024,
        max_body_bytes_detail: 1024,
        max_body_bytes_fflogs_results: 1024,
        max_multiple_batch_size: 10,
        max_players_batch_size: 10,
        max_fflogs_results_batch_size: 10,
        max_detail_member_count: 8,
        ingest_security: crate::web::IngestSecurityConfig {
            require_signature: true,
            shared_secret: "test-shared-secret".to_string(),
            clock_skew_seconds: 300,
            nonce_ttl_seconds: 300,
            require_capabilities_for_protected_endpoints: true,
            capability_secret: "test-capability-secret".to_string(),
            capability_session_ttl_seconds: 300,
            capability_detail_ttl_seconds: 300,
            rate_limits: crate::web::IngestRateLimits {
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

async fn test_state_with_open_ingest() -> Arc<crate::web::State> {
    let mongo = mongodb::Client::with_uri_str("mongodb://127.0.0.1:27017")
        .await
        .expect("construct test mongo client");
    let (change_tx, _) = tokio::sync::broadcast::channel(1);

    Arc::new(crate::web::State {
        mongo,
        stats: Default::default(),
        listings_change_channel: change_tx,
        listings_revision: Default::default(),
        listings_revision_pending: Default::default(),
        listings_revision_notify: tokio::sync::Notify::new(),
        listings_revision_coalesce_window: std::time::Duration::from_millis(1),
        listings_snapshot_cache: Default::default(),
        listings_snapshot_source: crate::config::ListingsSnapshotSource::Inline,
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
        max_body_bytes_contribute: 1024,
        max_body_bytes_multiple: 1024,
        max_body_bytes_players: 1024,
        max_body_bytes_detail: 1024,
        max_body_bytes_fflogs_results: 1024,
        max_multiple_batch_size: 10,
        max_players_batch_size: 10,
        max_fflogs_results_batch_size: 10,
        max_detail_member_count: 8,
        ingest_security: crate::web::IngestSecurityConfig {
            require_signature: false,
            shared_secret: "test-shared-secret".to_string(),
            clock_skew_seconds: 300,
            nonce_ttl_seconds: 300,
            require_capabilities_for_protected_endpoints: false,
            capability_secret: "test-capability-secret".to_string(),
            capability_session_ttl_seconds: 300,
            capability_detail_ttl_seconds: 300,
            rate_limits: crate::web::IngestRateLimits {
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

#[test]
fn build_detail_update_doc_includes_raw_slot_flags() {
    let detail = UploadablePartyDetail {
        listing_id: 1,
        leader_content_id: 2,
        leader_name: "Leader".to_string(),
        home_world: 73,
        member_content_ids: vec![11, 0, 22],
        member_jobs: Some(vec![37, 0, 24]),
        slot_flags: Some(vec![
            "0x0000000000000001".to_string(),
            "0x0000000000000000".to_string(),
            "0x0000000000000004".to_string(),
        ]),
    };

    let update = build_detail_update_doc(&detail);

    assert_eq!(
        update.get_array("listing.detail_slot_flags").unwrap().len(),
        3
    );
}

#[test]
fn detail_payload_is_too_large_rejects_excess_slot_flags() {
    let detail = UploadablePartyDetail {
        listing_id: 1,
        leader_content_id: 2,
        leader_name: "Leader".to_string(),
        home_world: 73,
        member_content_ids: vec![11, 0, 22],
        member_jobs: Some(vec![37, 0, 24]),
        slot_flags: Some(vec![
            "0x1".to_string(),
            "0x2".to_string(),
            "0x3".to_string(),
            "0x4".to_string(),
        ]),
    };

    assert_eq!(
        detail_payload_is_too_large(&detail, 3),
        Some("too many slot flags in request")
    );
}

#[test]
fn identity_endpoint_rejects_empty_name_or_invalid_world() {
    let observed_at = chrono::DateTime::parse_from_rfc3339("2026-04-12T12:30:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let empty_name = crate::player::UploadableCharacterIdentity {
        content_id: 4001,
        name: "   ".to_string(),
        home_world: 74,
        world_name: "Tonberry".to_string(),
        source: "chara_card".to_string(),
        observed_at,
    };
    let invalid_world = crate::player::UploadableCharacterIdentity {
        content_id: 4002,
        name: "Alpha Beta".to_string(),
        home_world: 4_000,
        world_name: "Nowhere".to_string(),
        source: "chara_card".to_string(),
        observed_at,
    };

    assert!(matches!(
        crate::mongo::prepare_character_identity_upsert(&empty_name, observed_at, 300),
        Err(crate::mongo::CharacterIdentityRejectReason::MissingName)
    ));
    assert!(matches!(
        crate::mongo::prepare_character_identity_upsert(&invalid_world, observed_at, 300),
        Err(crate::mongo::CharacterIdentityRejectReason::InvalidHomeWorld)
    ));
}

#[test]
fn identity_upsert_preserves_newer_existing_player_data_when_enrichment_is_stale() {
    let existing_observed_at = chrono::DateTime::parse_from_rfc3339("2026-04-12T13:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let stale_observed_at = chrono::DateTime::parse_from_rfc3339("2026-04-12T12:30:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let existing = crate::player::Player {
        content_id: 6006,
        name: "Fresh Name".to_string(),
        home_world: 79,
        current_world: 79,
        last_seen: existing_observed_at,
        seen_count: 3,
        account_id: "12345".to_string(),
    };
    let stale_identity = crate::player::UploadableCharacterIdentity {
        content_id: 6006,
        name: "Stale Name".to_string(),
        home_world: 74,
        world_name: "Tonberry".to_string(),
        source: "party_detail".to_string(),
        observed_at: stale_observed_at,
    };

    let merged = crate::player::merge_identity_into_player(
        Some(&existing),
        Some(existing_observed_at),
        &stale_identity,
        existing_observed_at + chrono::TimeDelta::minutes(1),
    );

    assert!(!merged.applied_incoming_identity);
    assert_eq!(merged.player.name, "Fresh Name");
    assert_eq!(merged.player.home_world, 79);
    assert_eq!(merged.player.current_world, 79);
}

#[tokio::test]
async fn identity_endpoint_requires_standard_ingest_authorization() {
    let state = test_state_with_signature_required().await;
    let payload = vec![crate::player::UploadableCharacterIdentity {
        content_id: 7007,
        name: "Authorized Name".to_string(),
        home_world: 74,
        world_name: "Tonberry".to_string(),
        source: "chara_card".to_string(),
        observed_at: chrono::DateTime::parse_from_rfc3339("2026-04-12T12:30:00Z")
            .unwrap()
            .with_timezone(&Utc),
    }];

    let response =
        super::contribute_character_identity_handler(state, HeaderMap::new(), None, payload)
            .await
            .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn identity_endpoint_rejects_future_observed_at_beyond_clock_skew() {
    let state = test_state_with_open_ingest().await;
    let payload = vec![crate::player::UploadableCharacterIdentity {
        content_id: 7008,
        name: "Future Name".to_string(),
        home_world: 74,
        world_name: "Tonberry".to_string(),
        source: "chara_card".to_string(),
        observed_at: Utc::now() + chrono::TimeDelta::seconds(301),
    }];

    let response =
        super::contribute_character_identity_handler(state, HeaderMap::new(), None, payload)
            .await
            .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
