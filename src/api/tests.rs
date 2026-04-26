use chrono::Utc;
use hmac::{Hmac, Mac};
use mongodb::Client;
use serde_json::json;
use sestring::payload::{AutoTranslatePayload, TextPayload};
use sestring::Payload;
use sestring::SeString;
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{Notify, RwLock};
use warp::http::{HeaderMap, HeaderValue, StatusCode};

use crate::config::ListingsSnapshotSource;
use crate::listing::{
    ConditionFlags, DutyCategory, DutyFinderSettingsFlags, DutyType, JobFlags, LootRuleFlags,
    ObjectiveFlags, PartyFinderListing, PartyFinderSlot, SearchAreaFlags,
};
use crate::listings_snapshot::ApiReadableListing;
use crate::parse_resolver::ParseSource;
use crate::player::Player;
use crate::template::listings::{ParseDisplay, ProgressDisplay, RenderableMember};
use crate::web::{
    CachedListingsSnapshot, IngestRateLimits, IngestSecurityConfig, ListingsSnapshotCacheState,
    State,
};

use super::refresh::{
    apply_materialized_snapshot_if_newer, authorize_snapshot_refresh_request,
    reconcile_materialized_snapshot_once_with_loader, refresh_materialized_snapshot_with_loader,
    SnapshotRefreshRequest,
};
use super::snapshot::{
    build_snapshot_payload, get_materialized_cached_snapshot_with_loader,
    get_or_build_cached_snapshot, materialized_snapshot_result_response, snapshot_json_response,
};

fn sample_listing() -> PartyFinderListing {
    PartyFinderListing {
        id: 166_086,
        content_id_lower: 40_205_661,
        name: SeString::parse(b"Yuki Coffee").unwrap(),
        description: SeString::parse(b"Test Description").unwrap(),
        created_world: 44,
        home_world: 44,
        current_world: 44,
        category: DutyCategory::HighEndDuty,
        duty: 1010,
        duty_type: DutyType::Normal,
        beginners_welcome: false,
        seconds_remaining: 1667,
        min_item_level: 0,
        num_parties: 3,
        slots_available: 24,
        last_server_restart: 1_774_336_035,
        objective: ObjectiveFlags::DUTY_COMPLETION,
        conditions: ConditionFlags::DUTY_COMPLETE,
        duty_finder_settings: DutyFinderSettingsFlags::NONE,
        loot_rules: LootRuleFlags::NONE,
        search_area: SearchAreaFlags::DATA_CENTRE,
        slots: std::iter::repeat_with(|| PartyFinderSlot {
            accepting: JobFlags::all() - JobFlags::GLADIATOR,
        })
        .take(8)
        .collect(),
        jobs_present: vec![0; 8],
        member_content_ids: vec![
            1, 0, 2, 0, 3, 0, 4, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 7, 8, 9, 0, 10,
        ],
        member_jobs: vec![
            37, 0, 24, 0, 22, 0, 31, 0, 20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 33, 28, 34, 22, 0, 38,
        ],
        leader_content_id: 5,
    }
}

fn sample_cached_snapshot(revision: u64) -> CachedListingsSnapshot {
    CachedListingsSnapshot {
        revision,
        body: format!(r#"{{"revision":{revision}}}"#).into_bytes().into(),
        etag: None,
        content_encoding: None,
    }
}

fn sample_gzipped_cached_snapshot(revision: u64) -> CachedListingsSnapshot {
    CachedListingsSnapshot {
        revision,
        body: vec![0x1f, 0x8b, 0x08, 0x00].into(),
        etag: Some(format!("sha256-test-{revision}")),
        content_encoding: Some("gzip".to_string()),
    }
}

async fn test_state(source: ListingsSnapshotSource, current_revision: u64) -> Arc<State> {
    test_state_with_ingest_skew(source, current_revision, 300).await
}

async fn test_state_with_ingest_skew(
    source: ListingsSnapshotSource,
    current_revision: u64,
    ingest_clock_skew_seconds: i64,
) -> Arc<State> {
    let mongo = Client::with_uri_str("mongodb://127.0.0.1:27017")
        .await
        .expect("construct test mongo client");
    let (change_tx, _) = tokio::sync::broadcast::channel(8);

    Arc::new(State {
        mongo,
        stats: Default::default(),
        listings_change_channel: change_tx,
        listings_revision: std::sync::atomic::AtomicU64::new(current_revision),
        listings_revision_pending: Default::default(),
        listings_revision_notify: Notify::new(),
        listings_revision_coalesce_window: std::time::Duration::from_millis(1),
        listings_snapshot_cache: Default::default(),
        listings_snapshot_source: source,
        listings_snapshot_collection_name: "listings_snapshots".to_string(),
        listings_snapshot_document_id: "current".to_string(),
        listing_source_state_collection_name: "listing_source_state".to_string(),
        listing_source_state_document_id: "current".to_string(),
        listing_snapshot_revision_state_collection_name: "listing_snapshot_revision_state"
            .to_string(),
        listing_snapshot_worker_lease_collection_name: "listing_snapshot_worker_leases".to_string(),
        materialized_snapshot_reconcile_interval_seconds: 30,
        snapshot_refresh_shared_secret: "refresh-secret".to_string(),
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
        ingest_security: IngestSecurityConfig {
            require_signature: false,
            shared_secret: "public-ingest-secret".to_string(),
            clock_skew_seconds: ingest_clock_skew_seconds,
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
        snapshot_refresh_nonces: RwLock::new(HashMap::new()),
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

fn signed_refresh_headers(
    secret: &str,
    client_id: &str,
    nonce: &str,
    document_id: &str,
    expected_revision: u64,
) -> HeaderMap {
    signed_refresh_headers_at(
        secret,
        client_id,
        nonce,
        document_id,
        expected_revision,
        Utc::now().timestamp(),
    )
}

fn signed_refresh_headers_at(
    secret: &str,
    client_id: &str,
    nonce: &str,
    document_id: &str,
    expected_revision: u64,
    timestamp: i64,
) -> HeaderMap {
    let timestamp = timestamp.to_string();
    let payload = format!(
        "POST\n/internal/listings/snapshot/refresh\n{}\n{}\n{}\n{}\n{}",
        timestamp, nonce, client_id, document_id, expected_revision
    );
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("test hmac should initialize");
    mac.update(payload.as_bytes());
    let signature = base64::encode(mac.finalize().into_bytes());

    let mut headers = HeaderMap::new();
    headers.insert("x-rpf-client-id", HeaderValue::from_str(client_id).unwrap());
    headers.insert(
        "x-rpf-snapshot-document-id",
        HeaderValue::from_str(document_id).unwrap(),
    );
    headers.insert(
        "x-rpf-expected-revision",
        HeaderValue::from_str(&expected_revision.to_string()).unwrap(),
    );
    headers.insert(
        "x-rpf-timestamp",
        HeaderValue::from_str(&timestamp).unwrap(),
    );
    headers.insert("x-rpf-nonce", HeaderValue::from_str(nonce).unwrap());
    headers.insert("x-rpf-signature-version", HeaderValue::from_static("v1"));
    headers.insert(
        "x-rpf-signature",
        HeaderValue::from_str(&signature).unwrap(),
    );
    headers
}

fn sample_renderable_member(party_index: u8, parse: ParseDisplay) -> RenderableMember {
    RenderableMember {
        job_id: 19,
        player: Player {
            content_id: 42,
            name: "Neutral Member".to_string(),
            home_world: 73,
            current_world: 73,
            last_seen: Utc::now(),
            seen_count: 1,
            account_id: "-1".to_string(),
        },
        parse,
        progress: ProgressDisplay::default(),
        slot_index: 0,
        party_index,
        party_header: None,
        identity_fallback: false,
    }
}

#[test]
fn api_listing_uses_display_slot_model_for_alliance_layouts() {
    let listing = ApiReadableListing::from_parts(sample_listing(), Vec::new(), Default::default());

    assert_eq!(listing.slot_count, 24);
    assert_eq!(listing.display_slots.len(), 24);
    assert_eq!(listing.slots_filled_count, 10);
}

#[test]
fn api_listing_serializes_language_neutral_text_tokens() {
    let mut listing = sample_listing();
    listing.description = SeString(vec![
        Payload::Text(TextPayload("Need ".to_string())),
        Payload::AutoTranslate(AutoTranslatePayload { group: 1, key: 101 }),
    ]);

    let serialized = serde_json::to_value(ApiReadableListing::from_parts(
        listing,
        Vec::new(),
        Default::default(),
    ))
    .expect("api listing should serialize");

    assert_eq!(
        serialized["creator_name"],
        json!([{ "type": "text", "text": "Yuki Coffee" }])
    );
    assert_eq!(
        serialized["description"],
        json!([
            { "type": "text", "text": "Need " },
            { "type": "auto_translate", "group": 1, "key": 101 }
        ])
    );
    assert_eq!(serialized["duty_id"], 1010);
    assert_eq!(serialized["duty_type"], DutyType::Normal as u8);
    assert_eq!(serialized["category"], DutyCategory::HighEndDuty as u32);
    assert!(
        serialized.get("duty_name").is_none(),
        "snapshot should not include localized duty names"
    );
}

#[test]
fn api_listing_serializes_neutral_member_display_state() {
    let hidden_fallback_parse = ParseDisplay::new(
        Some(84),
        "parse-purple".to_string(),
        None,
        "parse-none".to_string(),
        false,
        false,
        true,
        false,
        ParseSource::ReportParse,
    );
    let hidden_leader_parse = ParseDisplay::new(
        None,
        "parse-none".to_string(),
        None,
        "parse-none".to_string(),
        false,
        false,
        true,
        false,
        ParseSource::ReportParse,
    );

    let serialized = serde_json::to_value(ApiReadableListing::from_parts(
        sample_listing(),
        vec![sample_renderable_member(1, hidden_fallback_parse)],
        hidden_leader_parse,
    ))
    .expect("api listing should serialize");

    let member = &serialized["members"][0];
    assert!(
        member.get("party_label").is_none(),
        "snapshot should expose party_index, not localized alliance labels"
    );
    assert!(
        member["parse"].get("hidden_rail_tag_label").is_none(),
        "snapshot should expose hidden state, not display labels"
    );
    assert!(
        member["parse"].get("hidden_rail_tag_title").is_none(),
        "snapshot should expose hidden state, not display tooltips"
    );
    assert_eq!(member["parse"]["originally_hidden"], json!(true));
    assert!(
        serialized["leader_parse"]
            .get("hidden_rail_tag_title")
            .is_none(),
        "leader parse should also be language-neutral"
    );
    assert_eq!(serialized["leader_parse"]["originally_hidden"], json!(true));
}

#[tokio::test]
async fn snapshot_cache_reuses_ready_entry_for_matching_revision() {
    let cache = RwLock::new(ListingsSnapshotCacheState::Ready(sample_cached_snapshot(7)));
    let builds = AtomicUsize::new(0);

    let snapshot = get_or_build_cached_snapshot(&cache, 7, || async {
        builds.fetch_add(1, Ordering::SeqCst);
        Ok::<_, ()>(sample_cached_snapshot(8))
    })
    .await
    .expect("cached snapshot should resolve");

    assert_eq!(snapshot.revision, 7);
    assert_eq!(builds.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn snapshot_cache_coalesces_concurrent_builders() {
    let cache = RwLock::new(ListingsSnapshotCacheState::Empty);
    let builds = AtomicUsize::new(0);

    let build = || async {
        builds.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        Ok::<_, ()>(sample_cached_snapshot(11))
    };

    let (left, right) = tokio::join!(
        get_or_build_cached_snapshot(&cache, 11, build),
        get_or_build_cached_snapshot(&cache, 11, build)
    );

    assert_eq!(left.expect("left snapshot should resolve").revision, 11);
    assert_eq!(right.expect("right snapshot should resolve").revision, 11);
    assert_eq!(builds.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn snapshot_payload_uses_stable_revision_without_retrying() {
    let revisions = Arc::new([5_u64, 5]);
    let cursor = Arc::new(AtomicUsize::new(0));
    let builds = AtomicUsize::new(0);

    let (revision, payload) = build_snapshot_payload(
        || async {
            builds.fetch_add(1, Ordering::SeqCst);
            "payload"
        },
        {
            let revisions = Arc::clone(&revisions);
            let cursor = Arc::clone(&cursor);
            move || {
                let index = cursor.fetch_add(1, Ordering::SeqCst);
                revisions[index]
            }
        },
    )
    .await;

    assert_eq!(revision, 5);
    assert_eq!(payload, "payload");
    assert_eq!(builds.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn snapshot_payload_uses_lower_bound_when_revision_races() {
    let revisions = Arc::new([7_u64, 8]);
    let cursor = Arc::new(AtomicUsize::new(0));

    let (revision, payload) = build_snapshot_payload(|| async { "payload" }, {
        let revisions = Arc::clone(&revisions);
        let cursor = Arc::clone(&cursor);
        move || {
            let index = cursor.fetch_add(1, Ordering::SeqCst);
            revisions[index]
        }
    })
    .await;

    assert_eq!(revision, 7);
    assert_eq!(payload, "payload");
}

#[test]
fn snapshot_response_emits_etag_and_gzip_headers_for_materialized_cache() {
    let response = snapshot_json_response(sample_gzipped_cached_snapshot(9));

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/json; charset=utf-8"
    );
    assert_eq!(response.headers().get("cache-control").unwrap(), "no-store");
    assert_eq!(response.headers().get("etag").unwrap(), "sha256-test-9");
    assert_eq!(response.headers().get("content-encoding").unwrap(), "gzip");
}

#[tokio::test]
async fn materialized_absent_snapshot_returns_service_unavailable() {
    let response = materialized_snapshot_result_response(Ok(None));

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = warp::hyper::body::to_bytes(response.into_body())
        .await
        .expect("body should collect");
    assert_eq!(body.as_ref(), b"snapshot not ready",);
}

#[tokio::test]
async fn materialized_reconciliation_loads_newer_snapshot_and_ignores_same_or_older_revision() {
    let state = test_state(ListingsSnapshotSource::Materialized, 5).await;
    let mut receiver = state.listings_change_channel.subscribe();

    let newer = reconcile_materialized_snapshot_once_with_loader(Arc::clone(&state), || async {
        Ok(Some(sample_cached_snapshot(6)))
    })
    .await
    .expect("reconcile should load newer snapshot");

    assert_eq!(newer.applied_revision, Some(6));
    assert_eq!(state.current_listings_revision(), 6);
    assert_eq!(receiver.recv().await.expect("revision should broadcast"), 6);

    let same = reconcile_materialized_snapshot_once_with_loader(Arc::clone(&state), || async {
        Ok(Some(sample_cached_snapshot(6)))
    })
    .await
    .expect("same revision should be ignored");
    let older = reconcile_materialized_snapshot_once_with_loader(Arc::clone(&state), || async {
        Ok(Some(sample_cached_snapshot(4)))
    })
    .await
    .expect("older revision should be ignored");

    assert_eq!(same.applied_revision, None);
    assert_eq!(older.applied_revision, None);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), receiver.recv())
            .await
            .is_err(),
        "same/older snapshots must not rebroadcast"
    );
}

#[tokio::test]
async fn refresh_signature_rejects_missing_bad_and_replayed_nonce() {
    let state = test_state(ListingsSnapshotSource::Materialized, 5).await;

    let missing = HeaderMap::new();
    assert!(authorize_snapshot_refresh_request(&state, &missing)
        .await
        .is_err());

    let mut bad = signed_refresh_headers("wrong-secret", "worker", "bad-nonce", "current", 6);
    assert!(authorize_snapshot_refresh_request(&state, &bad)
        .await
        .is_err());

    bad = signed_refresh_headers(
        "refresh-secret",
        "listings-snapshot-worker",
        "replayed-nonce",
        "current",
        6,
    );
    authorize_snapshot_refresh_request(&state, &bad)
        .await
        .expect("first use of nonce should authorize");
    let replay = authorize_snapshot_refresh_request(&state, &bad).await;
    assert!(replay.is_err(), "replayed nonce should be rejected");
}

#[tokio::test]
async fn refresh_signature_rejects_missing_expected_revision() {
    let state = test_state(ListingsSnapshotSource::Materialized, 5).await;
    let timestamp = Utc::now().timestamp().to_string();
    let nonce = "missing-expected-revision";
    let client_id = "listings-snapshot-worker";
    let payload = format!(
        "POST\n/internal/listings/snapshot/refresh\n{}\n{}\n{}",
        timestamp, nonce, client_id
    );
    let mut mac =
        Hmac::<Sha256>::new_from_slice(b"refresh-secret").expect("test hmac should initialize");
    mac.update(payload.as_bytes());
    let signature = base64::encode(mac.finalize().into_bytes());

    let mut headers = HeaderMap::new();
    headers.insert("x-rpf-client-id", HeaderValue::from_static(client_id));
    headers.insert(
        "x-rpf-snapshot-document-id",
        HeaderValue::from_static("current"),
    );
    headers.insert(
        "x-rpf-timestamp",
        HeaderValue::from_str(&timestamp).unwrap(),
    );
    headers.insert("x-rpf-nonce", HeaderValue::from_static(nonce));
    headers.insert("x-rpf-signature-version", HeaderValue::from_static("v1"));
    headers.insert(
        "x-rpf-signature",
        HeaderValue::from_str(&signature).unwrap(),
    );

    let error = authorize_snapshot_refresh_request(&state, &headers)
        .await
        .expect_err("refresh auth must require expected revision");

    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert!(
        error.message.contains("expected revision"),
        "unexpected auth error: {}",
        error.message
    );
}

#[tokio::test]
async fn refresh_signature_uses_snapshot_refresh_skew_independent_of_public_ingest_skew() {
    let state = test_state_with_ingest_skew(ListingsSnapshotSource::Materialized, 5, 1).await;
    let headers = signed_refresh_headers_at(
        "refresh-secret",
        "listings-snapshot-worker",
        "snapshot-skew-independent",
        "current",
        6,
        Utc::now().timestamp() - 2,
    );

    authorize_snapshot_refresh_request(&state, &headers)
        .await
        .expect("snapshot refresh skew should not use public ingest skew");
}

#[tokio::test]
async fn refresh_signature_uses_dedicated_nonce_store() {
    let state = test_state(ListingsSnapshotSource::Materialized, 5).await;
    let nonce = "dedicated-refresh-nonce";
    state.ingest_nonces.write().await.insert(
        format!("snapshot-refresh:listings-snapshot-worker:{nonce}"),
        Utc::now(),
    );
    let headers = signed_refresh_headers(
        "refresh-secret",
        "listings-snapshot-worker",
        nonce,
        "current",
        6,
    );

    authorize_snapshot_refresh_request(&state, &headers)
        .await
        .expect("public ingest nonce store must not affect snapshot refresh nonces");

    assert_eq!(state.ingest_nonces.read().await.len(), 1);
    assert!(state
        .snapshot_refresh_nonces
        .read()
        .await
        .contains_key(&format!(
            "snapshot-refresh:listings-snapshot-worker:{nonce}"
        )));
}

#[tokio::test]
async fn refresh_signature_rejects_timestamp_outside_snapshot_refresh_skew() {
    let state = test_state(ListingsSnapshotSource::Materialized, 5).await;
    let headers = signed_refresh_headers_at(
        "refresh-secret",
        "listings-snapshot-worker",
        "snapshot-skew-too-old",
        "current",
        6,
        Utc::now().timestamp() - 301,
    );

    let error = authorize_snapshot_refresh_request(&state, &headers)
        .await
        .expect_err("snapshot refresh skew should reject old timestamps");

    assert_eq!(error.status, StatusCode::FORBIDDEN);
    assert_eq!(error.message, "timestamp skew exceeded");
}

#[tokio::test]
async fn valid_refresh_broadcasts_only_after_snapshot_revision_advances() {
    let state = test_state(ListingsSnapshotSource::Materialized, 7).await;
    let mut receiver = state.listings_change_channel.subscribe();

    let same = refresh_materialized_snapshot_with_loader(
        Arc::clone(&state),
        SnapshotRefreshRequest {
            document_id: "current".to_string(),
            expected_revision: 7,
        },
        || async { Ok(Some(sample_cached_snapshot(7))) },
    )
    .await
    .expect("same revision refresh should succeed");

    assert_eq!(same.applied_revision, None);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), receiver.recv())
            .await
            .is_err(),
        "same revision refresh must not broadcast"
    );

    let advanced = refresh_materialized_snapshot_with_loader(
        Arc::clone(&state),
        SnapshotRefreshRequest {
            document_id: "current".to_string(),
            expected_revision: 8,
        },
        || async { Ok(Some(sample_cached_snapshot(8))) },
    )
    .await
    .expect("advanced revision refresh should succeed");

    assert_eq!(advanced.applied_revision, Some(8));
    assert_eq!(receiver.recv().await.expect("revision should broadcast"), 8);
}

#[tokio::test]
async fn refresh_fails_when_loaded_snapshot_is_older_than_expected_revision() {
    let state = test_state(ListingsSnapshotSource::Materialized, 7).await;

    let error = refresh_materialized_snapshot_with_loader(
        Arc::clone(&state),
        SnapshotRefreshRequest {
            document_id: "current".to_string(),
            expected_revision: 8,
        },
        || async { Ok(Some(sample_cached_snapshot(7))) },
    )
    .await
    .expect_err("refresh must fail when Mongo snapshot is older than worker expectation");

    assert!(
        error.to_string().contains("expected revision"),
        "unexpected refresh error: {error:#}"
    );
}

#[tokio::test]
async fn refresh_fails_when_expected_document_id_differs_from_server_document_id() {
    let state = test_state(ListingsSnapshotSource::Materialized, 7).await;

    let error = refresh_materialized_snapshot_with_loader(
        Arc::clone(&state),
        SnapshotRefreshRequest {
            document_id: "other-current".to_string(),
            expected_revision: 8,
        },
        || async { Ok(Some(sample_cached_snapshot(8))) },
    )
    .await
    .expect_err("refresh must fail when worker and server target different snapshot docs");

    assert!(
        error.to_string().contains("document id"),
        "unexpected refresh error: {error:#}"
    );
}

#[tokio::test]
async fn materialized_cache_applies_only_newer_snapshot() {
    let state = test_state(ListingsSnapshotSource::Materialized, 3).await;

    let applied = apply_materialized_snapshot_if_newer(&state, sample_cached_snapshot(4)).await;
    let ignored = apply_materialized_snapshot_if_newer(&state, sample_cached_snapshot(4)).await;

    assert_eq!(applied.applied_revision, Some(4));
    assert_eq!(ignored.applied_revision, None);
}

#[tokio::test]
async fn materialized_snapshot_fetch_ignores_cache_older_than_visible_revision() {
    let state = test_state(ListingsSnapshotSource::Materialized, 8).await;
    *state.listings_snapshot_cache.write().await =
        ListingsSnapshotCacheState::Ready(sample_cached_snapshot(7));
    let loads = Arc::new(AtomicUsize::new(0));

    let snapshot = get_materialized_cached_snapshot_with_loader(Arc::clone(&state), {
        let loads = Arc::clone(&loads);
        || async move {
            loads.fetch_add(1, Ordering::SeqCst);
            Ok(Some(sample_cached_snapshot(8)))
        }
    })
    .await
    .expect("materialized snapshot lookup should succeed")
    .expect("loader should return a snapshot");

    assert_eq!(loads.load(Ordering::SeqCst), 1);
    assert_eq!(snapshot.revision, 8);
}

#[tokio::test]
async fn materialized_snapshot_fetch_reconciles_stale_process_revision_on_cadence() {
    let state = test_state(ListingsSnapshotSource::Materialized, 7).await;
    *state.listings_snapshot_cache.write().await =
        ListingsSnapshotCacheState::Ready(sample_cached_snapshot(7));
    let loads = Arc::new(AtomicUsize::new(0));

    let snapshot = get_materialized_cached_snapshot_with_loader(Arc::clone(&state), {
        let loads = Arc::clone(&loads);
        || async move {
            loads.fetch_add(1, Ordering::SeqCst);
            Ok(Some(sample_cached_snapshot(8)))
        }
    })
    .await
    .expect("materialized snapshot lookup should succeed")
    .expect("loader should return a snapshot");

    assert_eq!(snapshot.revision, 8);
    assert_eq!(state.current_listings_revision(), 8);
    assert_eq!(loads.load(Ordering::SeqCst), 1);

    let cached = get_materialized_cached_snapshot_with_loader(Arc::clone(&state), {
        let loads = Arc::clone(&loads);
        || async move {
            loads.fetch_add(1, Ordering::SeqCst);
            Ok(Some(sample_cached_snapshot(9)))
        }
    })
    .await
    .expect("materialized snapshot lookup should succeed")
    .expect("cache should remain ready");

    assert_eq!(cached.revision, 8);
    assert_eq!(
        loads.load(Ordering::SeqCst),
        1,
        "freshness cadence should avoid loading Mongo on every request"
    );
}
