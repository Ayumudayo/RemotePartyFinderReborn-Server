use crate::config::ListingsSnapshotSource;
use crate::listings_snapshot::{
    build_listings_payload, load_current_materialized_snapshot, serialize_snapshot,
    BuiltListingsPayload,
};
use crate::web::{CachedListingsSnapshot, ListingsSnapshotCacheState, State};
use crate::ws::WsApiClient;
use anyhow::Context;
use base64::encode as base64_encode;
use chrono::{TimeDelta, Utc};
use hmac::{Hmac, Mac};
use std::convert::Infallible;
use std::future::Future;
use std::sync::{atomic::Ordering as AtomicOrdering, Arc};
use tokio::sync::{Notify, RwLock};
use warp::filters::BoxedFilter;
use warp::http::{
    header::{HeaderValue, CACHE_CONTROL, CONTENT_ENCODING, CONTENT_TYPE, ETAG},
    HeaderMap, StatusCode,
};
use warp::hyper::Body;
use warp::{Filter, Reply};

const SNAPSHOT_REFRESH_PATH: &str = "/internal/listings/snapshot/refresh";

pub fn api(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    warp::path("api")
        .and(ws(state.clone()).or(listings_snapshot(state.clone())))
        .boxed()
}

pub fn internal_routes(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    snapshot_refresh(state).boxed()
}

async fn build_snapshot_payload<T, Build, BuildFut, CurrentRevision>(
    mut build: Build,
    current_revision: CurrentRevision,
) -> (u64, T)
where
    Build: FnMut() -> BuildFut,
    BuildFut: Future<Output = T>,
    CurrentRevision: Fn() -> u64,
{
    let revision_before = current_revision();
    let payload = build().await;
    let revision_after = current_revision();

    if revision_before == revision_after {
        return (revision_after, payload);
    }

    // If writes race the build, report the lower bound that the payload is
    // guaranteed to include. The next client/server revision check will catch up
    // without multiplying the expensive listing-enrichment pass.
    (revision_before, payload)
}

async fn build_api_snapshot(state: Arc<State>) -> anyhow::Result<CachedListingsSnapshot> {
    let (revision, payload): (u64, anyhow::Result<BuiltListingsPayload>) = build_snapshot_payload(
        || build_listings_payload(Arc::clone(&state)),
        || state.current_listings_revision(),
    )
    .await;
    let payload = payload?;
    let snapshot = serialize_snapshot(
        revision
            .try_into()
            .context("listings snapshot revision exceeded i64")?,
        payload,
    )?;

    Ok(CachedListingsSnapshot {
        revision,
        body: snapshot.body.into(),
        etag: Some(snapshot.etag),
        content_encoding: None,
    })
}

async fn build_cached_api_snapshot(state: Arc<State>) -> anyhow::Result<CachedListingsSnapshot> {
    build_api_snapshot(state).await
}

async fn get_or_build_cached_snapshot<F, Fut, E>(
    cache: &RwLock<ListingsSnapshotCacheState>,
    min_revision: u64,
    build: F,
) -> Result<CachedListingsSnapshot, E>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<CachedListingsSnapshot, E>>,
{
    loop {
        let maybe_wait = {
            let cache = cache.read().await;
            match &*cache {
                ListingsSnapshotCacheState::Ready(snapshot)
                    if snapshot.revision >= min_revision =>
                {
                    return Ok(snapshot.clone());
                }
                ListingsSnapshotCacheState::Building { notify } => Some(Arc::clone(notify)),
                ListingsSnapshotCacheState::Ready(_) | ListingsSnapshotCacheState::Empty => None,
            }
        };

        if let Some(notify) = maybe_wait {
            notify.notified().await;
            continue;
        }

        let notify = Arc::new(Notify::new());
        let maybe_wait = {
            let mut cache = cache.write().await;
            match &*cache {
                ListingsSnapshotCacheState::Ready(snapshot)
                    if snapshot.revision >= min_revision =>
                {
                    return Ok(snapshot.clone());
                }
                ListingsSnapshotCacheState::Building { notify } => Some(Arc::clone(notify)),
                ListingsSnapshotCacheState::Ready(_) | ListingsSnapshotCacheState::Empty => {
                    *cache = ListingsSnapshotCacheState::Building {
                        notify: Arc::clone(&notify),
                    };
                    None
                }
            }
        };

        if let Some(notify) = maybe_wait {
            notify.notified().await;
            continue;
        }

        let built = build().await;

        {
            let mut cache = cache.write().await;
            match &built {
                Ok(snapshot) => {
                    *cache = ListingsSnapshotCacheState::Ready(snapshot.clone());
                }
                Err(_) => {
                    *cache = ListingsSnapshotCacheState::Empty;
                }
            }
        }

        notify.notify_waiters();
        return built;
    }
}

async fn get_cached_api_snapshot(state: Arc<State>) -> anyhow::Result<CachedListingsSnapshot> {
    let min_revision = state.current_listings_revision();
    get_or_build_cached_snapshot(&state.listings_snapshot_cache, min_revision, || {
        build_cached_api_snapshot(Arc::clone(&state))
    })
    .await
}

async fn get_materialized_cached_snapshot_with_loader<F, Fut>(
    state: Arc<State>,
    load: F,
) -> anyhow::Result<Option<CachedListingsSnapshot>>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = anyhow::Result<Option<CachedListingsSnapshot>>>,
{
    {
        let cache = state.listings_snapshot_cache.read().await;
        if let ListingsSnapshotCacheState::Ready(snapshot) = &*cache {
            if snapshot.revision >= state.current_listings_revision() {
                return Ok(Some(snapshot.clone()));
            }
        }
    }

    let Some(snapshot) = load().await? else {
        return Ok(None);
    };

    let outcome = apply_materialized_snapshot_if_newer(&state, snapshot.clone()).await;
    if outcome.applied_revision.is_none() {
        let current_revision = state.current_listings_revision();
        if snapshot.revision < current_revision {
            let cache = state.listings_snapshot_cache.read().await;
            if let ListingsSnapshotCacheState::Ready(cached) = &*cache {
                if cached.revision >= current_revision {
                    return Ok(Some(cached.clone()));
                }
            }
            return Ok(None);
        }

        let mut cache = state.listings_snapshot_cache.write().await;
        if matches!(&*cache, ListingsSnapshotCacheState::Empty)
            || matches!(&*cache, ListingsSnapshotCacheState::Ready(cached) if cached.revision < snapshot.revision)
        {
            *cache = ListingsSnapshotCacheState::Ready(snapshot.clone());
        }
    }

    Ok(Some(snapshot))
}

async fn get_materialized_cached_api_snapshot(
    state: Arc<State>,
) -> anyhow::Result<Option<CachedListingsSnapshot>> {
    get_materialized_cached_snapshot_with_loader(Arc::clone(&state), || async {
        load_current_materialized_snapshot(&state).await
    })
    .await
}

async fn get_api_snapshot(state: Arc<State>) -> anyhow::Result<Option<CachedListingsSnapshot>> {
    match state.listings_snapshot_source {
        ListingsSnapshotSource::Inline => get_cached_api_snapshot(state).await.map(Some),
        ListingsSnapshotSource::Materialized => get_materialized_cached_api_snapshot(state).await,
    }
}

fn snapshot_json_response(snapshot: CachedListingsSnapshot) -> warp::reply::Response {
    let mut response = warp::http::Response::new(Body::from(snapshot.body));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/json; charset=utf-8"),
    );
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if let Some(etag) = snapshot.etag {
        if let Ok(value) = HeaderValue::from_str(&etag) {
            response.headers_mut().insert(ETAG, value);
        }
    }
    if let Some(content_encoding) = snapshot.content_encoding {
        if let Ok(value) = HeaderValue::from_str(&content_encoding) {
            response.headers_mut().insert(CONTENT_ENCODING, value);
        }
    }
    response
}

fn materialized_snapshot_result_response(
    result: anyhow::Result<Option<CachedListingsSnapshot>>,
) -> warp::reply::Response {
    match result {
        Ok(Some(snapshot)) => snapshot_json_response(snapshot),
        Ok(None) => warp::reply::with_status("snapshot not ready", StatusCode::SERVICE_UNAVAILABLE)
            .into_response(),
        Err(error) => {
            tracing::error!(
                "failed to load materialized listings snapshot: {:#?}",
                error
            );
            warp::reply::with_status(
                "failed to load listings snapshot",
                StatusCode::INTERNAL_SERVER_ERROR,
            )
            .into_response()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ApplyMaterializedSnapshotOutcome {
    pub(crate) applied_revision: Option<u64>,
}

async fn apply_materialized_snapshot_if_newer(
    state: &Arc<State>,
    snapshot: CachedListingsSnapshot,
) -> ApplyMaterializedSnapshotOutcome {
    let revision = snapshot.revision;
    let mut cache = state.listings_snapshot_cache.write().await;
    let current = state.current_listings_revision();

    if revision <= current {
        return ApplyMaterializedSnapshotOutcome {
            applied_revision: None,
        };
    }

    // Publish the cache before the visible revision so clients can never observe
    // a revision that this process cannot serve from memory.
    *cache = ListingsSnapshotCacheState::Ready(snapshot);
    state
        .listings_revision
        .store(revision, AtomicOrdering::Relaxed);
    drop(cache);

    let _ = state.listings_change_channel.send(revision);
    ApplyMaterializedSnapshotOutcome {
        applied_revision: Some(revision),
    }
}

async fn reconcile_materialized_snapshot_once_with_loader<F, Fut>(
    state: Arc<State>,
    load: F,
) -> anyhow::Result<ApplyMaterializedSnapshotOutcome>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = anyhow::Result<Option<CachedListingsSnapshot>>>,
{
    let Some(snapshot) = load().await? else {
        tracing::debug!("materialized listings snapshot is not ready during reconciliation");
        return Ok(ApplyMaterializedSnapshotOutcome {
            applied_revision: None,
        });
    };

    Ok(apply_materialized_snapshot_if_newer(&state, snapshot).await)
}

pub(crate) async fn reconcile_materialized_snapshot_once(
    state: Arc<State>,
) -> anyhow::Result<ApplyMaterializedSnapshotOutcome> {
    reconcile_materialized_snapshot_once_with_loader(Arc::clone(&state), || async {
        load_current_materialized_snapshot(&state).await
    })
    .await
}

async fn refresh_materialized_snapshot_with_loader<F, Fut>(
    state: Arc<State>,
    load: F,
) -> anyhow::Result<ApplyMaterializedSnapshotOutcome>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = anyhow::Result<Option<CachedListingsSnapshot>>>,
{
    reconcile_materialized_snapshot_once_with_loader(state, load).await
}

#[derive(Debug)]
struct SnapshotRefreshAuthError {
    status: StatusCode,
    message: &'static str,
}

fn header_value<'a>(headers: &'a HeaderMap, key: &str) -> Option<&'a str> {
    headers.get(key).and_then(|value| value.to_str().ok())
}

fn compute_refresh_signature(
    secret: &str,
    payload: &str,
) -> Result<String, hmac::digest::InvalidLength> {
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(secret.as_bytes())?;
    mac.update(payload.as_bytes());
    Ok(base64_encode(mac.finalize().into_bytes()))
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }

    let mut diff = 0u8;
    for (a, b) in left.iter().zip(right.iter()) {
        diff |= a ^ b;
    }

    diff == 0
}

async fn authorize_snapshot_refresh_request(
    state: &Arc<State>,
    headers: &HeaderMap,
) -> Result<(), SnapshotRefreshAuthError> {
    let client_id = header_value(headers, "x-rpf-client-id")
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(SnapshotRefreshAuthError {
            status: StatusCode::UNAUTHORIZED,
            message: "missing client id",
        })?;
    if client_id != state.snapshot_refresh_client_id {
        return Err(SnapshotRefreshAuthError {
            status: StatusCode::FORBIDDEN,
            message: "invalid client id",
        });
    }

    match header_value(headers, "x-rpf-signature-version") {
        Some("v1") => {}
        _ => {
            return Err(SnapshotRefreshAuthError {
                status: StatusCode::UNAUTHORIZED,
                message: "unsupported signature version",
            });
        }
    }

    let timestamp_raw =
        header_value(headers, "x-rpf-timestamp").ok_or(SnapshotRefreshAuthError {
            status: StatusCode::UNAUTHORIZED,
            message: "missing timestamp",
        })?;
    let nonce = header_value(headers, "x-rpf-nonce").ok_or(SnapshotRefreshAuthError {
        status: StatusCode::UNAUTHORIZED,
        message: "missing nonce",
    })?;
    let signature = header_value(headers, "x-rpf-signature").ok_or(SnapshotRefreshAuthError {
        status: StatusCode::UNAUTHORIZED,
        message: "missing signature",
    })?;

    let timestamp = timestamp_raw
        .parse::<i64>()
        .map_err(|_| SnapshotRefreshAuthError {
            status: StatusCode::BAD_REQUEST,
            message: "invalid timestamp",
        })?;
    let skew = (Utc::now().timestamp() - timestamp).abs();
    if skew > state.ingest_security.clock_skew_seconds {
        return Err(SnapshotRefreshAuthError {
            status: StatusCode::FORBIDDEN,
            message: "timestamp skew exceeded",
        });
    }

    let payload = format!(
        "POST\n{}\n{}\n{}\n{}",
        SNAPSHOT_REFRESH_PATH, timestamp_raw, nonce, client_id
    );
    let expected = compute_refresh_signature(&state.snapshot_refresh_shared_secret, &payload)
        .map_err(|_| SnapshotRefreshAuthError {
            status: StatusCode::UNAUTHORIZED,
            message: "signature setup invalid",
        })?;
    if !constant_time_eq(signature.as_bytes(), expected.as_bytes()) {
        return Err(SnapshotRefreshAuthError {
            status: StatusCode::UNAUTHORIZED,
            message: "invalid signature",
        });
    }

    let Some(ttl) = TimeDelta::try_seconds(state.ingest_security.nonce_ttl_seconds) else {
        return Err(SnapshotRefreshAuthError {
            status: StatusCode::BAD_REQUEST,
            message: "invalid nonce ttl",
        });
    };

    let now = Utc::now();
    let nonce_key = format!("snapshot-refresh:{client_id}:{nonce}");
    let mut nonces = state.ingest_nonces.write().await;
    nonces.retain(|_, seen_at| *seen_at + ttl > now);
    if nonces.contains_key(&nonce_key) {
        return Err(SnapshotRefreshAuthError {
            status: StatusCode::FORBIDDEN,
            message: "replayed nonce",
        });
    }

    nonces.insert(nonce_key, now);
    Ok(())
}

fn listings_snapshot(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    async fn logic(state: Arc<State>) -> Result<warp::reply::Response, Infallible> {
        let response = match state.listings_snapshot_source {
            ListingsSnapshotSource::Inline => match get_api_snapshot(state).await {
                Ok(Some(snapshot)) => snapshot_json_response(snapshot),
                Ok(None) => warp::reply::with_status(
                    "failed to build listings snapshot",
                    StatusCode::INTERNAL_SERVER_ERROR,
                )
                .into_response(),
                Err(error) => {
                    tracing::error!("failed to build listings snapshot json: {:#?}", error);
                    warp::reply::with_status(
                        "failed to build listings snapshot",
                        StatusCode::INTERNAL_SERVER_ERROR,
                    )
                    .into_response()
                }
            },
            ListingsSnapshotSource::Materialized => {
                materialized_snapshot_result_response(get_api_snapshot(state).await)
            }
        };

        Ok(response)
    }

    warp::get()
        .and(warp::path("listings"))
        .and(warp::path("snapshot"))
        .and(warp::path::end())
        .and_then(move || logic(state.clone()))
        .boxed()
}

fn snapshot_refresh(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    async fn logic(
        state: Arc<State>,
        headers: HeaderMap,
    ) -> Result<warp::reply::Response, Infallible> {
        let response = if state.listings_snapshot_source != ListingsSnapshotSource::Materialized {
            warp::reply::with_status("snapshot refresh disabled", StatusCode::NOT_FOUND)
                .into_response()
        } else if let Err(error) = authorize_snapshot_refresh_request(&state, &headers).await {
            warp::reply::with_status(error.message, error.status).into_response()
        } else {
            match refresh_materialized_snapshot_with_loader(Arc::clone(&state), || async {
                load_current_materialized_snapshot(&state).await
            })
            .await
            {
                Ok(_) => warp::reply::with_status("ok", StatusCode::OK).into_response(),
                Err(error) => {
                    tracing::error!(
                        "failed to refresh materialized listings snapshot: {:#?}",
                        error
                    );
                    warp::reply::with_status(
                        "failed to refresh listings snapshot",
                        StatusCode::INTERNAL_SERVER_ERROR,
                    )
                    .into_response()
                }
            }
        };

        Ok(response)
    }

    warp::post()
        .and(warp::path("internal"))
        .and(warp::path("listings"))
        .and(warp::path("snapshot"))
        .and(warp::path("refresh"))
        .and(warp::path::end())
        .and(warp::header::headers_cloned())
        .and_then(move |headers| logic(state.clone(), headers))
        .boxed()
}

fn ws(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route =
        warp::path("ws")
            .and(warp::ws())
            .and(warp::path::end())
            .map(move |ws: warp::ws::Ws| {
                let state = Arc::clone(&state);
                ws.on_upgrade(move |websocket| async move {
                    WsApiClient::run(state, websocket).await;
                })
            });

    warp::get().and(route).boxed()
}

#[cfg(test)]
mod tests {
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
    use crate::web::{IngestRateLimits, IngestSecurityConfig, ListingsSnapshotCacheState, State};

    use super::{
        apply_materialized_snapshot_if_newer, authorize_snapshot_refresh_request,
        build_snapshot_payload, get_materialized_cached_snapshot_with_loader,
        get_or_build_cached_snapshot, materialized_snapshot_result_response,
        reconcile_materialized_snapshot_once_with_loader,
        refresh_materialized_snapshot_with_loader, snapshot_json_response, CachedListingsSnapshot,
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
            listing_snapshot_worker_lease_collection_name: "listing_snapshot_worker_leases"
                .to_string(),
            materialized_snapshot_reconcile_interval_seconds: 30,
            snapshot_refresh_shared_secret: "refresh-secret".to_string(),
            snapshot_refresh_client_id: "listings-snapshot-worker".to_string(),
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

    fn signed_refresh_headers(secret: &str, client_id: &str, nonce: &str) -> HeaderMap {
        let timestamp = Utc::now().timestamp().to_string();
        let payload = format!(
            "POST\n/internal/listings/snapshot/refresh\n{}\n{}\n{}",
            timestamp, nonce, client_id
        );
        let mut mac =
            Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("test hmac should initialize");
        mac.update(payload.as_bytes());
        let signature = base64::encode(mac.finalize().into_bytes());

        let mut headers = HeaderMap::new();
        headers.insert("x-rpf-client-id", HeaderValue::from_str(client_id).unwrap());
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
        let listing =
            ApiReadableListing::from_parts(sample_listing(), Vec::new(), Default::default());

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

        let newer =
            reconcile_materialized_snapshot_once_with_loader(Arc::clone(&state), || async {
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
        let older =
            reconcile_materialized_snapshot_once_with_loader(Arc::clone(&state), || async {
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

        let mut bad = signed_refresh_headers("wrong-secret", "worker", "bad-nonce");
        assert!(authorize_snapshot_refresh_request(&state, &bad)
            .await
            .is_err());

        bad = signed_refresh_headers(
            "refresh-secret",
            "listings-snapshot-worker",
            "replayed-nonce",
        );
        authorize_snapshot_refresh_request(&state, &bad)
            .await
            .expect("first use of nonce should authorize");
        let replay = authorize_snapshot_refresh_request(&state, &bad).await;
        assert!(replay.is_err(), "replayed nonce should be rejected");
    }

    #[tokio::test]
    async fn valid_refresh_broadcasts_only_after_snapshot_revision_advances() {
        let state = test_state(ListingsSnapshotSource::Materialized, 7).await;
        let mut receiver = state.listings_change_channel.subscribe();

        let same = refresh_materialized_snapshot_with_loader(Arc::clone(&state), || async {
            Ok(Some(sample_cached_snapshot(7)))
        })
        .await
        .expect("same revision refresh should succeed");

        assert_eq!(same.applied_revision, None);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), receiver.recv())
                .await
                .is_err(),
            "same revision refresh must not broadcast"
        );

        let advanced = refresh_materialized_snapshot_with_loader(Arc::clone(&state), || async {
            Ok(Some(sample_cached_snapshot(8)))
        })
        .await
        .expect("advanced revision refresh should succeed");

        assert_eq!(advanced.applied_revision, Some(8));
        assert_eq!(receiver.recv().await.expect("revision should broadcast"), 8);
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
}
