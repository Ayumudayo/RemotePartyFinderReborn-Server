use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex, OnceLock, Weak},
    time::{Duration, Instant},
};

use crate::config::ListingsSnapshotSource;
use crate::listings_snapshot::{
    build_listings_payload, load_current_materialized_snapshot, serialize_snapshot,
    BuiltListingsPayload,
};
use crate::web::{CachedListingsSnapshot, ListingsSnapshotCacheState, State};
use anyhow::Context;
use tokio::sync::{Notify, RwLock};
use warp::http::{
    header::{HeaderValue, CACHE_CONTROL, CONTENT_ENCODING, CONTENT_TYPE, ETAG},
    StatusCode,
};
use warp::hyper::Body;
use warp::Reply;

use super::refresh::apply_materialized_snapshot_if_newer;
const MIN_MATERIALIZED_SNAPSHOT_REQUEST_RECONCILE_INTERVAL_SECONDS: u64 = 5;

type MaterializedSnapshotFreshnessChecks = HashMap<usize, (Weak<State>, Instant)>;

fn materialized_snapshot_freshness_checks() -> &'static Mutex<MaterializedSnapshotFreshnessChecks> {
    static CHECKS: OnceLock<Mutex<MaterializedSnapshotFreshnessChecks>> = OnceLock::new();
    CHECKS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn materialized_snapshot_request_reconcile_interval(state: &State) -> Duration {
    Duration::from_secs(
        state
            .materialized_snapshot_reconcile_interval_seconds
            .max(MIN_MATERIALIZED_SNAPSHOT_REQUEST_RECONCILE_INTERVAL_SECONDS),
    )
}

pub(super) fn record_materialized_snapshot_freshness_check(
    state: &Arc<State>,
    checked_at: Instant,
) {
    let key = Arc::as_ptr(state) as usize;
    let mut checks = materialized_snapshot_freshness_checks()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    checks.retain(|_, (state, _)| state.strong_count() > 0);
    checks.insert(key, (Arc::downgrade(state), checked_at));
}

fn should_check_materialized_snapshot_freshness(state: &Arc<State>) -> bool {
    let key = Arc::as_ptr(state) as usize;
    let now = Instant::now();
    let interval = materialized_snapshot_request_reconcile_interval(state);
    let mut checks = materialized_snapshot_freshness_checks()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    checks.retain(|_, (state, _)| state.strong_count() > 0);

    if let Some((cached_state, last_checked_at)) = checks.get(&key) {
        if cached_state.strong_count() > 0
            && now
                .checked_duration_since(*last_checked_at)
                .unwrap_or_default()
                < interval
        {
            return false;
        }
    }

    checks.insert(key, (Arc::downgrade(state), now));
    true
}

pub(super) async fn build_snapshot_payload<T, Build, BuildFut, CurrentRevision>(
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

pub(super) async fn get_or_build_cached_snapshot<F, Fut, E>(
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

pub(super) async fn get_materialized_cached_snapshot_with_loader<F, Fut>(
    state: Arc<State>,
    load: F,
) -> anyhow::Result<Option<CachedListingsSnapshot>>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = anyhow::Result<Option<CachedListingsSnapshot>>>,
{
    let cached_candidate = {
        let cache = state.listings_snapshot_cache.read().await;
        if let ListingsSnapshotCacheState::Ready(snapshot) = &*cache {
            if snapshot.revision >= state.current_listings_revision() {
                Some(snapshot.clone())
            } else {
                None
            }
        } else {
            None
        }
    };

    if let Some(snapshot) = &cached_candidate {
        if !should_check_materialized_snapshot_freshness(&state) {
            return Ok(Some(snapshot.clone()));
        }
    }

    let loaded = match load().await {
        Ok(snapshot) => snapshot,
        Err(error) if cached_candidate.is_some() => {
            tracing::warn!(
                error = ?error,
                "failed opportunistic materialized listings snapshot freshness check; serving cached snapshot"
            );
            return Ok(cached_candidate);
        }
        Err(error) => return Err(error),
    };

    let Some(snapshot) = loaded else {
        return Ok(cached_candidate);
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

pub(super) async fn get_api_snapshot(
    state: Arc<State>,
) -> anyhow::Result<Option<CachedListingsSnapshot>> {
    match state.listings_snapshot_source {
        ListingsSnapshotSource::Inline => get_cached_api_snapshot(state).await.map(Some),
        ListingsSnapshotSource::Materialized => get_materialized_cached_api_snapshot(state).await,
    }
}

pub(super) fn snapshot_json_response(snapshot: CachedListingsSnapshot) -> warp::reply::Response {
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

pub(super) fn materialized_snapshot_result_response(
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
