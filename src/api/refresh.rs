use std::{
    future::Future,
    sync::{atomic::Ordering as AtomicOrdering, Arc},
    time::Instant,
};

use crate::listings_snapshot::load_current_materialized_snapshot;
use crate::web::{CachedListingsSnapshot, ListingsSnapshotCacheState, State};
use base64::encode as base64_encode;
use chrono::{TimeDelta, Utc};
use hmac::{Hmac, Mac};
use warp::http::{HeaderMap, StatusCode};

use super::snapshot::record_materialized_snapshot_freshness_check;

pub(super) const SNAPSHOT_REFRESH_PATH: &str = "/internal/listings/snapshot/refresh";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ApplyMaterializedSnapshotOutcome {
    pub(crate) applied_revision: Option<u64>,
}

pub(super) async fn apply_materialized_snapshot_if_newer(
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

pub(super) async fn reconcile_materialized_snapshot_once_with_loader<F, Fut>(
    state: Arc<State>,
    load: F,
) -> anyhow::Result<ApplyMaterializedSnapshotOutcome>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = anyhow::Result<Option<CachedListingsSnapshot>>>,
{
    let loaded = load().await;
    record_materialized_snapshot_freshness_check(&state, Instant::now());
    let Some(snapshot) = loaded? else {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SnapshotRefreshRequest {
    pub(super) document_id: String,
    pub(super) expected_revision: u64,
}

pub(super) async fn refresh_materialized_snapshot_with_loader<F, Fut>(
    state: Arc<State>,
    request: SnapshotRefreshRequest,
    load: F,
) -> anyhow::Result<ApplyMaterializedSnapshotOutcome>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = anyhow::Result<Option<CachedListingsSnapshot>>>,
{
    if request.document_id != state.listings_snapshot_document_id {
        anyhow::bail!(
            "snapshot refresh document id mismatch: request document id {} does not match server document id {}",
            request.document_id,
            state.listings_snapshot_document_id
        );
    }

    let loaded = load().await;
    record_materialized_snapshot_freshness_check(&state, Instant::now());
    let Some(snapshot) = loaded? else {
        anyhow::bail!(
            "materialized listings snapshot missing for expected revision {}",
            request.expected_revision
        );
    };

    if snapshot.revision < request.expected_revision {
        anyhow::bail!(
            "materialized listings snapshot revision {} is older than expected revision {}",
            snapshot.revision,
            request.expected_revision
        );
    }

    let outcome = apply_materialized_snapshot_if_newer(&state, snapshot.clone()).await;
    if outcome.applied_revision.is_none() {
        let current_revision = state.current_listings_revision();
        if snapshot.revision >= current_revision {
            let mut cache = state.listings_snapshot_cache.write().await;
            if matches!(&*cache, ListingsSnapshotCacheState::Empty)
                || matches!(&*cache, ListingsSnapshotCacheState::Ready(cached) if cached.revision < snapshot.revision)
            {
                *cache = ListingsSnapshotCacheState::Ready(snapshot);
            }
        }
    }

    Ok(outcome)
}

#[derive(Debug)]
pub(super) struct SnapshotRefreshAuthError {
    pub(super) status: StatusCode,
    pub(super) message: &'static str,
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

pub(super) async fn authorize_snapshot_refresh_request(
    state: &Arc<State>,
    headers: &HeaderMap,
) -> Result<SnapshotRefreshRequest, SnapshotRefreshAuthError> {
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

    let document_id = header_value(headers, "x-rpf-snapshot-document-id")
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(SnapshotRefreshAuthError {
            status: StatusCode::BAD_REQUEST,
            message: "missing snapshot document id",
        })?;
    let expected_revision_raw = header_value(headers, "x-rpf-expected-revision")
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(SnapshotRefreshAuthError {
            status: StatusCode::BAD_REQUEST,
            message: "missing expected revision",
        })?;
    let expected_revision =
        expected_revision_raw
            .parse::<u64>()
            .map_err(|_| SnapshotRefreshAuthError {
                status: StatusCode::BAD_REQUEST,
                message: "invalid expected revision",
            })?;

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
    if skew > state.snapshot_refresh_clock_skew_seconds {
        return Err(SnapshotRefreshAuthError {
            status: StatusCode::FORBIDDEN,
            message: "timestamp skew exceeded",
        });
    }

    let payload = format!(
        "POST\n{}\n{}\n{}\n{}\n{}\n{}",
        SNAPSHOT_REFRESH_PATH, timestamp_raw, nonce, client_id, document_id, expected_revision
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

    let Some(ttl) = TimeDelta::try_seconds(state.snapshot_refresh_nonce_ttl_seconds) else {
        return Err(SnapshotRefreshAuthError {
            status: StatusCode::BAD_REQUEST,
            message: "invalid nonce ttl",
        });
    };

    let now = Utc::now();
    let nonce_key = format!("snapshot-refresh:{client_id}:{nonce}");
    let mut nonces = state.snapshot_refresh_nonces.write().await;
    nonces.retain(|_, seen_at| *seen_at + ttl > now);
    if nonces.contains_key(&nonce_key) {
        return Err(SnapshotRefreshAuthError {
            status: StatusCode::FORBIDDEN,
            message: "replayed nonce",
        });
    }

    nonces.insert(nonce_key, now);
    Ok(SnapshotRefreshRequest {
        document_id: document_id.to_string(),
        expected_revision,
    })
}
