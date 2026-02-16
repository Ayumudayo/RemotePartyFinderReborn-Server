use std::sync::Arc;

use base64::encode as base64_encode;
use chrono::{DateTime, TimeDelta, Utc};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use warp::{
    Reply,
    http::{HeaderMap, StatusCode},
};

use super::{IngestRateKey, IngestRateWindow, IngestRateLimits, State};

const WINDOW_SECONDS: i64 = 60;

#[derive(Debug, Clone, Copy)]
pub enum IngestEndpoint {
    Contribute,
    ContributeMultiple,
    ContributePlayers,
    ContributeDetail,
    ContributeFflogsJobs,
    ContributeFflogsResults,
}

impl IngestEndpoint {
    fn key(self) -> &'static str {
        match self {
            Self::Contribute => "contribute",
            Self::ContributeMultiple => "contribute_multiple",
            Self::ContributePlayers => "contribute_players",
            Self::ContributeDetail => "contribute_detail",
            Self::ContributeFflogsJobs => "contribute_fflogs_jobs",
            Self::ContributeFflogsResults => "contribute_fflogs_results",
        }
    }

    fn limit_per_minute(self, limits: &IngestRateLimits) -> u32 {
        match self {
            Self::Contribute => limits.contribute_per_minute,
            Self::ContributeMultiple => limits.multiple_per_minute,
            Self::ContributePlayers => limits.players_per_minute,
            Self::ContributeDetail => limits.detail_per_minute,
            Self::ContributeFflogsJobs => limits.fflogs_jobs_per_minute,
            Self::ContributeFflogsResults => limits.fflogs_results_per_minute,
        }
    }
}

#[derive(Debug, Clone)]
pub struct GuardContext {
    pub client_id: String,
}

pub enum GuardError {
    BadRequest(&'static str),
    Unauthorized(&'static str),
    Forbidden(&'static str),
    TooManyRequests { retry_after_seconds: u64 },
}

pub async fn authorize_request(
    state: &Arc<State>,
    endpoint: IngestEndpoint,
    headers: &HeaderMap,
    method: &str,
    path: &str,
) -> Result<GuardContext, GuardError> {
    let client_id = extract_client_id(headers);

    enforce_rate_limit(state, endpoint, &client_id).await?;

    let should_verify_signature = state.ingest_security.require_signature;

    if should_verify_signature {
        verify_signature(state, headers, method, path, &client_id)?;
        enforce_nonce(state, headers, &client_id).await?;
    }

    Ok(GuardContext { client_id })
}

pub fn guard_error_reply(error: GuardError) -> warp::reply::Response {
    match error {
        GuardError::BadRequest(message) => {
            warp::reply::with_status(message, StatusCode::BAD_REQUEST).into_response()
        }
        GuardError::Unauthorized(message) => {
            warp::reply::with_status(message, StatusCode::UNAUTHORIZED).into_response()
        }
        GuardError::Forbidden(message) => {
            warp::reply::with_status(message, StatusCode::FORBIDDEN).into_response()
        }
        GuardError::TooManyRequests {
            retry_after_seconds,
        } => {
            let base = warp::reply::with_status("too many requests", StatusCode::TOO_MANY_REQUESTS);
            warp::reply::with_header(base, "Retry-After", retry_after_seconds.to_string()).into_response()
        }
    }
}

fn extract_client_id(headers: &HeaderMap) -> String {
    header_value(headers, "x-rpf-client-id")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "anonymous".to_string())
}

fn header_value<'a>(headers: &'a HeaderMap, key: &str) -> Option<&'a str> {
    headers.get(key).and_then(|value| value.to_str().ok())
}

fn verify_signature(
    state: &Arc<State>,
    headers: &HeaderMap,
    method: &str,
    path: &str,
    client_id: &str,
) -> Result<(), GuardError> {
    if let Some(version) = header_value(headers, "x-rpf-signature-version") {
        if version != "v1" {
            return Err(GuardError::Unauthorized("unsupported signature version"));
        }
    }

    let timestamp_raw = header_value(headers, "x-rpf-timestamp")
        .ok_or(GuardError::Unauthorized("missing timestamp"))?;
    let nonce = header_value(headers, "x-rpf-nonce").ok_or(GuardError::Unauthorized("missing nonce"))?;
    let signature = header_value(headers, "x-rpf-signature")
        .ok_or(GuardError::Unauthorized("missing signature"))?;

    let timestamp = timestamp_raw
        .parse::<i64>()
        .map_err(|_| GuardError::BadRequest("invalid timestamp"))?;

    let now = Utc::now().timestamp();
    let skew = (now - timestamp).abs();
    if skew > state.ingest_security.clock_skew_seconds {
        return Err(GuardError::Forbidden("timestamp skew exceeded"));
    }

    let payload = format!("{}\n{}\n{}\n{}\n{}", method, path, timestamp_raw, nonce, client_id);
    let expected = compute_signature(&state.ingest_security.shared_secret, &payload)
        .map_err(|_| GuardError::Unauthorized("signature setup invalid"))?;

    if !constant_time_eq(signature.as_bytes(), expected.as_bytes()) {
        return Err(GuardError::Unauthorized("invalid signature"));
    }

    Ok(())
}

async fn enforce_nonce(state: &Arc<State>, headers: &HeaderMap, client_id: &str) -> Result<(), GuardError> {
    let nonce = header_value(headers, "x-rpf-nonce").ok_or(GuardError::Unauthorized("missing nonce"))?;
    let nonce_key = format!("{}:{}", client_id, nonce);

    let Some(ttl) = TimeDelta::try_seconds(state.ingest_security.nonce_ttl_seconds) else {
        return Err(GuardError::BadRequest("invalid nonce ttl"));
    };

    let now = Utc::now();
    let mut nonces = state.ingest_nonces.write().await;
    nonces.retain(|_, seen_at| *seen_at + ttl > now);

    if nonces.contains_key(&nonce_key) {
        return Err(GuardError::Forbidden("replayed nonce"));
    }

    nonces.insert(nonce_key, now);
    Ok(())
}

async fn enforce_rate_limit(
    state: &Arc<State>,
    endpoint: IngestEndpoint,
    client_id: &str,
) -> Result<(), GuardError> {
    let now = Utc::now();
    let key = IngestRateKey {
        endpoint: endpoint.key(),
        client_id: client_id.to_string(),
    };
    let limit = endpoint.limit_per_minute(&state.ingest_security.rate_limits).max(1);

    let mut windows = state.ingest_rate_windows.write().await;
    let stale_after = TimeDelta::seconds(WINDOW_SECONDS * 2);
    windows.retain(|_, window| now - window.window_started <= stale_after);

    let window = windows.entry(key).or_insert_with(|| IngestRateWindow {
        window_started: now,
        count: 0,
    });

    let elapsed = now - window.window_started;
    if elapsed.num_seconds() >= WINDOW_SECONDS {
        window.window_started = now;
        window.count = 0;
    }

    if window.count >= limit {
        let retry_after = remaining_window_seconds(window.window_started, now);
        return Err(GuardError::TooManyRequests {
            retry_after_seconds: retry_after,
        });
    }

    window.count += 1;
    Ok(())
}

fn remaining_window_seconds(window_started: DateTime<Utc>, now: DateTime<Utc>) -> u64 {
    let elapsed = (now - window_started).num_seconds().max(0);
    let remaining = (WINDOW_SECONDS - elapsed).max(1);
    remaining as u64
}

fn compute_signature(secret: &str, payload: &str) -> Result<String, hmac::digest::InvalidLength> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())?;
    mac.update(payload.as_bytes());
    let signature = mac.finalize().into_bytes();
    Ok(base64_encode(signature))
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
