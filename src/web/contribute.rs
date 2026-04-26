use chrono::Utc;
use futures_util::StreamExt;
use mongodb::{
    bson::{doc, Document},
    options::UpdateOptions,
};
use std::{collections::HashSet, convert::Infallible, net::SocketAddr, sync::Arc};
use warp::{
    http::{HeaderMap, StatusCode},
    Reply,
};

use crate::listing::PartyFinderListing;
use crate::mongo::{insert_listing, upsert_character_identities, upsert_players};
use crate::player::{UploadableCharacterIdentity, UploadablePlayer};

use super::ingest_guard::{self, CapabilityClaims, CapabilityScope, IngestEndpoint};
use super::State;
pub async fn contribute_handler(
    state: Arc<State>,
    headers: HeaderMap,
    remote_addr: Option<SocketAddr>,
    listing: PartyFinderListing,
) -> std::result::Result<warp::reply::Response, Infallible> {
    if let Err(error) = ingest_guard::authorize_request(
        &state,
        IngestEndpoint::Contribute,
        &headers,
        remote_addr,
        "POST",
        "/contribute",
    )
    .await
    {
        return Ok(ingest_guard::guard_error_reply(error));
    }

    if listing.seconds_remaining > 60 * 60 {
        return Ok(
            warp::reply::with_status("invalid listing", StatusCode::BAD_REQUEST).into_response(),
        );
    }

    let upsert_result = insert_listing(state.collection(), &listing).await;
    if upsert_result.is_ok() {
        state.notify_listings_changed(1);
    }
    Ok(format!("{:#?}", upsert_result).into_response())
}

fn is_listing_acceptable(listing: &PartyFinderListing) -> bool {
    if listing.seconds_remaining > 60 * 60 {
        return false;
    }

    listing.created_world < 1_000 && listing.home_world < 1_000 && listing.current_world < 1_000
}

fn build_listing_upsert(
    listing: &PartyFinderListing,
) -> anyhow::Result<(u32, Document, Vec<Document>)> {
    let mut listing_doc = mongodb::bson::to_document(listing)?;
    listing_doc.remove("member_content_ids");
    listing_doc.remove("member_jobs");
    listing_doc.remove("leader_content_id");

    let filter = doc! {
        "listing.id": listing.id,
        "listing.last_server_restart": listing.last_server_restart,
        "listing.created_world": listing.created_world as u32,
    };
    let update = vec![doc! {
        "$set": {
            "updated_at": "$$NOW",
            "created_at": { "$ifNull": ["$created_at", "$$NOW"] },
            "listing": {
                "$mergeObjects": [
                    "$listing",
                    listing_doc,
                ]
            },
        }
    }];

    Ok((listing.id, filter, update))
}

pub async fn contribute_multiple_handler(
    state: Arc<State>,
    headers: HeaderMap,
    remote_addr: Option<SocketAddr>,
    listings: Vec<PartyFinderListing>,
) -> std::result::Result<warp::reply::Response, Infallible> {
    let guard_ctx = match ingest_guard::authorize_request(
        &state,
        IngestEndpoint::ContributeMultiple,
        &headers,
        remote_addr,
        "POST",
        "/contribute/multiple",
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(error) => return Ok(ingest_guard::guard_error_reply(error)),
    };

    let total = listings.len();
    if total > state.max_multiple_batch_size {
        return Ok(warp::reply::with_status(
            "too many listings in request",
            StatusCode::PAYLOAD_TOO_LARGE,
        )
        .into_response());
    }

    let mut upserts = Vec::new();
    for listing in &listings {
        if !is_listing_acceptable(listing) {
            continue;
        }

        match build_listing_upsert(listing) {
            Ok(op) => upserts.push(op),
            Err(error) => {
                tracing::debug!("Failed to serialize listing {}: {:#?}", listing.id, error);
            }
        }
    }

    let upsert_total = upserts.len();
    let mut successful = 0usize;
    let mut failed_writes = 0usize;
    let mut successful_listing_ids = Vec::with_capacity(upsert_total);
    let mut writes =
        futures_util::stream::iter(upserts.into_iter().map(|(listing_id, filter, update)| {
            let collection = state.collection();
            async move {
                collection
                    .update_one(
                        filter,
                        update,
                        UpdateOptions::builder().upsert(true).build(),
                    )
                    .await
                    .map(|_| listing_id)
                    .map_err(|error| (listing_id, error))
            }
        }))
        .buffer_unordered(state.listing_upsert_concurrency);

    while let Some(write_result) = writes.next().await {
        match write_result {
            Ok(listing_id) => {
                successful += 1;
                successful_listing_ids.push(listing_id);
            }
            Err((listing_id, error)) => {
                failed_writes += 1;
                tracing::debug!("Failed to insert listing {}: {:#?}", listing_id, error);
            }
        }
    }

    if failed_writes > 0 {
        tracing::warn!(
            requested_total = total,
            attempted_upserts = upsert_total,
            successful_upserts = successful,
            failed_writes,
            "Processed /contribute/multiple request with insert failures"
        );
    }

    if !successful_listing_ids.is_empty() {
        let successful_listing_ids_set: HashSet<u32> =
            successful_listing_ids.iter().copied().collect();
        let changed_listings: Vec<PartyFinderListing> = listings
            .into_iter()
            .filter(|listing| successful_listing_ids_set.contains(&listing.id))
            .collect();
        if !changed_listings.is_empty() {
            let changed_count = changed_listings.len();
            state.notify_listings_changed(changed_count);
        }
    }
    let response = ContributeMultipleResponse {
        status: if failed_writes == 0 { "ok" } else { "partial" },
        requested: total,
        accepted: upsert_total,
        updated: successful,
        failed: failed_writes,
        detail_capabilities: issue_detail_capabilities(
            &state,
            &guard_ctx.client_id,
            &successful_listing_ids,
        ),
        protected_endpoints: issue_protected_endpoint_capabilities(&state, &guard_ctx.client_id),
    };
    Ok(warp::reply::json(&response).into_response())
}

pub async fn contribute_players_handler(
    state: Arc<State>,
    headers: HeaderMap,
    remote_addr: Option<SocketAddr>,
    players: Vec<UploadablePlayer>,
) -> std::result::Result<warp::reply::Response, Infallible> {
    let guard_ctx = match ingest_guard::authorize_request(
        &state,
        IngestEndpoint::ContributePlayers,
        &headers,
        remote_addr,
        "POST",
        "/contribute/players",
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(error) => return Ok(ingest_guard::guard_error_reply(error)),
    };

    let total = players.len();
    if total > state.max_players_batch_size {
        return Ok(warp::reply::with_status(
            "too many players in request",
            StatusCode::PAYLOAD_TOO_LARGE,
        )
        .into_response());
    }

    let result = upsert_players(
        state.players_collection(),
        &players,
        state.player_upsert_concurrency,
    )
    .await;

    #[derive(Debug, serde::Serialize)]
    struct ContributePlayersResponse {
        status: &'static str,
        requested: usize,
        accepted: usize,
        updated: usize,
        invalid: usize,
        failed: usize,
        #[serde(skip_serializing_if = "Option::is_none")]
        protected_endpoints: Option<ProtectedEndpointCapabilities>,
    }

    match result {
        Ok(report) => {
            let (status_code, status) = if report.requested == 0 {
                (StatusCode::OK, "empty")
            } else if report.failed > 0 {
                (StatusCode::INTERNAL_SERVER_ERROR, "write_failure")
            } else if report.invalid > 0 {
                (StatusCode::BAD_REQUEST, "invalid_payload")
            } else {
                (StatusCode::OK, "ok")
            };

            if status_code == StatusCode::OK {
                tracing::debug!(
                    requested = report.requested,
                    accepted = report.accepted,
                    updated = report.updated,
                    invalid = report.invalid,
                    failed = report.failed,
                    "Processed /contribute/players request"
                );
            } else if status == "write_failure" {
                tracing::warn!(
                    requested = report.requested,
                    accepted = report.accepted,
                    updated = report.updated,
                    invalid = report.invalid,
                    failed = report.failed,
                    status,
                    "Processed /contribute/players request with issues"
                );
            } else {
                tracing::debug!(
                    requested = report.requested,
                    accepted = report.accepted,
                    updated = report.updated,
                    invalid = report.invalid,
                    failed = report.failed,
                    status,
                    "Processed /contribute/players request with issues"
                );
            }

            if report.updated > 0 {
                state.notify_listings_changed(report.updated);
            }

            let response = ContributePlayersResponse {
                status,
                requested: report.requested,
                accepted: report.accepted,
                updated: report.updated,
                invalid: report.invalid,
                failed: report.failed,
                protected_endpoints: issue_protected_endpoint_capabilities(
                    &state,
                    &guard_ctx.client_id,
                ),
            };

            Ok(warp::reply::with_status(warp::reply::json(&response), status_code).into_response())
        }
        Err(e) => {
            tracing::error!("error upserting players: {:#?}", e);

            let response = ContributePlayersResponse {
                status: "error",
                requested: total,
                accepted: 0,
                updated: 0,
                invalid: 0,
                failed: total,
                protected_endpoints: issue_protected_endpoint_capabilities(
                    &state,
                    &guard_ctx.client_id,
                ),
            };

            Ok(warp::reply::with_status(
                warp::reply::json(&response),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
            .into_response())
        }
    }
}

pub async fn contribute_character_identity_handler(
    state: Arc<State>,
    headers: HeaderMap,
    remote_addr: Option<SocketAddr>,
    identities: Vec<UploadableCharacterIdentity>,
) -> std::result::Result<warp::reply::Response, Infallible> {
    let guard_ctx = match ingest_guard::authorize_request(
        &state,
        IngestEndpoint::ContributePlayers,
        &headers,
        remote_addr,
        "POST",
        "/contribute/character-identity",
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(error) => return Ok(ingest_guard::guard_error_reply(error)),
    };

    let total = identities.len();
    if total > state.max_players_batch_size {
        return Ok(warp::reply::with_status(
            "too many character identities in request",
            StatusCode::PAYLOAD_TOO_LARGE,
        )
        .into_response());
    }

    let result = upsert_character_identities(
        state.players_collection(),
        &identities,
        state.player_upsert_concurrency,
        state.ingest_security.clock_skew_seconds,
    )
    .await;

    #[derive(Debug, serde::Serialize)]
    struct ContributeCharacterIdentityResponse {
        status: &'static str,
        requested: usize,
        accepted: usize,
        updated: usize,
        invalid: usize,
        failed: usize,
        #[serde(skip_serializing_if = "Option::is_none")]
        protected_endpoints: Option<ProtectedEndpointCapabilities>,
    }

    match result {
        Ok(report) => {
            let (status_code, status) = if report.requested == 0 {
                (StatusCode::OK, "empty")
            } else if report.failed > 0 {
                (StatusCode::INTERNAL_SERVER_ERROR, "write_failure")
            } else if report.invalid > 0 {
                (StatusCode::BAD_REQUEST, "invalid_payload")
            } else {
                (StatusCode::OK, "ok")
            };

            if status_code == StatusCode::OK {
                tracing::debug!(
                    requested = report.requested,
                    accepted = report.accepted,
                    updated = report.updated,
                    invalid = report.invalid,
                    failed = report.failed,
                    "Processed /contribute/character-identity request"
                );
            } else if status == "write_failure" {
                tracing::warn!(
                    requested = report.requested,
                    accepted = report.accepted,
                    updated = report.updated,
                    invalid = report.invalid,
                    failed = report.failed,
                    status,
                    "Processed /contribute/character-identity request with issues"
                );
            } else {
                tracing::debug!(
                    requested = report.requested,
                    accepted = report.accepted,
                    updated = report.updated,
                    invalid = report.invalid,
                    failed = report.failed,
                    status,
                    "Processed /contribute/character-identity request with issues"
                );
            }

            if report.updated > 0 {
                state.notify_listings_changed(report.updated);
            }

            let response = ContributeCharacterIdentityResponse {
                status,
                requested: report.requested,
                accepted: report.accepted,
                updated: report.updated,
                invalid: report.invalid,
                failed: report.failed,
                protected_endpoints: issue_protected_endpoint_capabilities(
                    &state,
                    &guard_ctx.client_id,
                ),
            };

            Ok(warp::reply::with_status(warp::reply::json(&response), status_code).into_response())
        }
        Err(error) => {
            tracing::error!("error upserting character identities: {:#?}", error);

            let response = ContributeCharacterIdentityResponse {
                status: "error",
                requested: total,
                accepted: 0,
                updated: 0,
                invalid: 0,
                failed: total,
                protected_endpoints: issue_protected_endpoint_capabilities(
                    &state,
                    &guard_ctx.client_id,
                ),
            };

            Ok(warp::reply::with_status(
                warp::reply::json(&response),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
            .into_response())
        }
    }
}

/// 파티 상세 정보 (멤버 ContentId 목록)
#[derive(Debug, serde::Deserialize)]
pub struct UploadablePartyDetail {
    pub listing_id: u32,
    pub leader_content_id: u64,
    pub leader_name: String,
    pub home_world: u16,
    pub member_content_ids: Vec<u64>,
    #[serde(default)]
    pub member_jobs: Option<Vec<u8>>,
    #[serde(default)]
    pub slot_flags: Option<Vec<String>>,
}

#[derive(Debug, serde::Serialize)]
struct ContributeDetailResponse {
    status: &'static str,
    matched_count: u64,
    modified_count: u64,
}

#[derive(Debug, serde::Serialize, Clone)]
struct ProtectedEndpointCapability {
    token: String,
    expires_at: i64,
}

#[derive(Debug, serde::Serialize, Clone)]
struct ProtectedEndpointCapabilities {
    fflogs_jobs: ProtectedEndpointCapability,
    fflogs_results: ProtectedEndpointCapability,
    fflogs_leases_abandon: ProtectedEndpointCapability,
}

#[derive(Debug, serde::Serialize, Clone)]
struct ListingDetailCapability {
    listing_id: u32,
    token: String,
    expires_at: i64,
}

#[derive(Debug, serde::Serialize)]
struct ContributeMultipleResponse {
    status: &'static str,
    requested: usize,
    accepted: usize,
    updated: usize,
    failed: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    detail_capabilities: Vec<ListingDetailCapability>,
    #[serde(skip_serializing_if = "Option::is_none")]
    protected_endpoints: Option<ProtectedEndpointCapabilities>,
}

fn build_detail_update_doc(detail: &UploadablePartyDetail) -> Document {
    let member_ids_i64 = detail
        .member_content_ids
        .iter()
        .map(|id| *id as i64)
        .collect::<Vec<_>>();

    let mut set_doc = doc! {
        "listing.member_content_ids": member_ids_i64,
        "listing.leader_content_id": detail.leader_content_id as i64,
    };

    if let Some(member_jobs) = detail.member_jobs.as_ref() {
        let member_jobs_i32 = member_jobs
            .iter()
            .map(|job| i32::from(*job))
            .collect::<Vec<_>>();
        set_doc.insert("listing.member_jobs", member_jobs_i32);
    }

    if let Some(slot_flags) = detail.slot_flags.as_ref() {
        set_doc.insert("listing.detail_slot_flags", slot_flags.clone());
    }

    set_doc
}

fn issue_capability(
    state: &State,
    client_id: &str,
    scope: CapabilityScope,
    resource_id: Option<String>,
    expires_at: i64,
) -> Option<ProtectedEndpointCapability> {
    let claims = CapabilityClaims::new(
        client_id.to_string(),
        scope,
        resource_id,
        Utc::now().timestamp(),
        expires_at,
    );

    match ingest_guard::issue_capability_token(&state.ingest_security.capability_secret, &claims) {
        Ok(token) => Some(ProtectedEndpointCapability { token, expires_at }),
        Err(error) => {
            tracing::warn!(
                ?scope,
                ?error,
                "failed to issue protected endpoint capability"
            );
            None
        }
    }
}

fn issue_protected_endpoint_capabilities(
    state: &State,
    client_id: &str,
) -> Option<ProtectedEndpointCapabilities> {
    let expires_at = Utc::now().timestamp() + state.ingest_security.capability_session_ttl_seconds;
    Some(ProtectedEndpointCapabilities {
        fflogs_jobs: issue_capability(
            state,
            client_id,
            CapabilityScope::FflogsJobs,
            None,
            expires_at,
        )?,
        fflogs_results: issue_capability(
            state,
            client_id,
            CapabilityScope::FflogsResults,
            None,
            expires_at,
        )?,
        fflogs_leases_abandon: issue_capability(
            state,
            client_id,
            CapabilityScope::FflogsLeasesAbandon,
            None,
            expires_at,
        )?,
    })
}

fn issue_detail_capabilities(
    state: &State,
    client_id: &str,
    listing_ids: &[u32],
) -> Vec<ListingDetailCapability> {
    let expires_at = Utc::now().timestamp() + state.ingest_security.capability_detail_ttl_seconds;
    let mut deduped = HashSet::new();
    let mut issued = Vec::with_capacity(listing_ids.len());

    for listing_id in listing_ids {
        if !deduped.insert(*listing_id) {
            continue;
        }

        let Some(capability) = issue_capability(
            state,
            client_id,
            CapabilityScope::Detail,
            Some(format!("listing:{listing_id}")),
            expires_at,
        ) else {
            continue;
        };

        issued.push(ListingDetailCapability {
            listing_id: *listing_id,
            token: capability.token,
            expires_at: capability.expires_at,
        });
    }

    issued
}

fn detail_payload_is_too_large(
    detail: &UploadablePartyDetail,
    limit: usize,
) -> Option<&'static str> {
    if detail.member_content_ids.len() > limit {
        return Some("too many members in request");
    }

    if let Some(member_jobs) = detail.member_jobs.as_ref() {
        if member_jobs.len() > limit {
            return Some("too many member jobs in request");
        }
    }

    if let Some(slot_flags) = detail.slot_flags.as_ref() {
        if slot_flags.len() > limit {
            return Some("too many slot flags in request");
        }
    }

    None
}

async fn upsert_detail_leader_if_valid(state: &State, detail: &UploadablePartyDetail) {
    if detail.leader_content_id != 0 && !detail.leader_name.is_empty() && detail.home_world < 1000 {
        let leader = crate::player::UploadablePlayer {
            content_id: detail.leader_content_id,
            name: detail.leader_name.clone(),
            home_world: detail.home_world,
            current_world: 0,
            account_id: 0,
        };

        let result = upsert_players(
            state.players_collection(),
            &[leader],
            state.player_upsert_concurrency,
        )
        .await;

        match result {
            Ok(report) => {
                if report.updated == 1 && report.failed == 0 && report.invalid == 0 {
                    tracing::debug!(
                        leader_content_id = detail.leader_content_id,
                        "Upserted detail leader into players collection"
                    );
                } else {
                    tracing::debug!(
                        leader_content_id = detail.leader_content_id,
                        requested = report.requested,
                        accepted = report.accepted,
                        updated = report.updated,
                        invalid = report.invalid,
                        failed = report.failed,
                        "Detail leader upsert finished with issues"
                    );
                }
            }
            Err(error) => {
                tracing::error!(
                    leader_content_id = detail.leader_content_id,
                    error = ?error,
                    "Failed to upsert detail leader"
                );
            }
        }

        return;
    }

    tracing::debug!(
        "Skipping leader upsert: ID={} Name='{}' World={}",
        detail.leader_content_id,
        detail.leader_name,
        detail.home_world,
    );
}

pub async fn contribute_detail_handler(
    state: Arc<State>,
    headers: HeaderMap,
    remote_addr: Option<SocketAddr>,
    detail: UploadablePartyDetail,
) -> std::result::Result<warp::reply::Response, Infallible> {
    let guard_ctx = match ingest_guard::authorize_request(
        &state,
        IngestEndpoint::ContributeDetail,
        &headers,
        remote_addr,
        "POST",
        "/contribute/detail",
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(error) => return Ok(ingest_guard::guard_error_reply(error)),
    };

    let resource_id = format!("listing:{}", detail.listing_id);
    if let Err(error) = ingest_guard::require_capability(
        &state,
        &headers,
        &guard_ctx.client_id,
        CapabilityScope::Detail,
        Some(&resource_id),
    ) {
        return Ok(ingest_guard::guard_error_reply(error));
    }

    if let Some(message) = detail_payload_is_too_large(&detail, state.max_detail_member_count) {
        return Ok(
            warp::reply::with_status(message, StatusCode::PAYLOAD_TOO_LARGE).into_response(),
        );
    }

    let non_zero_member_count = detail
        .member_content_ids
        .iter()
        .filter(|id| **id != 0)
        .count();
    let member_jobs_len = detail.member_jobs.as_ref().map_or(0, |jobs| jobs.len());
    let non_zero_job_count = detail
        .member_jobs
        .as_ref()
        .map_or(0, |jobs| jobs.iter().filter(|job| **job != 0).count());
    let slot_flags_len = detail.slot_flags.as_ref().map_or(0, |flags| flags.len());
    tracing::debug!(
        listing_id = detail.listing_id,
        member_count = detail.member_content_ids.len(),
        non_zero_member_count,
        member_jobs_len,
        non_zero_job_count,
        slot_flags_len,
        "received party detail payload"
    );

    upsert_detail_leader_if_valid(&state, &detail).await;

    let set_doc = build_detail_update_doc(&detail);
    let update_result = state
        .collection()
        .update_one(
            doc! { "listing.id": detail.listing_id },
            doc! { "$set": set_doc },
            None,
        )
        .await;

    match update_result {
        Ok(result) => {
            tracing::debug!(
                listing_id = detail.listing_id,
                matched_count = result.matched_count,
                modified_count = result.modified_count,
                "updated listing members",
            );

            let status = if result.matched_count == 0 {
                "missing"
            } else {
                "ok"
            };
            if result.modified_count > 0 {
                state.notify_listings_changed(result.modified_count as usize);
            }
            Ok(warp::reply::json(&ContributeDetailResponse {
                status,
                matched_count: result.matched_count,
                modified_count: result.modified_count,
            })
            .into_response())
        }
        Err(error) => {
            tracing::warn!(
                listing_id = detail.listing_id,
                error = ?error,
                "failed to update listing members"
            );
            Ok(warp::reply::with_status(
                "failed to update listing detail",
                StatusCode::INTERNAL_SERVER_ERROR,
            )
            .into_response())
        }
    }
}

#[cfg(test)]
mod tests;
