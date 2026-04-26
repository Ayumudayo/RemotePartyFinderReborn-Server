use chrono::Utc;
use futures_util::StreamExt;
use mongodb::{
    bson::{doc, Document},
    options::UpdateOptions,
};
use std::{
    cmp::Ordering as CmpOrdering,
    collections::{HashMap, HashSet},
    convert::Infallible,
    future::Future,
    net::SocketAddr,
    sync::{atomic::Ordering as AtomicOrdering, Arc},
    time::Duration,
};
use warp::{
    http::{HeaderMap, StatusCode},
    Reply,
};

use crate::listing::PartyFinderListing;

use super::ingest_guard::{self, CapabilityClaims, CapabilityScope, IngestEndpoint};
use super::State;
use crate::mongo::{
    get_current_listings, get_parse_docs, get_players_by_content_ids, insert_listing,
    upsert_character_identities, upsert_players, ParseCacheDoc,
};
use crate::player::{UploadableCharacterIdentity, UploadablePlayer};
use crate::sestring_ext::SeStringExt;
use crate::{
    ffxiv::Language, template::listings::ListingsTemplate, template::stats::StatsTemplate,
};

const LISTINGS_PLAYER_LOOKUP_TIMEOUT: Duration = Duration::from_millis(1_500);
const LISTINGS_PARSE_DOC_LOOKUP_TIMEOUT: Duration = Duration::from_millis(1_500);
const LISTINGS_REPORT_PARSE_LOOKUP_TIMEOUT: Duration = Duration::from_millis(1_500);

async fn bounded_listing_lookup<T, Fut>(lookup: &'static str, timeout: Duration, future: Fut) -> T
where
    T: Default,
    Fut: Future<Output = anyhow::Result<T>>,
{
    match tokio::time::timeout(timeout, future).await {
        Ok(Ok(value)) => value,
        Ok(Err(error)) => {
            tracing::warn!(lookup, error = ?error, "listing enrichment lookup failed");
            T::default()
        }
        Err(_) => {
            tracing::warn!(
                lookup,
                timeout_ms = timeout.as_millis(),
                "listing enrichment lookup timed out"
            );
            T::default()
        }
    }
}

fn resolve_member_player(
    players: &HashMap<u64, crate::player::Player>,
    uid: u64,
    is_leader_member: bool,
    leader_name: &str,
    leader_home_world: u16,
    leader_current_world: u16,
) -> (crate::player::Player, bool) {
    if let Some(player) = players.get(&uid) {
        return (player.clone(), false);
    }

    if is_leader_member {
        let leader_name = if leader_name.trim().is_empty() {
            "Party Leader".to_string()
        } else {
            leader_name.to_string()
        };

        return (
            crate::player::Player {
                content_id: uid,
                name: leader_name,
                home_world: leader_home_world,
                current_world: leader_current_world,
                last_seen: chrono::Utc::now(),
                seen_count: 0,
                account_id: "-1".to_string(),
            },
            true,
        );
    }

    (
        crate::player::Player {
            content_id: uid,
            name: "Unknown Member".to_string(),
            home_world: 0,
            current_world: 0,
            last_seen: chrono::Utc::now(),
            seen_count: 0,
            account_id: "-1".to_string(),
        },
        false,
    )
}

fn alliance_party_label(party_index: u8) -> &'static str {
    match party_index {
        0 => "Alliance A",
        1 => "Alliance B",
        2 => "Alliance C",
        3 => "Alliance D",
        4 => "Alliance E",
        5 => "Alliance F",
        _ => "Alliance ?",
    }
}

/// FFLogs 표시 데이터 조회 헬퍼
///
/// - Parse percentile (Best)
/// - Progress (boss remaining HP %)
/// - Hidden 처리
fn lookup_fflogs_displays(
    parse_docs: &HashMap<u64, ParseCacheDoc>,
    report_parse_summaries: Option<
        &HashMap<crate::mongo::ReportParseIdentityKey, crate::mongo::ReportParseZoneSummary>,
    >,
    content_id: u64,
    fallback_name: Option<&str>,
    fallback_home_world: Option<u16>,
    zone_key: &str,
    legacy_zone_key: Option<&str>,
    encounter_id: u32,
    secondary_encounter_id: Option<u32>,
) -> (
    crate::template::listings::ParseDisplay,
    crate::template::listings::ProgressDisplay,
) {
    let plugin_zone_cache = parse_docs.get(&content_id).and_then(|doc| {
        doc.zones
            .get(zone_key)
            .or_else(|| legacy_zone_key.and_then(|key| doc.zones.get(key)))
    });
    let fallback_summary = match (report_parse_summaries, fallback_name, fallback_home_world) {
        (Some(summaries), Some(name), Some(home_world)) if home_world != 0 => {
            summaries.get(&crate::mongo::ReportParseIdentityKey::new(name, home_world))
        }
        _ => None,
    };
    let resolved = crate::parse_resolver::resolve_parse_data(
        plugin_zone_cache,
        fallback_summary.map(|summary| &summary.encounters),
        fallback_summary.map(|summary| summary.updated_at),
        encounter_id,
        secondary_encounter_id,
    );

    (
        crate::template::listings::ParseDisplay::new(
            resolved.primary_percentile,
            resolved.primary_color_class,
            resolved.secondary_percentile,
            resolved.secondary_color_class,
            resolved.has_secondary,
            resolved.hidden,
            resolved.originally_hidden,
            resolved.estimated,
            resolved.source,
        ),
        crate::template::listings::ProgressDisplay::new(
            resolved.primary_boss_percentage,
            resolved.secondary_boss_percentage,
            resolved.primary_percentile,
            resolved.secondary_percentile,
            resolved.primary_clear_count,
            resolved.secondary_clear_count,
            resolved.has_secondary,
            resolved.hidden,
        ),
    )
}

pub(crate) async fn build_listings_template(
    state: Arc<State>,
    codes: Option<String>,
) -> ListingsTemplate {
    let lang = Language::from_codes(codes.as_deref());

    let res = get_current_listings(state.collection()).await;
    match res {
        Ok(mut containers) => {
            // 단일 정렬로 통합: updated_minute DESC → pf_category DESC → time_left ASC
            containers.sort_by(|a, b| {
                b.updated_minute
                    .cmp(&a.updated_minute)
                    .then_with(|| b.listing.pf_category().cmp(&a.listing.pf_category()))
                    .then_with(|| {
                        a.time_left
                            .partial_cmp(&b.time_left)
                            .unwrap_or(CmpOrdering::Equal)
                    })
            });

            // Collect all member IDs + leader IDs
            let mut all_content_ids: Vec<u64> = containers
                .iter()
                .flat_map(|l| {
                    let member_ids = l.listing.member_content_ids.iter().map(|&id| id as u64);
                    let leader_id = std::iter::once(l.listing.leader_content_id);
                    member_ids.chain(leader_id)
                })
                .filter(|&id| id != 0)
                .collect();
            all_content_ids.sort_unstable();
            all_content_ids.dedup();

            // Fetch players; if enrichment is slow, keep the listings visible with fallbacks.
            let players_list: Vec<crate::player::Player> = bounded_listing_lookup(
                "players",
                LISTINGS_PLAYER_LOOKUP_TIMEOUT,
                get_players_by_content_ids(state.players_collection(), &all_content_ids),
            )
            .await;
            let players: HashMap<u64, crate::player::Player> = players_list
                .into_iter()
                .map(|p| (p.content_id, p))
                .collect();

            // Optimisation: Pre-fetch all parse docs for all visible players.
            let all_parse_docs: HashMap<u64, ParseCacheDoc> = bounded_listing_lookup(
                "parse docs",
                LISTINGS_PARSE_DOC_LOOKUP_TIMEOUT,
                get_parse_docs(state.parse_collection(), &all_content_ids),
            )
            .await;
            let mut report_parse_requests: HashMap<
                String,
                Vec<crate::mongo::ReportParseIdentityKey>,
            > = HashMap::new();

            for container in &containers {
                let duty_id = container.listing.duty as u16;
                let Some(info) = (if container.listing.high_end() {
                    crate::fflogs::mapping::get_fflogs_encounter(duty_id)
                } else {
                    None
                }) else {
                    continue;
                };

                let zone_key = crate::fflogs::make_zone_cache_key(
                    info.zone_id,
                    info.difficulty_id.unwrap_or(0) as i32,
                    crate::fflogs::mapping::FFLOGS_ZONES
                        .get(&info.zone_id)
                        .map(|zone| zone.partition)
                        .unwrap_or(0) as i32,
                );
                let leader_name_text = container.listing.name.full_text(&lang);

                report_parse_requests
                    .entry(zone_key.clone())
                    .or_default()
                    .push(crate::mongo::ReportParseIdentityKey::new(
                        &leader_name_text,
                        container.listing.home_world,
                    ));

                for (i, member_id) in container.listing.member_content_ids.iter().enumerate() {
                    let uid = *member_id as u64;
                    if uid == 0 {
                        continue;
                    }

                    let is_leader_member = uid == container.listing.leader_content_id || i == 0;
                    let (player, _) = resolve_member_player(
                        &players,
                        uid,
                        is_leader_member,
                        &leader_name_text,
                        container.listing.home_world,
                        container.listing.current_world,
                    );

                    if player.home_world != 0 {
                        report_parse_requests
                            .entry(zone_key.clone())
                            .or_default()
                            .push(crate::mongo::ReportParseIdentityKey::new(
                                &player.name,
                                player.home_world,
                            ));
                    }
                }
            }

            let report_parse_summary_collection = state.report_parse_summary_collection();
            let all_report_parse_summaries: HashMap<
                String,
                HashMap<crate::mongo::ReportParseIdentityKey, crate::mongo::ReportParseZoneSummary>,
            > = bounded_listing_lookup(
                "report parse summaries",
                LISTINGS_REPORT_PARSE_LOOKUP_TIMEOUT,
                async move {
                    let mut all_report_parse_summaries = HashMap::new();
                    for (zone_key, identities) in report_parse_requests {
                        let summaries = crate::mongo::get_report_parse_summaries_by_zone(
                            report_parse_summary_collection.clone(),
                            &zone_key,
                            &identities,
                        )
                        .await
                        .unwrap_or_default();
                        all_report_parse_summaries.insert(zone_key, summaries);
                    }
                    Ok(all_report_parse_summaries)
                },
            )
            .await;

            // Match players to listings with job info
            let mut renderable_containers = Vec::new();
            let mut leader_fallback_total = 0usize;
            let mut leader_fallback_listing_count = 0usize;

            for container in containers {
                // Determine FFLogs Zone ID/Encounter ID
                let duty_id = container.listing.duty as u16;
                let high_end = container.listing.high_end();
                let fflogs_info = if high_end {
                    crate::fflogs::mapping::get_fflogs_encounter(duty_id)
                } else {
                    None
                };

                let (zone_id, difficulty_id, partition, encounter_id, secondary_encounter_id) =
                    if let Some(info) = fflogs_info {
                        (
                            info.zone_id,
                            info.difficulty_id.unwrap_or(0) as i32,
                            crate::fflogs::mapping::FFLOGS_ZONES
                                .get(&info.zone_id)
                                .map(|z| z.partition)
                                .unwrap_or(0) as i32,
                            info.encounter_id,
                            info.secondary_encounter_id,
                        )
                    } else {
                        (0, 0, 0, 0, None)
                    };

                let jobs = &container.listing.jobs_present;
                let detail_jobs = &container.listing.member_jobs;
                let content_ids = &container.listing.member_content_ids;
                let leader_name_text = container.listing.name.full_text(&lang);
                let alliance_like = container.listing.num_parties >= 3
                    || content_ids.len() > 8
                    || detail_jobs.len() > 8;

                let zone_key =
                    crate::fflogs::make_zone_cache_key(zone_id, difficulty_id, partition);
                let legacy_zone_key = zone_id.to_string();

                let mut listing_leader_fallback_hits = 0usize;
                let mut emitted_party_headers: HashSet<u8> = HashSet::new();
                let members: Vec<crate::template::listings::RenderableMember> = content_ids
                    .iter()
                    .enumerate()
                    .filter(|(_, id)| **id != 0) // 빈 슬롯 제외
                    .filter_map(|(i, id)| {
                        let uid = *id as u64;
                        let party_index = (i / 8) as u8;
                        let job_id = detail_jobs
                            .get(i)
                            .copied()
                            .or_else(|| jobs.get(i).copied())
                            .unwrap_or(0);
                        let is_leader_member = uid == container.listing.leader_content_id || i == 0;

                        let (player, used_leader_fallback) = resolve_member_player(
                            &players,
                            uid,
                            is_leader_member,
                            &leader_name_text,
                            container.listing.home_world,
                            container.listing.current_world,
                        );
                        if used_leader_fallback {
                            listing_leader_fallback_hits += 1;
                        }

                        let (parse, progress) = if zone_id > 0 {
                            lookup_fflogs_displays(
                                &all_parse_docs,
                                all_report_parse_summaries.get(&zone_key),
                                uid,
                                Some(&player.name),
                                Some(player.home_world),
                                &zone_key,
                                Some(&legacy_zone_key),
                                encounter_id,
                                secondary_encounter_id,
                            )
                        } else {
                            (
                                crate::template::listings::ParseDisplay::new(
                                    None,
                                    "parse-none".to_string(),
                                    None,
                                    "parse-none".to_string(),
                                    false,
                                    false,
                                    false,
                                    false,
                                    crate::parse_resolver::ParseSource::None,
                                ),
                                crate::template::listings::ProgressDisplay::new(
                                    None, None, None, None, None, None, false, false,
                                ),
                            )
                        };

                        Some(crate::template::listings::RenderableMember {
                            job_id,
                            player,
                            parse,
                            progress,
                            slot_index: i,
                            party_index,
                            party_header: if alliance_like
                                && emitted_party_headers.insert(party_index)
                            {
                                Some(alliance_party_label(party_index))
                            } else {
                                None
                            },
                            identity_fallback: used_leader_fallback,
                        })
                    })
                    .collect();

                if listing_leader_fallback_hits > 0 {
                    leader_fallback_total += listing_leader_fallback_hits;
                    leader_fallback_listing_count += 1;

                    tracing::debug!(
                        listing_id = container.listing.id,
                        leader_content_id = container.listing.leader_content_id,
                        fallback_hits = listing_leader_fallback_hits,
                        "members: applied leader metadata fallback",
                    );
                }

                // 파티장 로그 계산 (leader_content_id 사용) - 헬퍼 함수 사용
                let leader_content_id = container.listing.leader_content_id;
                let (leader_parse, _) = if zone_id > 0 && leader_content_id != 0 {
                    lookup_fflogs_displays(
                        &all_parse_docs,
                        all_report_parse_summaries.get(&zone_key),
                        leader_content_id,
                        Some(&leader_name_text),
                        Some(container.listing.home_world),
                        &zone_key,
                        Some(&legacy_zone_key),
                        encounter_id,
                        secondary_encounter_id,
                    )
                } else {
                    (
                        crate::template::listings::ParseDisplay::new(
                            None,
                            "parse-none".to_string(),
                            None,
                            "parse-none".to_string(),
                            false,
                            false,
                            false,
                            false,
                            crate::parse_resolver::ParseSource::None,
                        ),
                        crate::template::listings::ProgressDisplay::new(
                            None, None, None, None, None, None, false, false,
                        ),
                    )
                };

                renderable_containers.push(crate::template::listings::RenderableListing {
                    container,
                    members,
                    leader_parse,
                });
            }

            if leader_fallback_total > 0 {
                let batch_fallback_hits = leader_fallback_total as u64;
                let total_fallback_hits = state
                    .fflogs_leader_fallback_total
                    .fetch_add(batch_fallback_hits, AtomicOrdering::Relaxed)
                    + batch_fallback_hits;

                tracing::info!(
                    listing_count = leader_fallback_listing_count,
                    fallback_hits = leader_fallback_total,
                    cumulative_fallback_hits = total_fallback_hits,
                    "members: leader metadata fallback used in listings response",
                );
            }

            ListingsTemplate::new(renderable_containers, lang)
        }
        Err(e) => {
            tracing::error!("Failed to get listings: {:#?}", e);
            ListingsTemplate::new(Default::default(), lang)
        }
    }
}

pub async fn listings_handler(
    _state: Arc<State>,
    codes: Option<String>,
) -> std::result::Result<impl Reply, Infallible> {
    Ok(ListingsTemplate::new(
        Vec::new(),
        Language::from_codes(codes.as_deref()),
    ))
}

pub async fn stats_handler(
    state: Arc<State>,
    codes: Option<String>,
    seven_days: bool,
) -> std::result::Result<impl Reply, Infallible> {
    let lang = Language::from_codes(codes.as_deref());
    let stats = state.stats.read().await.clone();
    Ok(match stats {
        Some(stats) => StatsTemplate {
            stats: if seven_days {
                stats.seven_days
            } else {
                stats.all_time
            },
            lang,
        }
        .into_response(),
        None => "Stats haven't been calculated yet. Please wait :(".into_response(),
    })
}

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
mod tests {
    use super::{
        build_detail_update_doc, detail_payload_is_too_large, resolve_member_player,
        UploadablePartyDetail,
    };
    use chrono::Utc;
    use std::{collections::HashMap, sync::Arc};
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
            listing_snapshot_worker_lease_collection_name: "listing_snapshot_worker_leases"
                .to_string(),
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
            listing_snapshot_worker_lease_collection_name: "listing_snapshot_worker_leases"
                .to_string(),
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

    #[tokio::test]
    async fn bounded_listing_lookup_returns_default_on_timeout() {
        let result: Vec<i32> = super::bounded_listing_lookup(
            "test slow lookup",
            std::time::Duration::from_millis(5),
            async {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                Ok::<_, anyhow::Error>(vec![1, 2, 3])
            },
        )
        .await;

        assert!(result.is_empty());
    }

    #[test]
    fn resolve_member_player_uses_existing_player_when_present() {
        let uid = 101u64;
        let mut players = HashMap::new();
        players.insert(
            uid,
            crate::player::Player {
                content_id: uid,
                name: "Known Player".to_string(),
                home_world: 73,
                current_world: 73,
                last_seen: Utc::now(),
                seen_count: 5,
                account_id: "123".to_string(),
            },
        );

        let (player, used_fallback) = resolve_member_player(&players, uid, true, "Leader", 74, 75);

        assert!(!used_fallback);
        assert_eq!(player.name, "Known Player");
        assert_eq!(player.home_world, 73);
    }

    #[test]
    fn resolve_member_player_falls_back_to_leader_metadata_when_missing() {
        let players: HashMap<u64, crate::player::Player> = HashMap::new();

        let (player, used_fallback) =
            resolve_member_player(&players, 202, true, "Leader Name", 79, 80);

        assert!(used_fallback);
        assert_eq!(player.name, "Leader Name");
        assert_eq!(player.home_world, 79);
        assert_eq!(player.current_world, 80);
    }

    #[test]
    fn resolve_member_player_keeps_unknown_for_non_leader_missing_player() {
        let players: HashMap<u64, crate::player::Player> = HashMap::new();

        let (player, used_fallback) =
            resolve_member_player(&players, 303, false, "Leader Name", 79, 80);

        assert!(!used_fallback);
        assert_eq!(player.name, "Unknown Member");
        assert_eq!(player.home_world, 0);
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
    fn identity_upsert_updates_player_lookup_used_by_listing_render() {
        let observed_at = chrono::DateTime::parse_from_rfc3339("2026-04-12T12:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let now = observed_at + chrono::TimeDelta::minutes(1);
        let identity = crate::player::UploadableCharacterIdentity {
            content_id: 5150,
            name: "Resolved Member".to_string(),
            home_world: 74,
            world_name: "Tonberry".to_string(),
            source: "chara_card".to_string(),
            observed_at,
        };

        let merged = crate::player::merge_identity_into_player(None, None, &identity, now);
        let mut players = HashMap::new();
        players.insert(merged.player.content_id, merged.player.clone());

        let (player, used_fallback) =
            resolve_member_player(&players, identity.content_id, false, "Leader", 79, 79);

        assert!(merged.applied_incoming_identity);
        assert!(!used_fallback);
        assert_eq!(player.name, "Resolved Member");
        assert_eq!(player.home_world, 74);
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
}
