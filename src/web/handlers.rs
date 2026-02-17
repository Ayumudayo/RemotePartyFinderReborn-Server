use std::{
    cmp::Ordering as CmpOrdering,
    collections::{HashMap, HashSet},
    convert::Infallible,
    sync::{
        Arc,
        atomic::Ordering as AtomicOrdering,
    },
};
use warp::{
    Reply,
    http::{HeaderMap, StatusCode},
};
use futures_util::StreamExt;
use mongodb::{bson::doc, options::UpdateOptions};
use chrono::{TimeDelta, Utc};
use uuid::Uuid;

use crate::listing::PartyFinderListing;

use crate::mongo::{get_current_listings, insert_listing, upsert_players, get_players_by_content_ids, get_parse_docs, ParseCacheDoc};
use crate::player::UploadablePlayer;
use crate::sestring_ext::SeStringExt;
use crate::{
    ffxiv::Language,
    template::listings::ListingsTemplate,
    template::stats::StatsTemplate,
};
use super::State;
use super::ingest_guard::{self, IngestEndpoint};

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

fn build_candidate_servers(anchor_world_id: u16) -> Vec<ParseJobCandidateServer> {
    let mut world_ids: Vec<u32> = crate::ffxiv::WORLDS.keys().copied().collect();
    world_ids.sort_unstable();

    let (anchor_dc, anchor_region) = match crate::ffxiv::WORLDS.get(&(anchor_world_id as u32)) {
        Some(w) => (Some(w.data_center()), crate::fflogs::get_region_from_server(w.as_str())),
        None => (None, "NA"),
    };

    let mut out = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();

    if let Some(dc) = anchor_dc {
        for wid in &world_ids {
            let Some(w) = crate::ffxiv::WORLDS.get(wid) else { continue };
            if w.data_center() != dc {
                continue;
            }
            let server = w.as_str().to_string();
            if !seen.insert(server.clone()) {
                continue;
            }
            out.push(ParseJobCandidateServer {
                region: crate::fflogs::get_region_from_server(&server).to_string(),
                server,
            });
        }
    }

    for wid in &world_ids {
        let Some(w) = crate::ffxiv::WORLDS.get(wid) else { continue };
        let server = w.as_str().to_string();
        let region = crate::fflogs::get_region_from_server(&server);
        if region != anchor_region {
            continue;
        }
        if !seen.insert(server.clone()) {
            continue;
        }
        out.push(ParseJobCandidateServer {
            region: region.to_string(),
            server,
        });
    }

    out
}

/// FFLogs 표시 데이터 조회 헬퍼
///
/// - Parse percentile (Best)
/// - Progress (boss remaining HP %)
/// - Hidden 처리
fn lookup_fflogs_displays(
    parse_docs: &HashMap<u64, ParseCacheDoc>,
    content_id: u64,
    zone_key: &str,
    legacy_zone_key: Option<&str>,
    encounter_id: u32,
    secondary_encounter_id: Option<u32>,
) -> (
    crate::template::listings::ParseDisplay,
    crate::template::listings::ProgressDisplay,
) {
    let has_secondary = secondary_encounter_id.is_some();
    let mut hidden = false;
    let mut estimated = false;

    // Parse
    let mut p1_percentile = None;
    let mut p1_class = "parse-none".to_string();
    let mut p2_percentile = None;
    let mut p2_class = "parse-none".to_string();

    // Progress (boss remaining HP %)
    let mut p1_boss = None;
    let mut p2_boss = None;

    if let Some(doc) = parse_docs.get(&content_id) {
        let zone_cache = doc
            .zones
            .get(zone_key)
            .or_else(|| legacy_zone_key.and_then(|k| doc.zones.get(k)));

        if let Some(zone_cache) = zone_cache {
            if zone_cache.estimated {
                estimated = true;
            }
            if zone_cache.hidden {
                hidden = true;
            } else {
                // Primary (P1)
                if let Some(enc_parse) = zone_cache.encounters.get(&encounter_id.to_string()) {
                    if enc_parse.percentile >= 0.0 {
                        p1_percentile = Some(enc_parse.percentile as u8);
                        p1_class = crate::fflogs::mapping::percentile_color_class(enc_parse.percentile).to_string();
                    }
                    if let Some(bp) = enc_parse.boss_percentage {
                        p1_boss = Some(bp.round().clamp(0.0, 100.0) as u8);
                    }
                }

                // Secondary (P2)
                if let Some(sec_id) = secondary_encounter_id {
                    if let Some(enc_parse) = zone_cache.encounters.get(&sec_id.to_string()) {
                        if enc_parse.percentile >= 0.0 {
                            p2_percentile = Some(enc_parse.percentile as u8);
                            p2_class = crate::fflogs::mapping::percentile_color_class(enc_parse.percentile).to_string();
                        }
                        if let Some(bp) = enc_parse.boss_percentage {
                            p2_boss = Some(bp.round().clamp(0.0, 100.0) as u8);
                        }
                    }
                }
            }
        }
    }

    (
        crate::template::listings::ParseDisplay::new(
            p1_percentile,
            p1_class,
            p2_percentile,
            p2_class,
            has_secondary,
            hidden,
            estimated,
        ),
        crate::template::listings::ProgressDisplay::new(
            p1_boss,
            p2_boss,
            has_secondary,
            hidden,
        ),
    )
}

pub async fn listings_handler(
    state: Arc<State>,
    codes: Option<String>,
) -> std::result::Result<impl Reply, Infallible> {
    let lang = Language::from_codes(codes.as_deref());

    let res = get_current_listings(state.collection()).await;
    Ok(match res {
        Ok(mut containers) => {
            // 단일 정렬로 통합: updated_minute DESC → pf_category DESC → time_left ASC
            containers.sort_by(|a, b| {
                b.updated_minute.cmp(&a.updated_minute)
                    .then_with(|| b.listing.pf_category().cmp(&a.listing.pf_category()))
                    .then_with(|| a.time_left.partial_cmp(&b.time_left).unwrap_or(CmpOrdering::Equal))
            });

            // Collect all member IDs + leader IDs
            let mut all_content_ids: Vec<u64> = containers.iter()
                .flat_map(|l| {
                    let member_ids = l.listing.member_content_ids.iter().map(|&id| id as u64);
                    let leader_id = std::iter::once(l.listing.leader_content_id);
                    member_ids.chain(leader_id)
                })
                .filter(|&id| id != 0)
                .collect();
            all_content_ids.sort_unstable();
            all_content_ids.dedup();
            
            // Fetch players
            let players_list = get_players_by_content_ids(state.players_collection(), &all_content_ids).await.unwrap_or_default();
            let players: HashMap<u64, crate::player::Player> = players_list.into_iter().map(|p| (p.content_id, p)).collect();

            // Optimisation: Pre-fetch all parse docs for all visible players
            let all_parse_docs = get_parse_docs(state.parse_collection(), &all_content_ids).await.unwrap_or_default();

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
                
                let (zone_id, difficulty_id, partition, encounter_id, secondary_encounter_id) = if let Some(info) = fflogs_info {
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
                 
                let zone_key = crate::fflogs::make_zone_cache_key(zone_id, difficulty_id, partition);
                let legacy_zone_key = zone_id.to_string();

                let mut listing_leader_fallback_hits = 0usize;
                let mut emitted_party_headers: HashSet<u8> = HashSet::new();
                let members: Vec<crate::template::listings::RenderableMember> = content_ids.iter()
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
                                uid,
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
                                 ),
                                 crate::template::listings::ProgressDisplay::new(None, None, false, false),
                             )
                         };

                        Some(crate::template::listings::RenderableMember {
                            job_id,
                            player,
                            parse,
                            progress,
                            slot_index: i,
                            party_index,
                            party_header: if alliance_like && emitted_party_headers.insert(party_index) {
                                Some(alliance_party_label(party_index))
                            } else {
                                None
                            },
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
                        leader_content_id,
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
                         ),
                         crate::template::listings::ProgressDisplay::new(None, None, false, false),
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

            ListingsTemplate { containers: renderable_containers, lang }
        }
        Err(e) => {
            tracing::error!("Failed to get listings: {:#?}", e);
            ListingsTemplate {
                containers: Default::default(),
                lang,
            }
        }
    })
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
        }.into_response(),
        None => "Stats haven't been calculated yet. Please wait :(".into_response(),
    })
}

pub async fn contribute_handler(
    state: Arc<State>,
    headers: HeaderMap,
    listing: PartyFinderListing,
) -> std::result::Result<warp::reply::Response, Infallible> {
    if let Err(error) = ingest_guard::authorize_request(
        &state,
        IngestEndpoint::Contribute,
        &headers,
        "POST",
        "/contribute",
    )
    .await
    {
        return Ok(ingest_guard::guard_error_reply(error));
    }

    if listing.seconds_remaining > 60 * 60 {
        return Ok(warp::reply::with_status("invalid listing", StatusCode::BAD_REQUEST).into_response());
    }

    let result = insert_listing(state.collection(), &listing).await;

    // publish listings to websockets
    let _ = state.listings_channel.send(vec![listing].into()); 
    Ok(format!("{:#?}", result).into_response())
}

pub async fn contribute_multiple_handler(
    state: Arc<State>,
    headers: HeaderMap,
    listings: Vec<PartyFinderListing>,
) -> std::result::Result<warp::reply::Response, Infallible> {
    if let Err(error) = ingest_guard::authorize_request(
        &state,
        IngestEndpoint::ContributeMultiple,
        &headers,
        "POST",
        "/contribute/multiple",
    )
    .await
    {
        return Ok(ingest_guard::guard_error_reply(error));
    }

    let total = listings.len();
    if total > state.max_multiple_batch_size {
        return Ok(warp::reply::with_status("too many listings in request", StatusCode::PAYLOAD_TOO_LARGE).into_response());
    }

    let mut successful = 0;

    let mut write_ops = Vec::new();
    for listing in &listings {
        if listing.seconds_remaining > 60 * 60 {
            continue;
        }

        if listing.created_world >= 1_000
            || listing.home_world >= 1_000
            || listing.current_world >= 1_000
        {
            continue;
        }

        let mut listing_doc = match mongodb::bson::to_document(listing) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("Failed to serialize listing {}: {:#?}", listing.id, e);
                continue;
            }
        };

        // Preserve detail-derived fields populated by /contribute/detail.
        listing_doc.remove("member_content_ids");
        listing_doc.remove("member_jobs");
        listing_doc.remove("leader_content_id");

        let filter = doc! {
            "listing.id": listing.id,
            "listing.last_server_restart": listing.last_server_restart,
            "listing.created_world": listing.created_world as u32,
        };
        let update = vec![
            doc! {
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
            },
        ];
        write_ops.push((listing.id, filter, update));
    }

    let mut writes = futures_util::stream::iter(
        write_ops
            .into_iter()
            .map(|(listing_id, filter, update)| {
                let collection = state.collection();
                async move {
                    collection
                        .update_one(filter, update, UpdateOptions::builder().upsert(true).build())
                        .await
                        .map_err(|e| (listing_id, e))
                }
            }),
    )
    .buffer_unordered(state.listing_upsert_concurrency);

    while let Some(result) = writes.next().await {
        match result {
            Ok(_) => successful += 1,
            Err((listing_id, e)) => {
                tracing::warn!("Failed to insert listing {}: {:#?}", listing_id, e);
            }
        }
    }

    let _ = state.listings_channel.send(listings.into());
    Ok(format!("{}/{} updated", successful, total).into_response())
}

pub async fn contribute_players_handler(
    state: Arc<State>,
    headers: HeaderMap,
    players: Vec<UploadablePlayer>,
) -> std::result::Result<warp::reply::Response, Infallible> {
    if let Err(error) = ingest_guard::authorize_request(
        &state,
        IngestEndpoint::ContributePlayers,
        &headers,
        "POST",
        "/contribute/players",
    )
    .await
    {
        return Ok(ingest_guard::guard_error_reply(error));
    }

    let total = players.len();
    if total > state.max_players_batch_size {
        return Ok(warp::reply::with_status("too many players in request", StatusCode::PAYLOAD_TOO_LARGE).into_response());
    }

    let result = upsert_players(
        state.players_collection(),
        &players,
        state.player_upsert_concurrency,
    )
    .await;

    match result {
        Ok(successful) => Ok(format!("{}/{} players updated", successful, total).into_response()),
        Err(e) => {
            tracing::error!("error upserting players: {:#?}", e);
            Ok(format!("0/{} players updated (error)", total).into_response())
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
}

#[derive(Debug, serde::Serialize)]
struct ContributeDetailResponse {
    status: &'static str,
    matched_count: u64,
    modified_count: u64,
}

pub async fn contribute_detail_handler(
    state: Arc<State>,
    headers: HeaderMap,
    detail: UploadablePartyDetail,
) -> std::result::Result<warp::reply::Response, Infallible> {
    if let Err(error) = ingest_guard::authorize_request(
        &state,
        IngestEndpoint::ContributeDetail,
        &headers,
        "POST",
        "/contribute/detail",
    )
    .await
    {
        return Ok(ingest_guard::guard_error_reply(error));
    }

    if detail.member_content_ids.len() > state.max_detail_member_count {
        return Ok(warp::reply::with_status("too many members in request", StatusCode::PAYLOAD_TOO_LARGE).into_response());
    }

    if let Some(member_jobs) = detail.member_jobs.as_ref() {
        if member_jobs.len() > state.max_detail_member_count {
            return Ok(warp::reply::with_status("too many member jobs in request", StatusCode::PAYLOAD_TOO_LARGE).into_response());
        }
    }

    let non_zero_member_count = detail.member_content_ids.iter().filter(|&&id| id != 0).count();
    let member_jobs_len = detail.member_jobs.as_ref().map_or(0, |jobs| jobs.len());
    let non_zero_job_count = detail
        .member_jobs
        .as_ref()
        .map_or(0, |jobs| jobs.iter().filter(|&&job| job != 0).count());
    tracing::debug!(
        listing_id = detail.listing_id,
        member_count = detail.member_content_ids.len(),
        non_zero_member_count,
        member_jobs_len,
        non_zero_job_count,
        "received party detail payload"
    );

    // 리더 정보를 플레이어로 저장
    if detail.leader_content_id != 0 && !detail.leader_name.is_empty() && detail.home_world < 1000 {
        let leader = crate::player::UploadablePlayer {
            content_id: detail.leader_content_id,
            name: detail.leader_name.clone(),
            home_world: detail.home_world,
            current_world: 0,
            account_id: 0, // UploadablePlayer는 u64 유지
        };
        let upsert_res = upsert_players(
            state.players_collection(),
            &[leader],
            state.player_upsert_concurrency,
        )
        .await;
        tracing::debug!("Upserted leader {}: {:?}", detail.leader_content_id, upsert_res);
    } else {
        tracing::debug!("Skipping leader upsert: ID={} Name='{}' World={}", detail.leader_content_id, detail.leader_name, detail.home_world);
    }

    // listing에 member_content_ids 및 leader_content_id 저장
    let member_ids_i64: Vec<i64> = detail.member_content_ids.iter().map(|&id| id as i64).collect();
    let mut set_doc = doc! {
        "listing.member_content_ids": member_ids_i64,
        "listing.leader_content_id": detail.leader_content_id as i64,
    };

    if let Some(member_jobs) = detail.member_jobs.as_ref() {
        let member_jobs_i32: Vec<i32> = member_jobs.iter().map(|&job| i32::from(job)).collect();
        set_doc.insert("listing.member_jobs", member_jobs_i32);
    }

    let update_result = state.collection()
        .update_one(
            doc! { "listing.id": detail.listing_id },
            doc! {
                "$set": set_doc
            },
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

            let status = if result.matched_count == 0 { "missing" } else { "ok" };
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

/// FFLogs 작업 요청 구조체 (Plugin -> Server response)
#[derive(Debug, serde::Serialize, Clone)]
pub struct ParseJobCandidateServer {
    pub server: String,
    pub region: String,
}

#[derive(Debug, serde::Serialize)]
pub struct ParseJob {
    pub content_id: u64,
    pub name: String,
    pub server: String,
    pub region: String,
    /// home_world를 모를 때 후보 서버(월드) 목록. 있으면 플러그인이 추정 매칭을 시도한다.
    #[serde(default)]
    pub candidate_servers: Vec<ParseJobCandidateServer>,
    pub zone_id: u32,
    pub difficulty_id: i32,
    pub partition: i32,
    pub encounter_id: u32,
    #[serde(default)]
    pub secondary_encounter_id: Option<u32>,
    #[serde(default)]
    pub lease_token: String,
}

/// FFLogs 파싱 결과 구조체 (Plugin -> Server request)
#[derive(Debug, serde::Deserialize)]
pub struct ParseResult {
    pub content_id: u64,
    pub zone_id: u32,
    #[serde(default)]
    pub difficulty_id: i32,
    #[serde(default)]
    pub partition: i32,
    pub encounters: HashMap<i32, f64>,
    #[serde(default)]
    pub boss_percentages: HashMap<i32, f64>,
    #[serde(default)]
    pub is_hidden: bool,
    /// 후보 서버(동명이인) 탐색을 통해 추정 매칭된 결과인지 여부
    #[serde(default)]
    pub is_estimated: bool,
    /// 매칭에 사용된 서버(월드) slug
    #[serde(default)]
    pub matched_server: Option<String>,
    /// FFLogs job lease token (jobs 응답과 묶이는 단회성 토큰)
    #[serde(default)]
    pub lease_token: String,
}

/// 플러그인에게 파싱 작업 할당
pub async fn contribute_fflogs_jobs_handler(
    state: Arc<State>,
    headers: HeaderMap,
) -> std::result::Result<warp::reply::Response, Infallible> {
    let guard_ctx = match ingest_guard::authorize_request(
        &state,
        IngestEndpoint::ContributeFflogsJobs,
        &headers,
        "GET",
        "/contribute/fflogs/jobs",
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(error) => return Ok(ingest_guard::guard_error_reply(error)),
    };

    // 1. 현재 활성 파티 목록 가져오기 (1시간 이내)
    let listings = match get_current_listings(state.collection()).await {
        Ok(l) => l,
        Err(_) => return Ok(warp::reply::json(&Vec::<ParseJob>::new()).into_response()),
    };

    let mut jobs = Vec::new();
    let mut hidden_refresh_candidates_in_batch = 0u64;
    let limit = state.fflogs_jobs_limit; // 한 번에 할당할 작업 수

    // Zone별 플레이어 수집, background.rs 로직 재사용
    // 여기서는 간단하게 순회하며 필요한 작업 찾으면 바로 반환 (Greedy)
    
    // Shuffle listings to distribute load? (Optional, maybe later)

    for container in listings {
        if jobs.len() >= limit {
            break;
        }

        if !container.listing.high_end() {
            continue;
        }

        let duty_id = container.listing.duty as u16;
        let fflogs_info = match crate::fflogs::mapping::get_fflogs_encounter(duty_id) {
            Some(info) => info,
            None => continue,
        };

        let difficulty_id = fflogs_info.difficulty_id.unwrap_or(0) as i32;
        let partition = crate::fflogs::mapping::FFLOGS_ZONES
            .get(&fflogs_info.zone_id)
            .map(|z| z.partition)
            .unwrap_or(0) as i32;
        let zone_key = crate::fflogs::make_zone_cache_key(fflogs_info.zone_id, difficulty_id, partition);

        let member_ids: Vec<u64> = container.listing.member_content_ids.iter()
            .map(|&id| id as u64)
            .filter(|&id| id != 0)
            .collect();

        if member_ids.is_empty() {
             continue;
        }

        // 캐시 확인 (Batch)
        let cached_zones = match crate::mongo::get_zone_caches(
            state.parse_collection(),
            &member_ids,
            &zone_key,
        ).await {
            Ok(map) => map,
            Err(_) => continue,
        };

        // 작업이 필요한 멤버 식별
        let mut needing_fetch = Vec::new();
        for id in member_ids {
            let (needed, hidden_refresh_due) = match cached_zones.get(&id) {
                Some(cache) => {
                    let needed = crate::mongo::is_zone_cache_expired_with_hidden_ttl_hours(
                        cache,
                        state.fflogs_hidden_cache_ttl_hours,
                    );
                    (needed, cache.hidden && needed)
                }
                None => (true, false),
            };
            if hidden_refresh_due {
                hidden_refresh_candidates_in_batch += 1;
            }
            if needed {
                needing_fetch.push(id);
            }
        }

        if needing_fetch.is_empty() {
            continue;
        }

        // 플레이어 정보 조회 (이름/서버)
        let players = match get_players_by_content_ids(state.players_collection(), &needing_fetch).await {
            Ok(p) => p,
            Err(_) => continue,
        };

        // 홈월드가 없는 플레이어를 위한 후보 서버 목록(리스트 단위)
        let listing_anchor_world = [
            container.listing.current_world,
            container.listing.created_world,
            container.listing.home_world,
        ]
        .into_iter()
        .find(|wid| *wid != 0 && crate::ffxiv::WORLDS.get(&(*wid as u32)).is_some())
        .unwrap_or(0);

        let listing_candidates = if listing_anchor_world != 0 {
            build_candidate_servers(listing_anchor_world)
        } else {
            Vec::new()
        };

        for player in players {
            if jobs.len() >= limit {
                break;
            }

            let home_world_known = player.home_world != 0
                && crate::ffxiv::WORLDS.get(&(player.home_world as u32)).is_some();

            let (server, region, candidate_servers) = if home_world_known {
                let server = player.home_world_name().to_string();
                let region = crate::fflogs::get_region_from_server(&server).to_string();
                (server, region, Vec::new())
            } else {
                // 홈월드를 모르면: 현재 월드/리스트(데이터센터)를 앵커로 후보 서버를 구성하고, 플러그인이 추정 매칭을 수행
                let anchor_world = if player.current_world != 0
                    && crate::ffxiv::WORLDS.get(&(player.current_world as u32)).is_some()
                {
                    player.current_world
                } else {
                    listing_anchor_world
                };

                let candidates = if anchor_world != 0 {
                    build_candidate_servers(anchor_world)
                } else {
                    listing_candidates.clone()
                };

                if candidates.is_empty() {
                    continue;
                }

                let server = candidates[0].server.clone();
                let region = candidates[0].region.clone();
                (server, region, candidates)
            };
            
            jobs.push(ParseJob {
                content_id: player.content_id as u64,
                name: player.name,
                server,
                region,
                candidate_servers,
                zone_id: fflogs_info.zone_id,
                difficulty_id,
                partition,
                encounter_id: fflogs_info.encounter_id,
                secondary_encounter_id: fflogs_info.secondary_encounter_id,
                lease_token: String::new(),
            });
        }
    }

    // lease selected jobs to avoid duplicate dispatch across concurrent workers
    let Some(lease_ttl) = TimeDelta::try_minutes(3) else {
        return Ok(warp::reply::json(&Vec::<ParseJob>::new()).into_response());
    };
    let now = Utc::now();

    {
        let mut leases = state.fflogs_job_leases.write().await;

        // cleanup expired leases (worker crash / timeout safety)
        leases.retain(|_, lease| (lease.leased_at + lease_ttl) > now);

        let mut seen: HashSet<super::FflogsLeaseKey> = HashSet::new();
        let mut leased_jobs = Vec::with_capacity(jobs.len());

        for mut job in jobs {
            let key = super::FflogsLeaseKey {
                content_id: job.content_id,
                zone_id: job.zone_id,
                difficulty_id: job.difficulty_id,
                partition: job.partition,
            };

            // de-dup within this response payload
            if !seen.insert(key.clone()) {
                continue;
            }

            // skip if another worker already leased this key
            if leases.contains_key(&key) {
                continue;
            }

            let lease_token = Uuid::new_v4().to_string();
            leases.insert(key, super::FflogsLeaseEntry {
                leased_at: now,
                client_id: guard_ctx.client_id.clone(),
                lease_token: lease_token.clone(),
            });

            job.lease_token = lease_token;
            leased_jobs.push(job);
        }

        jobs = leased_jobs;
    }

    let dispatched_in_batch = jobs.len() as u64;
    if dispatched_in_batch > 0 {
        let dispatched_total = state
            .fflogs_jobs_dispatched_total
            .fetch_add(dispatched_in_batch, AtomicOrdering::Relaxed)
            + dispatched_in_batch;

        tracing::debug!(
            batch_jobs = dispatched_in_batch,
            total_jobs_dispatched = dispatched_total,
            "fflogs jobs dispatched",
        );
    }

    if hidden_refresh_candidates_in_batch > 0 {
        let hidden_refresh_total = state
            .fflogs_hidden_refresh_total
            .fetch_add(hidden_refresh_candidates_in_batch, AtomicOrdering::Relaxed)
            + hidden_refresh_candidates_in_batch;

        tracing::debug!(
            hidden_refresh_candidates_in_batch,
            hidden_refresh_total,
            "fflogs hidden cache refresh candidates detected",
        );
    }

    Ok(warp::reply::json(&jobs).into_response())
}

/// 플러그인으로부터 파싱 결과 수신
pub async fn contribute_fflogs_results_handler(
    state: Arc<State>,
    headers: HeaderMap,
    results: Vec<ParseResult>,
) -> std::result::Result<warp::reply::Response, Infallible> {
    let guard_ctx = match ingest_guard::authorize_request(
        &state,
        IngestEndpoint::ContributeFflogsResults,
        &headers,
        "POST",
        "/contribute/fflogs/results",
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(error) => return Ok(ingest_guard::guard_error_reply(error)),
    };

    let submitted_results_total = results.len();
    if submitted_results_total > state.max_fflogs_results_batch_size {
        return Ok(warp::reply::with_status("too many FFLogs results in request", StatusCode::PAYLOAD_TOO_LARGE).into_response());
    }

    let mut accepted_results = Vec::with_capacity(submitted_results_total);
    let mut lease_keys_to_release: Vec<super::FflogsLeaseKey> = Vec::new();

    {
        let leases = state.fflogs_job_leases.read().await;
        for res in results {
            let key = super::FflogsLeaseKey {
                content_id: res.content_id,
                zone_id: res.zone_id,
                difficulty_id: res.difficulty_id,
                partition: res.partition,
            };

            let Some(lease) = leases.get(&key) else {
                continue;
            };

            if lease.client_id != guard_ctx.client_id {
                continue;
            }

            if res.lease_token.is_empty() || lease.lease_token != res.lease_token {
                continue;
            }

            lease_keys_to_release.push(key);
            accepted_results.push(res);
        }
    }

    let accepted_results_total = accepted_results.len() as u64;
    let mut success_count = 0;

    for res in accepted_results {
        // ParseResult -> ZoneCache 변환
        let boss_percentages = res.boss_percentages;
        let mut encounter_map = HashMap::new();

        for (enc_id, percentile) in res.encounters {
            let boss_percentage = boss_percentages
                .get(&enc_id)
                .copied()
                .map(|v| v as f32);

            encounter_map.insert(
                enc_id.to_string(),
                crate::mongo::EncounterParse {
                    percentile: percentile as f32, // f64 -> f32
                    job_id: 0,
                    boss_percentage,
                },
            );
        }

        // progress-only 데이터가 들어온 경우도 저장
        for (enc_id, boss_percentage) in boss_percentages {
            let key = enc_id.to_string();
            if encounter_map.contains_key(&key) {
                continue;
            }
            encounter_map.insert(
                key,
                crate::mongo::EncounterParse {
                    percentile: -1.0,
                    job_id: 0,
                    boss_percentage: Some(boss_percentage as f32),
                },
            );
        }

        let zone_cache = crate::mongo::ZoneCache {
            fetched_at: chrono::Utc::now(),
            estimated: res.is_estimated,
            matched_server: res.matched_server.filter(|s| !s.trim().is_empty()),
            hidden: res.is_hidden,
            encounters: encounter_map,
        };

        let zone_key = crate::fflogs::make_zone_cache_key(res.zone_id, res.difficulty_id, res.partition);

        // DB Upsert
        if let Ok(_) = crate::mongo::upsert_zone_cache(
            state.parse_collection(),
            res.content_id,
            &zone_key,
            &zone_cache
        ).await {
            success_count += 1;
        }
    }

    // release leases for returned results (success/failure both) so next polling can retry quickly
    {
        let mut leases = state.fflogs_job_leases.write().await;
        for key in lease_keys_to_release {
            leases.remove(&key);
        }
    }

    if accepted_results_total > 0 {
        let results_total = state
            .fflogs_results_received_total
            .fetch_add(accepted_results_total, AtomicOrdering::Relaxed)
            + accepted_results_total;

        tracing::debug!(
            accepted_results_total,
            submitted_results_total,
            success_count,
            results_total,
            "fflogs results processed",
        );
    }

    Ok(warp::reply::json(&format!(
        "Updated {} records (accepted {}/{})",
        success_count,
        accepted_results_total,
        submitted_results_total
    ))
    .into_response())
}

#[cfg(test)]
mod tests {
    use super::resolve_member_player;
    use chrono::Utc;
    use std::collections::HashMap;

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
}
