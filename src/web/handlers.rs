use std::{cmp::Ordering, collections::{HashMap, HashSet}, convert::Infallible, sync::Arc};
use warp::Reply;
use futures_util::StreamExt;
use mongodb::{bson::doc, options::UpdateOptions};
use chrono::{TimeDelta, Utc};

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
                    .then_with(|| a.time_left.partial_cmp(&b.time_left).unwrap_or(Ordering::Equal))
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
                let content_ids = &container.listing.member_content_ids;
                
                let zone_key = crate::fflogs::make_zone_cache_key(zone_id, difficulty_id, partition);
                let legacy_zone_key = zone_id.to_string();

                let members: Vec<crate::template::listings::RenderableMember> = content_ids.iter()
                    .enumerate()
                    .filter(|(_, id)| **id != 0) // 빈 슬롯 제외
                    .filter_map(|(i, id)| {
                        let uid = *id as u64;
                        let job_id = jobs.get(i).copied().unwrap_or(0);
                        let is_leader_member = uid == container.listing.leader_content_id || i == 0;

                        let player = players.get(&uid).cloned().unwrap_or_else(|| {
                            if is_leader_member {
                                let leader_name = container.listing.name.full_text(&lang);
                                let leader_name = if leader_name.trim().is_empty() {
                                    "Party Leader".to_string()
                                } else {
                                    leader_name
                                };

                                crate::player::Player {
                                    content_id: uid,
                                    name: leader_name,
                                    home_world: container.listing.home_world,
                                    current_world: container.listing.current_world,
                                    last_seen: chrono::Utc::now(),
                                    seen_count: 0,
                                    account_id: "-1".to_string(),
                                }
                            } else {
                                crate::player::Player {
                                    content_id: uid,
                                    name: "Unknown Member".to_string(),
                                    home_world: 0,
                                    current_world: 0,
                                    last_seen: chrono::Utc::now(),
                                    seen_count: 0,
                                    account_id: "-1".to_string(),
                                }
                            }
                        });
                        
                        // 잡 정보가 없는 멤버는 표시하지 않음 (Ghost Member 방지)
                        // 리스팅 정보(jobs)와 세부 정보(content_ids) 간의 불일치 시, 리스팅 정보를 신뢰함
                        if job_id == 0 {
                            return None;
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
                        })
                    })
                    .collect();
                
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
    listing: PartyFinderListing,
) -> std::result::Result<impl Reply, Infallible> {
    if listing.seconds_remaining > 60 * 60 {
        return Ok("invalid listing".to_string());
    }

    let result = insert_listing(state.collection(), &listing).await;

    // publish listings to websockets
    let _ = state.listings_channel.send(vec![listing].into()); 
    Ok(format!("{:#?}", result))
}

pub async fn contribute_multiple_handler(
    state: Arc<State>,
    listings: Vec<PartyFinderListing>,
) -> std::result::Result<impl Reply, Infallible> {
    let total = listings.len();
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

        let bson_value = match mongodb::bson::to_bson(listing) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("Failed to serialize listing {}: {:#?}", listing.id, e);
                continue;
            }
        };

        let filter = doc! {
            "listing.id": listing.id,
            "listing.last_server_restart": listing.last_server_restart,
            "listing.created_world": listing.created_world as u32,
        };
        let update = doc! {
            "$currentDate": {
                "updated_at": true,
            },
            "$set": {
                "listing": bson_value,
            },
            "$setOnInsert": {
                "created_at": Utc::now(),
            },
        };
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
    Ok(format!("{}/{} updated", successful, total))
}

pub async fn contribute_players_handler(
    state: Arc<State>,
    players: Vec<UploadablePlayer>,
) -> std::result::Result<impl Reply, Infallible> {
    let total = players.len();
    let result = upsert_players(
        state.players_collection(),
        &players,
        state.player_upsert_concurrency,
    )
    .await;

    match result {
        Ok(successful) => Ok(format!("{}/{} players updated", successful, total)),
        Err(e) => {
            tracing::error!("error upserting players: {:#?}", e);
            Ok(format!("0/{} players updated (error)", total))
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
}

pub async fn contribute_detail_handler(
    state: Arc<State>,
    detail: UploadablePartyDetail,
) -> std::result::Result<impl Reply, Infallible> {
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

    let update_result = state.collection()
        .update_one(
            doc! { "listing.id": detail.listing_id },
            doc! {
                "$set": {
                    "listing.member_content_ids": member_ids_i64,
                    "listing.leader_content_id": detail.leader_content_id as i64,
                }
            },
            None,
        )
        .await;

    tracing::debug!("Updated listing {} members: {:?}", detail.listing_id, update_result);

    Ok(warp::reply::json(&"ok"))
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
}

/// 플러그인에게 파싱 작업 할당
pub async fn contribute_fflogs_jobs_handler(
    state: Arc<State>,
) -> std::result::Result<impl Reply, Infallible> {
    // 1. 현재 활성 파티 목록 가져오기 (1시간 이내)
    let listings = match get_current_listings(state.collection()).await {
        Ok(l) => l,
        Err(_) => return Ok(warp::reply::json(&Vec::<ParseJob>::new())),
    };

    let mut jobs = Vec::new();
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
            let needed = match cached_zones.get(&id) {
                Some(cache) => crate::mongo::is_zone_cache_expired_with_hidden_ttl_hours(
                    cache,
                    state.fflogs_hidden_cache_ttl_hours,
                ),
                None => true,
            };
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
            });
        }
    }

    // lease selected jobs to avoid duplicate dispatch across concurrent workers
    let Some(lease_ttl) = TimeDelta::try_minutes(3) else {
        return Ok(warp::reply::json(&Vec::<ParseJob>::new()));
    };
    let now = Utc::now();

    {
        let mut leases = state.fflogs_job_leases.write().await;

        // cleanup expired leases (worker crash / timeout safety)
        leases.retain(|_, leased_at| (*leased_at + lease_ttl) > now);

        let mut seen: HashSet<super::FflogsLeaseKey> = HashSet::new();
        jobs.retain(|job| {
            let key = super::FflogsLeaseKey {
                content_id: job.content_id,
                zone_id: job.zone_id,
                difficulty_id: job.difficulty_id,
                partition: job.partition,
            };

            // de-dup within this response payload
            if !seen.insert(key.clone()) {
                return false;
            }

            // skip if another worker already leased this key
            if leases.contains_key(&key) {
                return false;
            }

            leases.insert(key, now);
            true
        });
    }

    Ok(warp::reply::json(&jobs))
}

/// 플러그인으로부터 파싱 결과 수신
pub async fn contribute_fflogs_results_handler(
    state: Arc<State>,
    results: Vec<ParseResult>,
) -> std::result::Result<impl Reply, Infallible> {
    let mut success_count = 0;

    let lease_keys: Vec<super::FflogsLeaseKey> = results
        .iter()
        .map(|r| super::FflogsLeaseKey {
            content_id: r.content_id,
            zone_id: r.zone_id,
            difficulty_id: r.difficulty_id,
            partition: r.partition,
        })
        .collect();

    for res in results {
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
        for key in lease_keys {
            leases.remove(&key);
        }
    }

    Ok(warp::reply::json(&format!("Updated {} records", success_count)))
}
