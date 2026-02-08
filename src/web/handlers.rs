use std::{cmp::Ordering, collections::HashMap, convert::Infallible, sync::Arc};
use warp::Reply;
use mongodb::bson::doc;

use crate::listing::PartyFinderListing;

use crate::mongo::{get_current_listings, insert_listing, upsert_players, get_players_by_content_ids, get_parse_docs, ParseCacheDoc};
use crate::player::UploadablePlayer;
use crate::{
    ffxiv::Language,
    template::listings::ListingsTemplate,
    template::stats::StatsTemplate,
};
use super::State;

/// Parse percentile 조회 헬퍼 함수
/// 
/// Returns: (p1_percentile, p1_color_class, p2_percentile, p2_color_class)
fn lookup_parse_percentiles(
    parse_docs: &HashMap<u64, ParseCacheDoc>,
    content_id: u64,
    zone_key: &str,
    encounter_id: u32,
    secondary_encounter_id: Option<u32>,
) -> (Option<u8>, String, Option<u8>, String) {
    let mut p1_percentile = None;
    let mut p1_class = "parse-none".to_string();
    let mut p2_percentile = None;
    let mut p2_class = "parse-none".to_string();
    
    if let Some(doc) = parse_docs.get(&content_id) {
        if let Some(zone_cache) = doc.zones.get(zone_key) {
            // Primary (P1)
            if let Some(enc_parse) = zone_cache.encounters.get(&encounter_id.to_string()) {
                if enc_parse.percentile >= 0.0 {
                    p1_percentile = Some(enc_parse.percentile as u8);
                    p1_class = crate::fflogs::mapping::percentile_color_class(enc_parse.percentile).to_string();
                }
            }
            
            // Secondary (P2)
            if let Some(sec_id) = secondary_encounter_id {
                if let Some(enc_parse) = zone_cache.encounters.get(&sec_id.to_string()) {
                    if enc_parse.percentile >= 0.0 {
                        p2_percentile = Some(enc_parse.percentile as u8);
                        p2_class = crate::fflogs::mapping::percentile_color_class(enc_parse.percentile).to_string();
                    }
                }
            }
        }
    }
    
    (p1_percentile, p1_class, p2_percentile, p2_class)
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
                
                let (zone_id, encounter_id, secondary_encounter_id) = if let Some(info) = fflogs_info {
                    (info.zone_id, info.encounter_id, info.secondary_encounter_id)
                } else {
                    (0, 0, None)
                };

                let jobs = &container.listing.jobs_present;
                let content_ids = &container.listing.member_content_ids;
                
                let zone_key = zone_id.to_string();

                let members: Vec<crate::template::listings::RenderableMember> = content_ids.iter()
                    .enumerate()
                    .filter(|(_, id)| **id != 0) // 빈 슬롯 제외
                    .filter_map(|(i, id)| {
                        let uid = *id as u64;
                        let job_id = jobs.get(i).copied().unwrap_or(0);
                        let player = players.get(&uid).cloned().unwrap_or(crate::player::Player {
                            content_id: uid,
                            name: "Unknown Member".to_string(),
                            home_world: 0,
                            last_seen: chrono::Utc::now(),
                            seen_count: 0,
                        });
                        
                        // 잡 정보가 없는 멤버는 표시하지 않음 (Ghost Member 방지)
                        // 리스팅 정보(jobs)와 세부 정보(content_ids) 간의 불일치 시, 리스팅 정보를 신뢰함
                        if job_id == 0 {
                            return None;
                        }

                        // Parse Data (P1 & P2) - 헬퍼 함수 사용
                        let (p1_percentile, p1_class, p2_percentile, p2_class) = if zone_id > 0 {
                            lookup_parse_percentiles(&all_parse_docs, uid, &zone_key, encounter_id, secondary_encounter_id)
                        } else {
                            (None, "parse-none".to_string(), None, "parse-none".to_string())
                        };

                        Some(crate::template::listings::RenderableMember { 
                            job_id, 
                            player,
                            parse: crate::template::listings::ParseDisplay::new(
                                p1_percentile, p1_class,
                                p2_percentile, p2_class,
                                secondary_encounter_id.is_some(),
                            ),
                        })
                    })
                    .collect();
                
                // 파티장 로그 계산 (leader_content_id 사용) - 헬퍼 함수 사용
                let leader_content_id = container.listing.leader_content_id;
                let (leader_p1_percentile, leader_p1_class, leader_p2_percentile, leader_p2_class) = 
                    if zone_id > 0 && leader_content_id != 0 {
                        lookup_parse_percentiles(&all_parse_docs, leader_content_id, &zone_key, encounter_id, secondary_encounter_id)
                    } else {
                        (None, "parse-none".to_string(), None, "parse-none".to_string())
                    };

                renderable_containers.push(crate::template::listings::RenderableListing {
                    container,
                    members,
                    leader_parse: crate::template::listings::ParseDisplay::new(
                        leader_p1_percentile, leader_p1_class,
                        leader_p2_percentile, leader_p2_class,
                        secondary_encounter_id.is_some(),
                    ),
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

    for listing in &listings {
        if listing.seconds_remaining > 60 * 60 {
            continue;
        }

        let result = insert_listing(state.collection(), listing).await;
        if result.is_ok() {
            successful += 1;
        } else {
            tracing::warn!("Failed to insert listing: {:#?}", result);
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
    let result = upsert_players(state.players_collection(), &players).await;

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
        };
        let upsert_res = upsert_players(state.players_collection(), &[leader]).await;
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
#[derive(Debug, serde::Serialize)]
pub struct ParseJob {
    pub content_id: u64,
    pub name: String,
    pub server: String,
    pub region: String,
    pub zone_id: u32,
    pub difficulty_id: i32,
    pub partition: i32,
}

/// FFLogs 파싱 결과 구조체 (Plugin -> Server request)
#[derive(Debug, serde::Deserialize)]
pub struct ParseResult {
    pub content_id: u64,
    pub zone_id: u32,
    pub encounters: HashMap<i32, f64>,
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
    let limit = 20; // 한 번에 할당할 작업 수

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
            fflogs_info.zone_id
        ).await {
            Ok(map) => map,
            Err(_) => continue,
        };

        // 작업이 필요한 멤버 식별
        let mut needing_fetch = Vec::new();
        for id in member_ids {
            let needed = match cached_zones.get(&id) {
                Some(cache) => crate::mongo::is_zone_cache_expired(cache),
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

        for player in players {
            if jobs.len() >= limit {
                break;
            }
            let server = player.home_world_name().to_string();
            let region = crate::fflogs::get_region_from_server(&server);
            
            jobs.push(ParseJob {
                content_id: player.content_id as u64,
                name: player.name,
                server,
                region: region.to_string(),
                zone_id: fflogs_info.zone_id,
                difficulty_id: fflogs_info.difficulty_id.unwrap_or(0) as i32,
                partition: crate::fflogs::mapping::FFLOGS_ZONES.get(&fflogs_info.zone_id).map(|z| z.partition).unwrap_or(0) as i32,
            });
        }
    }

    Ok(warp::reply::json(&jobs))
}

/// 플러그인으로부터 파싱 결과 수신
pub async fn contribute_fflogs_results_handler(
    state: Arc<State>,
    results: Vec<ParseResult>,
) -> std::result::Result<impl Reply, Infallible> {
    let mut success_count = 0;

    for res in results {
        // ParseResult -> ZoneCache 변환
        let mut encounter_map = HashMap::new();
        for (enc_id, percentile) in res.encounters {
            encounter_map.insert(
                enc_id.to_string(),
                crate::mongo::EncounterParse {
                    percentile: percentile as f32, // f64 -> f32
                    job_id: 0, 
                }
            );
        }

        let zone_cache = crate::mongo::ZoneCache {
            fetched_at: chrono::Utc::now(),
            encounters: encounter_map,
        };

        // DB Upsert
        if let Ok(_) = crate::mongo::upsert_zone_cache(
            state.parse_collection(),
            res.content_id,
            res.zone_id,
            &zone_cache
        ).await {
            success_count += 1;
        }
    }

    Ok(warp::reply::json(&format!("Updated {} records", success_count)))
}
