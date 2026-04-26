use chrono::{TimeDelta, Utc};
use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    net::SocketAddr,
    sync::{atomic::Ordering as AtomicOrdering, Arc},
};
use uuid::Uuid;
use warp::{
    http::{HeaderMap, StatusCode},
    Reply,
};

use super::ingest_guard::{self, CapabilityScope, IngestEndpoint};
use super::{State, FFLOGS_LEASE_TTL_MINUTES};
use crate::mongo::{get_current_listings, get_players_by_content_ids};

const FFLOGS_JOB_MODE_HEADER: &str = "x-rpf-fflogs-job-mode";
const FFLOGS_EXACT_ONLY_JOB_MODE: &str = "exact-only";

fn fflogs_exact_home_world_only(headers: &HeaderMap) -> bool {
    headers
        .get(FFLOGS_JOB_MODE_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .map(|value| value.eq_ignore_ascii_case(FFLOGS_EXACT_ONLY_JOB_MODE))
        .unwrap_or(false)
}

fn build_candidate_servers(anchor_world_id: u16) -> Vec<ParseJobCandidateServer> {
    let mut world_ids: Vec<u32> = crate::ffxiv::WORLDS.keys().copied().collect();
    world_ids.sort_unstable();

    let (anchor_dc, anchor_region) = match crate::ffxiv::WORLDS.get(&(anchor_world_id as u32)) {
        Some(w) => (
            Some(w.data_center()),
            crate::fflogs::get_region_from_server(w.as_str()),
        ),
        None => (None, "NA"),
    };

    let mut out = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();

    if let Some(dc) = anchor_dc {
        for wid in &world_ids {
            let Some(w) = crate::ffxiv::WORLDS.get(wid) else {
                continue;
            };
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
        let Some(w) = crate::ffxiv::WORLDS.get(wid) else {
            continue;
        };
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
    pub clear_counts: HashMap<i32, i32>,
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

fn positive_clear_count(clear_counts: &HashMap<i32, i32>, enc_id: i32) -> Option<u32> {
    clear_counts
        .get(&enc_id)
        .copied()
        .filter(|value| *value > 0)
        .map(|value| value as u32)
}

fn percentile_has_clear_evidence(percentile: f64, clear_count: Option<u32>) -> bool {
    percentile > 0.0 || (percentile == 0.0 && clear_count.is_some())
}

fn build_fflogs_result_encounter_map(
    encounters: HashMap<i32, f64>,
    boss_percentages: HashMap<i32, f64>,
    clear_counts: HashMap<i32, i32>,
) -> HashMap<String, crate::mongo::EncounterParse> {
    let mut encounter_map = HashMap::new();

    for (enc_id, percentile) in encounters {
        let boss_percentage = boss_percentages.get(&enc_id).copied().map(|v| v as f32);
        let clear_count = positive_clear_count(&clear_counts, enc_id);

        if percentile_has_clear_evidence(percentile, clear_count) {
            encounter_map.insert(
                enc_id.to_string(),
                crate::mongo::EncounterParse {
                    percentile: percentile as f32,
                    job_id: 0,
                    boss_percentage,
                    clear_count,
                },
            );
        } else if let Some(boss_percentage) = boss_percentage {
            encounter_map.insert(
                enc_id.to_string(),
                crate::mongo::EncounterParse {
                    percentile: -1.0,
                    job_id: 0,
                    boss_percentage: Some(boss_percentage),
                    clear_count,
                },
            );
        }
    }

    // progress-only 데이터가 들어온 경우도 저장
    for (enc_id, boss_percentage) in boss_percentages {
        let key = enc_id.to_string();
        if encounter_map.contains_key(&key) {
            continue;
        }
        let clear_count = positive_clear_count(&clear_counts, enc_id);
        encounter_map.insert(
            key,
            crate::mongo::EncounterParse {
                percentile: -1.0,
                job_id: 0,
                boss_percentage: Some(boss_percentage as f32),
                clear_count,
            },
        );
    }

    encounter_map
}

/// FFLogs lease 즉시 반납 요청 구조체 (Plugin -> Server request)
#[derive(Debug, serde::Deserialize)]
pub struct AbandonFflogsLease {
    pub content_id: u64,
    pub zone_id: u32,
    #[serde(default)]
    pub difficulty_id: i32,
    #[serde(default)]
    pub partition: i32,
    #[serde(default)]
    pub lease_token: String,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, serde::Serialize)]
struct ContributeFflogsResultsResponse {
    status: &'static str,
    submitted: usize,
    accepted: usize,
    updated: usize,
    rejected: usize,
}

#[derive(Debug, serde::Serialize)]
struct ContributeFflogsLeaseAbandonResponse {
    status: &'static str,
    submitted: usize,
    released: usize,
    rejected: usize,
}

/// 플러그인에게 파싱 작업 할당
pub async fn contribute_fflogs_jobs_handler(
    state: Arc<State>,
    headers: HeaderMap,
    remote_addr: Option<SocketAddr>,
) -> std::result::Result<warp::reply::Response, Infallible> {
    let guard_ctx = match ingest_guard::authorize_request(
        &state,
        IngestEndpoint::ContributeFflogsJobs,
        &headers,
        remote_addr,
        "GET",
        "/contribute/fflogs/jobs",
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(error) => return Ok(ingest_guard::guard_error_reply(error)),
    };
    if let Err(error) = ingest_guard::require_capability(
        &state,
        &headers,
        &guard_ctx.client_id,
        CapabilityScope::FflogsJobs,
        None,
    ) {
        return Ok(ingest_guard::guard_error_reply(error));
    }

    let exact_home_world_only = fflogs_exact_home_world_only(&headers);

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
        let zone_key =
            crate::fflogs::make_zone_cache_key(fflogs_info.zone_id, difficulty_id, partition);

        let member_ids: Vec<u64> = container
            .listing
            .member_content_ids
            .iter()
            .map(|&id| id as u64)
            .filter(|&id| id != 0)
            .collect();

        if member_ids.is_empty() {
            continue;
        }

        // 캐시 확인 (Batch)
        let cached_zones =
            match crate::mongo::get_zone_caches(state.parse_collection(), &member_ids, &zone_key)
                .await
            {
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
        let players =
            match get_players_by_content_ids(state.players_collection(), &needing_fetch).await {
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
                && crate::ffxiv::WORLDS
                    .get(&(player.home_world as u32))
                    .is_some();

            if !home_world_known && exact_home_world_only {
                continue;
            }

            let (server, region, candidate_servers) = if home_world_known {
                let server = player.home_world_name().to_string();
                let region = crate::fflogs::get_region_from_server(&server).to_string();
                (server, region, Vec::new())
            } else {
                // 홈월드를 모르면: 현재 월드/리스트(데이터센터)를 앵커로 후보 서버를 구성하고, 플러그인이 추정 매칭을 수행
                let anchor_world = if player.current_world != 0
                    && crate::ffxiv::WORLDS
                        .get(&(player.current_world as u32))
                        .is_some()
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
    let Some(lease_ttl) = TimeDelta::try_minutes(FFLOGS_LEASE_TTL_MINUTES) else {
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
            leases.insert(
                key,
                super::FflogsLeaseEntry {
                    leased_at: now,
                    client_id: guard_ctx.client_id.clone(),
                    lease_token: lease_token.clone(),
                },
            );

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
    remote_addr: Option<SocketAddr>,
    results: Vec<ParseResult>,
) -> std::result::Result<warp::reply::Response, Infallible> {
    let guard_ctx = match ingest_guard::authorize_request(
        &state,
        IngestEndpoint::ContributeFflogsResults,
        &headers,
        remote_addr,
        "POST",
        "/contribute/fflogs/results",
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(error) => return Ok(ingest_guard::guard_error_reply(error)),
    };
    if let Err(error) = ingest_guard::require_capability(
        &state,
        &headers,
        &guard_ctx.client_id,
        CapabilityScope::FflogsResults,
        None,
    ) {
        return Ok(ingest_guard::guard_error_reply(error));
    }

    let submitted_results_total = results.len();
    if submitted_results_total > state.max_fflogs_results_batch_size {
        return Ok(warp::reply::with_status(
            "too many FFLogs results in request",
            StatusCode::PAYLOAD_TOO_LARGE,
        )
        .into_response());
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

    let accepted_results_total = accepted_results.len();
    let mut success_count = 0;

    for res in accepted_results {
        // ParseResult -> ZoneCache 변환
        let encounter_map = build_fflogs_result_encounter_map(
            res.encounters,
            res.boss_percentages,
            res.clear_counts,
        );

        let zone_cache = crate::mongo::ZoneCache {
            fetched_at: chrono::Utc::now(),
            estimated: res.is_estimated,
            matched_server: res.matched_server.filter(|s| !s.trim().is_empty()),
            hidden: res.is_hidden,
            encounters: encounter_map,
        };

        let zone_key =
            crate::fflogs::make_zone_cache_key(res.zone_id, res.difficulty_id, res.partition);

        // DB Upsert
        if let Ok(_) = crate::mongo::upsert_zone_cache(
            state.parse_collection(),
            res.content_id,
            &zone_key,
            &zone_cache,
        )
        .await
        {
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
            .fetch_add(accepted_results_total as u64, AtomicOrdering::Relaxed)
            + accepted_results_total as u64;

        tracing::debug!(
            accepted_results_total,
            submitted_results_total,
            success_count,
            results_total,
            "fflogs results processed",
        );
    }
    if success_count > 0 {
        state.notify_listings_changed(success_count);
    }

    let rejected_results_total = submitted_results_total.saturating_sub(accepted_results_total);
    if rejected_results_total > 0 {
        state
            .fflogs_results_rejected_total
            .fetch_add(rejected_results_total as u64, AtomicOrdering::Relaxed);
    }

    let status = if rejected_results_total == 0 {
        "ok"
    } else {
        "partial"
    };

    Ok(warp::reply::json(&ContributeFflogsResultsResponse {
        status,
        submitted: submitted_results_total,
        accepted: accepted_results_total,
        updated: success_count,
        rejected: rejected_results_total,
    })
    .into_response())
}

/// 플러그인으로부터 미처리 FFLogs lease 즉시 반납 수신
pub async fn contribute_fflogs_leases_abandon_handler(
    state: Arc<State>,
    headers: HeaderMap,
    remote_addr: Option<SocketAddr>,
    abandoned_leases: Vec<AbandonFflogsLease>,
) -> std::result::Result<warp::reply::Response, Infallible> {
    let guard_ctx = match ingest_guard::authorize_request(
        &state,
        IngestEndpoint::ContributeFflogsLeasesAbandon,
        &headers,
        remote_addr,
        "POST",
        "/contribute/fflogs/leases/abandon",
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(error) => return Ok(ingest_guard::guard_error_reply(error)),
    };
    if let Err(error) = ingest_guard::require_capability(
        &state,
        &headers,
        &guard_ctx.client_id,
        CapabilityScope::FflogsLeasesAbandon,
        None,
    ) {
        return Ok(ingest_guard::guard_error_reply(error));
    }

    let submitted_total = abandoned_leases.len();
    if submitted_total > state.max_fflogs_results_batch_size {
        return Ok(warp::reply::with_status(
            "too many FFLogs lease abandons in request",
            StatusCode::PAYLOAD_TOO_LARGE,
        )
        .into_response());
    }

    let mut released_total = 0usize;
    let mut rejected_total = 0usize;
    let mut seen = HashSet::new();
    let mut reasons = HashSet::new();

    {
        let mut leases = state.fflogs_job_leases.write().await;

        for abandoned in abandoned_leases {
            if let Some(reason) = abandoned.reason.as_deref() {
                let reason = reason.trim();
                if !reason.is_empty() {
                    reasons.insert(reason.to_string());
                }
            }

            let key = super::FflogsLeaseKey {
                content_id: abandoned.content_id,
                zone_id: abandoned.zone_id,
                difficulty_id: abandoned.difficulty_id,
                partition: abandoned.partition,
            };

            let dedup_key = format!(
                "{}:{}:{}:{}:{}",
                key.content_id,
                key.zone_id,
                key.difficulty_id,
                key.partition,
                abandoned.lease_token
            );

            if !seen.insert(dedup_key) {
                continue;
            }

            let valid = match leases.get(&key) {
                Some(lease) => {
                    !abandoned.lease_token.is_empty()
                        && lease.client_id == guard_ctx.client_id
                        && lease.lease_token == abandoned.lease_token
                }
                None => false,
            };

            if !valid {
                rejected_total += 1;
                continue;
            }

            if leases.remove(&key).is_some() {
                released_total += 1;
            } else {
                rejected_total += 1;
            }
        }
    }

    if released_total > 0 {
        state
            .fflogs_leases_abandoned_total
            .fetch_add(released_total as u64, AtomicOrdering::Relaxed);

        tracing::debug!(
            client_id = guard_ctx.client_id,
            submitted_total,
            released_total,
            rejected_total,
            reasons = ?reasons,
            "fflogs lease abandon processed"
        );
    }

    if rejected_total > 0 {
        state
            .fflogs_leases_abandon_rejected_total
            .fetch_add(rejected_total as u64, AtomicOrdering::Relaxed);
    }

    let status = if rejected_total == 0 { "ok" } else { "partial" };
    Ok(warp::reply::json(&ContributeFflogsLeaseAbandonResponse {
        status,
        submitted: submitted_total,
        released: released_total,
        rejected: rejected_total,
    })
    .into_response())
}

#[cfg(test)]
mod tests;
