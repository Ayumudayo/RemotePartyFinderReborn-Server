use std::{
    cmp::Ordering as CmpOrdering,
    collections::{HashMap, HashSet},
    future::Future,
    sync::{atomic::Ordering as AtomicOrdering, Arc},
    time::Duration,
};

use crate::mongo::{
    get_current_listings, get_parse_docs, get_players_by_content_ids, ParseCacheDoc,
};
use crate::sestring_ext::SeStringExt;
use crate::{ffxiv::Language, template::listings::ListingsTemplate, web::State};

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

#[cfg(test)]
mod tests;
