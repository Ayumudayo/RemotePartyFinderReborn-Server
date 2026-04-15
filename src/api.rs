use crate::ffxiv;
use crate::ffxiv::duties::DutyInfo;
use crate::ffxiv::Language;
use crate::listing::{ConditionFlags, DutyFinderSettingsFlags, LootRuleFlags, ObjectiveFlags, PartyFinderListing, PartyFinderSlot, SearchAreaFlags};
use crate::listing_container::QueriedListing;
use crate::mongo::{get_current_listings, get_parse_docs, get_players_by_content_ids};
use crate::sestring_ext::SeStringExt;
use crate::web::State;
use crate::ws::WsApiClient;
use chrono::{DateTime, Utc};
use serde::Serialize;
use sestring::SeString;
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use warp::filters::BoxedFilter;
use warp::http::StatusCode;
use warp::{Filter, Reply};

pub fn api(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    warp::path("api")
        .and(ws(state.clone()).or(listings(state.clone())))
        .boxed()
}

fn resolve_api_member_identity(
    player_map: &HashMap<u64, crate::player::Player>,
    uid: u64,
    is_leader_member: bool,
    leader_name: &str,
    leader_home_world: u16,
) -> Option<(String, u16)> {
    if let Some(player) = player_map.get(&uid) {
        return Some((player.name.clone(), player.home_world));
    }

    if is_leader_member {
        let leader_name = if leader_name.trim().is_empty() {
            "Party Leader".to_string()
        } else {
            leader_name.to_string()
        };

        return Some((leader_name, leader_home_world));
    }

    None
}

fn listings(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    async fn logic(state: Arc<State>) -> Result<warp::reply::Response, Infallible> {
        let listings = get_current_listings(state.collection()).await;

        match listings {
            Ok(listings) => {
                // Collect all member IDs for player fetch
                let all_content_ids: Vec<u64> = listings.iter()
                    .flat_map(|l| l.listing.member_content_ids.iter().map(|&id| id as u64))
                    .collect();
                
                // Fetch players (Batch 1)
                let players = get_players_by_content_ids(state.players_collection(), &all_content_ids).await.unwrap_or_default();
                let player_map: HashMap<u64, crate::player::Player> = players.into_iter().map(|p| (p.content_id, p)).collect();
                let parse_docs = get_parse_docs(state.parse_collection(), &all_content_ids).await.unwrap_or_default();
                let mut report_parse_requests: HashMap<String, Vec<crate::mongo::ReportParseIdentityKey>> =
                    HashMap::new();
                let mut listing_meta: HashMap<u32, (u32, u32, String, String)> = HashMap::new();

                for ql in &listings {
                    let duty_id = ql.listing.duty;
                    let fflogs_info = crate::fflogs::mapping::get_fflogs_encounter(duty_id);
                    let (zone_id, encounter_id, zone_key) = if let Some(info) = fflogs_info {
                        let difficulty_id = info.difficulty_id.unwrap_or(0) as i32;
                        let partition = crate::fflogs::mapping::FFLOGS_ZONES
                            .get(&info.zone_id)
                            .map(|z| z.partition)
                            .unwrap_or(0) as i32;
                        (
                            info.zone_id as u32,
                            info.encounter_id,
                            crate::fflogs::make_zone_cache_key(info.zone_id, difficulty_id, partition),
                        )
                    } else {
                        (0, 0, String::new())
                    };
                    let legacy_zone_key = if zone_id > 0 {
                        zone_id.to_string()
                    } else {
                        String::new()
                    };

                    listing_meta.insert(ql.listing.id, (zone_id, encounter_id, zone_key.clone(), legacy_zone_key));

                    if zone_id > 0 && ql.listing.high_end() {
                        let leader_name = ql.listing.name.full_text(&Language::English);
                        report_parse_requests
                            .entry(zone_key.clone())
                            .or_default()
                            .push(crate::mongo::ReportParseIdentityKey::new(
                                &leader_name,
                                ql.listing.home_world,
                            ));

                        for (i, mid) in ql.listing.member_content_ids.iter().enumerate() {
                            let uid = *mid as u64;
                            if uid == 0 {
                                continue;
                            }

                            let is_leader_member = uid == ql.listing.leader_content_id || i == 0;
                            if let Some((name, home_world)) = resolve_api_member_identity(
                                &player_map,
                                uid,
                                is_leader_member,
                                &leader_name,
                                ql.listing.home_world,
                            ) {
                                if home_world != 0 {
                                    report_parse_requests
                                        .entry(zone_key.clone())
                                        .or_default()
                                        .push(crate::mongo::ReportParseIdentityKey::new(
                                            &name,
                                            home_world,
                                        ));
                                }
                            }
                        }
                    }
                }

                let mut report_parse_data_map: HashMap<
                    String,
                    HashMap<crate::mongo::ReportParseIdentityKey, crate::mongo::ReportParseZoneSummary>,
                > = HashMap::new();
                for (zone_key, identities) in report_parse_requests {
                    if let Ok(summaries) = crate::mongo::get_report_parse_summaries_by_zone(
                        state.report_parse_summary_collection(),
                        &zone_key,
                        &identities,
                    )
                    .await
                    {
                        report_parse_data_map.insert(zone_key, summaries);
                    }
                }
                
                let mut listings_with_members = Vec::new();
                for ql in listings {
                    let leader_content_id = ql.listing.leader_content_id;
                    let leader_name = ql.listing.name.full_text(&Language::English);
                    let leader_home_world = ql.listing.home_world;
                    let member_ids = ql.listing.member_content_ids.clone();
                    let mut container: ApiReadableListingContainer = ql.into();
                    
                    // Retrieve pre-calculated info
                    let (zone_id, encounter_id, zone_key, legacy_zone_key) = listing_meta
                        .get(&container.listing.id)
                        .cloned()
                        .unwrap_or((0, 0, String::new(), String::new()));
                    
                    let mut members = Vec::new();
                    
                    for (i, id) in member_ids.into_iter().enumerate() {
                        let uid = id as u64;
                        if uid == 0 {
                            continue;
                        }

                        let is_leader_member = uid == leader_content_id || i == 0;
                        let Some((name, home_world)) = resolve_api_member_identity(
                            &player_map,
                            uid,
                            is_leader_member,
                            &leader_name,
                            leader_home_world,
                        ) else {
                            continue;
                        };
                        let fallback_summary = if zone_id > 0 {
                            report_parse_data_map
                                .get(&zone_key)
                                .and_then(|summaries| {
                                    summaries.get(&crate::mongo::ReportParseIdentityKey::new(
                                        &name,
                                        home_world,
                                    ))
                                })
                        } else {
                            None
                        };
                        let plugin_zone_cache = if zone_id > 0 {
                            parse_docs.get(&uid).and_then(|doc| {
                                doc.zones
                                    .get(&zone_key)
                                    .or_else(|| (!legacy_zone_key.is_empty()).then(|| legacy_zone_key.as_str()).and_then(|key| doc.zones.get(key)))
                            })
                        } else {
                            None
                        };
                        let resolved = crate::parse_resolver::resolve_parse_data(
                            plugin_zone_cache,
                            fallback_summary.map(|summary| &summary.encounters),
                            encounter_id,
                            None,
                        );

                        members.push(ApiReadableMember {
                            content_id: uid,
                            name,
                            home_world: home_world.into(),
                            parse_percentile: resolved.primary_percentile,
                            parse_color_class: resolved.primary_color_class,
                            parse_source: resolved.source,
                        });
                    }
                    
                    container.listing.members = members;
                    listings_with_members.push(container);
                }

                Ok(warp::reply::json(&listings_with_members).into_response())
            },
            Err(_) => Ok(warp::reply::with_status(
                warp::reply(),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
            .into_response()),
        }
    }

    warp::get()
        .and(warp::path("listings"))
        .and(warp::path::end())
        .and_then(move || logic(state.clone()))
        .boxed()
}

fn ws(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route =
        warp::path("ws")
            .and(warp::ws())
            .and(warp::path::end())
            .map(move |ws: warp::ws::Ws| {
                let state = Arc::clone(&state);
                ws.on_upgrade(move |websocket| async move {
                    WsApiClient::run(state, websocket).await;
                })
            });

    warp::get().and(route).boxed()
}

/// A version of `QueriedListingContainer` with more sensible formatting,
/// implementation details hidden, and resolved names for duties, etc.
#[derive(Serialize)]
struct ApiReadableListingContainer {
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    time_left: f64,
    listing: ApiReadableListing,
}

impl From<QueriedListing> for ApiReadableListingContainer {
    fn from(value: QueriedListing) -> Self {
        Self {
            created_at: value.created_at,
            updated_at: value.updated_at,
            time_left: value.time_left,
            listing: value.listing.into(),
        }
    }
}

#[derive(Serialize)]
struct ApiReadableListing {
    id: u32,
    // pub content_id: u32,
    recruiter: String,
    description: ApiLocalizedString,
    created_world: ApiReadableWorld,
    home_world: ApiReadableWorld,
    current_world: ApiReadableWorld,
    // `Debug` of `DutyCategory`
    category: String,
    duty_info: Option<ApiReadableDutyInfo>,
    // `Debug` of `DutyType`
    duty_type: String,
    beginners_welcome: bool,
    seconds_remaining: u16,
    min_item_level: u16,
    num_parties: u8,
    slot_count: u8, // = slots_available
    last_server_restart: u32,
    objective: ApiReadableObjectiveFlags,
    conditions: ApiReadableConditionFlags,
    duty_finder_settings: ApiReadableDutyFinderSettingsFlags,
    loot_rules: ApiReadableLootRuleFlags,
    search_area: ApiReadableSearchAreaFlags,
    slots: Vec<ApiReadablePartyFinderSlot>,
    slots_filled: Vec<Option<&'static str>>, // None if not filled, otherwise the job code
    members: Vec<ApiReadableMember>,
}

#[derive(Serialize)]
struct ApiReadableMember {
    content_id: u64,
    name: String,
    home_world: ApiReadableWorld,
    parse_percentile: Option<u8>,
    parse_color_class: String,
    parse_source: crate::parse_resolver::ParseSource,
}

#[derive(Serialize)]
struct ApiLocalizedString {
    en: String,
    ja: String,
    de: String,
    fr: String,
}

impl From<SeString> for ApiLocalizedString {
    fn from(value: SeString) -> Self {
        Self {
            en: value.full_text(&Language::English),
            ja: value.full_text(&Language::Japanese),
            de: value.full_text(&Language::German),
            fr: value.full_text(&Language::French),
        }
    }
}

impl From<PartyFinderListing> for ApiReadableListing {
    fn from(value: PartyFinderListing) -> Self {
        let duty_info = ffxiv::duty(value.duty as u32)
            .map(|di| ApiReadableDutyInfo {
                id: value.duty as u32,
                name: di.name,
                high_end: di.high_end,
                content_kind_id: di.content_kind.as_u32(),
                content_kind: format!("{:?}", di.content_kind),
            });
        let display_jobs = value.display_jobs();
        let slots_filled = display_jobs
            .iter()
            .copied()
            .into_iter()
            .map(|job| if job == 0 {
                None
            } else {
                ffxiv::jobs::JOBS.get(&(job as u32))
                    .map(|j| j.code())
            })
            .collect();
        let slots = (0..value.slots_available as usize)
            .map(|index| {
                value
                    .slots
                    .get(index)
                    .cloned()
                    .map(ApiReadablePartyFinderSlot::from)
                    .unwrap_or_else(|| ApiReadablePartyFinderSlot(Vec::new()))
            })
            .collect();

        Self {
            id: value.id,
            recruiter: value.name.text(),
            description: value.description.into(),
            created_world: value.created_world.into(),
            home_world: value.home_world.into(),
            current_world: value.current_world.into(),
            category: format!("{:?}", value.category),
            duty_info,
            duty_type: format!("{:?}", value.duty_type),
            beginners_welcome: value.beginners_welcome,
            seconds_remaining: value.seconds_remaining,
            min_item_level: value.min_item_level,
            num_parties: value.num_parties,
            slot_count: value.slots_available,
            last_server_restart: value.last_server_restart,
            objective: value.objective.into(),
            conditions: value.conditions.into(),
            duty_finder_settings: value.duty_finder_settings.into(),
            loot_rules: value.loot_rules.into(),
            search_area: value.search_area.into(),
            slots,
            slots_filled,
            members: Vec::new(),
        }
    }
}

#[derive(Serialize)]
struct ApiReadableWorld {
    id: u16,
    name: &'static str,
}

impl From<u16> for ApiReadableWorld {
    fn from(value: u16) -> Self {
        Self {
            id: value,
            name: crate::ffxiv::WORLDS.get(&(value as u32))
                .map(|w| w.as_str())
                .unwrap_or("Unknown")
        }
    }
}

#[derive(Serialize)]
struct ApiReadableDutyInfo {
    pub id: u32,
    pub name: ffxiv::LocalisedText,
    pub high_end: bool,
    pub content_kind_id: u32,
    pub content_kind: String,
}

impl From<&DutyInfo> for ApiReadableDutyInfo {
    fn from(value: &DutyInfo) -> Self {
        // Need to find the ID from the value, but DutyInfo doesn't store its own ID.
        // We need to pass the ID when converting or find a way to get it.
        // Actually, listing.rs:172 `ffxiv::duty(value.duty as u32).map(|di| di.into())` passes &DutyInfo.
        // We should change `ApiReadableListing::from` to pass the ID or make `DutyInfo` carry it (unlikely).
        // Let's modify `ApiReadableListing::from` to instantiate `ApiReadableDutyInfo` manually or pass the ID.
        Self {
            id: 0, // Placeholder, will be fixed in ApiReadableListing::from
            name: value.name,
            high_end: value.high_end,
            content_kind_id: value.content_kind.as_u32(),
            content_kind: format!("{:?}", value.content_kind),
        }
    }
}

#[derive(Serialize)]
struct ApiReadableObjectiveFlags {
    duty_completion: bool,
    practice: bool,
    loot: bool,
}

impl From<ObjectiveFlags> for ApiReadableObjectiveFlags {
    fn from(value: ObjectiveFlags) -> Self {
        Self {
            duty_completion: value.contains(ObjectiveFlags::DUTY_COMPLETION),
            practice: value.contains(ObjectiveFlags::PRACTICE),
            loot: value.contains(ObjectiveFlags::LOOT),
        }
    }
}

#[derive(Serialize)]
struct ApiReadableConditionFlags {
    duty_complete: bool,
    duty_incomplete: bool,
    duty_complete_reward_unclaimed: bool,
}

impl From<ConditionFlags> for ApiReadableConditionFlags {
    fn from(value: ConditionFlags) -> Self {
        Self {
            duty_complete: value.contains(ConditionFlags::DUTY_COMPLETE),
            duty_incomplete: value.contains(ConditionFlags::DUTY_INCOMPLETE),
            duty_complete_reward_unclaimed: value.contains(ConditionFlags::DUTY_COMPLETE_WEEKLY_REWARD_UNCLAIMED),
        }
    }
}

#[derive(Serialize)]
struct ApiReadableDutyFinderSettingsFlags {
    undersized_party: bool,
    minimum_item_level: bool,
    silence_echo: bool,
}

impl From<DutyFinderSettingsFlags> for ApiReadableDutyFinderSettingsFlags {
    fn from(value: DutyFinderSettingsFlags) -> Self {
        Self {
            undersized_party: value.contains(DutyFinderSettingsFlags::UNDERSIZED_PARTY),
            minimum_item_level: value.contains(DutyFinderSettingsFlags::MINIMUM_ITEM_LEVEL),
            silence_echo: value.contains(DutyFinderSettingsFlags::SILENCE_ECHO),
        }
    }
}

#[derive(Serialize)]
struct ApiReadableLootRuleFlags {
    greed_only: bool,
    lootmaster: bool,
}

impl From<LootRuleFlags> for ApiReadableLootRuleFlags {
    fn from(value: LootRuleFlags) -> Self {
        Self {
            greed_only: value.contains(LootRuleFlags::GREED_ONLY),
            lootmaster: value.contains(LootRuleFlags::LOOTMASTER),
        }
    }
}

#[derive(Serialize)]
struct ApiReadableSearchAreaFlags {
    data_centre: bool,
    private: bool,
    alliance_raid: bool,
    world: bool,
    one_player_per_job: bool,
}

impl From<SearchAreaFlags> for ApiReadableSearchAreaFlags {
    fn from(value: SearchAreaFlags) -> Self {
        Self {
            data_centre: value.contains(SearchAreaFlags::DATA_CENTRE),
            private: value.contains(SearchAreaFlags::PRIVATE),
            alliance_raid: value.contains(SearchAreaFlags::ALLIANCE_RAID),
            world: value.contains(SearchAreaFlags::WORLD),
            one_player_per_job: value.contains(SearchAreaFlags::ONE_PLAYER_PER_JOB),
        }
    }
}

#[derive(Serialize)]
struct ApiReadablePartyFinderSlot(Vec<&'static str>); // list of job codes

impl From<PartyFinderSlot> for ApiReadablePartyFinderSlot {
    fn from(value: PartyFinderSlot) -> Self {
        Self(
            value
                .accepting
                .classjobs()
                .into_iter()
                .map(|cj| cj.code())
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use sestring::SeString;

    use crate::listing::{
        ConditionFlags, DutyCategory, DutyFinderSettingsFlags, DutyType, JobFlags,
        LootRuleFlags, ObjectiveFlags, PartyFinderListing, PartyFinderSlot, SearchAreaFlags,
    };

    use super::ApiReadableListing;

    fn sample_listing() -> PartyFinderListing {
        PartyFinderListing {
            id: 166_086,
            content_id_lower: 40_205_661,
            name: SeString::parse(b"Yuki Coffee").unwrap(),
            description: SeString::parse(b"Test Description").unwrap(),
            created_world: 44,
            home_world: 44,
            current_world: 44,
            category: DutyCategory::HighEndDuty,
            duty: 1010,
            duty_type: DutyType::Normal,
            beginners_welcome: false,
            seconds_remaining: 1667,
            min_item_level: 0,
            num_parties: 3,
            slots_available: 24,
            last_server_restart: 1_774_336_035,
            objective: ObjectiveFlags::DUTY_COMPLETION,
            conditions: ConditionFlags::DUTY_COMPLETE,
            duty_finder_settings: DutyFinderSettingsFlags::NONE,
            loot_rules: LootRuleFlags::NONE,
            search_area: SearchAreaFlags::DATA_CENTRE,
            slots: std::iter::repeat_with(|| PartyFinderSlot {
                accepting: JobFlags::all() - JobFlags::GLADIATOR,
            })
            .take(8)
            .collect(),
            jobs_present: vec![0; 8],
            member_content_ids: vec![
                1, 0, 2, 0, 3, 0, 4, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 7, 8, 9, 0, 10,
            ],
            member_jobs: vec![
                37, 0, 24, 0, 22, 0, 31, 0, 20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 33, 28, 34, 22, 0,
                38,
            ],
            leader_content_id: 5,
        }
    }

    #[test]
    fn api_listing_uses_display_slot_model_for_alliance_layouts() {
        let listing = ApiReadableListing::from(sample_listing());

        assert_eq!(listing.slot_count, 24);
        assert_eq!(listing.slots.len(), 24);
        assert_eq!(
            listing
                .slots_filled
                .iter()
                .filter(|slot| slot.is_some())
                .count(),
            10
        );
    }
}
