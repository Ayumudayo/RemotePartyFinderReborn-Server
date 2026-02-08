use crate::ffxiv;
use crate::ffxiv::duties::DutyInfo;
use crate::ffxiv::Language;
use crate::listing::{ConditionFlags, DutyFinderSettingsFlags, LootRuleFlags, ObjectiveFlags, PartyFinderListing, PartyFinderSlot, SearchAreaFlags};
use crate::listing_container::QueriedListing;
use crate::mongo::{get_current_listings, get_players_by_content_ids};
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

                // Prepare for Batch 2: Collect Content IDs per Zone ID
                let mut zone_requests: HashMap<u16, Vec<u64>> = HashMap::new();
                // Store calculated zone info to avoid recalculating in the second loop
                let mut listing_meta: HashMap<u32, (u16, u16)> = HashMap::new();

                for ql in &listings {
                    let duty_id = ql.listing.duty;
                    let fflogs_info = crate::fflogs::mapping::get_fflogs_encounter(duty_id);
                    let (zone_id, encounter_id) = if let Some(info) = fflogs_info {
                        (info.zone_id as u32, info.encounter_id)
                    } else {
                        (0, 0)
                    };
                    
                    listing_meta.insert(ql.listing.id, (zone_id as u16, encounter_id as u16));

                    if zone_id > 0 {
                        let entry = zone_requests.entry(zone_id as u16).or_insert_with(Vec::new);
                        for &mid in &ql.listing.member_content_ids {
                            entry.push(mid as u64);
                        }
                    }
                }

                // Batch Query: Fetch parses for each Zone
                // (ZoneID, ContentID) -> ZoneCache
                let mut parse_data_map: HashMap<(u16, u64), crate::mongo::ZoneCache> = HashMap::new();

                for (zone_id, content_ids) in zone_requests {
                    // Dedup content_ids
                    let mut unique_ids = content_ids;
                    unique_ids.sort_unstable();
                    unique_ids.dedup();

                    if let Ok(caches) = crate::mongo::get_zone_caches(state.parse_collection(), &unique_ids, zone_id as u32).await {
                        for (cid, cache) in caches {
                            parse_data_map.insert((zone_id, cid), cache);
                        }
                    }
                }
                
                let mut listings_with_members = Vec::new();
                for ql in listings {
                    let member_ids = ql.listing.member_content_ids.clone();
                    let mut container: ApiReadableListingContainer = ql.into();
                    
                    // Retrieve pre-calculated info
                    let (zone_id, encounter_id) = listing_meta.get(&container.listing.id).copied().unwrap_or((0, 0));

                    let mut members = Vec::new();
                    
                    for id in member_ids {
                        let uid = id as u64;
                        if let Some(p) = player_map.get(&uid) {
                            // Lookup in pre-fetched map
                            let (percentile, color_class) = if zone_id > 0 {
                                if let Some(zone_cache) = parse_data_map.get(&(zone_id, uid)) {
                                    let enc_key = encounter_id.to_string();
                                    if let Some(enc_parse) = zone_cache.encounters.get(&enc_key) {
                                        if enc_parse.percentile < 0.0 {
                                            (None, "parse-none".to_string())
                                        } else {
                                            (
                                                Some(enc_parse.percentile.round() as u8),
                                                crate::fflogs::mapping::percentile_color_class(enc_parse.percentile).to_string(),
                                            )
                                        }
                                    } else {
                                        (None, "parse-none".to_string())
                                    }
                                } else {
                                    (None, "parse-none".to_string())
                                }
                            } else {
                                (None, "parse-none".to_string())
                            };
                            
                            members.push(ApiReadableMember {
                                content_id: p.content_id,
                                name: p.name.clone(),
                                home_world: p.home_world.into(),
                                parse_percentile: percentile,
                                parse_color_class: color_class,
                            });
                        }
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
        let slots_filled = value.jobs_present
            .into_iter()
            .map(|job| if job == 0 {
                None
            } else {
                ffxiv::jobs::JOBS.get(&(job as u32))
                    .map(|j| j.code())
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
            slots: value.slots.into_iter().map(|s| s.into()).collect(),
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
