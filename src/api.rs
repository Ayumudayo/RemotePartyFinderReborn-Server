use crate::listing::{ConditionFlags, ObjectiveFlags, PartyFinderListing, SearchAreaFlags};
use crate::listing_container::QueriedListing;
use crate::template::listings::{
    ParseDisplay, ProgressDisplay, RenderableListing, RenderableMember,
};
use crate::web::handlers::build_listings_template;
use crate::web::{CachedListingsSnapshot, ListingsSnapshotCacheState, State};
use crate::ws::WsApiClient;
use serde::Serialize;
use sestring::{Payload, SeString};
use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::{Notify, RwLock};
use warp::filters::BoxedFilter;
use warp::http::{
    header::{HeaderValue, CACHE_CONTROL, CONTENT_TYPE},
    StatusCode,
};
use warp::hyper::Body;
use warp::{Filter, Reply};

pub fn api(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    warp::path("api")
        .and(ws(state.clone()).or(listings_snapshot(state.clone())))
        .boxed()
}

async fn build_api_listings(state: Arc<State>) -> Vec<ApiReadableListingContainer> {
    build_listings_template(state, None)
        .await
        .containers
        .into_iter()
        .map(ApiReadableListingContainer::from_renderable)
        .collect()
}

async fn build_snapshot_payload<T, Build, BuildFut, CurrentRevision>(
    mut build: Build,
    current_revision: CurrentRevision,
) -> (u64, T)
where
    Build: FnMut() -> BuildFut,
    BuildFut: Future<Output = T>,
    CurrentRevision: Fn() -> u64,
{
    let revision_before = current_revision();
    let payload = build().await;
    let revision_after = current_revision();

    if revision_before == revision_after {
        return (revision_after, payload);
    }

    // If writes race the build, report the lower bound that the payload is
    // guaranteed to include. The next client/server revision check will catch up
    // without multiplying the expensive listing-enrichment pass.
    (revision_before, payload)
}

async fn build_api_snapshot(state: Arc<State>) -> ApiListingsSnapshot {
    let (revision, listings) = build_snapshot_payload(
        || build_api_listings(Arc::clone(&state)),
        || state.current_listings_revision(),
    )
    .await;

    ApiListingsSnapshot { revision, listings }
}

async fn build_cached_api_snapshot(
    state: Arc<State>,
) -> Result<CachedListingsSnapshot, serde_json::Error> {
    let snapshot = build_api_snapshot(state).await;
    let revision = snapshot.revision;
    let body = serde_json::to_vec(&snapshot)?;

    Ok(CachedListingsSnapshot {
        revision,
        body: body.into(),
    })
}

async fn get_or_build_cached_snapshot<F, Fut, E>(
    cache: &RwLock<ListingsSnapshotCacheState>,
    min_revision: u64,
    build: F,
) -> Result<CachedListingsSnapshot, E>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<CachedListingsSnapshot, E>>,
{
    loop {
        let maybe_wait = {
            let cache = cache.read().await;
            match &*cache {
                ListingsSnapshotCacheState::Ready(snapshot)
                    if snapshot.revision >= min_revision =>
                {
                    return Ok(snapshot.clone());
                }
                ListingsSnapshotCacheState::Building { notify } => Some(Arc::clone(notify)),
                ListingsSnapshotCacheState::Ready(_) | ListingsSnapshotCacheState::Empty => None,
            }
        };

        if let Some(notify) = maybe_wait {
            notify.notified().await;
            continue;
        }

        let notify = Arc::new(Notify::new());
        let maybe_wait = {
            let mut cache = cache.write().await;
            match &*cache {
                ListingsSnapshotCacheState::Ready(snapshot)
                    if snapshot.revision >= min_revision =>
                {
                    return Ok(snapshot.clone());
                }
                ListingsSnapshotCacheState::Building { notify } => Some(Arc::clone(notify)),
                ListingsSnapshotCacheState::Ready(_) | ListingsSnapshotCacheState::Empty => {
                    *cache = ListingsSnapshotCacheState::Building {
                        notify: Arc::clone(&notify),
                    };
                    None
                }
            }
        };

        if let Some(notify) = maybe_wait {
            notify.notified().await;
            continue;
        }

        let built = build().await;

        {
            let mut cache = cache.write().await;
            match &built {
                Ok(snapshot) => {
                    *cache = ListingsSnapshotCacheState::Ready(snapshot.clone());
                }
                Err(_) => {
                    *cache = ListingsSnapshotCacheState::Empty;
                }
            }
        }

        notify.notify_waiters();
        return built;
    }
}

async fn get_cached_api_snapshot(
    state: Arc<State>,
) -> Result<CachedListingsSnapshot, serde_json::Error> {
    let min_revision = state.current_listings_revision();
    get_or_build_cached_snapshot(&state.listings_snapshot_cache, min_revision, || {
        build_cached_api_snapshot(Arc::clone(&state))
    })
    .await
}

fn snapshot_json_response(snapshot: CachedListingsSnapshot) -> warp::reply::Response {
    let mut response = warp::http::Response::new(Body::from(snapshot.body));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/json; charset=utf-8"),
    );
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn listings_snapshot(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    async fn logic(state: Arc<State>) -> Result<warp::reply::Response, Infallible> {
        let response = match get_cached_api_snapshot(state).await {
            Ok(snapshot) => snapshot_json_response(snapshot),
            Err(error) => {
                tracing::error!("failed to build listings snapshot json: {:#?}", error);
                warp::reply::with_status(
                    "failed to build listings snapshot",
                    StatusCode::INTERNAL_SERVER_ERROR,
                )
                .into_response()
            }
        };

        Ok(response)
    }

    warp::get()
        .and(warp::path("listings"))
        .and(warp::path("snapshot"))
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

fn sestring_payloads(value: &SeString) -> Vec<ApiSeStringPayload> {
    value
        .0
        .iter()
        .filter_map(|payload| match payload {
            Payload::Text(text) => Some(ApiSeStringPayload::Text {
                text: text.0.clone(),
            }),
            Payload::AutoTranslate(auto_translate) => Some(ApiSeStringPayload::AutoTranslate {
                group: auto_translate.group,
                key: auto_translate.key,
            }),
            _ => None,
        })
        .collect()
}

fn description_badges(listing: &PartyFinderListing) -> (&'static str, Vec<&'static str>) {
    let mut colour_class = "";
    let mut badges = Vec::new();

    if listing.objective.contains(ObjectiveFlags::PRACTICE) {
        badges.push("practice");
        colour_class = "desc-green";
    }

    if listing.objective.contains(ObjectiveFlags::DUTY_COMPLETION) {
        badges.push("duty_completion");
        colour_class = "desc-blue";
    }

    if listing.objective.contains(ObjectiveFlags::LOOT) {
        badges.push("loot");
        colour_class = "desc-yellow";
    }

    if listing.conditions.contains(ConditionFlags::DUTY_COMPLETE) {
        badges.push("duty_complete");
    }

    if listing
        .conditions
        .contains(ConditionFlags::DUTY_COMPLETE_WEEKLY_REWARD_UNCLAIMED)
    {
        badges.push("weekly_reward_unclaimed");
    }

    if listing.conditions.contains(ConditionFlags::DUTY_INCOMPLETE) {
        badges.push("duty_incomplete");
    }

    if listing
        .search_area
        .contains(SearchAreaFlags::ONE_PLAYER_PER_JOB)
    {
        badges.push("one_player_per_job");
    }

    (colour_class, badges)
}

fn role_class_from_class_job(class_job: &ffxiv_types::jobs::ClassJob) -> &'static str {
    use ffxiv_types::Role;

    match class_job.role() {
        Some(Role::Tank) => "tank",
        Some(Role::Healer) => "healer",
        Some(Role::Dps) => "dps",
        None => "",
    }
}

fn party_label(party_index: u8) -> Option<&'static str> {
    match party_index {
        0 => Some("Alliance A"),
        1 => Some("Alliance B"),
        2 => Some("Alliance C"),
        _ => None,
    }
}

fn build_display_slots(listing: &PartyFinderListing) -> Vec<ApiDisplaySlot> {
    listing
        .slots()
        .into_iter()
        .map(|slot| match slot {
            Ok(class_job) => ApiDisplaySlot {
                filled: true,
                role_class: role_class_from_class_job(&class_job).to_string(),
                title: class_job.code().to_string(),
                icon_code: Some(class_job.code().to_string()),
            },
            Err((role_class, title)) => ApiDisplaySlot {
                filled: false,
                role_class,
                title,
                icon_code: None,
            },
        })
        .collect()
}

#[derive(Serialize)]
struct ApiListingsSnapshot {
    revision: u64,
    listings: Vec<ApiReadableListingContainer>,
}

/// A render-oriented JSON shape for client-side listings rendering.
#[derive(Serialize)]
struct ApiReadableListingContainer {
    time_left_seconds: i64,
    updated_at_timestamp: i64,
    listing: ApiReadableListing,
}

impl ApiReadableListingContainer {
    fn from_renderable(value: RenderableListing) -> Self {
        let RenderableListing {
            container,
            members,
            leader_parse,
        } = value;
        let QueriedListing {
            created_at: _,
            updated_at,
            updated_minute: _,
            time_left,
            listing,
        } = container;
        let time_left_seconds = time_left as i64;
        let updated_at_timestamp = updated_at.timestamp();

        Self {
            time_left_seconds,
            updated_at_timestamp,
            listing: ApiReadableListing::from_parts(listing, members, leader_parse),
        }
    }
}

#[derive(Serialize)]
struct ApiReadableListing {
    creator_name: Vec<ApiSeStringPayload>,
    description: Vec<ApiSeStringPayload>,
    duty_id: u16,
    duty_type: u8,
    category: u32,
    created_world: ApiReadableWorld,
    home_world: ApiReadableWorld,
    data_centre: Option<&'static str>,
    min_item_level: u16,
    num_parties: u8,
    slot_count: u8,
    slots_filled_count: usize,
    high_end: bool,
    cross_world: bool,
    content_kind: u32,
    joinable_roles: u32,
    objective_bits: u32,
    conditions_bits: u32,
    search_area_bits: u32,
    description_badge_class: &'static str,
    description_badges: Vec<&'static str>,
    display_slots: Vec<ApiDisplaySlot>,
    members: Vec<ApiReadableMember>,
    leader_parse: ApiParseDisplay,
    is_alliance_view: bool,
}

impl ApiReadableListing {
    fn from_parts(
        value: PartyFinderListing,
        members: Vec<RenderableMember>,
        leader_parse: ParseDisplay,
    ) -> Self {
        let (description_badge_class, description_badges) = description_badges(&value);
        let is_alliance_view =
            value.num_parties >= 3 || members.iter().any(|member| member.party_index > 0);

        Self {
            creator_name: sestring_payloads(&value.name),
            description: sestring_payloads(&value.description),
            duty_id: value.duty,
            duty_type: value.duty_type as u8,
            category: value.category as u32,
            created_world: value.created_world.into(),
            home_world: value.home_world.into(),
            data_centre: value.data_centre_name(),
            min_item_level: value.min_item_level,
            num_parties: value.num_parties,
            slot_count: value.slots_available,
            slots_filled_count: value.slots_filled(),
            high_end: value.high_end(),
            cross_world: value.is_cross_world(),
            content_kind: value.content_kind(),
            joinable_roles: value.joinable_roles(),
            objective_bits: value.objective.bits() as u32,
            conditions_bits: value.conditions.bits() as u32,
            search_area_bits: value.search_area.bits() as u32,
            description_badge_class,
            description_badges,
            display_slots: build_display_slots(&value),
            members: members.into_iter().map(ApiReadableMember::from).collect(),
            leader_parse: leader_parse.into(),
            is_alliance_view,
        }
    }
}

#[derive(Serialize)]
struct ApiReadableMember {
    name: String,
    home_world: ApiReadableWorld,
    job_id: u8,
    job_code: Option<&'static str>,
    role_class: &'static str,
    parse: ApiParseDisplay,
    progress: ApiProgressDisplay,
    slot_index: usize,
    party_index: u8,
    party_label: Option<&'static str>,
    fflogs_character_url: Option<String>,
}

impl From<RenderableMember> for ApiReadableMember {
    fn from(value: RenderableMember) -> Self {
        let job_code = value.job_code();
        let role_class = value.role_class();
        let fflogs_character_url = value.fflogs_character_url();

        Self {
            name: value.player.name,
            home_world: value.player.home_world.into(),
            job_id: value.job_id,
            job_code,
            role_class,
            parse: value.parse.into(),
            progress: value.progress.into(),
            slot_index: value.slot_index,
            party_index: value.party_index,
            party_label: party_label(value.party_index),
            fflogs_character_url,
        }
    }
}

#[derive(Serialize)]
struct ApiParseDisplay {
    primary_percentile: Option<u8>,
    primary_color_class: String,
    secondary_percentile: Option<u8>,
    secondary_color_class: String,
    has_secondary: bool,
    hidden: bool,
    hidden_rail_tag_label: Option<&'static str>,
    hidden_rail_tag_title: &'static str,
    estimated: bool,
}

impl From<ParseDisplay> for ApiParseDisplay {
    fn from(value: ParseDisplay) -> Self {
        let hidden_rail_tag_label = value.hidden_rail_tag_label();
        let hidden_rail_tag_title = value.hidden_rail_tag_title();

        Self {
            primary_percentile: value.primary_percentile,
            primary_color_class: value.primary_color_class,
            secondary_percentile: value.secondary_percentile,
            secondary_color_class: value.secondary_color_class,
            has_secondary: value.has_secondary,
            hidden: value.hidden,
            hidden_rail_tag_label,
            hidden_rail_tag_title,
            estimated: value.estimated,
        }
    }
}

#[derive(Serialize)]
struct ApiProgressDisplay {
    final_boss_percentage: Option<u8>,
    final_clear_count: Option<u16>,
}

impl From<ProgressDisplay> for ApiProgressDisplay {
    fn from(value: ProgressDisplay) -> Self {
        Self {
            final_boss_percentage: value.final_boss_percentage,
            final_clear_count: value.final_clear_count,
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ApiSeStringPayload {
    Text { text: String },
    AutoTranslate { group: u8, key: u32 },
}

#[derive(Serialize)]
struct ApiReadableWorld {
    name: &'static str,
}

impl From<u16> for ApiReadableWorld {
    fn from(value: u16) -> Self {
        Self {
            name: crate::ffxiv::WORLDS
                .get(&(value as u32))
                .map(|w| w.as_str())
                .unwrap_or("Unknown"),
        }
    }
}

#[derive(Serialize)]
struct ApiDisplaySlot {
    filled: bool,
    role_class: String,
    title: String,
    icon_code: Option<String>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use sestring::payload::{AutoTranslatePayload, TextPayload};
    use sestring::Payload;
    use sestring::SeString;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::RwLock;

    use crate::listing::{
        ConditionFlags, DutyCategory, DutyFinderSettingsFlags, DutyType, JobFlags, LootRuleFlags,
        ObjectiveFlags, PartyFinderListing, PartyFinderSlot, SearchAreaFlags,
    };
    use crate::web::ListingsSnapshotCacheState;

    use super::{
        build_snapshot_payload, get_or_build_cached_snapshot, ApiReadableListing,
        CachedListingsSnapshot,
    };

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
                37, 0, 24, 0, 22, 0, 31, 0, 20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 33, 28, 34, 22, 0, 38,
            ],
            leader_content_id: 5,
        }
    }

    fn sample_cached_snapshot(revision: u64) -> CachedListingsSnapshot {
        CachedListingsSnapshot {
            revision,
            body: format!(r#"{{"revision":{revision}}}"#).into_bytes().into(),
        }
    }

    #[test]
    fn api_listing_uses_display_slot_model_for_alliance_layouts() {
        let listing =
            ApiReadableListing::from_parts(sample_listing(), Vec::new(), Default::default());

        assert_eq!(listing.slot_count, 24);
        assert_eq!(listing.display_slots.len(), 24);
        assert_eq!(listing.slots_filled_count, 10);
    }

    #[test]
    fn api_listing_serializes_language_neutral_text_tokens() {
        let mut listing = sample_listing();
        listing.description = SeString(vec![
            Payload::Text(TextPayload("Need ".to_string())),
            Payload::AutoTranslate(AutoTranslatePayload { group: 1, key: 101 }),
        ]);

        let serialized = serde_json::to_value(ApiReadableListing::from_parts(
            listing,
            Vec::new(),
            Default::default(),
        ))
        .expect("api listing should serialize");

        assert_eq!(
            serialized["creator_name"],
            json!([{ "type": "text", "text": "Yuki Coffee" }])
        );
        assert_eq!(
            serialized["description"],
            json!([
                { "type": "text", "text": "Need " },
                { "type": "auto_translate", "group": 1, "key": 101 }
            ])
        );
        assert_eq!(serialized["duty_id"], 1010);
        assert_eq!(serialized["duty_type"], DutyType::Normal as u8);
        assert_eq!(serialized["category"], DutyCategory::HighEndDuty as u32);
        assert!(
            serialized.get("duty_name").is_none(),
            "snapshot should not include localized duty names"
        );
    }

    #[tokio::test]
    async fn snapshot_cache_reuses_ready_entry_for_matching_revision() {
        let cache = RwLock::new(ListingsSnapshotCacheState::Ready(sample_cached_snapshot(7)));
        let builds = AtomicUsize::new(0);

        let snapshot = get_or_build_cached_snapshot(&cache, 7, || async {
            builds.fetch_add(1, Ordering::SeqCst);
            Ok::<_, ()>(sample_cached_snapshot(8))
        })
        .await
        .expect("cached snapshot should resolve");

        assert_eq!(snapshot.revision, 7);
        assert_eq!(builds.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn snapshot_cache_coalesces_concurrent_builders() {
        let cache = RwLock::new(ListingsSnapshotCacheState::Empty);
        let builds = AtomicUsize::new(0);

        let build = || async {
            builds.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            Ok::<_, ()>(sample_cached_snapshot(11))
        };

        let (left, right) = tokio::join!(
            get_or_build_cached_snapshot(&cache, 11, build),
            get_or_build_cached_snapshot(&cache, 11, build)
        );

        assert_eq!(left.expect("left snapshot should resolve").revision, 11);
        assert_eq!(right.expect("right snapshot should resolve").revision, 11);
        assert_eq!(builds.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn snapshot_payload_uses_stable_revision_without_retrying() {
        let revisions = Arc::new([5_u64, 5]);
        let cursor = Arc::new(AtomicUsize::new(0));
        let builds = AtomicUsize::new(0);

        let (revision, payload) = build_snapshot_payload(
            || async {
                builds.fetch_add(1, Ordering::SeqCst);
                "payload"
            },
            {
                let revisions = Arc::clone(&revisions);
                let cursor = Arc::clone(&cursor);
                move || {
                    let index = cursor.fetch_add(1, Ordering::SeqCst);
                    revisions[index]
                }
            },
        )
        .await;

        assert_eq!(revision, 5);
        assert_eq!(payload, "payload");
        assert_eq!(builds.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn snapshot_payload_uses_lower_bound_when_revision_races() {
        let revisions = Arc::new([7_u64, 8]);
        let cursor = Arc::new(AtomicUsize::new(0));

        let (revision, payload) = build_snapshot_payload(|| async { "payload" }, {
            let revisions = Arc::clone(&revisions);
            let cursor = Arc::clone(&cursor);
            move || {
                let index = cursor.fetch_add(1, Ordering::SeqCst);
                revisions[index]
            }
        })
        .await;

        assert_eq!(revision, 7);
        assert_eq!(payload, "payload");
    }
}
