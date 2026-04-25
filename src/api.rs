use crate::listings_snapshot::{build_listings_payload, serialize_snapshot, BuiltListingsPayload};
use crate::web::{CachedListingsSnapshot, ListingsSnapshotCacheState, State};
use crate::ws::WsApiClient;
use anyhow::Context;
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

async fn build_api_snapshot(state: Arc<State>) -> anyhow::Result<CachedListingsSnapshot> {
    let (revision, payload): (u64, anyhow::Result<BuiltListingsPayload>) = build_snapshot_payload(
        || build_listings_payload(Arc::clone(&state)),
        || state.current_listings_revision(),
    )
    .await;
    let payload = payload?;
    let snapshot = serialize_snapshot(
        revision
            .try_into()
            .context("listings snapshot revision exceeded i64")?,
        payload,
    )?;

    Ok(CachedListingsSnapshot {
        revision,
        body: snapshot.body.into(),
        etag: Some(snapshot.etag),
        content_encoding: None,
    })
}

async fn build_cached_api_snapshot(state: Arc<State>) -> anyhow::Result<CachedListingsSnapshot> {
    build_api_snapshot(state).await
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

async fn get_cached_api_snapshot(state: Arc<State>) -> anyhow::Result<CachedListingsSnapshot> {
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

#[cfg(test)]
mod tests {
    use chrono::Utc;
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
    use crate::listings_snapshot::ApiReadableListing;
    use crate::parse_resolver::ParseSource;
    use crate::player::Player;
    use crate::template::listings::{ParseDisplay, ProgressDisplay, RenderableMember};
    use crate::web::ListingsSnapshotCacheState;

    use super::{build_snapshot_payload, get_or_build_cached_snapshot, CachedListingsSnapshot};

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
            etag: None,
            content_encoding: None,
        }
    }

    fn sample_renderable_member(party_index: u8, parse: ParseDisplay) -> RenderableMember {
        RenderableMember {
            job_id: 19,
            player: Player {
                content_id: 42,
                name: "Neutral Member".to_string(),
                home_world: 73,
                current_world: 73,
                last_seen: Utc::now(),
                seen_count: 1,
                account_id: "-1".to_string(),
            },
            parse,
            progress: ProgressDisplay::default(),
            slot_index: 0,
            party_index,
            party_header: None,
            identity_fallback: false,
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

    #[test]
    fn api_listing_serializes_neutral_member_display_state() {
        let hidden_fallback_parse = ParseDisplay::new(
            Some(84),
            "parse-purple".to_string(),
            None,
            "parse-none".to_string(),
            false,
            false,
            true,
            false,
            ParseSource::ReportParse,
        );
        let hidden_leader_parse = ParseDisplay::new(
            None,
            "parse-none".to_string(),
            None,
            "parse-none".to_string(),
            false,
            false,
            true,
            false,
            ParseSource::ReportParse,
        );

        let serialized = serde_json::to_value(ApiReadableListing::from_parts(
            sample_listing(),
            vec![sample_renderable_member(1, hidden_fallback_parse)],
            hidden_leader_parse,
        ))
        .expect("api listing should serialize");

        let member = &serialized["members"][0];
        assert!(
            member.get("party_label").is_none(),
            "snapshot should expose party_index, not localized alliance labels"
        );
        assert!(
            member["parse"].get("hidden_rail_tag_label").is_none(),
            "snapshot should expose hidden state, not display labels"
        );
        assert!(
            member["parse"].get("hidden_rail_tag_title").is_none(),
            "snapshot should expose hidden state, not display tooltips"
        );
        assert_eq!(member["parse"]["originally_hidden"], json!(true));
        assert!(
            serialized["leader_parse"]
                .get("hidden_rail_tag_title")
                .is_none(),
            "leader parse should also be language-neutral"
        );
        assert_eq!(serialized["leader_parse"]["originally_hidden"], json!(true));
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
