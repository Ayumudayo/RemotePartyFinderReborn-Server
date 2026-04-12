use std::collections::HashMap;

use anyhow::Context;
use chrono::{DateTime, TimeDelta, Utc};
use futures_util::StreamExt;
use mongodb::{
    bson::{doc, Document},
    options::UpdateOptions,
    results::UpdateResult,
    Collection,
};

use crate::listing::PartyFinderListing;
use crate::listing_container::{ListingContainer, QueriedListing};

fn is_valid_world_id(world_id: u16) -> bool {
    world_id < 1_000
}

fn strip_detail_managed_listing_fields(listing_doc: &mut Document) {
    listing_doc.remove("member_content_ids");
    listing_doc.remove("member_jobs");
    listing_doc.remove("detail_slot_flags");
    listing_doc.remove("leader_content_id");
}

fn listing_upsert_filter(listing: &PartyFinderListing) -> Document {
    doc! {
        "listing.id": listing.id,
        "listing.last_server_restart": listing.last_server_restart,
        "listing.created_world": listing.created_world as u32,
    }
}

fn listing_upsert_update(mut listing_doc: Document) -> Vec<Document> {
    strip_detail_managed_listing_fields(&mut listing_doc);
    vec![doc! {
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
    }]
}

pub async fn get_current_listings(
    collection: Collection<ListingContainer>,
) -> anyhow::Result<Vec<QueriedListing>> {
    let one_hour_ago = Utc::now() - TimeDelta::try_hours(1).unwrap();
    let cursor = collection
        .aggregate(
            [
                doc! {
                    "$match": {
                        "updated_at": { "$gte": one_hour_ago },
                    }
                },
                doc! {
                    "$match": {
                        "listing.search_area": { "$bitsAllClear": 2 },
                    }
                },
                doc! {
                    "$set": {
                        "time_left": {
                            "$divide": [
                                {
                                    "$subtract": [
                                        { "$multiply": ["$listing.seconds_remaining", 1000] },
                                        { "$subtract": ["$$NOW", "$updated_at"] },
                                    ]
                                },
                                1000,
                            ]
                        },
                        "updated_minute": {
                            "$dateTrunc": {
                                "date": "$updated_at",
                                "unit": "minute",
                                "binSize": 5,
                            },
                        },
                    }
                },
                doc! {
                    "$match": {
                        "time_left": { "$gte": 0 },
                    }
                },
            ],
            None,
        )
        .await?;

    let listings = cursor
        .filter_map(async |result| {
            result
                .ok()
                .and_then(|raw| mongodb::bson::from_document(raw).ok())
        })
        .collect::<Vec<_>>()
        .await;

    Ok(listings)
}

pub async fn insert_listing(
    collection: Collection<ListingContainer>,
    listing: &PartyFinderListing,
) -> anyhow::Result<UpdateResult> {
    if !is_valid_world_id(listing.created_world)
        || !is_valid_world_id(listing.home_world)
        || !is_valid_world_id(listing.current_world)
    {
        anyhow::bail!("invalid listing");
    }

    let listing_doc = mongodb::bson::to_document(listing)?;
    let filter = listing_upsert_filter(listing);
    let update = listing_upsert_update(listing_doc);
    let options = UpdateOptions::builder().upsert(true).build();

    collection
        .update_one(filter, update, options)
        .await
        .context("could not insert record")
}

#[derive(Clone)]
struct PreparedPlayerUpsert {
    content_id: u64,
    name: String,
    home_world: u16,
    current_world: u16,
    account_id: String,
}

#[derive(Clone)]
pub struct PreparedCharacterIdentityUpsert {
    content_id: u64,
    name: String,
    home_world: u16,
    world_name: String,
    source: String,
    observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy)]
enum PlayerUpsertRejectReason {
    MissingContentId,
    MissingName,
    InvalidHomeWorld,
}

#[derive(Debug, Clone, Copy)]
pub enum CharacterIdentityRejectReason {
    MissingContentId,
    MissingName,
    InvalidHomeWorld,
}

#[derive(Debug, Clone, Copy)]
pub struct PlayerUpsertReport {
    pub requested: usize,
    pub accepted: usize,
    pub updated: usize,
    pub invalid: usize,
    pub failed: usize,
}

fn prepare_player_upsert(
    player: &crate::player::UploadablePlayer,
) -> Result<PreparedPlayerUpsert, PlayerUpsertRejectReason> {
    if player.content_id == 0 {
        return Err(PlayerUpsertRejectReason::MissingContentId);
    }

    if player.name.trim().is_empty() {
        return Err(PlayerUpsertRejectReason::MissingName);
    }

    if !is_valid_world_id(player.home_world) {
        return Err(PlayerUpsertRejectReason::InvalidHomeWorld);
    }

    let current_world = if is_valid_world_id(player.current_world) {
        player.current_world
    } else {
        0
    };

    let account_id = if player.account_id != 0 {
        player.account_id.to_string()
    } else {
        "-1".to_string()
    };

    Ok(PreparedPlayerUpsert {
        content_id: player.content_id,
        name: player.name.clone(),
        home_world: player.home_world,
        current_world,
        account_id,
    })
}

pub fn prepare_character_identity_upsert(
    identity: &crate::player::UploadableCharacterIdentity,
) -> Result<PreparedCharacterIdentityUpsert, CharacterIdentityRejectReason> {
    if identity.content_id == 0 {
        return Err(CharacterIdentityRejectReason::MissingContentId);
    }

    if identity.name.trim().is_empty() {
        return Err(CharacterIdentityRejectReason::MissingName);
    }

    if !is_valid_world_id(identity.home_world) {
        return Err(CharacterIdentityRejectReason::InvalidHomeWorld);
    }

    Ok(PreparedCharacterIdentityUpsert {
        content_id: identity.content_id,
        name: identity.name.trim().to_string(),
        home_world: identity.home_world,
        world_name: identity.world_name.trim().to_string(),
        source: identity.source.trim().to_string(),
        observed_at: identity.observed_at,
    })
}

pub async fn upsert_players(
    collection: Collection<crate::player::Player>,
    players: &[crate::player::UploadablePlayer],
    concurrency: usize,
) -> anyhow::Result<PlayerUpsertReport> {
    let mut prepared = Vec::with_capacity(players.len());
    let mut invalid_missing_content_id = 0usize;
    let mut invalid_missing_name = 0usize;
    let mut invalid_home_world = 0usize;

    for player in players {
        match prepare_player_upsert(player) {
            Ok(op) => prepared.push(op),
            Err(PlayerUpsertRejectReason::MissingContentId) => invalid_missing_content_id += 1,
            Err(PlayerUpsertRejectReason::MissingName) => invalid_missing_name += 1,
            Err(PlayerUpsertRejectReason::InvalidHomeWorld) => invalid_home_world += 1,
        }
    }

    let invalid = invalid_missing_content_id + invalid_missing_name + invalid_home_world;
    if invalid > 0 {
        tracing::debug!(
            requested = players.len(),
            invalid,
            invalid_missing_content_id,
            invalid_missing_name,
            invalid_home_world,
            "Dropped invalid player rows before MongoDB upsert"
        );
    }

    let now = Utc::now();
    let accepted = prepared.len();
    let mut successful = 0usize;
    let mut failed = 0usize;

    let mut writes = futures_util::stream::iter(prepared.into_iter().map(|op| {
        let collection = collection.clone();
        let now = now.clone();
        async move {
            let content_id = op.content_id;
            let mut set_doc = doc! {
                "account_id": op.account_id,
                "name": op.name,
                "last_seen": now,
            };

            let mut set_on_insert_doc = doc! {
                "content_id": op.content_id as i64,
            };

            if op.home_world != 0 {
                set_doc.insert("home_world", op.home_world as u32);
            } else {
                set_on_insert_doc.insert("home_world", 0u32);
            }

            if op.current_world != 0 {
                set_doc.insert("current_world", op.current_world as u32);
            } else {
                set_on_insert_doc.insert("current_world", 0u32);
            }

            collection
                .update_one(
                    doc! { "content_id": op.content_id as i64 },
                    doc! {
                        "$set": set_doc,
                        "$inc": { "seen_count": 1 },
                        "$setOnInsert": set_on_insert_doc,
                    },
                    UpdateOptions::builder().upsert(true).build(),
                )
                .await
                .map_err(|error| (content_id, error))
        }
    }))
    .buffer_unordered(concurrency.max(1));

    while let Some(result) = writes.next().await {
        match result {
            Ok(_) => {
                successful += 1;
            }
            Err((content_id, error)) => {
                failed += 1;
                tracing::error!(
                    content_id,
                    error = ?error,
                    "MongoDB player upsert failed"
                );
            }
        }
    }

    let report = PlayerUpsertReport {
        requested: players.len(),
        accepted,
        updated: successful,
        invalid,
        failed,
    };

    if report.failed == 0 {
        tracing::debug!(
            requested = report.requested,
            accepted = report.accepted,
            updated = report.updated,
            invalid = report.invalid,
            failed = report.failed,
            "MongoDB player upsert batch completed"
        );
    } else {
        tracing::warn!(
            requested = report.requested,
            accepted = report.accepted,
            updated = report.updated,
            invalid = report.invalid,
            failed = report.failed,
            "MongoDB player upsert batch completed with write failures"
        );
    }

    Ok(report)
}

fn get_identity_observed_at(document: &Document) -> Option<DateTime<Utc>> {
    document
        .get_datetime("identity_observed_at")
        .ok()
        .map(|value| value.to_chrono())
}

pub async fn upsert_character_identities(
    collection: Collection<crate::player::Player>,
    identities: &[crate::player::UploadableCharacterIdentity],
    concurrency: usize,
) -> anyhow::Result<PlayerUpsertReport> {
    let mut prepared = Vec::with_capacity(identities.len());
    let mut invalid_missing_content_id = 0usize;
    let mut invalid_missing_name = 0usize;
    let mut invalid_home_world = 0usize;

    for identity in identities {
        match prepare_character_identity_upsert(identity) {
            Ok(op) => prepared.push(op),
            Err(CharacterIdentityRejectReason::MissingContentId) => invalid_missing_content_id += 1,
            Err(CharacterIdentityRejectReason::MissingName) => invalid_missing_name += 1,
            Err(CharacterIdentityRejectReason::InvalidHomeWorld) => invalid_home_world += 1,
        }
    }

    let invalid = invalid_missing_content_id + invalid_missing_name + invalid_home_world;
    if invalid > 0 {
        tracing::debug!(
            requested = identities.len(),
            invalid,
            invalid_missing_content_id,
            invalid_missing_name,
            invalid_home_world,
            "Dropped invalid character identity rows before MongoDB upsert"
        );
    }

    let accepted = prepared.len();
    let mut successful = 0usize;
    let mut failed = 0usize;
    let raw_collection = collection.clone_with_type::<Document>();

    let mut writes = futures_util::stream::iter(prepared.into_iter().map(|op| {
        let collection = collection.clone();
        let raw_collection = raw_collection.clone();
        async move {
            let content_id = op.content_id;
            let filter = doc! { "content_id": content_id as i64 };
            let existing_doc: Option<Document> = raw_collection
                .find_one(filter.clone(), None)
                .await
                .map_err(|error| (content_id, error))?;

            let existing_player = existing_doc.as_ref().and_then(|document| {
                mongodb::bson::from_document::<crate::player::Player>(document.clone()).ok()
            });
            let existing_identity_observed_at =
                existing_doc.as_ref().and_then(get_identity_observed_at);
            let incoming_identity = crate::player::UploadableCharacterIdentity {
                content_id: op.content_id,
                name: op.name.clone(),
                home_world: op.home_world,
                world_name: op.world_name.clone(),
                source: op.source.clone(),
                observed_at: op.observed_at,
            };
            let merged = crate::player::merge_identity_into_player(
                existing_player.as_ref(),
                existing_identity_observed_at,
                &incoming_identity,
                Utc::now(),
            );

            let mut set_doc = doc! {
                "content_id": merged.player.content_id as i64,
                "name": merged.player.name,
                "home_world": merged.player.home_world as u32,
                "current_world": merged.player.current_world as u32,
                "last_seen": merged.player.last_seen,
                "seen_count": merged.player.seen_count as u32,
                "account_id": merged.player.account_id,
                "identity_observed_at": merged.identity_observed_at,
            };

            if merged.applied_incoming_identity {
                set_doc.insert("world_name", merged.world_name);
                set_doc.insert(
                    "identity_source",
                    if merged.source.trim().is_empty() {
                        "unknown".to_string()
                    } else {
                        merged.source
                    },
                );
            }

            collection
                .update_one(
                    filter,
                    doc! {
                        "$set": set_doc,
                        "$setOnInsert": {
                            "content_id": content_id as i64,
                        },
                    },
                    UpdateOptions::builder().upsert(true).build(),
                )
                .await
                .map_err(|error| (content_id, error))
        }
    }))
    .buffer_unordered(concurrency.max(1));

    while let Some(result) = writes.next().await {
        match result {
            Ok(_) => {
                successful += 1;
            }
            Err((content_id, error)) => {
                failed += 1;
                tracing::error!(
                    content_id,
                    error = ?error,
                    "MongoDB character identity upsert failed"
                );
            }
        }
    }

    let report = PlayerUpsertReport {
        requested: identities.len(),
        accepted,
        updated: successful,
        invalid,
        failed,
    };

    if report.failed == 0 {
        tracing::debug!(
            requested = report.requested,
            accepted = report.accepted,
            updated = report.updated,
            invalid = report.invalid,
            failed = report.failed,
            "MongoDB character identity upsert batch completed"
        );
    } else {
        tracing::warn!(
            requested = report.requested,
            accepted = report.accepted,
            updated = report.updated,
            invalid = report.invalid,
            failed = report.failed,
            "MongoDB character identity upsert batch completed with write failures"
        );
    }

    Ok(report)
}

pub async fn get_players_by_content_ids(
    collection: Collection<crate::player::Player>,
    content_ids: &[u64],
) -> anyhow::Result<Vec<crate::player::Player>> {
    let as_i64 = content_ids.iter().map(|id| *id as i64).collect::<Vec<_>>();
    let cursor = collection
        .find(doc! { "content_id": { "$in": as_i64 } }, None)
        .await?;

    let players = cursor
        .filter_map(async |result| match result {
            Ok(player) => Some(player),
            Err(error) => {
                tracing::debug!("Error reading player: {:?}", error);
                None
            }
        })
        .collect::<Vec<_>>()
        .await;

    Ok(players)
}

pub async fn get_all_active_players(
    collection: Collection<crate::player::Player>,
) -> anyhow::Result<Vec<crate::player::Player>> {
    let seven_days_ago = Utc::now() - TimeDelta::try_days(7).unwrap();
    let cursor = collection
        .find(doc! { "last_seen": { "$gte": seven_days_ago } }, None)
        .await?;

    let players = cursor
        .filter_map(async |result| result.ok())
        .collect::<Vec<_>>()
        .await;

    Ok(players)
}

pub use crate::fflogs::cache::{
    is_zone_cache_expired, is_zone_cache_expired_with_hidden_ttl_hours, EncounterParse,
    ParseCacheDoc, ZoneCache,
};
pub use crate::report_parse::{ReportParseIdentityKey, ReportParseSummaryDoc};

pub async fn get_zone_cache(
    collection: Collection<ParseCacheDoc>,
    content_id: u64,
    zone_key: &str,
) -> anyhow::Result<Option<ZoneCache>> {
    let cache_doc = collection
        .find_one(doc! { "content_id": content_id as i64 }, None)
        .await?;

    Ok(cache_doc.and_then(|doc| doc.zones.get(zone_key).cloned()))
}

pub async fn get_zone_caches(
    collection: Collection<ParseCacheDoc>,
    content_ids: &[u64],
    zone_key: &str,
) -> anyhow::Result<HashMap<u64, ZoneCache>> {
    let ids = content_ids.iter().map(|id| *id as i64).collect::<Vec<_>>();
    let cursor = collection
        .find(doc! { "content_id": { "$in": ids } }, None)
        .await?;

    let docs = cursor
        .filter_map(async |result| result.ok())
        .collect::<Vec<_>>()
        .await;

    let mut caches = HashMap::new();
    for doc in docs {
        if let Some(zone_cache) = doc.zones.get(zone_key) {
            caches.insert(doc.content_id as u64, zone_cache.clone());
        }
    }

    Ok(caches)
}

pub async fn get_parse_docs(
    collection: Collection<ParseCacheDoc>,
    content_ids: &[u64],
) -> anyhow::Result<HashMap<u64, ParseCacheDoc>> {
    let ids = content_ids.iter().map(|id| *id as i64).collect::<Vec<_>>();
    let cursor = collection
        .find(doc! { "content_id": { "$in": ids } }, None)
        .await?;

    let docs = cursor
        .filter_map(async |result| result.ok())
        .collect::<Vec<_>>()
        .await;

    let mut parse_docs = HashMap::new();
    for doc in docs {
        parse_docs.insert(doc.content_id as u64, doc);
    }

    Ok(parse_docs)
}

pub async fn upsert_zone_cache(
    collection: Collection<ParseCacheDoc>,
    content_id: u64,
    zone_key: &str,
    zone_cache: &ZoneCache,
) -> anyhow::Result<()> {
    let zone_path = format!("zones.{}", zone_key);
    let zone_bson = mongodb::bson::to_bson(zone_cache)?;

    collection
        .update_one(
            doc! { "content_id": content_id as i64 },
            doc! {
                "$set": { &zone_path: zone_bson },
                "$setOnInsert": { "content_id": content_id as i64 },
            },
            UpdateOptions::builder().upsert(true).build(),
        )
        .await?;

    Ok(())
}

pub async fn get_report_parse_summaries_by_zone(
    collection: Collection<ReportParseSummaryDoc>,
    zone_key: &str,
    identities: &[ReportParseIdentityKey],
) -> anyhow::Result<HashMap<ReportParseIdentityKey, ReportParseSummaryDoc>> {
    if zone_key.trim().is_empty() || identities.is_empty() {
        return Ok(HashMap::new());
    }

    let mut unique_identities = identities.to_vec();
    unique_identities.sort_by(|a, b| {
        a.normalized_name
            .cmp(&b.normalized_name)
            .then_with(|| a.home_world.cmp(&b.home_world))
    });
    unique_identities.dedup();

    let filters = unique_identities
        .iter()
        .map(|identity| {
            doc! {
                "normalized_name": &identity.normalized_name,
                "home_world": identity.home_world as i32,
            }
        })
        .collect::<Vec<_>>();

    let cursor = collection
        .find(
            doc! {
                "zone_key": zone_key,
                "$or": filters,
            },
            None,
        )
        .await?;

    let docs = cursor
        .filter_map(async |result| result.ok())
        .collect::<Vec<_>>()
        .await;

    let mut summaries = HashMap::new();
    for doc in docs {
        summaries.insert(ReportParseIdentityKey::from_summary(&doc), doc);
    }

    Ok(summaries)
}
