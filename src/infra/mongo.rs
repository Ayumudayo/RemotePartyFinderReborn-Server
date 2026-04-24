use std::{collections::HashMap, time::Duration};

use anyhow::Context;
use chrono::{DateTime, TimeDelta, Utc};
use futures_util::StreamExt;
use mongodb::{
    bson::{doc, Document},
    options::{FindOptions, UpdateOptions},
    results::UpdateResult,
    Collection,
};

use crate::listing::PartyFinderListing;
use crate::listing_container::{ListingContainer, QueriedListing};

const IDENTITY_UPSERT_MAX_RETRIES: usize = 3;
const PLAYER_UPSERT_MAX_RETRIES: usize = 3;
const REPORT_PARSE_SUMMARY_LOOKUP_MAX_TIME: Duration = Duration::from_secs(2);

fn is_valid_world_id(world_id: u16) -> bool {
    world_id < 1_000
}

fn strip_detail_managed_listing_fields(listing_doc: &mut Document) {
    listing_doc.remove("member_content_ids");
    listing_doc.remove("member_jobs");
    listing_doc.remove("detail_slot_flags");
    listing_doc.remove("leader_content_id");
}

fn is_duplicate_key_error(error: &mongodb::error::Error) -> bool {
    match error.kind.as_ref() {
        mongodb::error::ErrorKind::Write(mongodb::error::WriteFailure::WriteError(write_error)) => {
            write_error.code == 11_000
        }
        mongodb::error::ErrorKind::BulkWrite(bulk_failure) => bulk_failure
            .write_errors
            .as_ref()
            .is_some_and(|errors| errors.iter().any(|error| error.code == 11_000)),
        mongodb::error::ErrorKind::Command(command_error) => command_error.code == 11_000,
        _ => false,
    }
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
    ObservedAtTooFarInFuture,
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

fn build_player_upsert_documents(
    op: &PreparedPlayerUpsert,
    now: DateTime<Utc>,
) -> (Document, Document) {
    let mut set_doc = doc! {
        "account_id": op.account_id.clone(),
        "name": op.name.clone(),
        "last_seen": now,
        "identity_observed_at": now,
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

    (set_doc, set_on_insert_doc)
}

pub fn prepare_character_identity_upsert(
    identity: &crate::player::UploadableCharacterIdentity,
    now: DateTime<Utc>,
    max_future_skew_seconds: i64,
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

    let max_allowed_observed_at = now + TimeDelta::seconds(max_future_skew_seconds.max(0));
    if identity.observed_at > max_allowed_observed_at {
        return Err(CharacterIdentityRejectReason::ObservedAtTooFarInFuture);
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
            let (set_doc, set_on_insert_doc) = build_player_upsert_documents(&op, now);
            let update = doc! {
                "$set": set_doc,
                "$inc": { "seen_count": 1 },
                "$setOnInsert": set_on_insert_doc,
            };

            for _ in 0..PLAYER_UPSERT_MAX_RETRIES {
                match collection
                    .update_one(
                        doc! { "content_id": op.content_id as i64 },
                        update.clone(),
                        UpdateOptions::builder().upsert(true).build(),
                    )
                    .await
                {
                    Ok(_) => return Ok(()),
                    Err(error) if is_duplicate_key_error(&error) => continue,
                    Err(error) => return Err((content_id, error)),
                }
            }

            Err((
                content_id,
                mongodb::error::Error::from(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "player content_id upsert retries exhausted",
                )),
            ))
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

fn get_last_seen(document: &Document) -> Option<DateTime<Utc>> {
    document
        .get_datetime("last_seen")
        .ok()
        .map(|value| value.to_chrono())
}

fn get_player_freshness(document: &Document) -> Option<DateTime<Utc>> {
    get_identity_observed_at(document).or_else(|| get_last_seen(document))
}

fn build_identity_compare_and_set_filter(
    content_id: u64,
    existing_doc: Option<&Document>,
) -> Document {
    match existing_doc.and_then(get_identity_observed_at) {
        Some(identity_observed_at) => doc! {
            "content_id": content_id as i64,
            "identity_observed_at": identity_observed_at,
        },
        None => match existing_doc.and_then(get_last_seen) {
            Some(last_seen) => doc! {
                "content_id": content_id as i64,
                "last_seen": last_seen,
                "$or": [
                    { "identity_observed_at": { "$exists": false } },
                    { "identity_observed_at": mongodb::bson::Bson::Null },
                ],
            },
            None => doc! {
                "content_id": content_id as i64,
                "$or": [
                    { "identity_observed_at": { "$exists": false } },
                    { "identity_observed_at": mongodb::bson::Bson::Null },
                ],
            },
        },
    }
}

fn build_identity_upsert_update(
    content_id: u64,
    merged: &crate::player::IdentityMergeResult,
) -> Document {
    let mut set_doc = doc! {
        "name": merged.player.name.clone(),
        "home_world": merged.player.home_world as u32,
        "current_world": merged.player.current_world as u32,
        "last_seen": merged.player.last_seen,
        "seen_count": merged.player.seen_count as u32,
        "account_id": merged.player.account_id.clone(),
        "identity_observed_at": merged.identity_observed_at,
    };

    if merged.applied_incoming_identity {
        set_doc.insert("world_name", merged.world_name.clone());
        set_doc.insert(
            "identity_source",
            if merged.source.trim().is_empty() {
                "unknown".to_string()
            } else {
                merged.source.clone()
            },
        );
    }

    doc! {
        "$set": set_doc,
        "$setOnInsert": {
            "content_id": content_id as i64,
        },
    }
}

pub async fn upsert_character_identities(
    collection: Collection<crate::player::Player>,
    identities: &[crate::player::UploadableCharacterIdentity],
    concurrency: usize,
    max_future_skew_seconds: i64,
) -> anyhow::Result<PlayerUpsertReport> {
    let mut prepared = Vec::with_capacity(identities.len());
    let mut invalid_missing_content_id = 0usize;
    let mut invalid_missing_name = 0usize;
    let mut invalid_home_world = 0usize;
    let mut invalid_future_observed_at = 0usize;
    let now = Utc::now();

    for identity in identities {
        match prepare_character_identity_upsert(identity, now, max_future_skew_seconds) {
            Ok(op) => prepared.push(op),
            Err(CharacterIdentityRejectReason::MissingContentId) => invalid_missing_content_id += 1,
            Err(CharacterIdentityRejectReason::MissingName) => invalid_missing_name += 1,
            Err(CharacterIdentityRejectReason::InvalidHomeWorld) => invalid_home_world += 1,
            Err(CharacterIdentityRejectReason::ObservedAtTooFarInFuture) => {
                invalid_future_observed_at += 1
            }
        }
    }

    let invalid = invalid_missing_content_id
        + invalid_missing_name
        + invalid_home_world
        + invalid_future_observed_at;
    if invalid > 0 {
        tracing::debug!(
            requested = identities.len(),
            invalid,
            invalid_missing_content_id,
            invalid_missing_name,
            invalid_home_world,
            invalid_future_observed_at,
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
            for _ in 0..IDENTITY_UPSERT_MAX_RETRIES {
                let read_filter = doc! { "content_id": content_id as i64 };
                let existing_doc: Option<Document> = raw_collection
                    .find_one(read_filter, None)
                    .await
                    .map_err(|error| (content_id, anyhow::Error::new(error)))?;
                let existing_player = existing_doc.as_ref().and_then(|document| {
                    mongodb::bson::from_document::<crate::player::Player>(document.clone()).ok()
                });
                let existing_identity_observed_at =
                    existing_doc.as_ref().and_then(get_player_freshness);
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

                if !merged.applied_incoming_identity {
                    return Ok(());
                }

                let compare_and_set_filter =
                    build_identity_compare_and_set_filter(content_id, existing_doc.as_ref());
                let update = build_identity_upsert_update(content_id, &merged);
                let write_result = collection
                    .update_one(
                        compare_and_set_filter,
                        update,
                        UpdateOptions::builder().upsert(true).build(),
                    )
                    .await;

                let write_result = match write_result {
                    Ok(result) => result,
                    Err(error) if is_duplicate_key_error(&error) => continue,
                    Err(error) => return Err((content_id, anyhow::Error::new(error))),
                };

                if write_result.modified_count > 0
                    || write_result.matched_count > 0
                    || write_result.upserted_id.is_some()
                {
                    return Ok(());
                }
            }

            Err((
                content_id,
                anyhow::anyhow!("character identity compare-and-set retries exhausted"),
            ))
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
pub use crate::report_parse::{
    ReportParseIdentityKey, ReportParseSummaryDoc, ReportParseZoneSummary,
};

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
) -> anyhow::Result<HashMap<ReportParseIdentityKey, ReportParseZoneSummary>> {
    if zone_key.trim().is_empty() || identities.is_empty() {
        return Ok(HashMap::new());
    }

    let Some(filter) = report_parse_summary_identity_filter(identities) else {
        return Ok(HashMap::new());
    };
    let options = report_parse_summary_find_options(zone_key);
    let cursor = collection.find(filter, Some(options)).await?;

    let docs = cursor
        .filter_map(async |result| result.ok())
        .collect::<Vec<_>>()
        .await;

    let mut summaries = HashMap::new();
    for doc in docs {
        if let Some(zone_summary) = doc.zones.get(zone_key) {
            summaries.insert(
                ReportParseIdentityKey::from_summary(&doc),
                zone_summary.clone(),
            );
        }
    }

    Ok(summaries)
}

fn report_parse_summary_identity_filter(identities: &[ReportParseIdentityKey]) -> Option<Document> {
    let mut unique_identities = identities.to_vec();
    unique_identities.sort_by(|a, b| {
        a.normalized_name
            .cmp(&b.normalized_name)
            .then_with(|| a.home_world.cmp(&b.home_world))
    });
    unique_identities.dedup();
    if unique_identities.is_empty() {
        return None;
    }

    let filters = unique_identities
        .iter()
        .map(|identity| {
            doc! {
                "normalized_name": &identity.normalized_name,
                "home_world": identity.home_world as i32,
            }
        })
        .collect::<Vec<_>>();

    Some(doc! { "$or": filters })
}

fn report_parse_summary_find_options(zone_key: &str) -> FindOptions {
    FindOptions::builder()
        .projection(doc! {
            "normalized_name": 1,
            "display_name": 1,
            "home_world": 1,
            "updated_at": 1,
            format!("zones.{}", zone_key): 1,
        })
        .max_time(REPORT_PARSE_SUMMARY_LOOKUP_MAX_TIME)
        .build()
}

#[cfg(test)]
mod tests {
    use super::{
        build_identity_compare_and_set_filter, build_identity_upsert_update,
        build_player_upsert_documents, is_duplicate_key_error, report_parse_summary_find_options,
        report_parse_summary_identity_filter, PreparedPlayerUpsert,
    };
    use chrono::Utc;
    use mongodb::bson::doc;

    #[test]
    fn identity_player_upsert_sets_shared_freshness_metadata() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-04-12T12:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let prepared = PreparedPlayerUpsert {
            content_id: 1001,
            name: "Known Player".to_string(),
            home_world: 74,
            current_world: 79,
            account_id: "123".to_string(),
        };

        let (set_doc, set_on_insert_doc) = build_player_upsert_documents(&prepared, now);

        assert_eq!(set_doc.get_datetime("last_seen").unwrap().to_chrono(), now);
        assert_eq!(
            set_doc
                .get_datetime("identity_observed_at")
                .unwrap()
                .to_chrono(),
            now
        );
        assert_eq!(set_on_insert_doc.get_i64("content_id").unwrap(), 1001);
    }

    #[test]
    fn identity_compare_and_set_filter_uses_previously_read_freshness() {
        let freshness = chrono::DateTime::parse_from_rfc3339("2026-04-12T12:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let existing = doc! {
            "content_id": 6006i64,
            "identity_observed_at": freshness,
        };

        let filter = build_identity_compare_and_set_filter(6006, Some(&existing));

        assert_eq!(filter.get_i64("content_id").unwrap(), 6006);
        assert_eq!(
            filter
                .get_datetime("identity_observed_at")
                .unwrap()
                .to_chrono(),
            freshness
        );
    }

    #[test]
    fn identity_compare_and_set_filter_falls_back_to_legacy_last_seen_when_freshness_missing() {
        let last_seen = chrono::DateTime::parse_from_rfc3339("2026-04-12T12:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let existing = doc! {
            "content_id": 7007i64,
            "last_seen": last_seen,
        };

        let filter = build_identity_compare_and_set_filter(7007, Some(&existing));

        assert_eq!(filter.get_i64("content_id").unwrap(), 7007);
        assert_eq!(
            filter.get_datetime("last_seen").unwrap().to_chrono(),
            last_seen
        );
        assert_eq!(filter.get_array("$or").unwrap().len(), 2);
    }

    #[test]
    fn identity_upsert_update_keeps_content_id_out_of_set_clause() {
        let observed_at = chrono::DateTime::parse_from_rfc3339("2026-04-12T12:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let merged = crate::player::IdentityMergeResult {
            player: crate::player::Player {
                content_id: 8008,
                name: "Known Player".to_string(),
                home_world: 74,
                current_world: 79,
                last_seen: observed_at,
                seen_count: 2,
                account_id: "123".to_string(),
            },
            identity_observed_at: observed_at,
            world_name: "Cerberus".to_string(),
            source: "plugin".to_string(),
            applied_incoming_identity: true,
        };

        let update = build_identity_upsert_update(8008, &merged);
        let set_doc = update.get_document("$set").unwrap();
        let set_on_insert_doc = update.get_document("$setOnInsert").unwrap();

        assert!(set_doc.get("content_id").is_none());
        assert_eq!(set_on_insert_doc.get_i64("content_id").unwrap(), 8008);
    }

    #[test]
    fn identity_duplicate_key_errors_are_retryable() {
        let write_error: mongodb::error::WriteError = serde_json::from_str(
            r#"{"code":11000,"codeName":"DuplicateKey","errmsg":"E11000 duplicate key error"}"#,
        )
        .unwrap();
        let error = mongodb::error::Error::from(mongodb::error::ErrorKind::Write(
            mongodb::error::WriteFailure::WriteError(write_error),
        ));

        assert!(is_duplicate_key_error(&error));
    }

    #[test]
    fn report_parse_summary_filter_targets_identity_index_only() {
        let identities = vec![
            crate::report_parse::ReportParseIdentityKey::new("Alice Example", 74),
            crate::report_parse::ReportParseIdentityKey::new("Alice Example", 74),
            crate::report_parse::ReportParseIdentityKey::new("Bob Example", 80),
        ];

        let filter = report_parse_summary_identity_filter(&identities)
            .expect("non-empty identities should produce a filter");

        assert_eq!(
            filter,
            doc! {
                "$or": [
                    { "normalized_name": "alice example", "home_world": 74i32 },
                    { "normalized_name": "bob example", "home_world": 80i32 },
                ],
            }
        );
        assert!(!filter.contains_key("zones.73:101:1"));
    }

    #[test]
    fn report_parse_summary_find_options_project_requested_zone_and_timeout() {
        let options = report_parse_summary_find_options("73:101:1");
        let projection = options
            .projection
            .expect("summary query should project only required fields");

        assert_eq!(projection.get_i32("normalized_name").unwrap(), 1);
        assert_eq!(projection.get_i32("display_name").unwrap(), 1);
        assert_eq!(projection.get_i32("home_world").unwrap(), 1);
        assert_eq!(projection.get_i32("updated_at").unwrap(), 1);
        assert_eq!(projection.get_i32("zones.73:101:1").unwrap(), 1);
        assert!(options.max_time.is_some());
    }
}
