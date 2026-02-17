use std::collections::HashMap;

use anyhow::Context;
use chrono::{TimeDelta, Utc};
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

fn prepare_player_upsert(player: &crate::player::UploadablePlayer) -> Option<PreparedPlayerUpsert> {
    if player.content_id == 0 || player.name.is_empty() || !is_valid_world_id(player.home_world) {
        return None;
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

    Some(PreparedPlayerUpsert {
        content_id: player.content_id,
        name: player.name.clone(),
        home_world: player.home_world,
        current_world,
        account_id,
    })
}

pub async fn upsert_players(
    collection: Collection<crate::player::Player>,
    players: &[crate::player::UploadablePlayer],
    concurrency: usize,
) -> anyhow::Result<usize> {
    let prepared = players
        .iter()
        .filter_map(prepare_player_upsert)
        .collect::<Vec<_>>();

    let now = Utc::now();
    let mut successful = 0usize;

    let mut writes = futures_util::stream::iter(prepared.into_iter().map(|op| {
        let collection = collection.clone();
        let now = now.clone();
        async move {
            let mut set_doc = doc! {
                "account_id": op.account_id,
                "name": op.name,
                "last_seen": now,
            };

            if op.home_world != 0 {
                set_doc.insert("home_world", op.home_world as u32);
            }

            if op.current_world != 0 {
                set_doc.insert("current_world", op.current_world as u32);
            }

            let set_on_insert_doc = doc! {
                "content_id": op.content_id as i64,
                "home_world": op.home_world as u32,
                "current_world": op.current_world as u32,
            };

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
        }
    }))
    .buffer_unordered(concurrency.max(1));

    while let Some(result) = writes.next().await {
        if result.is_ok() {
            successful += 1;
        }
    }

    Ok(successful)
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
                tracing::warn!("Error reading player: {:?}", error);
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
    is_zone_cache_expired,
    is_zone_cache_expired_with_hidden_ttl_hours,
    EncounterParse,
    ParseCacheDoc,
    ZoneCache,
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
