use std::collections::HashMap;

use futures_util::StreamExt;
use mongodb::{
    bson::{doc, Document},
    options::{FindOneOptions, FindOptions, UpdateOptions},
    Collection,
};

pub use crate::fflogs::cache::{
    is_zone_cache_expired, is_zone_cache_expired_with_hidden_ttl_hours, EncounterParse,
    ParseCacheDoc, ZoneCache,
};
pub async fn get_zone_cache(
    collection: Collection<ParseCacheDoc>,
    content_id: u64,
    zone_key: &str,
) -> anyhow::Result<Option<ZoneCache>> {
    let cache_doc = collection
        .find_one(
            doc! { "content_id": content_id as i64 },
            Some(zone_cache_find_one_options(zone_key)),
        )
        .await?;

    Ok(cache_doc.and_then(|doc| doc.zones.get(zone_key).cloned()))
}

fn zone_cache_projection(zone_key: &str) -> Document {
    let mut projection = doc! {
        "content_id": 1,
    };
    projection.insert(format!("zones.{zone_key}"), 1);
    projection
}

fn zone_cache_find_one_options(zone_key: &str) -> FindOneOptions {
    FindOneOptions::builder()
        .projection(zone_cache_projection(zone_key))
        .build()
}

pub(super) fn zone_cache_find_options(zone_key: &str) -> FindOptions {
    FindOptions::builder()
        .projection(zone_cache_projection(zone_key))
        .build()
}

pub async fn get_zone_caches(
    collection: Collection<ParseCacheDoc>,
    content_ids: &[u64],
    zone_key: &str,
) -> anyhow::Result<HashMap<u64, ZoneCache>> {
    let ids = content_ids.iter().map(|id| *id as i64).collect::<Vec<_>>();
    let cursor = collection
        .find(
            doc! { "content_id": { "$in": ids } },
            Some(zone_cache_find_options(zone_key)),
        )
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
