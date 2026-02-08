use anyhow::Context;
use crate::listing::PartyFinderListing;
use crate::listing_container::{ListingContainer, QueriedListing};
use chrono::{TimeDelta, Utc};
use futures_util::StreamExt;
use mongodb::bson::doc;
use mongodb::results::UpdateResult;
use mongodb::Collection;
use mongodb::options::UpdateOptions;

pub async fn get_current_listings(
    collection: Collection<ListingContainer>,
) -> anyhow::Result<Vec<QueriedListing>> {
    let one_hour_ago = Utc::now() - TimeDelta::try_hours(1).unwrap();
    let cursor = collection
        .aggregate(
            [
                // don't ask me why, but mongo shits itself unless you provide a hard date
                // doc! {
                //     "$match": {
                //         "created_at": {
                //             "$gte": {
                //                 "$dateSubtract": {
                //                     "startDate": "$$NOW",
                //                     "unit": "hour",
                //                     "amount": 2,
                //                 },
                //             },
                //         },
                //     }
                // },
                doc! {
                    "$match": {
                        "updated_at": { "$gte": one_hour_ago },
                    }
                },
                doc! {
                    "$match": {
                        // filter private pfs
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

    let collect = cursor
        .filter_map(async |res| {
            res.ok()
                .and_then(|doc| mongodb::bson::from_document(doc).ok())
        })
        .collect::<Vec<_>>()
        .await;

    Ok(collect)
}

pub async fn insert_listing(
    collection: Collection<ListingContainer>,
    listing: &PartyFinderListing,
) -> anyhow::Result<UpdateResult> {
    if listing.created_world >= 1_000
        || listing.home_world >= 1_000
        || listing.current_world >= 1_000
    {
        anyhow::bail!("invalid listing");
    }

    let opts = UpdateOptions::builder().upsert(true).build();
    let bson_value = mongodb::bson::to_bson(&listing)?;
    let now = Utc::now();
    collection
        .update_one(
            doc! {
                "listing.id": listing.id,
                "listing.last_server_restart": listing.last_server_restart,
                "listing.created_world": listing.created_world as u32,
            },
            doc! {
                "$currentDate": {
                    "updated_at": true,
                },
                "$set": {
                    "listing": bson_value,
                },
                "$setOnInsert": {
                    "created_at": now,
                },
            },
            opts,
        )
        .await
        .context("could not insert record")
}

/// 플레이어 정보를 upsert (있으면 업데이트, 없으면 삽입)
pub async fn upsert_players(
    collection: Collection<crate::player::Player>,
    players: &[crate::player::UploadablePlayer],
) -> anyhow::Result<usize> {
    let mut successful = 0;
    let now = Utc::now();

    for player in players {
        if player.content_id == 0 || player.name.is_empty() || player.home_world >= 1_000 {
            continue;
        }

        let opts = UpdateOptions::builder().upsert(true).build();
        let result = collection
            .update_one(
                doc! { "content_id": player.content_id as i64 },
                doc! {
                    "$set": {
                        "name": &player.name,
                        "home_world": player.home_world as u32,
                        "last_seen": now,
                    },
                    "$inc": { "seen_count": 1 },
                    "$setOnInsert": {
                        "content_id": player.content_id as i64,
                    },
                },
                opts,
            )
            .await;

        if result.is_ok() {
            successful += 1;
        }
    }

    Ok(successful)
}

/// ContentID 목록으로 플레이어 정보 조회
pub async fn get_players_by_content_ids(
    collection: Collection<crate::player::Player>,
    content_ids: &[u64],
) -> anyhow::Result<Vec<crate::player::Player>> {
    let ids: Vec<i64> = content_ids.iter().map(|&id| id as i64).collect();
    
    let cursor = collection
        .find(doc! { "content_id": { "$in": ids } }, None)
        .await?;

    let players = cursor
        .filter_map(async |res| {
            match res {
                Ok(p) => Some(p),
                Err(e) => {
                    tracing::warn!("Error reading player: {:?}", e);
                    None
                }
            }
        })
        .collect::<Vec<_>>()
        .await;

    Ok(players)
}

/// 최근 활성 플레이어 전체 조회 (last_seen 7일 이내)
pub async fn get_all_active_players(
    collection: Collection<crate::player::Player>,
) -> anyhow::Result<Vec<crate::player::Player>> {
    let seven_days_ago = Utc::now() - TimeDelta::try_days(7).unwrap();
    
    let cursor = collection
        .find(doc! { "last_seen": { "$gte": seven_days_ago } }, None)
        .await?;

    let players = cursor
        .filter_map(async |res| res.ok())
        .collect::<Vec<_>>()
        .await;

    Ok(players)
}

// =============================================================================
// FFLogs Parse 캐시 (타입은 fflogs::cache에 정의됨)
// =============================================================================

use std::collections::HashMap;
pub use crate::fflogs::cache::{ParseCacheDoc, ZoneCache, EncounterParse, is_zone_cache_expired};

/// 플레이어의 특정 Zone 캐시 조회
pub async fn get_zone_cache(
    collection: Collection<ParseCacheDoc>,
    content_id: u64,
    zone_id: u32,
) -> anyhow::Result<Option<ZoneCache>> {
    let zone_key = zone_id.to_string();
    
    let doc = collection
        .find_one(
            doc! { "content_id": content_id as i64 },
            None,
        )
        .await?;
    
    Ok(doc.and_then(|d| d.zones.get(&zone_key).cloned()))
}

/// 여러 플레이어의 특정 Zone 캐시 일괄 조회
pub async fn get_zone_caches(
    collection: Collection<ParseCacheDoc>,
    content_ids: &[u64],
    zone_id: u32,
) -> anyhow::Result<HashMap<u64, ZoneCache>> {
    let ids: Vec<i64> = content_ids.iter().map(|&id| id as i64).collect();
    let zone_key = zone_id.to_string();
    
    let cursor = collection
        .find(
            doc! { "content_id": { "$in": ids } },
            None,
        )
        .await?;
    
    let docs: Vec<ParseCacheDoc> = cursor
        .filter_map(async |res| res.ok())
        .collect::<Vec<_>>()
        .await;
    
    let mut result = HashMap::new();
    for doc in docs {
        if let Some(zone_cache) = doc.zones.get(&zone_key) {
            result.insert(doc.content_id as u64, zone_cache.clone());
        }
    }
    
    Ok(result)
}

/// 여러 플레이어의 전체 Parse 데이터 일괄 조회 (배치 최적화용)
pub async fn get_parse_docs(
    collection: Collection<ParseCacheDoc>,
    content_ids: &[u64],
) -> anyhow::Result<HashMap<u64, ParseCacheDoc>> {
    let ids: Vec<i64> = content_ids.iter().map(|&id| id as i64).collect();
    
    let cursor = collection
        .find(
            doc! { "content_id": { "$in": ids } },
            None,
        )
        .await?;
    
    let docs: Vec<ParseCacheDoc> = cursor
        .filter_map(async |res| res.ok())
        .collect::<Vec<_>>()
        .await;
    
    let mut result = HashMap::new();
    for doc in docs {
        result.insert(doc.content_id as u64, doc);
    }
    
    Ok(result)
}

/// Zone 전체 캐시 저장/업데이트
/// 
/// content_id 문서가 없으면 생성, 있으면 해당 zone만 갱신
pub async fn upsert_zone_cache(
    collection: Collection<ParseCacheDoc>,
    content_id: u64,
    zone_id: u32,
    zone_cache: &ZoneCache,
) -> anyhow::Result<()> {
    let opts = UpdateOptions::builder().upsert(true).build();
    let zone_key = format!("zones.{}", zone_id);
    
    // BSON으로 변환
    let zone_bson = mongodb::bson::to_bson(zone_cache)?;
    
    collection
        .update_one(
            doc! { "content_id": content_id as i64 },
            doc! {
                "$set": { &zone_key: zone_bson },
                "$setOnInsert": { "content_id": content_id as i64 },
            },
            opts,
        )
        .await?;
    
    Ok(())
}

// Note: 유저 요청에 따라 Parse 데이터에 대한 자동 삭제(TTL) 로직은 제거함.
// 데이터는 오직 갱신(overwrite)만 되며, 유실되지 않음.

