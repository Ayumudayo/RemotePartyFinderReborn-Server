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

use super::is_valid_world_id;
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
