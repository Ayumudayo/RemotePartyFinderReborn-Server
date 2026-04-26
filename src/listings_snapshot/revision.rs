use crate::web::State;
use anyhow::Context;
use chrono::{DateTime, Utc};
use mongodb::{
    bson::{doc, Document},
    options::{FindOneAndUpdateOptions, ReturnDocument, UpdateOptions},
    Collection,
};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListingSourceStateDoc {
    #[serde(rename = "_id")]
    pub id: String,
    pub revision: i64,
    #[serde(with = "mongodb::bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListingSnapshotRevisionStateDoc {
    #[serde(rename = "_id")]
    pub id: String,
    pub revision: i64,
    #[serde(with = "mongodb::bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub updated_at: DateTime<Utc>,
}

pub async fn load_listing_source_state(
    collection: &Collection<ListingSourceStateDoc>,
    id: &str,
) -> anyhow::Result<Option<ListingSourceStateDoc>> {
    collection
        .find_one(doc! { "_id": id }, None)
        .await
        .context("failed to load listing source state")
}

pub fn listing_source_revision_increment_update(
    document_id: &str,
    updated_at: DateTime<Utc>,
) -> (Document, Document, UpdateOptions) {
    let filter = doc! { "_id": document_id };
    let update = doc! {
        "$inc": { "revision": 1_i64 },
        "$set": { "updated_at": mongodb::bson::DateTime::from_chrono(updated_at) },
        "$setOnInsert": { "_id": document_id },
    };
    let options = UpdateOptions::builder().upsert(true).build();

    (filter, update, options)
}

pub async fn increment_listing_source_revision(state: &State) -> anyhow::Result<i64> {
    let (filter, update, _) = listing_source_revision_increment_update(
        &state.listing_source_state_document_id,
        Utc::now(),
    );
    let options = FindOneAndUpdateOptions::builder()
        .upsert(true)
        .return_document(ReturnDocument::After)
        .build();

    let doc = state
        .listing_source_state_collection()
        .find_one_and_update(filter, update, options)
        .await
        .context("failed to increment listing source revision")?
        .context("listing source revision update returned no document")?;

    Ok(doc.revision)
}

pub fn materialized_revision_allocate_update(
    document_id: &str,
    updated_at: DateTime<Utc>,
) -> (Document, Document, FindOneAndUpdateOptions) {
    let filter = doc! { "_id": document_id };
    let update = doc! {
        "$inc": { "revision": 1_i64 },
        "$set": { "updated_at": mongodb::bson::DateTime::from_chrono(updated_at) },
        "$setOnInsert": { "_id": document_id },
    };
    let options = FindOneAndUpdateOptions::builder()
        .upsert(true)
        .return_document(ReturnDocument::After)
        .build();

    (filter, update, options)
}

pub fn materialized_revision_seed_update(
    document_id: &str,
    updated_at: DateTime<Utc>,
    minimum_revision: i64,
) -> (Document, Document, UpdateOptions) {
    let filter = doc! { "_id": document_id };
    let update = doc! {
        "$max": { "revision": minimum_revision },
        "$set": { "updated_at": mongodb::bson::DateTime::from_chrono(updated_at) },
        "$setOnInsert": { "_id": document_id },
    };
    let options = UpdateOptions::builder().upsert(true).build();

    (filter, update, options)
}

pub async fn ensure_materialized_revision_at_least(
    collection: &Collection<ListingSnapshotRevisionStateDoc>,
    id: &str,
    minimum_revision: i64,
) -> anyhow::Result<()> {
    let (filter, update, options) =
        materialized_revision_seed_update(id, Utc::now(), minimum_revision.max(0));
    collection
        .update_one(filter, update, options)
        .await
        .context("failed to seed materialized listings snapshot revision")?;

    Ok(())
}

pub async fn allocate_materialized_revision(
    collection: &Collection<ListingSnapshotRevisionStateDoc>,
    id: &str,
) -> anyhow::Result<i64> {
    let (filter, update, options) = materialized_revision_allocate_update(id, Utc::now());
    let doc = collection
        .find_one_and_update(filter, update, options)
        .await
        .context("failed to allocate materialized listings snapshot revision")?
        .context("materialized listings snapshot revision allocation returned no document")?;

    Ok(doc.revision)
}
