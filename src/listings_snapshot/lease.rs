use anyhow::Context;
use chrono::{DateTime, Utc};
use mongodb::{
    bson::{doc, Document},
    options::UpdateOptions,
    Collection,
};
use serde::{Deserialize, Serialize};

use super::is_duplicate_key_error;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListingSnapshotWorkerLeaseDoc {
    #[serde(rename = "_id")]
    pub id: String,
    pub owner_id: String,
    #[serde(with = "mongodb::bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub expires_at: DateTime<Utc>,
    #[serde(with = "mongodb::bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub updated_at: DateTime<Utc>,
}

pub fn snapshot_worker_lease_acquire_update(
    document_id: &str,
    owner_id: &str,
    now: DateTime<Utc>,
    expires_at: DateTime<Utc>,
) -> (Document, Document, UpdateOptions) {
    let filter = doc! {
        "_id": document_id,
        "$or": [
            { "expires_at": { "$lte": mongodb::bson::DateTime::from_chrono(now) } },
            { "owner_id": owner_id },
        ],
    };
    let update = doc! {
        "$set": {
            "owner_id": owner_id,
            "expires_at": mongodb::bson::DateTime::from_chrono(expires_at),
            "updated_at": mongodb::bson::DateTime::from_chrono(now),
        },
        "$setOnInsert": { "_id": document_id },
    };
    let options = UpdateOptions::builder().upsert(true).build();

    (filter, update, options)
}

pub fn snapshot_worker_lease_renew_update(
    document_id: &str,
    owner_id: &str,
    now: DateTime<Utc>,
    expires_at: DateTime<Utc>,
) -> (Document, Document, UpdateOptions) {
    let filter = doc! {
        "_id": document_id,
        "owner_id": owner_id,
        "expires_at": { "$gt": mongodb::bson::DateTime::from_chrono(now) },
    };
    let update = doc! {
        "$set": {
            "expires_at": mongodb::bson::DateTime::from_chrono(expires_at),
            "updated_at": mongodb::bson::DateTime::from_chrono(now),
        },
    };
    let options = UpdateOptions::builder().upsert(false).build();

    (filter, update, options)
}

pub fn snapshot_worker_lease_can_acquire(
    current: Option<&ListingSnapshotWorkerLeaseDoc>,
    owner_id: &str,
    now: DateTime<Utc>,
) -> bool {
    match current {
        Some(lease) => lease.expires_at <= now || lease.owner_id == owner_id,
        None => true,
    }
}

pub async fn try_acquire_snapshot_worker_lease(
    collection: &Collection<ListingSnapshotWorkerLeaseDoc>,
    id: &str,
    owner_id: &str,
    now: DateTime<Utc>,
    lease_ttl_seconds: i64,
) -> anyhow::Result<bool> {
    let expires_at = now + chrono::TimeDelta::seconds(lease_ttl_seconds.max(1));
    let (filter, update, options) =
        snapshot_worker_lease_acquire_update(id, owner_id, now, expires_at);

    match collection.update_one(filter, update, options).await {
        Ok(result) => Ok(result.matched_count == 1 || result.upserted_id.is_some()),
        Err(error) if is_duplicate_key_error(&error) => Ok(false),
        Err(error) => Err(error).context("failed to acquire listings snapshot worker lease"),
    }
}

pub async fn try_renew_snapshot_worker_lease(
    collection: &Collection<ListingSnapshotWorkerLeaseDoc>,
    id: &str,
    owner_id: &str,
    now: DateTime<Utc>,
    lease_ttl_seconds: i64,
) -> anyhow::Result<bool> {
    let expires_at = now + chrono::TimeDelta::seconds(lease_ttl_seconds.max(1));
    let (filter, update, options) =
        snapshot_worker_lease_renew_update(id, owner_id, now, expires_at);

    collection
        .update_one(filter, update, options)
        .await
        .map(|result| result.matched_count == 1)
        .context("failed to renew listings snapshot worker lease")
}
