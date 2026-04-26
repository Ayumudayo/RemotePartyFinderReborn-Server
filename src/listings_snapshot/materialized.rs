use crate::web::{CachedListingsSnapshot, State};
use anyhow::Context;
use chrono::{DateTime, Utc};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use mongodb::{
    bson::{doc, spec::BinarySubtype, Binary, Document},
    options::UpdateOptions,
    Collection,
};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};

use super::{is_duplicate_key_error, BuiltListingsSnapshot};
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializedListingsSnapshotDoc {
    #[serde(rename = "_id")]
    pub id: String,
    pub revision: i64,
    pub source_revision: i64,
    pub etag: String,
    pub payload_hash: String,
    pub content_type: String,
    pub content_encoding: String,
    #[serde(with = "mongodb::bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub generated_at: DateTime<Utc>,
    pub body_gzip: Binary,
}

pub fn gzip_body(body: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(body)
        .context("failed to write gzip body")?;
    encoder.finish().context("failed to finish gzip body")
}

pub fn gunzip_body(body: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut decoder = GzDecoder::new(body);
    let mut decoded = Vec::new();
    decoder
        .read_to_end(&mut decoded)
        .context("failed to read gzip body")?;
    Ok(decoded)
}

pub fn materialized_doc_from_snapshot(
    document_id: &str,
    snapshot: BuiltListingsSnapshot,
    source_revision: i64,
) -> anyhow::Result<MaterializedListingsSnapshotDoc> {
    let body_gzip = gzip_body(&snapshot.body)?;

    Ok(MaterializedListingsSnapshotDoc {
        id: document_id.to_string(),
        revision: snapshot.revision,
        source_revision,
        etag: snapshot.etag,
        payload_hash: snapshot.payload_hash,
        content_type: "application/json; charset=utf-8".to_string(),
        content_encoding: "gzip".to_string(),
        generated_at: Utc::now(),
        body_gzip: Binary {
            subtype: BinarySubtype::Generic,
            bytes: body_gzip,
        },
    })
}

pub async fn load_current_materialized_doc(
    collection: &Collection<MaterializedListingsSnapshotDoc>,
    id: &str,
) -> anyhow::Result<Option<MaterializedListingsSnapshotDoc>> {
    collection
        .find_one(doc! { "_id": id }, None)
        .await
        .context("failed to load current materialized listings snapshot document")
}

pub fn cached_snapshot_from_materialized_doc(
    doc: &MaterializedListingsSnapshotDoc,
) -> anyhow::Result<CachedListingsSnapshot> {
    Ok(CachedListingsSnapshot {
        revision: doc
            .revision
            .try_into()
            .context("materialized snapshot revision must be non-negative")?,
        body: doc.body_gzip.bytes.clone().into(),
        etag: Some(doc.etag.clone()),
        content_encoding: (!doc.content_encoding.trim().is_empty())
            .then(|| doc.content_encoding.clone()),
    })
}

pub async fn load_current_materialized_snapshot(
    state: &State,
) -> anyhow::Result<Option<CachedListingsSnapshot>> {
    let collection = state.listings_snapshot_collection();
    let doc = load_current_materialized_doc(&collection, &state.listings_snapshot_document_id)
        .await
        .context("failed to load current materialized listings snapshot")?;

    doc.as_ref()
        .map(cached_snapshot_from_materialized_doc)
        .transpose()
}

pub fn materialized_snapshot_cas_filter(
    document_id: &str,
    revision: i64,
    payload_hash: &str,
) -> Document {
    doc! {
        "_id": document_id,
        "$and": [
            {
                "$or": [
                    { "revision": { "$lt": revision } },
                    { "revision": { "$exists": false } },
                ],
            },
            {
                "$or": [
                    { "payload_hash": { "$ne": payload_hash } },
                    { "payload_hash": { "$exists": false } },
                ],
            },
        ],
    }
}

pub fn materialized_snapshot_cas_update(
    document_id: &str,
    built: BuiltListingsSnapshot,
    source_revision: i64,
) -> anyhow::Result<(Document, Document, UpdateOptions)> {
    let filter = materialized_snapshot_cas_filter(document_id, built.revision, &built.payload_hash);
    let doc = materialized_doc_from_snapshot(document_id, built, source_revision)?;
    let mut set_doc = mongodb::bson::to_document(&doc)
        .context("failed to serialize materialized listings snapshot document")?;
    set_doc.remove("_id");
    let update = doc! {
        "$set": set_doc,
        "$setOnInsert": { "_id": document_id },
    };
    let options = UpdateOptions::builder().upsert(true).build();

    Ok((filter, update, options))
}

pub async fn try_write_materialized_snapshot_cas(
    collection: &Collection<MaterializedListingsSnapshotDoc>,
    id: &str,
    built: BuiltListingsSnapshot,
    source_revision: i64,
) -> anyhow::Result<bool> {
    let (filter, update, options) = materialized_snapshot_cas_update(id, built, source_revision)?;
    match collection.update_one(filter, update, options).await {
        Ok(result) => Ok(result.matched_count == 1 || result.upserted_id.is_some()),
        Err(error) if is_duplicate_key_error(&error) => Ok(false),
        Err(error) => Err(error).context("failed to write materialized listings snapshot"),
    }
}
