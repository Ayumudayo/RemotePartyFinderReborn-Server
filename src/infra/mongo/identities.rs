use chrono::{DateTime, TimeDelta, Utc};
use futures_util::StreamExt;
use mongodb::{
    bson::{doc, Document},
    options::UpdateOptions,
    Collection,
};

use super::players::PlayerUpsertReport;
use super::{is_duplicate_key_error, is_valid_world_id};

const IDENTITY_UPSERT_MAX_RETRIES: usize = 3;

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
pub enum CharacterIdentityRejectReason {
    MissingContentId,
    MissingName,
    InvalidHomeWorld,
    ObservedAtTooFarInFuture,
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

pub(super) fn build_identity_compare_and_set_filter(
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

pub(super) fn build_identity_upsert_update(
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
