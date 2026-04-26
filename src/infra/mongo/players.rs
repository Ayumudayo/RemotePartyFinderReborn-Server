use std::collections::HashSet;

use chrono::{DateTime, TimeDelta, Utc};
use futures_util::StreamExt;
use mongodb::{
    bson::{doc, Document},
    options::{FindOptions, UpdateOptions},
    Collection,
};

use super::{is_duplicate_key_error, is_valid_world_id};

const PLAYER_UPSERT_MAX_RETRIES: usize = 3;
#[derive(Clone)]
pub(super) struct PreparedPlayerUpsert {
    pub(super) content_id: u64,
    pub(super) name: String,
    pub(super) home_world: u16,
    pub(super) current_world: u16,
    pub(super) account_id: String,
}

#[derive(Debug, Clone, Copy)]
enum PlayerUpsertRejectReason {
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

pub(super) fn build_player_upsert_documents(
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

pub async fn get_players_by_content_ids(
    collection: Collection<crate::player::Player>,
    content_ids: &[u64],
) -> anyhow::Result<Vec<crate::player::Player>> {
    let as_i64 = content_ids.iter().map(|id| *id as i64).collect::<Vec<_>>();
    let cursor = collection
        .find(
            doc! { "content_id": { "$in": as_i64 } },
            Some(player_lookup_find_options()),
        )
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

    Ok(dedupe_players_by_preferred_order(players))
}

pub(super) fn player_lookup_find_options() -> FindOptions {
    FindOptions::builder()
        .sort(doc! {
            "content_id": 1,
            "identity_observed_at": -1,
            "last_seen": -1,
            "seen_count": -1,
            "_id": 1,
        })
        .build()
}

pub(super) fn dedupe_players_by_preferred_order(
    players: Vec<crate::player::Player>,
) -> Vec<crate::player::Player> {
    let mut seen = HashSet::new();
    players
        .into_iter()
        .filter(|player| seen.insert(player.content_id))
        .collect()
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
