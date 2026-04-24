use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use mongodb::{options::IndexOptions, Client as MongoClient, Collection, IndexModel};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::broadcast::Sender;
use tokio::sync::{Notify, RwLock};
use uuid::Uuid;
use warp::hyper::body::Bytes;

use crate::config::Config;
use crate::listing_container::ListingContainer;
use crate::player::Player;
use crate::stats::CachedStatistics;

pub mod background;
pub mod handlers;
pub mod ingest_guard;
pub mod routes;

pub const FFLOGS_LEASE_TTL_MINUTES: i64 = 3;
pub const FFLOGS_LEASE_SWEEP_INTERVAL_SECONDS: u64 = 30;

pub async fn start(config: Arc<Config>) -> Result<()> {
    let state = State::new(Arc::clone(&config)).await?;

    // Background tasks
    background::spawn_stats_task(Arc::clone(&state));
    background::spawn_fflogs_lease_sweeper_task(Arc::clone(&state));
    background::spawn_listings_revision_publisher_task(Arc::clone(&state));
    background::spawn_monitor_snapshot_task(Arc::clone(&state));

    tracing::info!("listening at {}", config.web.host);
    warp::serve(routes::router(state))
        .run(config.web.host)
        .await;
    Ok(())
}

pub struct State {
    pub mongo: MongoClient,
    pub stats: RwLock<Option<CachedStatistics>>,
    pub listings_change_channel: Sender<u64>,
    pub listings_revision: AtomicU64,
    pub listings_revision_pending: AtomicBool,
    pub listings_revision_notify: Notify,
    pub listings_revision_coalesce_window: Duration,
    pub listings_snapshot_cache: RwLock<ListingsSnapshotCacheState>,
    pub fflogs_jobs_limit: usize,
    pub fflogs_hidden_cache_ttl_hours: i64,
    pub listing_upsert_concurrency: usize,
    pub player_upsert_concurrency: usize,
    pub max_body_bytes_contribute: u64,
    pub max_body_bytes_multiple: u64,
    pub max_body_bytes_players: u64,
    pub max_body_bytes_detail: u64,
    pub max_body_bytes_fflogs_results: u64,
    pub max_multiple_batch_size: usize,
    pub max_players_batch_size: usize,
    pub max_fflogs_results_batch_size: usize,
    pub max_detail_member_count: usize,
    pub ingest_security: IngestSecurityConfig,
    pub ingest_rate_windows: RwLock<HashMap<IngestRateKey, IngestRateWindow>>,
    pub ingest_nonces: RwLock<HashMap<String, DateTime<Utc>>>,
    /// in-flight FFLogs job lease map to avoid duplicate dispatch across workers
    pub fflogs_job_leases: RwLock<HashMap<FflogsLeaseKey, FflogsLeaseEntry>>,
    /// total number of FFLogs jobs dispatched to workers
    pub fflogs_jobs_dispatched_total: AtomicU64,
    /// total number of FFLogs result payload items received from workers
    pub fflogs_results_received_total: AtomicU64,
    /// total number of FFLogs result payload items rejected by lease/token validation
    pub fflogs_results_rejected_total: AtomicU64,
    /// total number of FFLogs lease items released via explicit abandon endpoint
    pub fflogs_leases_abandoned_total: AtomicU64,
    /// total number of FFLogs lease abandon items rejected by validation
    pub fflogs_leases_abandon_rejected_total: AtomicU64,
    /// total hidden-cache refresh candidates detected during jobs allocation
    pub fflogs_hidden_refresh_total: AtomicU64,
    /// total leader fallback applications when rendering listings
    pub fflogs_leader_fallback_total: AtomicU64,
    /// periodic monitor snapshot interval (seconds). set 0 to disable.
    pub monitor_snapshot_interval_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FflogsLeaseKey {
    pub content_id: u64,
    pub zone_id: u32,
    pub difficulty_id: i32,
    pub partition: i32,
}

#[derive(Debug, Clone)]
pub struct FflogsLeaseEntry {
    pub leased_at: DateTime<Utc>,
    pub client_id: String,
    pub lease_token: String,
}

#[derive(Debug, Clone)]
pub struct IngestSecurityConfig {
    pub require_signature: bool,
    pub shared_secret: String,
    pub clock_skew_seconds: i64,
    pub nonce_ttl_seconds: i64,
    pub require_capabilities_for_protected_endpoints: bool,
    pub capability_secret: String,
    pub capability_session_ttl_seconds: i64,
    pub capability_detail_ttl_seconds: i64,
    pub rate_limits: IngestRateLimits,
}

#[derive(Debug, Clone)]
pub struct IngestRateLimits {
    pub contribute_per_minute: u32,
    pub multiple_per_minute: u32,
    pub players_per_minute: u32,
    pub detail_per_minute: u32,
    pub fflogs_jobs_per_minute: u32,
    pub fflogs_results_per_minute: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IngestRateKey {
    pub endpoint: &'static str,
    pub client_id: String,
}

#[derive(Debug, Clone)]
pub struct IngestRateWindow {
    pub window_started: DateTime<Utc>,
    pub count: u32,
}

#[derive(Debug, Clone)]
pub struct CachedListingsSnapshot {
    pub revision: u64,
    pub body: Bytes,
}

#[derive(Debug, Default)]
pub enum ListingsSnapshotCacheState {
    #[default]
    Empty,
    Building {
        notify: Arc<Notify>,
    },
    Ready(CachedListingsSnapshot),
}

#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
struct PlayerDuplicateGroup {
    #[serde(rename = "_id")]
    content_id: i64,
    winner_id: mongodb::bson::oid::ObjectId,
    duplicate_ids: Vec<mongodb::bson::oid::ObjectId>,
    count: i32,
}

fn players_content_id_unique_index_model() -> IndexModel {
    IndexModel::builder()
        .keys(mongodb::bson::doc! {
            "content_id": 1,
        })
        .options(IndexOptions::builder().unique(true).build())
        .build()
}

fn report_parse_summary_identity_index_model() -> IndexModel {
    IndexModel::builder()
        .keys(mongodb::bson::doc! {
            "normalized_name": 1,
            "home_world": 1,
        })
        .build()
}

fn players_content_id_dedupe_pipeline() -> Vec<mongodb::bson::Document> {
    vec![
        mongodb::bson::doc! {
            "$match": {
                "content_id": { "$exists": true },
            }
        },
        mongodb::bson::doc! {
            "$sort": {
                "content_id": 1,
                "identity_observed_at": -1,
                "last_seen": -1,
                "seen_count": -1,
                "_id": 1,
            }
        },
        mongodb::bson::doc! {
            "$group": {
                "_id": "$content_id",
                "winner_id": { "$first": "$_id" },
                "duplicate_ids": { "$push": "$_id" },
                "count": { "$sum": 1 },
            }
        },
        mongodb::bson::doc! {
            "$match": {
                "count": { "$gt": 1 },
            }
        },
    ]
}

fn duplicate_ids_to_remove(group: &PlayerDuplicateGroup) -> Vec<mongodb::bson::oid::ObjectId> {
    group
        .duplicate_ids
        .iter()
        .copied()
        .filter(|id| *id != group.winner_id)
        .collect()
}

impl State {
    pub async fn new(config: Arc<Config>) -> Result<Arc<Self>> {
        let mongo = MongoClient::with_uri_str(&config.mongo.url)
            .await
            .context("could not create mongodb client")?;

        let (change_tx, _) = tokio::sync::broadcast::channel(64);
        let state = Arc::new(Self {
            mongo,
            stats: Default::default(),
            listings_change_channel: change_tx,
            listings_revision: AtomicU64::new(1),
            listings_revision_pending: AtomicBool::new(false),
            listings_revision_notify: Notify::new(),
            listings_revision_coalesce_window: Duration::from_millis(
                config.web.listings_revision_coalesce_millis.max(1),
            ),
            listings_snapshot_cache: Default::default(),
            fflogs_jobs_limit: config.web.fflogs_jobs_limit.max(1),
            fflogs_hidden_cache_ttl_hours: config.web.fflogs_hidden_cache_ttl_hours.max(1),
            listing_upsert_concurrency: config.web.listing_upsert_concurrency.max(1),
            player_upsert_concurrency: config.web.player_upsert_concurrency.max(1),
            max_body_bytes_contribute: config.web.max_body_bytes_contribute.max(1024),
            max_body_bytes_multiple: config.web.max_body_bytes_multiple.max(1024),
            max_body_bytes_players: config.web.max_body_bytes_players.max(1024),
            max_body_bytes_detail: config.web.max_body_bytes_detail.max(1024),
            max_body_bytes_fflogs_results: config.web.max_body_bytes_fflogs_results.max(1024),
            max_multiple_batch_size: config.web.max_multiple_batch_size.max(1),
            max_players_batch_size: config.web.max_players_batch_size.max(1),
            max_fflogs_results_batch_size: config.web.max_fflogs_results_batch_size.max(1),
            max_detail_member_count: config.web.max_detail_member_count.max(1),
            ingest_security: IngestSecurityConfig {
                require_signature: config.web.ingest_require_signature,
                shared_secret: config.web.ingest_shared_secret.clone(),
                clock_skew_seconds: config.web.ingest_clock_skew_seconds.max(1),
                nonce_ttl_seconds: config.web.ingest_nonce_ttl_seconds.max(1),
                require_capabilities_for_protected_endpoints: config
                    .web
                    .ingest_require_capabilities_for_protected_endpoints,
                capability_secret: if config.web.ingest_capability_secret.trim().is_empty() {
                    let generated = format!("runtime-capability-{}", Uuid::new_v4());
                    if config
                        .web
                        .ingest_require_capabilities_for_protected_endpoints
                    {
                        tracing::warn!(
                            "web.ingest_capability_secret is empty; generated an ephemeral runtime secret for protected endpoint capabilities"
                        );
                    }
                    generated
                } else {
                    config.web.ingest_capability_secret.trim().to_string()
                },
                capability_session_ttl_seconds: config
                    .web
                    .ingest_capability_session_ttl_seconds
                    .max(60),
                capability_detail_ttl_seconds: config
                    .web
                    .ingest_capability_detail_ttl_seconds
                    .max(60),
                rate_limits: IngestRateLimits {
                    contribute_per_minute: config
                        .web
                        .ingest_rate_limit_contribute_per_minute
                        .max(1),
                    multiple_per_minute: config.web.ingest_rate_limit_multiple_per_minute.max(1),
                    players_per_minute: config.web.ingest_rate_limit_players_per_minute.max(1),
                    detail_per_minute: config.web.ingest_rate_limit_detail_per_minute.max(1),
                    fflogs_jobs_per_minute: config
                        .web
                        .ingest_rate_limit_fflogs_jobs_per_minute
                        .max(1),
                    fflogs_results_per_minute: config
                        .web
                        .ingest_rate_limit_fflogs_results_per_minute
                        .max(1),
                },
            },
            ingest_rate_windows: Default::default(),
            ingest_nonces: Default::default(),
            fflogs_job_leases: Default::default(),
            fflogs_jobs_dispatched_total: Default::default(),
            fflogs_results_received_total: Default::default(),
            fflogs_results_rejected_total: Default::default(),
            fflogs_leases_abandoned_total: Default::default(),
            fflogs_leases_abandon_rejected_total: Default::default(),
            fflogs_hidden_refresh_total: Default::default(),
            fflogs_leader_fallback_total: Default::default(),
            monitor_snapshot_interval_seconds: config.web.monitor_snapshot_interval_seconds,
        });

        // Initialize Indexes
        state.ensure_indexes().await?;

        Ok(state)
    }

    async fn ensure_indexes(&self) -> Result<()> {
        // Listings Unique Index
        self.collection()
            .create_index(
                IndexModel::builder()
                    .keys(mongodb::bson::doc! {
                        "listing.id": 1,
                        "listing.last_server_restart": 1,
                        "listing.created_world": 1,
                    })
                    .options(IndexOptions::builder().unique(true).build())
                    .build(),
                None,
            )
            .await
            .context("could not create unique index")?;

        // Listings TTL Index
        let listings_index_model = IndexModel::builder()
            .keys(mongodb::bson::doc! {
                "updated_at": 1,
            })
            .options(
                IndexOptions::builder()
                    .expire_after(Duration::from_secs(3600 * 2))
                    .build(),
            )
            .build();

        if let Err(e) = self
            .collection()
            .create_index(listings_index_model.clone(), None)
            .await
        {
            // Check for IndexOptionsConflict (Error code 85)
            let is_conflict = match &*e.kind {
                mongodb::error::ErrorKind::Command(cmd_err) => cmd_err.code == 85,
                _ => false,
            };

            if is_conflict {
                tracing::debug!("Index option conflict detected for 'updated_at'. Dropping old index and recreating...");
                self.collection()
                    .drop_index("updated_at_1", None)
                    .await
                    .context("could not drop conflicting updated_at index")?;

                self.collection()
                    .create_index(listings_index_model, None)
                    .await
                    .context("could not create updated_at index after restart")?;
                tracing::info!("Index 'updated_at' recreated with new options.");
            } else {
                return Err(e).context("could not create updated_at index");
            }
        }

        // Parse collection indexes
        self.parse_collection()
            .create_index(
                IndexModel::builder()
                    .keys(mongodb::bson::doc! {
                        "content_id": 1,
                    })
                    .options(IndexOptions::builder().unique(true).build())
                    .build(),
                None,
            )
            .await
            .context("could not create parse index")?;

        self.report_parse_summary_collection()
            .create_index(report_parse_summary_identity_index_model(), None)
            .await
            .context("could not create report parse summary identity index")?;

        self.ensure_players_content_id_unique_index().await?;

        Ok(())
    }

    async fn ensure_players_content_id_unique_index(&self) -> Result<()> {
        let collection = self
            .players_collection()
            .clone_with_type::<mongodb::bson::Document>();
        self.dedupe_players_by_content_id(collection.clone())
            .await?;

        let mut has_unique_content_id_index = false;
        let mut existing_indexes = collection
            .list_indexes(None)
            .await
            .context("could not list players indexes")?;
        while let Some(index) = existing_indexes.next().await {
            let index = index.context("could not read players index")?;
            let is_content_id_index = index
                .keys
                .get_i32("content_id")
                .ok()
                .is_some_and(|value| value == 1)
                && index.keys.len() == 1;
            if !is_content_id_index {
                continue;
            }

            let is_unique = index
                .options
                .as_ref()
                .and_then(|options| options.unique)
                .unwrap_or(false);
            let name = index
                .options
                .as_ref()
                .and_then(|options| options.name.as_deref())
                .unwrap_or("content_id_1");

            if is_unique {
                has_unique_content_id_index = true;
                continue;
            }

            collection.drop_index(name, None).await.with_context(|| {
                format!("could not drop non-unique players content_id index '{name}'")
            })?;
        }

        if !has_unique_content_id_index {
            collection
                .create_index(players_content_id_unique_index_model(), None)
                .await
                .context("could not create unique players content_id index")?;
        }

        Ok(())
    }

    async fn dedupe_players_by_content_id(
        &self,
        collection: Collection<mongodb::bson::Document>,
    ) -> Result<()> {
        let mut duplicates = collection
            .aggregate(players_content_id_dedupe_pipeline(), None)
            .await
            .context("could not scan duplicate players by content_id")?;

        while let Some(result) = duplicates.next().await {
            let Some(duplicate) = result
                .ok()
                .and_then(|doc| mongodb::bson::from_document::<PlayerDuplicateGroup>(doc).ok())
            else {
                continue;
            };
            let ids_to_remove = duplicate_ids_to_remove(&duplicate);
            if ids_to_remove.is_empty() {
                continue;
            }

            collection
                .delete_many(
                    mongodb::bson::doc! {
                        "_id": { "$in": ids_to_remove },
                    },
                    None,
                )
                .await
                .with_context(|| {
                    format!(
                        "could not delete duplicate player rows for content_id {}",
                        duplicate.content_id
                    )
                })?;
        }

        Ok(())
    }

    pub fn collection(&self) -> Collection<ListingContainer> {
        self.mongo.database("rpf").collection("listings")
    }

    pub fn players_collection(&self) -> Collection<Player> {
        self.mongo.database("rpf").collection("players")
    }

    pub fn parse_collection(&self) -> Collection<crate::mongo::ParseCacheDoc> {
        self.mongo.database("rpf").collection("parses")
    }

    pub fn report_parse_summary_collection(
        &self,
    ) -> Collection<crate::mongo::ReportParseSummaryDoc> {
        self.mongo
            .database("rpf")
            .collection("report_parse_summaries")
    }

    pub fn notify_listings_changed(&self, changed_count: usize) {
        if changed_count > 0 {
            self.listings_revision_pending
                .store(true, AtomicOrdering::Relaxed);
            self.listings_revision_notify.notify_one();
        }
    }

    pub fn current_listings_revision(&self) -> u64 {
        self.listings_revision.load(AtomicOrdering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        duplicate_ids_to_remove, players_content_id_dedupe_pipeline,
        players_content_id_unique_index_model, report_parse_summary_identity_index_model,
        PlayerDuplicateGroup,
    };
    use mongodb::bson::{doc, oid::ObjectId};

    #[test]
    fn identity_players_unique_index_model_marks_content_id_as_unique() {
        let index = players_content_id_unique_index_model();

        assert_eq!(index.keys.get_i32("content_id").unwrap(), 1);
        assert_eq!(
            index
                .options
                .as_ref()
                .and_then(|options| options.unique)
                .unwrap_or(false),
            true
        );
    }

    #[test]
    fn report_parse_summary_identity_index_matches_listing_lookup_shape() {
        let index = report_parse_summary_identity_index_model();

        assert_eq!(index.keys, doc! { "normalized_name": 1, "home_world": 1 });
    }

    #[test]
    fn identity_players_dedupe_pipeline_prefers_freshest_player_documents() {
        let pipeline = players_content_id_dedupe_pipeline();

        assert_eq!(pipeline.len(), 4);
        assert_eq!(
            pipeline[1].get_document("$sort").unwrap(),
            &doc! {
                "content_id": 1,
                "identity_observed_at": -1,
                "last_seen": -1,
                "seen_count": -1,
                "_id": 1,
            }
        );
        assert_eq!(
            pipeline[3].get_document("$match").unwrap(),
            &doc! { "count": { "$gt": 1 } }
        );
    }

    #[test]
    fn identity_players_duplicate_cleanup_removes_all_non_winner_rows() {
        let winner = ObjectId::new();
        let loser_a = ObjectId::new();
        let loser_b = ObjectId::new();
        let group = PlayerDuplicateGroup {
            content_id: 7007,
            winner_id: winner,
            duplicate_ids: vec![winner, loser_a, loser_b],
            count: 3,
        };

        let to_remove = duplicate_ids_to_remove(&group);

        assert_eq!(group.count, 3);
        assert_eq!(to_remove, vec![loser_a, loser_b]);
    }
}
