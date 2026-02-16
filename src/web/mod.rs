use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::AtomicU64,
    },
    time::Duration,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use mongodb::{
    options::IndexOptions,
    Client as MongoClient, Collection, IndexModel,
};
use tokio::sync::broadcast::Sender;
use tokio::sync::RwLock;

use crate::config::Config;
use crate::listing::PartyFinderListing;
use crate::listing_container::ListingContainer;
use crate::player::Player;
use crate::stats::CachedStatistics;

pub mod routes;
pub mod handlers;
pub mod background;

pub async fn start(config: Arc<Config>) -> Result<()> {
    let state = State::new(Arc::clone(&config)).await?;

    // Background tasks
    background::spawn_stats_task(Arc::clone(&state));

    tracing::info!("listening at {}", config.web.host);
    warp::serve(routes::router(state)).run(config.web.host).await;
    Ok(())
}

pub struct State {
    pub mongo: MongoClient,
    pub stats: RwLock<Option<CachedStatistics>>,
    pub listings_channel: Sender<Arc<[PartyFinderListing]>>,
    pub fflogs_jobs_limit: usize,
    pub fflogs_hidden_cache_ttl_hours: i64,
    pub listing_upsert_concurrency: usize,
    pub player_upsert_concurrency: usize,
    /// in-flight FFLogs job lease map to avoid duplicate dispatch across workers
    pub fflogs_job_leases: RwLock<HashMap<FflogsLeaseKey, DateTime<Utc>>>,
    /// total number of FFLogs jobs dispatched to workers
    pub fflogs_jobs_dispatched_total: AtomicU64,
    /// total number of FFLogs result payload items received from workers
    pub fflogs_results_received_total: AtomicU64,
    /// total hidden-cache refresh candidates detected during jobs allocation
    pub fflogs_hidden_refresh_total: AtomicU64,
    /// total leader fallback applications when rendering listings
    pub fflogs_leader_fallback_total: AtomicU64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FflogsLeaseKey {
    pub content_id: u64,
    pub zone_id: u32,
    pub difficulty_id: i32,
    pub partition: i32,
}

impl State {
    pub async fn new(config: Arc<Config>) -> Result<Arc<Self>> {
        let mongo = MongoClient::with_uri_str(&config.mongo.url)
            .await
            .context("could not create mongodb client")?;
            
        let (tx, _) = tokio::sync::broadcast::channel(16);
        let state = Arc::new(Self {
            mongo,
            stats: Default::default(),
            listings_channel: tx,
            fflogs_jobs_limit: config.web.fflogs_jobs_limit.max(1),
            fflogs_hidden_cache_ttl_hours: config.web.fflogs_hidden_cache_ttl_hours.max(1),
            listing_upsert_concurrency: config.web.listing_upsert_concurrency.max(1),
            player_upsert_concurrency: config.web.player_upsert_concurrency.max(1),
            fflogs_job_leases: Default::default(),
            fflogs_jobs_dispatched_total: Default::default(),
            fflogs_results_received_total: Default::default(),
            fflogs_hidden_refresh_total: Default::default(),
            fflogs_leader_fallback_total: Default::default(),
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
            .options(IndexOptions::builder().expire_after(Duration::from_secs(3600 * 2)).build())
            .build();

        if let Err(e) = self.collection().create_index(listings_index_model.clone(), None).await {
            // Check for IndexOptionsConflict (Error code 85)
            let is_conflict = match &*e.kind {
                mongodb::error::ErrorKind::Command(cmd_err) => cmd_err.code == 85,
                _ => false,
            };

            if is_conflict {
                tracing::warn!("Index option conflict detected for 'updated_at'. Dropping old index and recreating...");
                self.collection().drop_index("updated_at_1", None).await
                    .context("could not drop conflicting updated_at index")?;
                
                self.collection().create_index(listings_index_model, None).await
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

        // Players collection index (frequent lookups by content_id)
        self.players_collection()
            .create_index(
                IndexModel::builder()
                    .keys(mongodb::bson::doc! {
                        "content_id": 1,
                    })
                    .build(),
                None,
            )
            .await
            .context("could not create players content_id index")?;

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
}
