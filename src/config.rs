use serde::Deserialize;
use std::net::SocketAddr;

fn default_fflogs_jobs_limit() -> usize {
    20
}

fn default_fflogs_hidden_cache_ttl_hours() -> i64 {
    24
}

fn default_listing_upsert_concurrency() -> usize {
    16
}

fn default_player_upsert_concurrency() -> usize {
    32
}

#[derive(Deserialize)]
pub struct Config {
    pub web: Web,
    pub mongo: Mongo,
}

#[derive(Deserialize)]
pub struct Web {
    pub host: SocketAddr,
    #[serde(default = "default_fflogs_jobs_limit")]
    pub fflogs_jobs_limit: usize,
    #[serde(default = "default_fflogs_hidden_cache_ttl_hours")]
    pub fflogs_hidden_cache_ttl_hours: i64,
    #[serde(default = "default_listing_upsert_concurrency")]
    pub listing_upsert_concurrency: usize,
    #[serde(default = "default_player_upsert_concurrency")]
    pub player_upsert_concurrency: usize,
}

#[derive(Deserialize)]
pub struct Mongo {
    pub url: String,
}
