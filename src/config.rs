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

fn default_max_body_bytes_contribute() -> u64 {
    256 * 1024
}

fn default_max_body_bytes_multiple() -> u64 {
    1024 * 1024
}

fn default_max_body_bytes_players() -> u64 {
    512 * 1024
}

fn default_max_body_bytes_detail() -> u64 {
    128 * 1024
}

fn default_max_body_bytes_fflogs_results() -> u64 {
    512 * 1024
}

fn default_max_multiple_batch_size() -> usize {
    200
}

fn default_max_players_batch_size() -> usize {
    100
}

fn default_max_fflogs_results_batch_size() -> usize {
    100
}

fn default_max_detail_member_count() -> usize {
    48
}

fn default_ingest_rate_limit_contribute_per_minute() -> u32 {
    120
}

fn default_ingest_rate_limit_multiple_per_minute() -> u32 {
    60
}

fn default_ingest_rate_limit_players_per_minute() -> u32 {
    120
}

fn default_ingest_rate_limit_detail_per_minute() -> u32 {
    120
}

fn default_ingest_rate_limit_fflogs_jobs_per_minute() -> u32 {
    30
}

fn default_ingest_rate_limit_fflogs_results_per_minute() -> u32 {
    60
}

fn default_ingest_require_signature() -> bool {
    false
}

fn default_ingest_shared_secret() -> String {
    "rpf-reborn-public-ingest-v1".to_string()
}

fn default_ingest_clock_skew_seconds() -> i64 {
    300
}

fn default_ingest_nonce_ttl_seconds() -> i64 {
    300
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
    #[serde(default = "default_max_body_bytes_contribute")]
    pub max_body_bytes_contribute: u64,
    #[serde(default = "default_max_body_bytes_multiple")]
    pub max_body_bytes_multiple: u64,
    #[serde(default = "default_max_body_bytes_players")]
    pub max_body_bytes_players: u64,
    #[serde(default = "default_max_body_bytes_detail")]
    pub max_body_bytes_detail: u64,
    #[serde(default = "default_max_body_bytes_fflogs_results")]
    pub max_body_bytes_fflogs_results: u64,
    #[serde(default = "default_max_multiple_batch_size")]
    pub max_multiple_batch_size: usize,
    #[serde(default = "default_max_players_batch_size")]
    pub max_players_batch_size: usize,
    #[serde(default = "default_max_fflogs_results_batch_size")]
    pub max_fflogs_results_batch_size: usize,
    #[serde(default = "default_max_detail_member_count")]
    pub max_detail_member_count: usize,
    #[serde(default = "default_ingest_rate_limit_contribute_per_minute")]
    pub ingest_rate_limit_contribute_per_minute: u32,
    #[serde(default = "default_ingest_rate_limit_multiple_per_minute")]
    pub ingest_rate_limit_multiple_per_minute: u32,
    #[serde(default = "default_ingest_rate_limit_players_per_minute")]
    pub ingest_rate_limit_players_per_minute: u32,
    #[serde(default = "default_ingest_rate_limit_detail_per_minute")]
    pub ingest_rate_limit_detail_per_minute: u32,
    #[serde(default = "default_ingest_rate_limit_fflogs_jobs_per_minute")]
    pub ingest_rate_limit_fflogs_jobs_per_minute: u32,
    #[serde(default = "default_ingest_rate_limit_fflogs_results_per_minute")]
    pub ingest_rate_limit_fflogs_results_per_minute: u32,
    #[serde(default = "default_ingest_require_signature")]
    pub ingest_require_signature: bool,
    #[serde(default = "default_ingest_shared_secret")]
    pub ingest_shared_secret: String,
    #[serde(default = "default_ingest_clock_skew_seconds")]
    pub ingest_clock_skew_seconds: i64,
    #[serde(default = "default_ingest_nonce_ttl_seconds")]
    pub ingest_nonce_ttl_seconds: i64,
}

#[derive(Deserialize)]
pub struct Mongo {
    pub url: String,
}
