use anyhow::{bail, Result};
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

fn default_ingest_require_capabilities_for_protected_endpoints() -> bool {
    false
}

fn default_ingest_capability_secret() -> String {
    String::new()
}

fn default_ingest_capability_session_ttl_seconds() -> i64 {
    60 * 60 * 12
}

fn default_ingest_capability_detail_ttl_seconds() -> i64 {
    60 * 15
}

fn default_monitor_snapshot_interval_seconds() -> u64 {
    0
}

fn default_listings_revision_coalesce_millis() -> u64 {
    1000
}

fn default_listings_snapshot_source() -> ListingsSnapshotSource {
    ListingsSnapshotSource::Inline
}

fn default_listings_snapshot_collection() -> String {
    "listings_snapshots".to_string()
}

fn default_listings_snapshot_document_id() -> String {
    "current".to_string()
}

fn default_listing_source_state_collection() -> String {
    "listing_source_state".to_string()
}

fn default_listing_source_state_document_id() -> String {
    "current".to_string()
}

fn default_listing_snapshot_revision_state_collection() -> String {
    "listing_snapshot_revision_state".to_string()
}

fn default_listing_snapshot_worker_lease_collection() -> String {
    "listing_snapshot_worker_leases".to_string()
}

fn default_materialized_snapshot_reconcile_interval_seconds() -> u64 {
    30
}

fn default_snapshot_refresh_shared_secret() -> String {
    String::new()
}

fn default_snapshot_refresh_client_id() -> String {
    "listings-snapshot-worker".to_string()
}

fn default_snapshot_worker_enabled() -> bool {
    true
}

fn default_snapshot_worker_tick_seconds() -> u64 {
    5
}

fn default_snapshot_worker_force_rebuild_interval_seconds() -> u64 {
    300
}

fn default_snapshot_worker_lease_ttl_seconds() -> u64 {
    120
}

fn default_snapshot_worker_owner_id() -> String {
    String::new()
}

fn default_snapshot_worker_refresh_url() -> String {
    String::new()
}

fn default_snapshot_worker_log_filter() -> String {
    "info,remote_party_finder_reborn=debug".to_string()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ListingsSnapshotSource {
    Inline,
    Materialized,
}

#[derive(Deserialize)]
pub struct Config {
    pub web: Web,
    pub mongo: Mongo,
    #[serde(default)]
    pub snapshot_worker: SnapshotWorker,
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        self.validate_for_server()
    }

    pub fn validate_for_server(&self) -> Result<()> {
        if self.web.listings_snapshot_source == ListingsSnapshotSource::Materialized
            && self.web.snapshot_refresh_shared_secret.trim().is_empty()
        {
            bail!(
                "web.snapshot_refresh_shared_secret must be set when listings_snapshot_source is materialized"
            );
        }

        Ok(())
    }

    pub fn validate_for_snapshot_worker(&self) -> Result<()> {
        if !self.snapshot_worker.enabled {
            return Ok(());
        }

        if self.web.listings_snapshot_source != ListingsSnapshotSource::Materialized {
            bail!(
                "web.listings_snapshot_source must be materialized when snapshot_worker.enabled is true"
            );
        }

        if self.web.snapshot_refresh_shared_secret.trim().is_empty() {
            bail!("web.snapshot_refresh_shared_secret must be set when snapshot_worker.enabled is true");
        }

        if self.snapshot_worker.refresh_url.trim().is_empty() {
            bail!("snapshot_worker.refresh_url must be set when snapshot_worker.enabled is true");
        }

        Ok(())
    }
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
    #[serde(default = "default_ingest_require_capabilities_for_protected_endpoints")]
    pub ingest_require_capabilities_for_protected_endpoints: bool,
    #[serde(default = "default_ingest_capability_secret")]
    pub ingest_capability_secret: String,
    #[serde(default = "default_ingest_capability_session_ttl_seconds")]
    pub ingest_capability_session_ttl_seconds: i64,
    #[serde(default = "default_ingest_capability_detail_ttl_seconds")]
    pub ingest_capability_detail_ttl_seconds: i64,
    #[serde(default = "default_monitor_snapshot_interval_seconds")]
    pub monitor_snapshot_interval_seconds: u64,
    #[serde(default = "default_listings_revision_coalesce_millis")]
    pub listings_revision_coalesce_millis: u64,
    #[serde(default = "default_listings_snapshot_source")]
    pub listings_snapshot_source: ListingsSnapshotSource,
    #[serde(default = "default_listings_snapshot_collection")]
    pub listings_snapshot_collection: String,
    #[serde(default = "default_listings_snapshot_document_id")]
    pub listings_snapshot_document_id: String,
    #[serde(default = "default_listing_source_state_collection")]
    pub listing_source_state_collection: String,
    #[serde(default = "default_listing_source_state_document_id")]
    pub listing_source_state_document_id: String,
    #[serde(default = "default_listing_snapshot_revision_state_collection")]
    pub listing_snapshot_revision_state_collection: String,
    #[serde(default = "default_listing_snapshot_worker_lease_collection")]
    pub listing_snapshot_worker_lease_collection: String,
    #[serde(default = "default_materialized_snapshot_reconcile_interval_seconds")]
    pub materialized_snapshot_reconcile_interval_seconds: u64,
    #[serde(default = "default_snapshot_refresh_shared_secret")]
    pub snapshot_refresh_shared_secret: String,
    #[serde(default = "default_snapshot_refresh_client_id")]
    pub snapshot_refresh_client_id: String,
}

#[derive(Deserialize)]
pub struct Mongo {
    pub url: String,
}

#[derive(Deserialize)]
pub struct SnapshotWorker {
    #[serde(default = "default_snapshot_worker_enabled")]
    pub enabled: bool,
    #[serde(default = "default_snapshot_worker_tick_seconds")]
    pub tick_seconds: u64,
    #[serde(default = "default_snapshot_worker_force_rebuild_interval_seconds")]
    pub force_rebuild_interval_seconds: u64,
    #[serde(default = "default_snapshot_worker_lease_ttl_seconds")]
    pub lease_ttl_seconds: u64,
    #[serde(default = "default_snapshot_worker_owner_id")]
    pub owner_id: String,
    #[serde(default = "default_snapshot_worker_refresh_url")]
    pub refresh_url: String,
    #[serde(default = "default_snapshot_worker_log_filter")]
    pub log_filter: String,
}

impl Default for SnapshotWorker {
    fn default() -> Self {
        Self {
            enabled: default_snapshot_worker_enabled(),
            tick_seconds: default_snapshot_worker_tick_seconds(),
            force_rebuild_interval_seconds: default_snapshot_worker_force_rebuild_interval_seconds(
            ),
            lease_ttl_seconds: default_snapshot_worker_lease_ttl_seconds(),
            owner_id: default_snapshot_worker_owner_id(),
            refresh_url: default_snapshot_worker_refresh_url(),
            log_filter: default_snapshot_worker_log_filter(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Config, ListingsSnapshotSource};

    fn minimal_config_toml(extra_web: &str) -> String {
        format!(
            r#"
[web]
host = "127.0.0.1:0"
{extra_web}

[mongo]
url = "mongodb://127.0.0.1:27017"
"#
        )
    }

    #[test]
    fn config_defaults_to_inline_listings_snapshot_source_and_safe_materialized_names() {
        let config: Config = toml::from_str(&minimal_config_toml("")).expect("config should parse");

        assert_eq!(
            config.web.listings_snapshot_source,
            ListingsSnapshotSource::Inline
        );
        assert_eq!(
            config.web.listings_snapshot_collection,
            "listings_snapshots"
        );
        assert_eq!(config.web.listings_snapshot_document_id, "current");
        assert_eq!(
            config.web.listing_source_state_collection,
            "listing_source_state"
        );
        assert_eq!(config.web.listing_source_state_document_id, "current");
        assert_eq!(
            config.web.listing_snapshot_revision_state_collection,
            "listing_snapshot_revision_state"
        );
        assert_eq!(
            config.web.listing_snapshot_worker_lease_collection,
            "listing_snapshot_worker_leases"
        );
        assert_eq!(
            config.web.materialized_snapshot_reconcile_interval_seconds,
            30
        );
        assert_eq!(config.web.snapshot_refresh_shared_secret, "");
        assert_eq!(
            config.web.snapshot_refresh_client_id,
            "listings-snapshot-worker"
        );
        config
            .validate_for_server()
            .expect("inline mode may use a blank refresh secret");
    }

    #[test]
    fn materialized_snapshot_source_rejects_blank_refresh_secret() {
        let config: Config = toml::from_str(&minimal_config_toml(
            r#"
listings_snapshot_source = "materialized"
snapshot_refresh_shared_secret = "   "
"#,
        ))
        .expect("config should parse");

        let error = config
            .validate_for_server()
            .expect_err("materialized mode must fail closed without refresh secret");

        assert!(
            error.to_string().contains("snapshot_refresh_shared_secret"),
            "unexpected validation error: {error:#}"
        );
    }

    #[test]
    fn snapshot_worker_defaults_are_safe_but_server_inline_validation_ignores_refresh_url() {
        let config: Config = toml::from_str(&minimal_config_toml("")).expect("config should parse");

        assert_eq!(config.snapshot_worker.enabled, true);
        assert_eq!(config.snapshot_worker.tick_seconds, 5);
        assert_eq!(config.snapshot_worker.force_rebuild_interval_seconds, 300);
        assert_eq!(config.snapshot_worker.lease_ttl_seconds, 120);
        assert_eq!(config.snapshot_worker.owner_id, "");
        assert_eq!(config.snapshot_worker.refresh_url, "");
        assert_eq!(
            config.snapshot_worker.log_filter,
            "info,remote_party_finder_reborn=debug"
        );
        config
            .validate_for_server()
            .expect("inline server mode should not require worker refresh URL");
    }

    #[test]
    fn snapshot_worker_validation_rejects_blank_refresh_url_and_secret_when_enabled() {
        let config: Config = toml::from_str(&minimal_config_toml(
            r#"
listings_snapshot_source = "materialized"
snapshot_refresh_shared_secret = "   "

[snapshot_worker]
enabled = true
refresh_url = " "
"#,
        ))
        .expect("config should parse");

        let error = config
            .validate_for_snapshot_worker()
            .expect_err("enabled worker must require refresh URL and secret");

        let message = error.to_string();
        assert!(
            message.contains("snapshot_refresh_shared_secret")
                || message.contains("snapshot_worker.refresh_url"),
            "unexpected validation error: {message}"
        );
    }

    #[test]
    fn snapshot_worker_validation_rejects_inline_snapshot_source_when_enabled() {
        let config: Config = toml::from_str(&minimal_config_toml(
            r#"
listings_snapshot_source = "inline"
snapshot_refresh_shared_secret = "worker-refresh-secret"

[snapshot_worker]
enabled = true
refresh_url = "https://example.test/internal/listings/snapshot/refresh"
"#,
        ))
        .expect("config should parse");

        let error = config
            .validate_for_snapshot_worker()
            .expect_err("enabled worker must require materialized server mode");

        assert!(
            error.to_string().contains("listings_snapshot_source"),
            "unexpected validation error: {error:#}"
        );
    }

    #[test]
    fn disabled_snapshot_worker_validation_allows_blank_refresh_url_and_secret() {
        let config: Config = toml::from_str(&minimal_config_toml(
            r#"
snapshot_refresh_shared_secret = "   "

[snapshot_worker]
enabled = false
refresh_url = " "
"#,
        ))
        .expect("config should parse");

        config
            .validate_for_snapshot_worker()
            .expect("disabled worker should not require runtime worker fields");
    }
}
