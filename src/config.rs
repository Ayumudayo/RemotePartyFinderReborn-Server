use anyhow::{bail, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::net::SocketAddr;

const SNAPSHOT_REFRESH_PATH: &str = "/internal/listings/snapshot/refresh";

fn validate_trimmed_non_empty(value: &str, field: &str, context: &str) -> Result<()> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("{field} must be set when {context}");
    }
    if trimmed.len() != value.len() {
        bail!("{field} must not contain leading or trailing whitespace when {context}");
    }
    Ok(())
}

fn is_local_snapshot_refresh_url(url: &reqwest::Url) -> bool {
    url.host_str().is_some_and(|host| {
        host.eq_ignore_ascii_case("localhost")
            || host == "127.0.0.1"
            || host == "::1"
            || host == "[::1]"
    })
}

fn validate_snapshot_refresh_url_transport(url: &reqwest::Url) -> Result<()> {
    match url.scheme() {
        "https" => Ok(()),
        "http" if is_local_snapshot_refresh_url(url) => Ok(()),
        _ => bail!(
            "snapshot_worker.refresh_url must use https except for local testing URLs on localhost, 127.0.0.1, or [::1]"
        ),
    }
}

fn env_value<'a>(
    values: &'a HashMap<String, String>,
    names: &[&'static str],
) -> Option<(&'static str, &'a str)> {
    names
        .iter()
        .find_map(|name| values.get(*name).map(|value| (*name, value.as_str())))
}

fn parse_env_u64(name: &str, value: &str) -> Result<u64> {
    value
        .parse::<u64>()
        .map_err(|error| anyhow::anyhow!("{name} must be an unsigned integer: {error}"))
}

fn parse_env_bool(name: &str, value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => bail!("{name} must be a boolean value"),
    }
}

fn parse_env_snapshot_source(name: &str, value: &str) -> Result<ListingsSnapshotSource> {
    match value.trim().to_ascii_lowercase().as_str() {
        "inline" => Ok(ListingsSnapshotSource::Inline),
        "materialized" => Ok(ListingsSnapshotSource::Materialized),
        _ => bail!("{name} must be either 'inline' or 'materialized'"),
    }
}

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

fn default_snapshot_refresh_clock_skew_seconds() -> i64 {
    300
}

fn default_snapshot_refresh_nonce_ttl_seconds() -> i64 {
    300
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
    pub fn apply_env_overrides_from_env(&mut self) -> Result<()> {
        self.apply_env_overrides(std::env::vars())
    }

    pub fn apply_env_overrides<I, K, V>(&mut self, vars: I) -> Result<()>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let values = vars
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect::<HashMap<_, _>>();

        if let Some((name, value)) = env_value(&values, &["RPF_WEB_HOST"]) {
            self.web.host = value
                .parse::<SocketAddr>()
                .map_err(|error| anyhow::anyhow!("{name} must be a socket address: {error}"))?;
        } else if let Some((name, value)) = env_value(&values, &["PORT"]) {
            let port = value
                .parse::<u16>()
                .map_err(|error| anyhow::anyhow!("{name} must be a TCP port: {error}"))?;
            self.web.host = format!("0.0.0.0:{port}")
                .parse::<SocketAddr>()
                .expect("generated socket address should parse");
        }

        if let Some((_, value)) = env_value(&values, &["RPF_MONGO_URL", "MONGO_URL", "MONGODB_URI"])
        {
            self.mongo.url = value.to_string();
        }

        if let Some((name, value)) = env_value(&values, &["RPF_LISTINGS_SNAPSHOT_SOURCE"]) {
            self.web.listings_snapshot_source = parse_env_snapshot_source(name, value)?;
        }
        if let Some((_, value)) = env_value(&values, &["RPF_LISTINGS_SNAPSHOT_COLLECTION"]) {
            self.web.listings_snapshot_collection = value.to_string();
        }
        if let Some((_, value)) = env_value(&values, &["RPF_LISTINGS_SNAPSHOT_DOCUMENT_ID"]) {
            self.web.listings_snapshot_document_id = value.to_string();
        }
        if let Some((_, value)) = env_value(&values, &["RPF_LISTING_SOURCE_STATE_COLLECTION"]) {
            self.web.listing_source_state_collection = value.to_string();
        }
        if let Some((_, value)) = env_value(&values, &["RPF_LISTING_SOURCE_STATE_DOCUMENT_ID"]) {
            self.web.listing_source_state_document_id = value.to_string();
        }
        if let Some((_, value)) =
            env_value(&values, &["RPF_LISTING_SNAPSHOT_REVISION_STATE_COLLECTION"])
        {
            self.web.listing_snapshot_revision_state_collection = value.to_string();
        }
        if let Some((_, value)) =
            env_value(&values, &["RPF_LISTING_SNAPSHOT_WORKER_LEASE_COLLECTION"])
        {
            self.web.listing_snapshot_worker_lease_collection = value.to_string();
        }
        if let Some((name, value)) = env_value(
            &values,
            &["RPF_MATERIALIZED_SNAPSHOT_RECONCILE_INTERVAL_SECONDS"],
        ) {
            self.web.materialized_snapshot_reconcile_interval_seconds = parse_env_u64(name, value)?;
        }
        if let Some((_, value)) = env_value(&values, &["RPF_SNAPSHOT_REFRESH_SHARED_SECRET"]) {
            self.web.snapshot_refresh_shared_secret = value.to_string();
        }
        if let Some((_, value)) = env_value(&values, &["RPF_SNAPSHOT_REFRESH_CLIENT_ID"]) {
            self.web.snapshot_refresh_client_id = value.to_string();
        }
        if let Some((name, value)) =
            env_value(&values, &["RPF_SNAPSHOT_REFRESH_CLOCK_SKEW_SECONDS"])
        {
            self.web.snapshot_refresh_clock_skew_seconds =
                parse_env_u64(name, value)?
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("{name} exceeds i64 range"))?;
        }
        if let Some((name, value)) = env_value(&values, &["RPF_SNAPSHOT_REFRESH_NONCE_TTL_SECONDS"])
        {
            self.web.snapshot_refresh_nonce_ttl_seconds = parse_env_u64(name, value)?
                .try_into()
                .map_err(|_| anyhow::anyhow!("{name} exceeds i64 range"))?;
        }

        if let Some((name, value)) = env_value(&values, &["RPF_SNAPSHOT_WORKER_ENABLED"]) {
            self.snapshot_worker.enabled = parse_env_bool(name, value)?;
        }
        if let Some((name, value)) = env_value(&values, &["RPF_SNAPSHOT_WORKER_TICK_SECONDS"]) {
            self.snapshot_worker.tick_seconds = parse_env_u64(name, value)?;
        }
        if let Some((name, value)) = env_value(
            &values,
            &["RPF_SNAPSHOT_WORKER_FORCE_REBUILD_INTERVAL_SECONDS"],
        ) {
            self.snapshot_worker.force_rebuild_interval_seconds = parse_env_u64(name, value)?;
        }
        if let Some((name, value)) = env_value(&values, &["RPF_SNAPSHOT_WORKER_LEASE_TTL_SECONDS"])
        {
            self.snapshot_worker.lease_ttl_seconds = parse_env_u64(name, value)?;
        }
        if let Some((_, value)) = env_value(&values, &["RPF_SNAPSHOT_WORKER_OWNER_ID"]) {
            self.snapshot_worker.owner_id = value.to_string();
        }
        if let Some((_, value)) = env_value(&values, &["RPF_SNAPSHOT_WORKER_REFRESH_URL"]) {
            self.snapshot_worker.refresh_url = value.to_string();
        }
        if let Some((_, value)) = env_value(&values, &["RPF_SNAPSHOT_WORKER_LOG_FILTER"]) {
            self.snapshot_worker.log_filter = value.to_string();
        }

        Ok(())
    }

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

        if self.web.listings_snapshot_source == ListingsSnapshotSource::Materialized {
            validate_trimmed_non_empty(
                &self.web.listings_snapshot_document_id,
                "web.listings_snapshot_document_id",
                "listings_snapshot_source is materialized",
            )?;
            validate_trimmed_non_empty(
                &self.web.snapshot_refresh_client_id,
                "web.snapshot_refresh_client_id",
                "listings_snapshot_source is materialized",
            )?;
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

        validate_trimmed_non_empty(
            &self.web.listings_snapshot_document_id,
            "web.listings_snapshot_document_id",
            "snapshot_worker.enabled is true",
        )?;
        validate_trimmed_non_empty(
            &self.web.snapshot_refresh_client_id,
            "web.snapshot_refresh_client_id",
            "snapshot_worker.enabled is true",
        )?;

        let refresh_url = self.snapshot_worker.refresh_url.trim();
        if refresh_url.is_empty() {
            bail!("snapshot_worker.refresh_url must be set when snapshot_worker.enabled is true");
        }

        let Ok(refresh_url) = reqwest::Url::parse(refresh_url) else {
            bail!("snapshot_worker.refresh_url must be an absolute URL when snapshot_worker.enabled is true");
        };
        validate_snapshot_refresh_url_transport(&refresh_url)?;
        if refresh_url.path() != SNAPSHOT_REFRESH_PATH {
            bail!(
                "snapshot_worker.refresh_url path must be {SNAPSHOT_REFRESH_PATH} because refresh signatures cover that path"
            );
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
    #[serde(default = "default_snapshot_refresh_clock_skew_seconds")]
    pub snapshot_refresh_clock_skew_seconds: i64,
    #[serde(default = "default_snapshot_refresh_nonce_ttl_seconds")]
    pub snapshot_refresh_nonce_ttl_seconds: i64,
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
        assert_eq!(config.web.snapshot_refresh_clock_skew_seconds, 300);
        assert_eq!(config.web.snapshot_refresh_nonce_ttl_seconds, 300);
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
    fn env_overrides_enable_materialized_snapshot_server_config() {
        let mut config: Config =
            toml::from_str(&minimal_config_toml("")).expect("config should parse");

        config
            .apply_env_overrides([
                ("PORT", "9000"),
                ("MONGODB_URI", "mongodb://env-mongo"),
                ("RPF_LISTINGS_SNAPSHOT_SOURCE", "materialized"),
                ("RPF_SNAPSHOT_REFRESH_SHARED_SECRET", "env-refresh-secret"),
                ("RPF_SNAPSHOT_REFRESH_CLIENT_ID", "env-worker"),
                ("RPF_MATERIALIZED_SNAPSHOT_RECONCILE_INTERVAL_SECONDS", "5"),
            ])
            .expect("env overrides should apply");

        assert_eq!(config.web.host.to_string(), "0.0.0.0:9000");
        assert_eq!(config.mongo.url, "mongodb://env-mongo");
        assert_eq!(
            config.web.listings_snapshot_source,
            ListingsSnapshotSource::Materialized
        );
        assert_eq!(
            config.web.snapshot_refresh_shared_secret,
            "env-refresh-secret"
        );
        assert_eq!(config.web.snapshot_refresh_client_id, "env-worker");
        assert_eq!(
            config.web.materialized_snapshot_reconcile_interval_seconds,
            5
        );
        config
            .validate_for_server()
            .expect("materialized env override should be valid");
    }

    #[test]
    fn env_overrides_configure_snapshot_worker() {
        let mut config: Config = toml::from_str(&minimal_config_toml(
            r#"
listings_snapshot_source = "materialized"
snapshot_refresh_shared_secret = "worker-refresh-secret"
"#,
        ))
        .expect("config should parse");

        config
            .apply_env_overrides([
                ("RPF_SNAPSHOT_WORKER_ENABLED", "true"),
                ("RPF_SNAPSHOT_WORKER_TICK_SECONDS", "7"),
                ("RPF_SNAPSHOT_WORKER_FORCE_REBUILD_INTERVAL_SECONDS", "301"),
                ("RPF_SNAPSHOT_WORKER_LEASE_TTL_SECONDS", "123"),
                ("RPF_SNAPSHOT_WORKER_OWNER_ID", "rpi-worker"),
                (
                    "RPF_SNAPSHOT_WORKER_REFRESH_URL",
                    "https://example.test/internal/listings/snapshot/refresh",
                ),
            ])
            .expect("worker env overrides should apply");

        assert!(config.snapshot_worker.enabled);
        assert_eq!(config.snapshot_worker.tick_seconds, 7);
        assert_eq!(config.snapshot_worker.force_rebuild_interval_seconds, 301);
        assert_eq!(config.snapshot_worker.lease_ttl_seconds, 123);
        assert_eq!(config.snapshot_worker.owner_id, "rpi-worker");
        assert_eq!(
            config.snapshot_worker.refresh_url,
            "https://example.test/internal/listings/snapshot/refresh"
        );
        config
            .validate_for_snapshot_worker()
            .expect("snapshot worker env override should be valid");
    }

    #[test]
    fn env_overrides_reject_invalid_snapshot_source() {
        let mut config: Config =
            toml::from_str(&minimal_config_toml("")).expect("config should parse");

        let error = config
            .apply_env_overrides([("RPF_LISTINGS_SNAPSHOT_SOURCE", "sometimes")])
            .expect_err("invalid snapshot source env should fail");

        assert!(
            error.to_string().contains("RPF_LISTINGS_SNAPSHOT_SOURCE"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn materialized_snapshot_source_rejects_whitespace_padded_refresh_identity() {
        let config: Config = toml::from_str(&minimal_config_toml(
            r#"
listings_snapshot_source = "materialized"
listings_snapshot_document_id = " current "
snapshot_refresh_shared_secret = "worker-refresh-secret"
snapshot_refresh_client_id = " listings-snapshot-worker "
"#,
        ))
        .expect("config should parse");

        let error = config
            .validate_for_server()
            .expect_err("materialized server mode must reject ambiguous refresh identity config");

        let message = error.to_string();
        assert!(
            message.contains("listings_snapshot_document_id")
                || message.contains("snapshot_refresh_client_id"),
            "unexpected validation error: {message}"
        );
    }

    #[test]
    fn snapshot_worker_defaults_are_safe_but_server_inline_validation_ignores_refresh_url() {
        let config: Config = toml::from_str(&minimal_config_toml("")).expect("config should parse");

        assert!(config.snapshot_worker.enabled);
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
    fn snapshot_worker_validation_rejects_blank_refresh_client_id_when_enabled() {
        let config: Config = toml::from_str(&minimal_config_toml(
            r#"
listings_snapshot_source = "materialized"
snapshot_refresh_shared_secret = "worker-refresh-secret"
snapshot_refresh_client_id = "   "

[snapshot_worker]
enabled = true
refresh_url = "https://example.test/internal/listings/snapshot/refresh"
"#,
        ))
        .expect("config should parse");

        let error = config
            .validate_for_snapshot_worker()
            .expect_err("enabled worker must require refresh client id");

        assert!(
            error.to_string().contains("snapshot_refresh_client_id"),
            "unexpected validation error: {error:#}"
        );
    }

    #[test]
    fn snapshot_worker_validation_rejects_whitespace_padded_refresh_identity() {
        let config: Config = toml::from_str(&minimal_config_toml(
            r#"
listings_snapshot_source = "materialized"
listings_snapshot_document_id = " current "
snapshot_refresh_shared_secret = "worker-refresh-secret"
snapshot_refresh_client_id = " listings-snapshot-worker "

[snapshot_worker]
enabled = true
refresh_url = "https://example.test/internal/listings/snapshot/refresh"
"#,
        ))
        .expect("config should parse");

        let error = config
            .validate_for_snapshot_worker()
            .expect_err("enabled worker must reject ambiguous refresh identity config");

        let message = error.to_string();
        assert!(
            message.contains("listings_snapshot_document_id")
                || message.contains("snapshot_refresh_client_id"),
            "unexpected validation error: {message}"
        );
    }

    #[test]
    fn snapshot_worker_validation_rejects_refresh_url_with_wrong_path() {
        let config: Config = toml::from_str(&minimal_config_toml(
            r#"
listings_snapshot_source = "materialized"
snapshot_refresh_shared_secret = "worker-refresh-secret"

[snapshot_worker]
enabled = true
refresh_url = "https://example.test/internal/listings/snapshot/wrong"
"#,
        ))
        .expect("config should parse");

        let error = config
            .validate_for_snapshot_worker()
            .expect_err("enabled worker must target the signed refresh path");

        assert!(
            error.to_string().contains("snapshot_worker.refresh_url"),
            "unexpected validation error: {error:#}"
        );
    }

    #[test]
    fn snapshot_worker_validation_accepts_https_refresh_url() {
        let config: Config = toml::from_str(&minimal_config_toml(
            r#"
listings_snapshot_source = "materialized"
snapshot_refresh_shared_secret = "worker-refresh-secret"

[snapshot_worker]
enabled = true
refresh_url = "https://example.test/internal/listings/snapshot/refresh"
"#,
        ))
        .expect("config should parse");

        config
            .validate_for_snapshot_worker()
            .expect("https refresh URL should be accepted");
    }

    #[test]
    fn snapshot_worker_validation_accepts_localhost_http_refresh_url() {
        for refresh_url in [
            "http://localhost/internal/listings/snapshot/refresh",
            "http://127.0.0.1/internal/listings/snapshot/refresh",
            "http://[::1]/internal/listings/snapshot/refresh",
        ] {
            let config: Config = toml::from_str(&minimal_config_toml(&format!(
                r#"
listings_snapshot_source = "materialized"
snapshot_refresh_shared_secret = "worker-refresh-secret"

[snapshot_worker]
enabled = true
refresh_url = "{refresh_url}"
"#
            )))
            .expect("config should parse");

            config
                .validate_for_snapshot_worker()
                .unwrap_or_else(|error| panic!("{refresh_url} should be accepted: {error:#}"));
        }
    }

    #[test]
    fn snapshot_worker_validation_rejects_ftp_refresh_url() {
        let config: Config = toml::from_str(&minimal_config_toml(
            r#"
listings_snapshot_source = "materialized"
snapshot_refresh_shared_secret = "worker-refresh-secret"

[snapshot_worker]
enabled = true
refresh_url = "ftp://example.test/internal/listings/snapshot/refresh"
"#,
        ))
        .expect("config should parse");

        let error = config
            .validate_for_snapshot_worker()
            .expect_err("enabled worker must reject non-http refresh URL schemes");

        assert!(
            error.to_string().contains("snapshot_worker.refresh_url"),
            "unexpected validation error: {error:#}"
        );
    }

    #[test]
    fn snapshot_worker_validation_rejects_non_localhost_http_refresh_url() {
        let config: Config = toml::from_str(&minimal_config_toml(
            r#"
listings_snapshot_source = "materialized"
snapshot_refresh_shared_secret = "worker-refresh-secret"

[snapshot_worker]
enabled = true
refresh_url = "http://example.test/internal/listings/snapshot/refresh"
"#,
        ))
        .expect("config should parse");

        let error = config
            .validate_for_snapshot_worker()
            .expect_err("enabled worker must require https for non-localhost refresh URLs");

        assert!(
            error.to_string().contains("snapshot_worker.refresh_url"),
            "unexpected validation error: {error:#}"
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
