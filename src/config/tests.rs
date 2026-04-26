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
    assert_eq!(config.web.listings_revision_coalesce_millis, 250);
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
    assert_eq!(config.snapshot_worker.tick_seconds, 1);
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
