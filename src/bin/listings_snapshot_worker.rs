use anyhow::{bail, Context};
use base64::encode as base64_encode;
use chrono::Utc;
use hmac::{Hmac, Mac};
use remote_party_finder_reborn::config::Config;
use remote_party_finder_reborn::listings_snapshot::{
    allocate_materialized_revision, build_listings_payload, ensure_materialized_revision_at_least,
    load_current_materialized_doc, load_listing_source_state, serialize_snapshot,
    try_acquire_snapshot_worker_lease, try_renew_snapshot_worker_lease,
    try_write_materialized_snapshot_cas,
};
use remote_party_finder_reborn::web::State;
use sha2::Sha256;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use uuid::Uuid;

const SNAPSHOT_REFRESH_PATH: &str = "/internal/listings/snapshot/refresh";
const REFRESH_CLIENT_TIMEOUT: Duration = Duration::from_secs(30);
const REFRESH_CLIENT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotWorkerDecision {
    Build,
    Skip,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkerArgs {
    config_path: String,
    once: bool,
}

pub fn should_build_snapshot(
    last_source_revision: Option<i64>,
    current_source_revision: i64,
    force_due: bool,
) -> SnapshotWorkerDecision {
    if last_source_revision.is_none()
        || last_source_revision.is_some_and(|revision| current_source_revision > revision)
        || force_due
    {
        SnapshotWorkerDecision::Build
    } else {
        SnapshotWorkerDecision::Skip
    }
}

pub fn compute_refresh_signature(
    secret: &str,
    payload: &str,
) -> Result<String, hmac::digest::InvalidLength> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())?;
    mac.update(payload.as_bytes());
    Ok(base64_encode(mac.finalize().into_bytes()))
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("listings snapshot worker failed: {error:#}");
        tracing::error!("listings snapshot worker failed: {error:#}");
        std::process::exit(1);
    }
}

async fn run() -> anyhow::Result<()> {
    let args = parse_args(std::env::args().skip(1))?;
    let config = Arc::new(load_config(&args.config_path).await?);

    init_tracing(&config.snapshot_worker.log_filter);
    config.validate_for_snapshot_worker()?;

    if !config.snapshot_worker.enabled {
        tracing::info!("listings snapshot worker disabled by config");
        return Ok(());
    }

    let owner_id = effective_owner_id(&config.snapshot_worker.owner_id);
    let refresh_url = config.snapshot_worker.refresh_url.trim().to_string();
    let tick_interval = Duration::from_secs(config.snapshot_worker.tick_seconds.max(1));
    let force_interval =
        Duration::from_secs(config.snapshot_worker.force_rebuild_interval_seconds.max(1));
    let lease_ttl_seconds = config.snapshot_worker.lease_ttl_seconds.max(1) as i64;
    let state = State::new_for_snapshot_worker(Arc::clone(&config)).await?;
    let client = build_refresh_client()?;
    let mut last_source_revision = None;
    let mut last_forced_build_at = Instant::now();

    loop {
        let force_due = last_forced_build_at.elapsed() >= force_interval;
        match run_tick(
            Arc::clone(&state),
            &client,
            &refresh_url,
            &owner_id,
            lease_ttl_seconds,
            force_due,
            &mut last_source_revision,
            &mut last_forced_build_at,
        )
        .await
        {
            Ok(outcome) => {
                tracing::debug!(?outcome, "listings snapshot worker tick completed");
            }
            Err(error) => {
                tracing::error!("listings snapshot worker tick failed: {error:#}");
                if args.once {
                    return Err(error);
                }
            }
        }

        if args.once {
            return Ok(());
        }

        tokio::time::sleep(tick_interval).await;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TickOutcome {
    SkippedNoChange,
    SkippedLeaseHeld,
    BuiltUnchanged,
    WroteAndRefreshed,
    CasRejected,
}

async fn run_tick(
    state: Arc<State>,
    client: &reqwest::Client,
    refresh_url: &str,
    owner_id: &str,
    lease_ttl_seconds: i64,
    force_due: bool,
    last_source_revision: &mut Option<i64>,
    last_forced_build_at: &mut Instant,
) -> anyhow::Result<TickOutcome> {
    let source_collection = state.listing_source_state_collection();
    let source_state =
        load_listing_source_state(&source_collection, &state.listing_source_state_document_id)
            .await?;
    let current_source_revision = source_state.as_ref().map(|doc| doc.revision).unwrap_or(0);

    if should_build_snapshot(*last_source_revision, current_source_revision, force_due)
        == SnapshotWorkerDecision::Skip
    {
        return Ok(TickOutcome::SkippedNoChange);
    }

    let lease_collection = state.listing_snapshot_worker_lease_collection();
    let lease_acquired = try_acquire_snapshot_worker_lease(
        &lease_collection,
        snapshot_worker_lease_document_id(&state.listings_snapshot_document_id),
        owner_id,
        Utc::now(),
        lease_ttl_seconds,
    )
    .await?;
    if !lease_acquired {
        return Ok(TickOutcome::SkippedLeaseHeld);
    }

    let (stop_renewal, renewal_handle) = spawn_lease_renewal_task(
        Arc::clone(&state),
        snapshot_worker_lease_document_id(&state.listings_snapshot_document_id).to_string(),
        owner_id.to_string(),
        lease_ttl_seconds,
    );

    run_tick_critical_section_with_lease_renewal(stop_renewal, renewal_handle, async {
        let payload = build_listings_payload(Arc::clone(&state)).await?;
        let snapshot_collection = state.listings_snapshot_collection();
        let current_doc = load_current_materialized_doc(
            &snapshot_collection,
            &state.listings_snapshot_document_id,
        )
        .await?;
        if current_doc
            .as_ref()
            .is_some_and(|doc| doc.payload_hash == payload.payload_hash)
        {
            *last_source_revision = Some(current_source_revision);
            if force_due {
                *last_forced_build_at = Instant::now();
            }
            return Ok(TickOutcome::BuiltUnchanged);
        }

        let revision_collection = state.listing_snapshot_revision_state_collection();
        if let Some(doc) = current_doc.as_ref() {
            ensure_materialized_revision_at_least(
                &revision_collection,
                &state.listings_snapshot_document_id,
                doc.revision,
            )
            .await?;
        }
        let revision = allocate_materialized_revision(
            &revision_collection,
            &state.listings_snapshot_document_id,
        )
        .await?;
        let expected_revision = revision
            .try_into()
            .context("materialized listings snapshot revision must be non-negative")?;
        let built = serialize_snapshot(revision, payload)?;
        let wrote = try_write_materialized_snapshot_cas(
            &snapshot_collection,
            &state.listings_snapshot_document_id,
            built,
            current_source_revision,
        )
        .await?;

        *last_source_revision = Some(current_source_revision);
        *last_forced_build_at = Instant::now();

        if !wrote {
            return Ok(TickOutcome::CasRejected);
        }

        send_refresh_request(
            client,
            refresh_url,
            &state.snapshot_refresh_client_id,
            &state.snapshot_refresh_shared_secret,
            &state.listings_snapshot_document_id,
            expected_revision,
        )
        .await?;

        Ok(TickOutcome::WroteAndRefreshed)
    })
    .await
}

fn parse_args(args: impl IntoIterator<Item = String>) -> anyhow::Result<WorkerArgs> {
    let mut config_path = None;
    let mut once = false;
    let mut iter = args.into_iter();

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--once" => once = true,
            "--config" => {
                let Some(path) = iter.next() else {
                    bail!("--config requires a path");
                };
                config_path = Some(path);
            }
            value if value.starts_with("--config=") => {
                config_path = Some(value["--config=".len()..].to_string());
            }
            value if value.starts_with('-') => bail!("unsupported argument: {value}"),
            value => {
                if config_path.is_some() {
                    bail!("multiple config paths supplied");
                }
                config_path = Some(value.to_string());
            }
        }
    }

    Ok(WorkerArgs {
        config_path: config_path.unwrap_or_else(|| "./config.toml".to_string()),
        once,
    })
}

async fn load_config(path: impl AsRef<Path>) -> anyhow::Result<Config> {
    let mut file = File::open(path)
        .await
        .context("could not open config file")?;
    let mut toml = String::new();
    file.read_to_string(&mut toml)
        .await
        .context("could not read config file")?;
    toml::from_str(&toml).context("could not parse config file")
}

fn init_tracing(log_filter: &str) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log_filter));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(true)
        .with_target(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .try_init();
}

fn refresh_client_timeout_settings() -> (Duration, Duration) {
    (REFRESH_CLIENT_TIMEOUT, REFRESH_CLIENT_CONNECT_TIMEOUT)
}

fn build_refresh_client() -> anyhow::Result<reqwest::Client> {
    let (timeout, connect_timeout) = refresh_client_timeout_settings();
    reqwest::Client::builder()
        .timeout(timeout)
        .connect_timeout(connect_timeout)
        .build()
        .context("failed to build snapshot refresh HTTP client")
}

fn effective_owner_id(configured: &str) -> String {
    let configured = configured.trim();
    if !configured.is_empty() {
        return configured.to_string();
    }

    let host = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown-host".to_string());
    format!("{}-{}-{}", host, std::process::id(), Uuid::new_v4())
}

fn snapshot_worker_lease_document_id(snapshot_document_id: &str) -> &str {
    snapshot_document_id
}

fn lease_renewal_interval(lease_ttl_seconds: i64) -> Duration {
    let ttl_seconds = u64::try_from(lease_ttl_seconds.max(1)).unwrap_or(1);
    Duration::from_millis(ttl_seconds.saturating_mul(1_000) / 3)
}

fn spawn_lease_renewal_task(
    state: Arc<State>,
    lease_document_id: String,
    owner_id: String,
    lease_ttl_seconds: i64,
) -> (
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<anyhow::Result<()>>,
) {
    let (stop_renewal, mut stop_requested) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        let collection = state.listing_snapshot_worker_lease_collection();
        let interval = lease_renewal_interval(lease_ttl_seconds);

        loop {
            tokio::select! {
                _ = &mut stop_requested => return Ok(()),
                _ = tokio::time::sleep(interval) => {
                    let renewed = try_renew_snapshot_worker_lease(
                        &collection,
                        &lease_document_id,
                        &owner_id,
                        Utc::now(),
                        lease_ttl_seconds,
                    )
                    .await?;

                    if !renewed {
                        bail!("lost listings snapshot worker lease before snapshot tick completed");
                    }
                }
            }
        }
    });

    (stop_renewal, handle)
}

async fn run_tick_critical_section_with_lease_renewal<T, Fut>(
    stop_renewal: tokio::sync::oneshot::Sender<()>,
    mut renewal_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
    critical_section: Fut,
) -> anyhow::Result<T>
where
    Fut: Future<Output = anyhow::Result<T>>,
{
    tokio::pin!(critical_section);

    tokio::select! {
        tick_result = &mut critical_section => {
            let renewal_result = stop_lease_renewal_task(stop_renewal, renewal_handle).await;
            match (tick_result, renewal_result) {
                (Ok(outcome), Ok(())) => Ok(outcome),
                (Err(error), _) => Err(error),
                (Ok(_), Err(error)) => Err(error),
            }
        }
        renewal_result = &mut renewal_handle => {
            drop(stop_renewal);
            match renewal_result.context("listings snapshot worker lease renewal task panicked")? {
                Ok(()) => bail!("listings snapshot worker lease renewal stopped before snapshot tick completed"),
                Err(error) => Err(error),
            }
        }
    }
}

async fn stop_lease_renewal_task(
    stop_renewal: tokio::sync::oneshot::Sender<()>,
    renewal_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
) -> anyhow::Result<()> {
    let _ = stop_renewal.send(());
    renewal_handle
        .await
        .context("listings snapshot worker lease renewal task panicked")?
}

async fn send_refresh_request(
    client: &reqwest::Client,
    refresh_url: &str,
    client_id: &str,
    secret: &str,
    snapshot_document_id: &str,
    expected_revision: u64,
) -> anyhow::Result<()> {
    let timestamp = Utc::now().timestamp().to_string();
    let nonce = Uuid::new_v4().to_string();
    let payload = refresh_signature_payload(
        &timestamp,
        &nonce,
        client_id,
        snapshot_document_id,
        expected_revision,
    );
    let signature = compute_refresh_signature(secret, &payload)
        .context("failed to sign snapshot refresh request")?;

    let response = client
        .post(refresh_url)
        .header("x-rpf-client-id", client_id)
        .header("x-rpf-snapshot-document-id", snapshot_document_id)
        .header("x-rpf-expected-revision", expected_revision)
        .header("x-rpf-timestamp", timestamp)
        .header("x-rpf-nonce", nonce)
        .header("x-rpf-signature-version", "v1")
        .header("x-rpf-signature", signature)
        .send()
        .await
        .context("failed to send snapshot refresh request")?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        bail!("snapshot refresh request failed with status {status}: {body}");
    }

    Ok(())
}

fn refresh_signature_payload(
    timestamp: &str,
    nonce: &str,
    client_id: &str,
    snapshot_document_id: &str,
    expected_revision: u64,
) -> String {
    format!(
        "POST\n{}\n{}\n{}\n{}\n{}\n{}",
        SNAPSHOT_REFRESH_PATH, timestamp, nonce, client_id, snapshot_document_id, expected_revision
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::const_new(());

    struct EnvGuard {
        saved: Vec<(&'static str, Option<String>)>,
    }

    impl EnvGuard {
        fn set(vars: &[(&'static str, &'static str)]) -> Self {
            let saved = vars
                .iter()
                .map(|(name, _)| (*name, std::env::var(name).ok()))
                .collect();
            for (name, value) in vars {
                std::env::set_var(name, value);
            }
            Self { saved }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (name, value) in self.saved.drain(..) {
                if let Some(value) = value {
                    std::env::set_var(name, value);
                } else {
                    std::env::remove_var(name);
                }
            }
        }
    }

    fn write_temp_config(contents: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("rpf-snapshot-worker-config-{nanos}.toml"));
        std::fs::write(&path, contents).expect("temp config should be writable");
        path
    }

    #[test]
    fn first_tick_builds_snapshot() {
        assert_eq!(
            should_build_snapshot(None, 0, false),
            SnapshotWorkerDecision::Build
        );
    }

    #[test]
    fn same_revision_does_not_build_unless_force_due() {
        assert_eq!(
            should_build_snapshot(Some(7), 7, false),
            SnapshotWorkerDecision::Skip
        );
        assert_eq!(
            should_build_snapshot(Some(7), 7, true),
            SnapshotWorkerDecision::Build
        );
    }

    #[test]
    fn advanced_revision_builds_snapshot() {
        assert_eq!(
            should_build_snapshot(Some(7), 8, false),
            SnapshotWorkerDecision::Build
        );
    }

    #[test]
    fn stale_source_revision_does_not_build_without_force() {
        assert_eq!(
            should_build_snapshot(Some(8), 7, false),
            SnapshotWorkerDecision::Skip
        );
    }

    #[test]
    fn refresh_signature_uses_server_payload_contract() {
        let payload = refresh_signature_payload("1700000000", "nonce-1", "worker-1", "current", 42);
        assert_eq!(
            payload,
            "POST\n/internal/listings/snapshot/refresh\n1700000000\nnonce-1\nworker-1\ncurrent\n42"
        );

        let signature =
            compute_refresh_signature("secret", &payload).expect("signature should compute");

        assert_eq!(signature, "u2G9q1s1WsLyP9wTEmIByvcciQTLEOvLtcn7pm8GNO0=");
    }

    #[test]
    fn args_accept_default_config_config_flag_positional_and_once() {
        assert_eq!(
            parse_args(Vec::<String>::new()).unwrap(),
            WorkerArgs {
                config_path: "./config.toml".to_string(),
                once: false
            }
        );
        assert_eq!(
            parse_args(["--config", "worker.toml", "--once"].map(str::to_string)).unwrap(),
            WorkerArgs {
                config_path: "worker.toml".to_string(),
                once: true
            }
        );
        assert_eq!(
            parse_args(["worker.toml"].map(str::to_string)).unwrap(),
            WorkerArgs {
                config_path: "worker.toml".to_string(),
                once: false
            }
        );
    }

    #[test]
    fn lease_document_id_uses_configured_snapshot_document_id() {
        assert_eq!(
            snapshot_worker_lease_document_id("custom-current"),
            "custom-current"
        );
    }

    #[test]
    fn lease_renewal_interval_is_conservative_relative_to_ttl() {
        assert_eq!(lease_renewal_interval(120), Duration::from_secs(40));
        assert_eq!(lease_renewal_interval(3), Duration::from_secs(1));
        assert_eq!(lease_renewal_interval(1), Duration::from_millis(333));
        assert_eq!(lease_renewal_interval(0), Duration::from_millis(333));
    }

    #[tokio::test]
    async fn load_config_uses_config_file_as_single_source() {
        let _lock = ENV_LOCK.lock().await;
        let _env = EnvGuard::set(&[
            ("RPF_MONGO_URL", "mongodb://env-worker"),
            ("RPF_SNAPSHOT_REFRESH_SHARED_SECRET", "env-secret"),
            ("RPF_SNAPSHOT_REFRESH_CLIENT_ID", "env-client"),
            ("RPF_SNAPSHOT_WORKER_TICK_SECONDS", "99"),
            (
                "RPF_SNAPSHOT_WORKER_REFRESH_URL",
                "https://env.example/internal/listings/snapshot/refresh",
            ),
        ]);
        let path = write_temp_config(
            r#"
[web]
host = "127.0.0.1:8124"
listings_snapshot_source = "materialized"
snapshot_refresh_shared_secret = "file-secret"
snapshot_refresh_client_id = "file-client"
listings_snapshot_document_id = "file-doc"

[mongo]
url = "mongodb://file-worker"

[snapshot_worker]
enabled = true
tick_seconds = 5
force_rebuild_interval_seconds = 300
lease_ttl_seconds = 120
owner_id = "file-owner"
refresh_url = "https://file.example/internal/listings/snapshot/refresh"
log_filter = "warn"
"#,
        );

        let config = load_config(&path).await.expect("file config should load");
        let _ = std::fs::remove_file(path);

        assert_eq!(config.mongo.url, "mongodb://file-worker");
        assert_eq!(config.web.snapshot_refresh_shared_secret, "file-secret");
        assert_eq!(config.web.snapshot_refresh_client_id, "file-client");
        assert_eq!(config.web.listings_snapshot_document_id, "file-doc");
        assert_eq!(config.snapshot_worker.tick_seconds, 5);
        assert_eq!(config.snapshot_worker.owner_id, "file-owner");
        assert_eq!(
            config.snapshot_worker.refresh_url,
            "https://file.example/internal/listings/snapshot/refresh"
        );
        assert_eq!(config.snapshot_worker.log_filter, "warn");
    }

    #[test]
    fn refresh_client_uses_bounded_timeouts() {
        let (request_timeout, connect_timeout) = refresh_client_timeout_settings();

        assert_eq!(request_timeout, Duration::from_secs(30));
        assert_eq!(connect_timeout, Duration::from_secs(5));
        build_refresh_client().expect("bounded refresh client should build");
    }

    #[tokio::test]
    async fn lease_renewal_failure_aborts_tick_before_side_effects() {
        let (stop_renewal, _stop_requested) = tokio::sync::oneshot::channel();
        let renewal_handle = tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(1)).await;
            Err(anyhow::anyhow!("lost lease"))
        });
        let side_effect_ran = Arc::new(AtomicBool::new(false));

        let result = run_tick_critical_section_with_lease_renewal(stop_renewal, renewal_handle, {
            let side_effect_ran = Arc::clone(&side_effect_ran);
            async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                side_effect_ran.store(true, Ordering::SeqCst);
                Ok(())
            }
        })
        .await;

        let error = result.expect_err("lease renewal failure must abort the tick");
        assert!(
            error.to_string().contains("lost lease"),
            "unexpected error: {error:#}"
        );
        assert!(
            !side_effect_ran.load(Ordering::SeqCst),
            "critical section must be cancelled before later side effects run"
        );
    }
}
