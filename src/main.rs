use anyhow::Context;
use remote_party_finder_reborn::config::Config;
use std::borrow::Cow;
use std::path::Path;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tracing_subscriber::fmt::writer::MakeWriterExt;

fn env_flag_enabled(name: &str) -> bool {
    match std::env::var(name) {
        Ok(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => false,
    }
}

#[tokio::main]
async fn main() {
    // Koyeb 환경에서는 콘솔 로그 접근성이 높으므로 콘솔 출력 우선.
    // 파일 로그가 필요할 때만 RPF_ENABLE_FILE_LOG=1 로 켠다.
    let file_logging_enabled = env_flag_enabled("RPF_ENABLE_FILE_LOG");

    let _file_guard = if file_logging_enabled {
        let file_appender = tracing_appender::rolling::Builder::new()
            .rotation(tracing_appender::rolling::Rotation::DAILY)
            .filename_prefix("server")
            .filename_suffix("log")
            .build("logs")
            .expect("initializing rolling file appender failed");

        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
            )
            .with_writer(std::io::stderr.and(non_blocking))
            .with_ansi(true)
            .with_target(true)
            .with_line_number(true)
            .with_thread_ids(true)
            .init();

        Some(guard)
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
            )
            .with_writer(std::io::stderr)
            .with_ansi(true)
            .with_target(true)
            .with_line_number(true)
            .with_thread_ids(true)
            .init();

        None
    };

    if file_logging_enabled {
        tracing::info!("logging mode: console + ./logs file");
    } else {
        tracing::info!(
            "logging mode: console-first (set RPF_ENABLE_FILE_LOG=1 to enable ./logs mirroring)"
        );
    }

    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let config_path = if args.is_empty() {
        Cow::from("./config.toml")
    } else {
        match args.remove(0).as_str() {
            "server" => Cow::from(
                args.into_iter()
                    .next()
                    .unwrap_or_else(|| "./config.toml".to_string()),
            ),
            path => Cow::from(path.to_string()),
        }
    };

    let config = match get_config(&*config_path).await {
        Ok(config) => config,
        Err(e) => {
            tracing::error!("Failed to load config: {}", e);
            return;
        }
    };

    let config = Arc::new(config);
    let result = remote_party_finder_reborn::web::start(Arc::clone(&config)).await;

    if let Err(e) = result {
        tracing::error!("Server error: {}", e);
        tracing::error!("  {:?}", e);
    }
}

async fn get_config<P: AsRef<Path>>(path: P) -> anyhow::Result<Config> {
    let mut f = File::open(path)
        .await
        .context("could not open config file")?;
    let mut toml = String::new();
    f.read_to_string(&mut toml)
        .await
        .context("could not read config file")?;
    toml::from_str(&toml).context("could not parse config file")
}

#[cfg(test)]
mod tests {
    use super::*;
    use remote_party_finder_reborn::config::ListingsSnapshotSource;
    use std::path::PathBuf;
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

    fn write_temp_config(contents: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("rpf-server-config-{nanos}.toml"));
        std::fs::write(&path, contents).expect("temp config should be writable");
        path
    }

    #[tokio::test]
    async fn get_config_uses_config_file_as_single_source() {
        let _lock = ENV_LOCK.lock().await;
        let _env = EnvGuard::set(&[
            ("PORT", "9999"),
            ("MONGODB_URI", "mongodb://env-mongo"),
            ("RPF_LISTINGS_SNAPSHOT_SOURCE", "materialized"),
            ("RPF_SNAPSHOT_REFRESH_SHARED_SECRET", "env-secret"),
            ("RPF_SNAPSHOT_REFRESH_CLIENT_ID", "env-client"),
        ]);
        let path = write_temp_config(
            r#"
[web]
host = "127.0.0.1:8123"

[mongo]
url = "mongodb://file-mongo"
"#,
        );

        let config = get_config(&path).await.expect("file config should load");
        let _ = std::fs::remove_file(path);

        assert_eq!(config.web.host.to_string(), "127.0.0.1:8123");
        assert_eq!(config.mongo.url, "mongodb://file-mongo");
        assert_eq!(
            config.web.listings_snapshot_source,
            ListingsSnapshotSource::Inline
        );
        assert_eq!(config.web.snapshot_refresh_shared_secret, "");
        assert_eq!(
            config.web.snapshot_refresh_client_id,
            "listings-snapshot-worker"
        );
    }
}
