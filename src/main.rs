use crate::config::Config;
use anyhow::Context;
use std::borrow::Cow;
use std::path::Path;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tracing_subscriber::fmt::writer::MakeWriterExt;

// =============================================================================
// 유틸리티 모듈
// =============================================================================
mod base64_sestring;
mod config;
mod sestring_ext;

// =============================================================================
// FFXIV 데이터 모듈
// =============================================================================
mod ffxiv;

// =============================================================================
// 도메인 레이어 (비즈니스 로직)
// =============================================================================
mod domain;
// 하위 호환성을 위한 re-export
pub use domain::listing;
pub use domain::listing::container as listing_container;
pub use domain::player;
pub use domain::stats;

// =============================================================================
// 인프라 레이어 (외부 시스템 연동)
// =============================================================================
mod infra;
// 하위 호환성을 위한 re-export
pub use infra::mongo;
pub use infra::fflogs;

// =============================================================================
// 웹 레이어
// =============================================================================
mod api;
mod template;
mod web;
mod ws;

#[cfg(test)]
mod test;

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
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
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
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
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
        Cow::from(args.remove(0))
    };

    let config = match get_config(&*config_path).await {
        Ok(config) => config,
        Err(e) => {
            tracing::error!("Failed to load config: {}", e);
            return;
        }
    };

    if let Err(e) = self::web::start(Arc::new(config)).await {
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
    let config = toml::from_str(&toml).context("could not parse config file")?;

    Ok(config)
}
