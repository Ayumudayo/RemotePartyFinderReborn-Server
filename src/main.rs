#![feature(try_blocks, iter_intersperse)]


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

#[tokio::main]
async fn main() {
    // 로깅 초기화: 콘솔 + 일별 로테이션 파일
    let file_appender = tracing_appender::rolling::Builder::new()
        .rotation(tracing_appender::rolling::Rotation::DAILY)
        .filename_prefix("server")
        .filename_suffix("log")
        .build("logs")
        .expect("initializing rolling file appender failed");

    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into())
        )
        .with_writer(std::io::stderr.and(non_blocking))
        .with_ansi(true)
        .init();

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
