//! FFLogs 관련 모듈
//!
//! - `client`: FFLogs API 클라이언트
//! - `mapping`: FFXIV Duty ID ↔ FFLogs Zone/Encounter 매핑
//! - `cache`: Parse 캐시 타입

pub mod cache;
pub mod client;
pub mod mapping;

// 편의를 위한 re-export
pub use cache::{
    is_zone_cache_expired, make_zone_cache_key, EncounterParse, ParseCacheDoc, ZoneCache,
};
pub use client::get_region_from_server;
pub use mapping::{
    get_fflogs_encounter, percentile_color_class, FFLogsEncounter, DUTY_TO_FFLOGS, FFLOGS_ZONES,
};
