//! FFLogs Parse 캐시 타입
//!
//! ContentID별 Parse 캐시 데이터 구조를 정의합니다.

use chrono::{TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// FFLogs Parse 캐시 문서 (ContentID당 1개)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParseCacheDoc {
    /// 플레이어 ContentId
    pub content_id: i64,
    /// Zone별 캐시 데이터 (key: zone_id as string)
    #[serde(default)]
    pub zones: HashMap<String, ZoneCache>,
}

/// Zone별 캐시 데이터
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZoneCache {
    /// 이 Zone의 조회 시각
    #[serde(with = "mongodb::bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub fetched_at: chrono::DateTime<Utc>,
    /// Encounter별 파싱 데이터 (key: encounter_id as string)
    #[serde(default)]
    pub encounters: HashMap<String, EncounterParse>,
}

/// Encounter별 파싱 데이터
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncounterParse {
    /// Best Percentile (0-100, -1이면 로그 없음)
    pub percentile: f32,
    /// 직업 ID (0이면 Best Job)
    #[serde(default)]
    pub job_id: u8,
}

/// Zone 캐시가 만료되었는지 확인 (갱신 기준: 24시간)
pub fn is_zone_cache_expired(zone_cache: &ZoneCache) -> bool {
    let expire_threshold = Utc::now() - TimeDelta::try_hours(24).unwrap();
    zone_cache.fetched_at < expire_threshold
}
