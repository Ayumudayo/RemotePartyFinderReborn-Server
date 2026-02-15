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
    /// FFLogs 캐릭터 매칭이 추정(동명이인 후보 탐색) 결과인지 여부
    ///
    /// NOTE: ContentId에 대해 정확한 home_world를 모를 때, name+world 후보를 시도하여
    /// 가장 그럴듯한 캐릭터를 선택한다. 오탐 가능성이 있으므로 UI에서 표시한다.
    #[serde(default)]
    pub estimated: bool,
    /// 추정 매칭에 사용된 서버(월드) slug
    #[serde(default)]
    pub matched_server: Option<String>,
    /// FFLogs에서 해당 캐릭터가 Hidden 처리되어 조회 불가한지 여부
    ///
    /// NOTE: Hidden은 FFLogs의 프라이버시 설정이며, OAuth(User Mode) 없이 우회 불가.
    #[serde(default)]
    pub hidden: bool,
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
    /// 해당 Encounter의 베스트 진행도(보스 남은 체력 %) (0-100)
    ///
    /// None이면 진행도 데이터를 아직 수집하지 않았거나, 조회할 리포트가 없는 상태.
    #[serde(default)]
    pub boss_percentage: Option<f32>,
}

/// Zone 캐시가 만료되었는지 확인 (갱신 기준: 24시간)
pub fn is_zone_cache_expired(zone_cache: &ZoneCache) -> bool {
    // 추정 매칭 캐시는 오탐 가능성이 있으므로, 비교적 빠르게 만료시켜 재평가한다.
    // (특히 hidden 오탐을 영구 캐시하면 치명적)
    if zone_cache.estimated {
        let expire_threshold = Utc::now() - TimeDelta::try_hours(6).unwrap();
        return zone_cache.fetched_at < expire_threshold;
    }

    // Hidden 캐릭터는 재조회해도 결과가 바뀌지 않으므로, 만료 처리하지 않는다.
    if zone_cache.hidden {
        return false;
    }

    let expire_threshold = Utc::now() - TimeDelta::try_hours(24).unwrap();
    zone_cache.fetched_at < expire_threshold
}
