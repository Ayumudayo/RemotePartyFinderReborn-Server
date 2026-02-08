use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

/// 플레이어 정보 (크라우드소싱으로 수집)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Player {
    /// 캐릭터 고유 ID (PK)
    pub content_id: u64,
    /// 캐릭터 이름
    pub name: String,
    /// 홈 서버 ID
    pub home_world: u16,
    /// 마지막으로 관측된 시각
    #[serde(with = "mongodb::bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub last_seen: DateTime<Utc>,
    /// 관측 횟수 (신뢰도 지표)
    pub seen_count: u32,
}

/// 플러그인에서 업로드하는 플레이어 데이터
#[derive(Debug, Deserialize)]
pub struct UploadablePlayer {
    pub content_id: u64,
    pub name: String,
    pub home_world: u16,
}

impl From<UploadablePlayer> for Player {
    fn from(value: UploadablePlayer) -> Self {
        Self {
            content_id: value.content_id,
            name: value.name,
            home_world: value.home_world,
            last_seen: Utc::now(),
            seen_count: 1,
        }
    }
}

#[allow(unused)]
impl Player {
    pub fn home_world_name(&self) -> Cow<'static, str> {
        crate::ffxiv::WORLDS
            .get(&(self.home_world as u32))
            .map(|w| Cow::Borrowed(w.as_str()))
            .unwrap_or_else(|| Cow::Owned(format!("Unknown ({})", self.home_world)))
    }
}
