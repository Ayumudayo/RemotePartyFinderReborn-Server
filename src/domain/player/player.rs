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
    /// 현재 서버 ID (관측 시점, World Visit 포함)
    #[serde(default)]
    pub current_world: u16,
    /// 마지막으로 관측된 시각
    #[serde(with = "mongodb::bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub last_seen: DateTime<Utc>,
    /// 관측 횟수 (신뢰도 지표)
    pub seen_count: u32,
    /// 계정 ID (AccountId) - Optional allows backward compatibility but we settle on default "-1"
    #[serde(
        default = "default_account_id",
        deserialize_with = "deserialize_account_id"
    )]
    pub account_id: String,
}

fn default_account_id() -> String {
    "-1".to_string()
}

fn deserialize_account_id<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum AccountIdRepr {
        String(String),
        Signed(i64),
        Unsigned(u64),
    }

    let value = Option::<AccountIdRepr>::deserialize(deserializer)?;
    Ok(match value {
        Some(AccountIdRepr::String(value)) => {
            if value.trim().is_empty() {
                default_account_id()
            } else {
                value
            }
        }
        Some(AccountIdRepr::Signed(value)) => value.to_string(),
        Some(AccountIdRepr::Unsigned(value)) => value.to_string(),
        None => default_account_id(),
    })
}

/// 플러그인에서 업로드하는 플레이어 데이터
#[derive(Debug, Deserialize)]
pub struct UploadablePlayer {
    pub content_id: u64,
    pub name: String,
    pub home_world: u16,
    #[serde(default)]
    pub current_world: u16,
    #[serde(default)]
    pub account_id: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UploadableCharacterIdentity {
    pub content_id: u64,
    pub name: String,
    pub home_world: u16,
    #[serde(default)]
    pub world_name: String,
    #[serde(default)]
    pub source: String,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct IdentityMergeResult {
    pub player: Player,
    pub identity_observed_at: DateTime<Utc>,
    pub world_name: String,
    pub source: String,
    pub applied_incoming_identity: bool,
}

pub fn merge_identity_into_player(
    existing: Option<&Player>,
    existing_identity_observed_at: Option<DateTime<Utc>>,
    identity: &UploadableCharacterIdentity,
    now: DateTime<Utc>,
) -> IdentityMergeResult {
    match existing {
        Some(existing) => {
            let freshness_cutoff = existing_identity_observed_at.unwrap_or(existing.last_seen);
            let missing_identity = existing.name.trim().is_empty() || existing.home_world == 0;
            let should_apply_incoming =
                missing_identity || identity.observed_at >= freshness_cutoff;

            let player = if should_apply_incoming {
                Player {
                    content_id: existing.content_id,
                    name: identity.name.clone(),
                    home_world: identity.home_world,
                    current_world: if existing.current_world != 0 {
                        existing.current_world
                    } else {
                        identity.home_world
                    },
                    last_seen: existing.last_seen,
                    seen_count: existing.seen_count,
                    account_id: existing.account_id.clone(),
                }
            } else {
                existing.clone()
            };

            IdentityMergeResult {
                player,
                identity_observed_at: if should_apply_incoming {
                    identity.observed_at
                } else {
                    freshness_cutoff
                },
                world_name: if should_apply_incoming {
                    identity.world_name.clone()
                } else {
                    String::new()
                },
                source: if should_apply_incoming {
                    identity.source.clone()
                } else {
                    String::new()
                },
                applied_incoming_identity: should_apply_incoming,
            }
        }
        None => IdentityMergeResult {
            player: Player {
                content_id: identity.content_id,
                name: identity.name.clone(),
                home_world: identity.home_world,
                current_world: identity.home_world,
                last_seen: now,
                seen_count: 0,
                account_id: default_account_id(),
            },
            identity_observed_at: identity.observed_at,
            world_name: identity.world_name.clone(),
            source: identity.source.clone(),
            applied_incoming_identity: true,
        },
    }
}

impl From<UploadablePlayer> for Player {
    fn from(value: UploadablePlayer) -> Self {
        Self {
            content_id: value.content_id,
            name: value.name,
            home_world: value.home_world,
            current_world: value.current_world,
            last_seen: Utc::now(),
            seen_count: 1,
            account_id: if value.account_id == 0 {
                "-1".to_string()
            } else {
                value.account_id.to_string()
            },
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
