use crate::ffxiv::Language;
use crate::listing::JobFlags;

use crate::listing_container::QueriedListing;
use crate::sestring_ext::SeStringExt;
use askama::Template;
use std::borrow::Borrow;
use std::sync::OnceLock;

pub fn listing_data_asset_version() -> &'static str {
    static VERSION: OnceLock<String> = OnceLock::new();

    VERSION
        .get_or_init(|| {
            use sha2::{Digest, Sha256};

            let digest = Sha256::digest(include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/assets/listing-data.js"
            )));

            digest
                .iter()
                .take(8)
                .map(|byte| format!("{byte:02x}"))
                .collect()
        })
        .as_str()
}

fn is_known_member_name(name: &str) -> bool {
    let trimmed = name.trim();
    !trimmed.is_empty()
        && !trimmed.eq_ignore_ascii_case("Unknown Member")
        && !trimmed.eq_ignore_ascii_case("Party Leader")
}

fn encode_fflogs_path_segment(input: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";

    let mut out = String::with_capacity(input.len());
    for b in input.as_bytes() {
        let b = *b;
        let is_unreserved = b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~');
        if is_unreserved {
            out.push(char::from(b));
        } else {
            out.push('%');
            out.push(char::from(HEX[(b >> 4) as usize]));
            out.push(char::from(HEX[(b & 0x0F) as usize]));
        }
    }

    out
}

#[derive(Debug, Template)]
#[template(path = "listings.html")]
pub struct ListingsTemplate {
    pub containers: Vec<RenderableListing>,
    pub lang: Language,
    pub listing_data_asset_version: &'static str,
}

impl ListingsTemplate {
    pub fn new(containers: Vec<RenderableListing>, lang: Language) -> Self {
        Self {
            containers,
            lang,
            listing_data_asset_version: listing_data_asset_version(),
        }
    }
}

#[derive(Debug)]
pub struct RenderableListing {
    pub container: QueriedListing,
    pub members: Vec<RenderableMember>,
    /// 파티장 로그 정보 (멤버 정보가 없어도 표시 가능)
    pub leader_parse: ParseDisplay,
}

#[derive(Debug)]
pub struct AllianceMemberGroup<'a> {
    pub label: &'static str,
    pub members: Vec<&'a RenderableMember>,
}

impl RenderableListing {
    pub fn is_alliance_view(&self) -> bool {
        self.container.listing.num_parties >= 3
            || self.members.iter().any(|member| member.party_index > 0)
    }

    pub fn alliance_member_groups(&self) -> Vec<AllianceMemberGroup<'_>> {
        let mut grouped = [
            AllianceMemberGroup {
                label: "Alliance A",
                members: Vec::new(),
            },
            AllianceMemberGroup {
                label: "Alliance B",
                members: Vec::new(),
            },
            AllianceMemberGroup {
                label: "Alliance C",
                members: Vec::new(),
            },
        ];

        for member in &self.members {
            if let Some(group) = grouped.get_mut(usize::from(member.party_index)) {
                group.members.push(member);
            }
        }

        grouped
            .into_iter()
            .filter(|group| !group.members.is_empty())
            .collect()
    }
}

/// Parse percentile 표시 정보
#[derive(Debug, Clone, Default)]
pub struct ParseDisplay {
    pub primary_percentile: Option<u8>,
    pub primary_color_class: String,
    pub secondary_percentile: Option<u8>,
    pub secondary_color_class: String,
    pub has_secondary: bool,
    /// FFLogs에서 Hidden 처리된 캐릭터인지 여부
    pub hidden: bool,
    /// Hidden plugin cache 때문에 fallback parse를 쓰는지 여부
    pub originally_hidden: bool,
    /// FFLogs 캐릭터 매칭이 추정(동명이인 후보 탐색) 결과인지 여부
    pub estimated: bool,
    pub source: crate::parse_resolver::ParseSource,
}

impl ParseDisplay {
    /// 데이터로부터 생성
    pub fn new(
        p1: Option<u8>,
        p1_class: String,
        p2: Option<u8>,
        p2_class: String,
        has_secondary: bool,
        hidden: bool,
        originally_hidden: bool,
        estimated: bool,
        source: crate::parse_resolver::ParseSource,
    ) -> Self {
        Self {
            primary_percentile: p1,
            primary_color_class: p1_class,
            secondary_percentile: p2,
            secondary_color_class: p2_class,
            has_secondary,
            hidden,
            originally_hidden,
            estimated,
            source,
        }
    }

    pub fn report_parse_badge(&self) -> Option<&'static str> {
        match self.source {
            crate::parse_resolver::ParseSource::ReportParse => Some("RP"),
            _ => None,
        }
    }

    /// UI policy helper:
    /// only report-parse fallback rows render the right-rail HID tag.
    /// plain hidden FFLogs rows keep the left-side HID parse state only.
    pub fn hidden_rail_tag_label(&self) -> Option<&'static str> {
        if self.originally_hidden {
            Some("HID")
        } else {
            None
        }
    }

    pub fn hidden_rail_tag_title(&self) -> &'static str {
        if self.originally_hidden {
            "FFLogs: Originally hidden player"
        } else {
            "FFLogs: Hidden"
        }
    }
}

/// 진행도(보스 남은 체력 %) 표시 정보
#[derive(Debug, Clone, Default)]
pub struct ProgressDisplay {
    /// 최종 보스 HP% (미클리어자만 표시; 클리어자는 None)
    pub final_boss_percentage: Option<u8>,
    /// 최종 보스 클리어 횟수 (클리어자인 경우 표시)
    pub final_clear_count: Option<u16>,
}

impl ProgressDisplay {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        p1_boss: Option<u8>,
        p2_boss: Option<u8>,
        p1_percentile: Option<u8>,
        p2_percentile: Option<u8>,
        p1_clear_count: Option<u16>,
        p2_clear_count: Option<u16>,
        has_secondary: bool,
        hidden: bool,
    ) -> Self {
        if hidden {
            return Self {
                final_boss_percentage: None,
                final_clear_count: None,
            };
        }

        let (final_boss_raw, final_percentile, final_clear_raw) = if has_secondary {
            (p2_boss, p2_percentile, p2_clear_count)
        } else {
            (p1_boss, p1_percentile, p1_clear_count)
        };

        let final_clear_count = final_clear_raw.filter(|v| *v > 0);

        // Best percentile(= 클리어 기록) 또는 clear_count가 있으면 진행도 HP는 노출하지 않는다.
        let final_boss_percentage = if final_percentile.is_some() || final_clear_count.is_some() {
            None
        } else {
            match final_boss_raw {
                Some(0) => None,
                Some(v) => Some(v),
                None => None,
            }
        };

        Self {
            final_boss_percentage,
            final_clear_count,
        }
    }
}

/// 멤버 정보 + 해당 슬롯의 잡 ID
#[derive(Debug)]
pub struct RenderableMember {
    pub job_id: u8,
    pub player: crate::player::Player,
    pub parse: ParseDisplay,
    pub progress: ProgressDisplay,
    pub slot_index: usize,
    pub party_index: u8,
    pub party_header: Option<&'static str>,
    pub identity_fallback: bool,
}

impl RenderableMember {
    /// 잡 코드 반환 (예: "WHM", "PLD")
    pub fn job_code(&self) -> Option<&'static str> {
        crate::ffxiv::JOBS
            .get(&(self.job_id as u32))
            .map(|cj| cj.code())
    }

    /// 역할에 따른 CSS 클래스 반환 ("tank", "healer", "dps")
    pub fn role_class(&self) -> &'static str {
        use ffxiv_types::Role;
        if let Some(cj) = crate::ffxiv::JOBS.get(&(self.job_id as u32)) {
            match cj.role() {
                Some(Role::Tank) => "tank",
                Some(Role::Healer) => "healer",
                Some(Role::Dps) => "dps",
                None => "",
            }
        } else {
            ""
        }
    }

    pub fn fflogs_character_url(&self) -> Option<String> {
        if self.identity_fallback {
            return None;
        }

        let name = self.player.name.trim();
        if !is_known_member_name(name) {
            return None;
        }

        let world = crate::ffxiv::WORLDS.get(&(self.player.home_world as u32))?;
        let server = world.as_str();
        let region = crate::fflogs::get_region_from_server(server).to_ascii_lowercase();

        Some(format!(
            "https://www.fflogs.com/character/{}/{}/{}",
            region,
            encode_fflogs_path_segment(server),
            encode_fflogs_path_segment(name),
        ))
    }
}

#[cfg(test)]
mod tests;

// Deref to QueriedListing to make template access compatible (e.g. methods)?
// Or just access .container in template.
// Actually, Deref might be easier for migration.
impl std::ops::Deref for RenderableListing {
    type Target = QueriedListing;
    fn deref(&self) -> &Self::Target {
        &self.container
    }
}
