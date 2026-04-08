use crate::ffxiv::Language;
use crate::listing::JobFlags;

use crate::listing_container::QueriedListing;
use crate::sestring_ext::SeStringExt;
use askama::Template;
use std::borrow::Borrow;

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
}

#[derive(Debug)]
pub struct RenderableListing {
    pub container: QueriedListing,
    pub members: Vec<RenderableMember>,
    /// 파티장 로그 정보 (멤버 정보가 없어도 표시 가능)
    pub leader_parse: ParseDisplay,
}

impl RenderableListing {
    pub fn is_alliance_view(&self) -> bool {
        self.container.listing.num_parties >= 3
            || self.members.iter().any(|member| member.party_index > 0)
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
mod tests {
    use super::{encode_fflogs_path_segment, ParseDisplay, RenderableMember};
    use chrono::Utc;

    fn sample_member(name: &str, home_world: u16) -> RenderableMember {
        RenderableMember {
            job_id: 19,
            player: crate::player::Player {
                content_id: 1,
                name: name.to_string(),
                home_world,
                current_world: home_world,
                last_seen: Utc::now(),
                seen_count: 1,
                account_id: "-1".to_string(),
            },
            parse: Default::default(),
            progress: Default::default(),
            slot_index: 0,
            party_index: 0,
            party_header: None,
        }
    }

    #[test]
    fn encode_fflogs_path_segment_percent_encodes_spaces_and_symbols() {
        assert_eq!(encode_fflogs_path_segment("Karen Fukada"), "Karen%20Fukada");
        assert_eq!(encode_fflogs_path_segment("A'zuki"), "A%27zuki");
    }

    #[test]
    fn member_link_is_generated_for_known_name_and_world() {
        // 73 = Masamune (known JP world)
        let member = sample_member("Sayo Shijima", 73);
        let link = member
            .fflogs_character_url()
            .expect("expected FFLogs profile link for known member");

        assert!(link.starts_with("https://www.fflogs.com/character/"));
        assert!(link.ends_with("/Sayo%20Shijima"));
    }

    #[test]
    fn member_link_is_not_generated_for_unknown_or_placeholder_names() {
        let unknown = sample_member("Unknown Member", 73);
        assert!(unknown.fflogs_character_url().is_none());

        let placeholder = sample_member("Party Leader", 73);
        assert!(placeholder.fflogs_character_url().is_none());
    }

    #[test]
    fn progress_hides_boss_hp_when_percentile_exists_and_shows_clear_count() {
        let progress = super::ProgressDisplay::new(
            Some(23),
            None,
            Some(95),
            None,
            Some(7),
            None,
            false,
            false,
        );

        assert_eq!(progress.final_boss_percentage, None);
        assert_eq!(progress.final_clear_count, Some(7));
    }

    #[test]
    fn progress_shows_boss_hp_when_not_cleared() {
        let progress =
            super::ProgressDisplay::new(Some(17), None, None, None, None, None, false, false);

        assert_eq!(progress.final_boss_percentage, Some(17));
        assert_eq!(progress.final_clear_count, None);
    }

    #[test]
    fn parse_display_exposes_report_parse_badge_only_for_report_parse_source() {
        let plugin = ParseDisplay::new(
            Some(95),
            "parse-orange".to_string(),
            None,
            "parse-none".to_string(),
            false,
            false,
            false,
            crate::parse_resolver::ParseSource::Plugin,
        );
        let fallback = ParseDisplay::new(
            Some(88),
            "parse-purple".to_string(),
            None,
            "parse-none".to_string(),
            false,
            false,
            false,
            crate::parse_resolver::ParseSource::ReportParse,
        );

        assert_eq!(plugin.report_parse_badge(), None);
        assert_eq!(fallback.report_parse_badge(), Some("RP"));
    }
}

// Deref to QueriedListing to make template access compatible (e.g. methods)?
// Or just access .container in template.
// Actually, Deref might be easier for migration.
impl std::ops::Deref for RenderableListing {
    type Target = QueriedListing;
    fn deref(&self) -> &Self::Target {
        &self.container
    }
}
