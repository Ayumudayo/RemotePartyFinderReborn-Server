use crate::ffxiv::Language;
use crate::listing::JobFlags;

use crate::listing_container::QueriedListing;
use crate::sestring_ext::SeStringExt;
use askama::Template;
use std::borrow::Borrow;

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
    /// 파티장 진행도(보스 남은 체력 %) 정보
    pub leader_progress: ProgressDisplay,
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
    ) -> Self {
        Self {
            primary_percentile: p1,
            primary_color_class: p1_class,
            secondary_percentile: p2,
            secondary_color_class: p2_class,
            has_secondary,
            hidden,
            estimated,
        }
    }
}

/// 진행도(보스 남은 체력 %) 표시 정보
#[derive(Debug, Clone, Default)]
pub struct ProgressDisplay {
    pub primary_boss_percentage: Option<u8>,
    pub secondary_boss_percentage: Option<u8>,
    pub has_secondary: bool,
    /// FFLogs에서 Hidden 처리된 캐릭터인지 여부
    pub hidden: bool,
    /// 최종 보스 HP% (미클리어자만 표시; 클리어자는 None)
    pub final_boss_percentage: Option<u8>,
}

impl ProgressDisplay {
    pub fn new(p1: Option<u8>, p2: Option<u8>, has_secondary: bool, hidden: bool) -> Self {
        let final_raw = if hidden {
            None
        } else if has_secondary {
            p2
        } else {
            p1
        };

        let final_boss_percentage = match final_raw {
            Some(0) => None,
            Some(v) => Some(v),
            None => None,
        };

        Self {
            primary_boss_percentage: p1,
            secondary_boss_percentage: p2,
            has_secondary,
            hidden,
            final_boss_percentage,
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
