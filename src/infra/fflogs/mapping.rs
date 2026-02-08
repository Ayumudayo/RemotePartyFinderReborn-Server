//! FFLogs API Zone/Encounter ID 매핑
//!
//! FFXIV Duty ID를 FFLogs의 Zone/Encounter ID로 매핑합니다.
//! 고난이도 컨텐츠(Savage, Ultimate, Extreme)만 매핑합니다.
//!
//! 참고: FFLogsViewer 플러그인의 Configuration.cs에서 Encounter ID 확인

use std::collections::HashMap;

/// FFLogs Encounter 정보
#[derive(Debug, Clone, Copy)]
pub struct FFLogsEncounter {
    /// FFLogs Zone ID (레이드 티어)
    pub zone_id: u32,
    /// FFLogs Encounter ID (개별 보스)
    pub encounter_id: u32,
    /// Difficulty ID (101=Savage, 100=Normal/Ult/Ext)
    pub difficulty_id: Option<u32>,
    /// Secondary Encounter ID (for split bosses like P2)
    pub secondary_encounter_id: Option<u32>,
    /// 컨텐츠 이름 (영문)
    pub name: &'static str,
}

lazy_static::lazy_static! {
    /// FFXIV Duty ID -> FFLogs Encounter 매핑
    ///
    /// NOTE: Duty ID는 duties.rs 파일에서 정의된 값을 사용합니다.
    /// FFLogs Zone/Encounter ID는 FFLogsViewer 플러그인에서 확인
    pub static ref DUTY_TO_FFLOGS: HashMap<u16, FFLogsEncounter> = {
        let mut m = HashMap::new();

        // Helper closures
        let ult = |zone, id, name| FFLogsEncounter { zone_id: zone, encounter_id: id, difficulty_id: Some(100), secondary_encounter_id: None, name };
        let sav = |zone, id, name| FFLogsEncounter { zone_id: zone, encounter_id: id, difficulty_id: Some(101), secondary_encounter_id: None, name };
        let ext = |zone, id, name| FFLogsEncounter { zone_id: zone, encounter_id: id, difficulty_id: Some(100), secondary_encounter_id: None, name };
        // Split encounter helper
        let sav_split = |zone, id1, id2, name| FFLogsEncounter { zone_id: zone, encounter_id: id1, difficulty_id: Some(101), secondary_encounter_id: Some(id2), name };

        // =================================================================
        // Dawntrail (7.4) - AAC Heavyweight Tier (Savage) - M9~M12
        // Zone ID: 73
        // duties.rs: 1069, 1071, 1073, 1075
        // FFLogsViewer: EncounterId 101, 102, 103, 104 (105 is P2)
        // =================================================================
        m.insert(1069, sav(73, 101, "AAC Heavyweight M1 (Savage)")); // M9S - Vamp Fatale
        m.insert(1071, sav(73, 102, "AAC Heavyweight M2 (Savage)")); // M10S - Red Hot and Deep Blue
        m.insert(1073, sav(73, 103, "AAC Heavyweight M3 (Savage)")); // M11S - The Tyrant
        m.insert(1075, sav_split(73, 104, 105, "AAC Heavyweight M4 (Savage)")); // M12S - The Lindwurm (P1 & P2)

        // =================================================================
        // Dawntrail (7.4) - Extreme Trial
        // Zone ID: 72
        // duties.rs: 1077
        // FFLogsViewer: Doomtrain = 1083
        // =================================================================
        m.insert(1077, ext(72, 1083, "Hell on Rails (Extreme)")); // 극 글라샬라볼라스

        // =================================================================
        // Dawntrail (7.2) - AAC Cruiserweight Tier (Savage) - M5~M8
        // Zone ID: 68
        // TODO: duties.rs에서 Duty ID 확인 필요
        // FFLogsViewer: 97, 98, 99, 100
        // =================================================================
        // m.insert(????, sav(68, 97, "AAC Cruiserweight M1 (Savage)")); // M5S
        // m.insert(????, sav(68, 98, "AAC Cruiserweight M2 (Savage)")); // M6S
        // m.insert(????, sav(68, 99, "AAC Cruiserweight M3 (Savage)")); // M7S
        // m.insert(????, sav(68, 100, "AAC Cruiserweight M4 (Savage)")); // M8S

        // =================================================================
        // Dawntrail (7.0) - AAC Light-heavyweight Tier (Savage) - M1~M4
        // Zone ID: 62
        // TODO: duties.rs에서 Duty ID 확인 필요
        // FFLogsViewer: 93, 94, 95, 96
        // =================================================================
        // m.insert(????, sav(62, 93, "AAC Light-heavyweight M1 (Savage)")); // M1S
        // m.insert(????, sav(62, 94, "AAC Light-heavyweight M2 (Savage)")); // M2S
        // m.insert(????, sav(62, 95, "AAC Light-heavyweight M3 (Savage)")); // M3S
        // m.insert(????, sav(62, 96, "AAC Light-heavyweight M4 (Savage)")); // M4S

        // =================================================================
        // Ultimates (Dawntrail - Zone 59 Legacy)
        // duties.rs: 280, 539, 694, 788, 908, 1006
        // FFLogsViewer: Zone 59 with ids 1073-1077, Zone 65 with 1079
        // =================================================================
        // 절바하 - Duty 280
        m.insert(280, ult(59, 1073, "The Unending Coil of Bahamut (Ultimate)"));
        // 절신 - Duty 539
        m.insert(539, ult(59, 1074, "The Weapon's Refrain (Ultimate)"));
        // 절알렉 - Duty 694
        m.insert(694, ult(59, 1075, "The Epic of Alexander (Ultimate)"));
        // 절용시 - Duty 788
        m.insert(788, ult(59, 1076, "Dragonsong's Reprise (Ultimate)"));
        // 절오메가 - Duty 908
        m.insert(908, ult(59, 1077, "The Omega Protocol (Ultimate)"));
        // 절미래 (절에덴) - Duty 1006, Zone 65
        m.insert(1006, ult(65, 1079, "Futures Rewritten (Ultimate)"));

        m
    };

    /// FFLogs Zone ID -> Zone 정보
    pub static ref FFLOGS_ZONES: HashMap<u32, FFLogsZone> = {
        let mut m = HashMap::new();
        m.insert(73, FFLogsZone { name: "AAC Heavyweight (Savage)", partition: 1 });
        m.insert(72, FFLogsZone { name: "Trials III (Extreme)", partition: 1 });
        m.insert(68, FFLogsZone { name: "AAC Cruiserweight (Savage)", partition: 1 });
        m.insert(65, FFLogsZone { name: "Futures Rewritten (Ultimate)", partition: 1 }); 
        m.insert(62, FFLogsZone { name: "AAC Light-heavyweight (Savage)", partition: 1 });
        m.insert(59, FFLogsZone { name: "Ultimates (Legacy)", partition: 1 });
        m
    };
}

/// FFLogs Zone 정보
#[derive(Debug, Clone, Copy)]
pub struct FFLogsZone {
    pub name: &'static str,
    pub partition: u32,
}

/// Duty ID로 FFLogs Encounter 조회
pub fn get_fflogs_encounter(duty_id: u16) -> Option<&'static FFLogsEncounter> {
    DUTY_TO_FFLOGS.get(&duty_id)
}

/// 해당 Duty가 FFLogs 조회 대상인지 확인
pub fn is_fflogs_supported(duty_id: u16) -> bool {
    DUTY_TO_FFLOGS.contains_key(&duty_id)
}

/// FFLogs percentile 색상 클래스 반환
pub fn percentile_color_class(percentile: f32) -> &'static str {
    match percentile as u32 {
        100 => "parse-gold",
        99 => "parse-pink",
        95..=98 => "parse-orange",
        75..=94 => "parse-purple",
        50..=74 => "parse-blue",
        25..=49 => "parse-green",
        _ => "parse-gray",
    }
}

/// FFLogs percentile RGB 색상 반환
pub fn percentile_color(percentile: f32) -> &'static str {
    match percentile as u32 {
        100 => "#E5CC80",
        99 => "#E268A8",
        95..=98 => "#FF8000",
        75..=94 => "#A335EE",
        50..=74 => "#0070FF",
        25..=49 => "#1EFF00",
        _ => "#666666",
    }
}
