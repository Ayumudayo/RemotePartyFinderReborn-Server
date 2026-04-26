use std::collections::HashMap;

use serde::Serialize;

use crate::fflogs::{EncounterParse, ZoneCache};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ParseSource {
    #[default]
    None,
    Plugin,
    ReportParse,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedParseData {
    pub source: ParseSource,
    pub primary_percentile: Option<u8>,
    pub primary_color_class: String,
    pub secondary_percentile: Option<u8>,
    pub secondary_color_class: String,
    pub has_secondary: bool,
    pub hidden: bool,
    pub originally_hidden: bool,
    pub estimated: bool,
    pub primary_boss_percentage: Option<u8>,
    pub secondary_boss_percentage: Option<u8>,
    pub primary_clear_count: Option<u16>,
    pub secondary_clear_count: Option<u16>,
}

impl ResolvedParseData {
    fn empty(has_secondary: bool) -> Self {
        Self {
            source: ParseSource::None,
            primary_percentile: None,
            primary_color_class: "parse-none".to_string(),
            secondary_percentile: None,
            secondary_color_class: "parse-none".to_string(),
            has_secondary,
            hidden: false,
            originally_hidden: false,
            estimated: false,
            primary_boss_percentage: None,
            secondary_boss_percentage: None,
            primary_clear_count: None,
            secondary_clear_count: None,
        }
    }
}

fn hydrate_slot(
    encounters: &HashMap<String, EncounterParse>,
    encounter_id: u32,
) -> (Option<u8>, String, Option<u8>, Option<u16>) {
    hydrate_slot_with_display_rule(encounters, encounter_id, encounter_has_display_percentile)
}

fn hydrate_fallback_slot(
    encounters: &HashMap<String, EncounterParse>,
    encounter_id: u32,
) -> (Option<u8>, String, Option<u8>, Option<u16>) {
    hydrate_slot_with_display_rule(
        encounters,
        encounter_id,
        encounter_has_report_fallback_display_percentile,
    )
}

fn hydrate_slot_with_display_rule(
    encounters: &HashMap<String, EncounterParse>,
    encounter_id: u32,
    has_display_percentile: fn(&EncounterParse) -> bool,
) -> (Option<u8>, String, Option<u8>, Option<u16>) {
    let Some(enc_parse) = encounters.get(&encounter_id.to_string()) else {
        return (None, "parse-none".to_string(), None, None);
    };

    let percentile = if has_display_percentile(enc_parse) {
        Some(enc_parse.percentile.clamp(0.0, 100.0).floor() as u8)
    } else {
        None
    };

    let color_class = percentile
        .map(|_| crate::fflogs::mapping::percentile_color_class(enc_parse.percentile).to_string())
        .unwrap_or_else(|| "parse-none".to_string());

    let boss_percentage = enc_parse
        .boss_percentage
        .map(|value| value.round().clamp(0.0, 100.0) as u8);
    let clear_count = enc_parse
        .clear_count
        .map(|value| value.min(u16::MAX as u32) as u16);

    (percentile, color_class, boss_percentage, clear_count)
}

fn encounter_has_positive_clear_count(encounter: &EncounterParse) -> bool {
    encounter.clear_count.is_some_and(|value| value > 0)
}

fn encounter_has_display_percentile(encounter: &EncounterParse) -> bool {
    encounter.percentile > 0.0
        || (encounter.percentile == 0.0 && encounter_has_positive_clear_count(encounter))
}

fn encounter_has_report_fallback_display_percentile(encounter: &EncounterParse) -> bool {
    encounter.percentile > 0.0
}

fn encounter_has_relevant_data(encounter: &EncounterParse) -> bool {
    encounter_has_display_percentile(encounter)
        || encounter_has_positive_clear_count(encounter)
        || encounter.boss_percentage.is_some_and(|value| value > 0.0)
}

fn requested_encounter_ids(
    encounter_id: u32,
    secondary_encounter_id: Option<u32>,
) -> impl Iterator<Item = u32> {
    std::iter::once(encounter_id).chain(secondary_encounter_id)
}

fn has_relevant_fallback_data(
    encounters: &HashMap<String, EncounterParse>,
    encounter_id: u32,
    secondary_encounter_id: Option<u32>,
) -> bool {
    requested_encounter_ids(encounter_id, secondary_encounter_id).any(|id| {
        encounters
            .get(&id.to_string())
            .is_some_and(encounter_has_relevant_data)
    })
}

fn hydrate_resolved(
    resolved: &mut ResolvedParseData,
    encounters: &HashMap<String, EncounterParse>,
    encounter_id: u32,
    secondary_encounter_id: Option<u32>,
) {
    let (p1, p1_class, p1_boss, p1_clears) = hydrate_slot(encounters, encounter_id);
    resolved.primary_percentile = p1;
    resolved.primary_color_class = p1_class;
    resolved.primary_boss_percentage = p1_boss;
    resolved.primary_clear_count = p1_clears;

    if let Some(sec_id) = secondary_encounter_id {
        let (p2, p2_class, p2_boss, p2_clears) = hydrate_slot(encounters, sec_id);
        resolved.secondary_percentile = p2;
        resolved.secondary_color_class = p2_class;
        resolved.secondary_boss_percentage = p2_boss;
        resolved.secondary_clear_count = p2_clears;
    }
}

fn hydrate_fallback_resolved(
    resolved: &mut ResolvedParseData,
    encounters: &HashMap<String, EncounterParse>,
    encounter_id: u32,
    secondary_encounter_id: Option<u32>,
) {
    let (p1, p1_class, p1_boss, p1_clears) = hydrate_fallback_slot(encounters, encounter_id);
    resolved.primary_percentile = p1;
    resolved.primary_color_class = p1_class;
    resolved.primary_boss_percentage = p1_boss;
    resolved.primary_clear_count = p1_clears;

    if let Some(sec_id) = secondary_encounter_id {
        let (p2, p2_class, p2_boss, p2_clears) = hydrate_fallback_slot(encounters, sec_id);
        resolved.secondary_percentile = p2;
        resolved.secondary_color_class = p2_class;
        resolved.secondary_boss_percentage = p2_boss;
        resolved.secondary_clear_count = p2_clears;
    }
}

fn relevant_fallback_encounters(
    fallback_encounters: Option<&HashMap<String, EncounterParse>>,
    encounter_id: u32,
    secondary_encounter_id: Option<u32>,
) -> Option<&HashMap<String, EncounterParse>> {
    fallback_encounters.filter(|encounters| {
        has_relevant_fallback_data(encounters, encounter_id, secondary_encounter_id)
    })
}

pub fn resolve_parse_data(
    plugin_zone_cache: Option<&ZoneCache>,
    fallback_encounters: Option<&HashMap<String, EncounterParse>>,
    _fallback_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    encounter_id: u32,
    secondary_encounter_id: Option<u32>,
) -> ResolvedParseData {
    let has_secondary = secondary_encounter_id.is_some();
    let mut resolved = ResolvedParseData::empty(has_secondary);

    match plugin_zone_cache {
        Some(zone_cache) if !zone_cache.hidden => {
            resolved.source = ParseSource::Plugin;
            resolved.estimated = zone_cache.estimated;
            hydrate_resolved(
                &mut resolved,
                &zone_cache.encounters,
                encounter_id,
                secondary_encounter_id,
            );
            resolved
        }
        Some(zone_cache) if zone_cache.hidden => {
            if let Some(encounters) = relevant_fallback_encounters(
                fallback_encounters,
                encounter_id,
                secondary_encounter_id,
            ) {
                resolved.source = ParseSource::ReportParse;
                resolved.originally_hidden = true;
                hydrate_fallback_resolved(
                    &mut resolved,
                    encounters,
                    encounter_id,
                    secondary_encounter_id,
                );
                resolved
            } else {
                resolved.source = ParseSource::Plugin;
                resolved.hidden = true;
                resolved
            }
        }
        None => {
            if let Some(encounters) = relevant_fallback_encounters(
                fallback_encounters,
                encounter_id,
                secondary_encounter_id,
            ) {
                resolved.source = ParseSource::ReportParse;
                hydrate_fallback_resolved(
                    &mut resolved,
                    encounters,
                    encounter_id,
                    secondary_encounter_id,
                );
            }

            resolved
        }
        Some(_) => resolved,
    }
}

#[cfg(test)]
mod tests;
