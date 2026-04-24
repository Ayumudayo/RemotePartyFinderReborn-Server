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
    let Some(enc_parse) = encounters.get(&encounter_id.to_string()) else {
        return (None, "parse-none".to_string(), None, None);
    };

    let percentile = if enc_parse.percentile >= 0.0 {
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

fn has_relevant_fallback_data(
    encounters: &HashMap<String, EncounterParse>,
    encounter_id: u32,
    secondary_encounter_id: Option<u32>,
) -> bool {
    encounters.contains_key(&encounter_id.to_string())
        || secondary_encounter_id
            .map(|secondary| encounters.contains_key(&secondary.to_string()))
            .unwrap_or(false)
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

fn relevant_fallback_encounters<'a>(
    fallback_encounters: Option<&'a HashMap<String, EncounterParse>>,
    encounter_id: u32,
    secondary_encounter_id: Option<u32>,
) -> Option<&'a HashMap<String, EncounterParse>> {
    fallback_encounters.filter(|encounters| {
        has_relevant_fallback_data(encounters, encounter_id, secondary_encounter_id)
    })
}

fn report_parse_is_newer_than_plugin(
    plugin_zone_cache: &ZoneCache,
    fallback_updated_at: Option<chrono::DateTime<chrono::Utc>>,
) -> bool {
    fallback_updated_at
        .map(|updated_at| updated_at > plugin_zone_cache.fetched_at)
        .unwrap_or(false)
}

pub fn resolve_parse_data(
    plugin_zone_cache: Option<&ZoneCache>,
    fallback_encounters: Option<&HashMap<String, EncounterParse>>,
    fallback_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    encounter_id: u32,
    secondary_encounter_id: Option<u32>,
) -> ResolvedParseData {
    let has_secondary = secondary_encounter_id.is_some();
    let mut resolved = ResolvedParseData::empty(has_secondary);

    match plugin_zone_cache {
        Some(zone_cache) if !zone_cache.hidden => {
            if let Some(encounters) = relevant_fallback_encounters(
                fallback_encounters,
                encounter_id,
                secondary_encounter_id,
            ) {
                if report_parse_is_newer_than_plugin(zone_cache, fallback_updated_at) {
                    resolved.source = ParseSource::ReportParse;
                    hydrate_resolved(
                        &mut resolved,
                        encounters,
                        encounter_id,
                        secondary_encounter_id,
                    );
                    return resolved;
                }
            }

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
                hydrate_resolved(
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
                hydrate_resolved(
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
mod tests {
    use std::collections::HashMap;

    use chrono::{TimeDelta, Utc};

    use crate::fflogs::{EncounterParse, ZoneCache};

    use super::{resolve_parse_data, ParseSource};

    fn zone_cache(
        hidden: bool,
        estimated: bool,
        encounters: HashMap<String, EncounterParse>,
    ) -> ZoneCache {
        zone_cache_at(Utc::now(), hidden, estimated, encounters)
    }

    fn zone_cache_at(
        fetched_at: chrono::DateTime<Utc>,
        hidden: bool,
        estimated: bool,
        encounters: HashMap<String, EncounterParse>,
    ) -> ZoneCache {
        ZoneCache {
            fetched_at,
            estimated,
            matched_server: None,
            hidden,
            encounters,
        }
    }

    fn encounter(
        percentile: f32,
        boss_percentage: Option<f32>,
        clear_count: Option<u32>,
    ) -> EncounterParse {
        EncounterParse {
            percentile,
            job_id: 0,
            boss_percentage,
            clear_count,
        }
    }

    #[test]
    fn resolve_parse_data_prefers_visible_plugin_data_over_report_parse_fallback() {
        let plugin = zone_cache(
            false,
            true,
            HashMap::from([("123".to_string(), encounter(97.3, None, Some(4)))]),
        );
        let fallback = HashMap::from([("123".to_string(), encounter(88.1, Some(12.0), Some(1)))]);

        let resolved = resolve_parse_data(Some(&plugin), Some(&fallback), None, 123, None);

        assert_eq!(resolved.source, ParseSource::Plugin);
        assert_eq!(resolved.primary_percentile, Some(97));
        assert!(resolved.estimated);
        assert!(!resolved.hidden);
    }

    #[test]
    fn resolve_parse_data_uses_report_parse_fallback_when_plugin_data_is_hidden() {
        let plugin = zone_cache(
            true,
            false,
            HashMap::from([("123".to_string(), encounter(99.0, None, Some(9)))]),
        );
        let fallback = HashMap::from([("123".to_string(), encounter(84.2, Some(7.0), Some(2)))]);

        let resolved = resolve_parse_data(Some(&plugin), Some(&fallback), None, 123, None);

        assert_eq!(resolved.source, ParseSource::ReportParse);
        assert_eq!(resolved.primary_percentile, Some(84));
        assert_eq!(resolved.primary_boss_percentage, Some(7));
        assert_eq!(resolved.primary_clear_count, Some(2));
        assert!(!resolved.hidden);
        assert!(!resolved.estimated);
    }

    #[test]
    fn resolve_parse_data_keeps_hidden_state_when_fallback_lacks_requested_encounter() {
        let plugin = zone_cache(true, false, HashMap::new());
        let fallback = HashMap::from([("999".to_string(), encounter(84.2, Some(7.0), Some(2)))]);

        let resolved = resolve_parse_data(Some(&plugin), Some(&fallback), None, 123, None);

        assert_eq!(resolved.source, ParseSource::Plugin);
        assert!(resolved.hidden);
        assert_eq!(resolved.primary_percentile, None);
        assert_eq!(resolved.primary_boss_percentage, None);
        assert_eq!(resolved.primary_clear_count, None);
    }

    #[test]
    fn resolve_parse_data_preserves_hidden_plugin_state_without_fallback() {
        let plugin = zone_cache(true, false, HashMap::new());

        let resolved = resolve_parse_data(Some(&plugin), None, None, 123, None);

        assert_eq!(resolved.source, ParseSource::Plugin);
        assert!(resolved.hidden);
        assert_eq!(resolved.primary_percentile, None);
    }

    #[test]
    fn resolve_parse_data_uses_report_parse_when_plugin_data_is_missing() {
        let fallback = HashMap::from([("123".to_string(), encounter(91.8, None, Some(3)))]);

        let resolved = resolve_parse_data(None, Some(&fallback), None, 123, None);

        assert_eq!(resolved.source, ParseSource::ReportParse);
        assert_eq!(resolved.primary_percentile, Some(91));
        assert!(!resolved.hidden);
        assert!(!resolved.originally_hidden);
    }

    #[test]
    fn resolve_parse_data_ignores_fallback_without_requested_encounter_when_plugin_missing() {
        let fallback = HashMap::from([("999".to_string(), encounter(91.8, None, Some(3)))]);

        let resolved = resolve_parse_data(None, Some(&fallback), None, 123, None);

        assert_eq!(resolved.source, ParseSource::None);
        assert_eq!(resolved.primary_percentile, None);
        assert!(!resolved.hidden);
    }

    #[test]
    fn resolve_parse_data_marks_fallback_as_originally_hidden_only_for_hidden_plugin_rows() {
        let hidden_plugin = zone_cache(
            true,
            false,
            HashMap::from([("123".to_string(), encounter(99.0, None, Some(9)))]),
        );
        let fallback = HashMap::from([("123".to_string(), encounter(84.2, Some(7.0), Some(2)))]);

        let hidden_resolved =
            resolve_parse_data(Some(&hidden_plugin), Some(&fallback), None, 123, None);
        assert_eq!(hidden_resolved.source, ParseSource::ReportParse);
        assert!(hidden_resolved.originally_hidden);
        assert!(!hidden_resolved.hidden);

        let missing_resolved = resolve_parse_data(None, Some(&fallback), None, 123, None);
        assert_eq!(missing_resolved.source, ParseSource::ReportParse);
        assert!(!missing_resolved.originally_hidden);
    }

    #[test]
    fn resolve_parse_data_uses_newer_report_parse_over_stale_visible_plugin_data() {
        let plugin_fetched_at = Utc::now() - TimeDelta::try_minutes(30).unwrap();
        let report_updated_at = plugin_fetched_at + TimeDelta::try_minutes(10).unwrap();
        let plugin = zone_cache_at(
            plugin_fetched_at,
            false,
            false,
            HashMap::from([("123".to_string(), encounter(72.1, None, Some(1)))]),
        );
        let fallback = HashMap::from([("123".to_string(), encounter(93.6, Some(4.0), Some(5)))]);

        let resolved = resolve_parse_data(
            Some(&plugin),
            Some(&fallback),
            Some(report_updated_at),
            123,
            None,
        );

        assert_eq!(resolved.source, ParseSource::ReportParse);
        assert_eq!(resolved.primary_percentile, Some(93));
        assert_eq!(resolved.primary_boss_percentage, Some(4));
        assert_eq!(resolved.primary_clear_count, Some(5));
        assert!(!resolved.hidden);
        assert!(!resolved.originally_hidden);
    }

    #[test]
    fn resolve_parse_data_keeps_newer_visible_plugin_data_over_older_report_parse() {
        let plugin_fetched_at = Utc::now();
        let report_updated_at = plugin_fetched_at - TimeDelta::try_minutes(10).unwrap();
        let plugin = zone_cache_at(
            plugin_fetched_at,
            false,
            false,
            HashMap::from([("123".to_string(), encounter(97.9, None, Some(8)))]),
        );
        let fallback = HashMap::from([("123".to_string(), encounter(55.0, Some(20.0), Some(1)))]);

        let resolved = resolve_parse_data(
            Some(&plugin),
            Some(&fallback),
            Some(report_updated_at),
            123,
            None,
        );

        assert_eq!(resolved.source, ParseSource::Plugin);
        assert_eq!(resolved.primary_percentile, Some(97));
        assert_eq!(resolved.primary_clear_count, Some(8));
        assert!(!resolved.hidden);
    }

    #[test]
    fn resolve_parse_data_preserves_hidden_fallback_policy_even_when_report_parse_is_older() {
        let plugin_fetched_at = Utc::now();
        let report_updated_at = plugin_fetched_at - TimeDelta::try_hours(1).unwrap();
        let plugin = zone_cache_at(
            plugin_fetched_at,
            true,
            false,
            HashMap::from([("123".to_string(), encounter(99.0, None, Some(9)))]),
        );
        let fallback = HashMap::from([("123".to_string(), encounter(84.2, Some(7.0), Some(2)))]);

        let resolved = resolve_parse_data(
            Some(&plugin),
            Some(&fallback),
            Some(report_updated_at),
            123,
            None,
        );

        assert_eq!(resolved.source, ParseSource::ReportParse);
        assert_eq!(resolved.primary_percentile, Some(84));
        assert!(resolved.originally_hidden);
        assert!(!resolved.hidden);
    }

    #[test]
    fn resolve_parse_data_truncates_percentile_instead_of_rounding_up_to_100() {
        let plugin = zone_cache(
            false,
            false,
            HashMap::from([("123".to_string(), encounter(99.9, None, Some(1)))]),
        );

        let resolved = resolve_parse_data(Some(&plugin), None, None, 123, None);

        assert_eq!(resolved.primary_percentile, Some(99));
        assert_eq!(resolved.primary_color_class, "parse-pink");
    }
}
