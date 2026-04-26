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
fn resolve_parse_data_keeps_report_parse_clear_count_without_zero_percentile() {
    let fallback = HashMap::from([("123".to_string(), encounter(0.0, None, Some(3)))]);

    let resolved = resolve_parse_data(None, Some(&fallback), None, 123, None);

    assert_eq!(resolved.source, ParseSource::ReportParse);
    assert_eq!(resolved.primary_percentile, None);
    assert_eq!(resolved.primary_color_class, "parse-none");
    assert_eq!(resolved.primary_clear_count, Some(3));
}

#[test]
fn resolve_parse_data_keeps_hidden_report_clear_count_without_zero_percentile() {
    let plugin = zone_cache(true, false, HashMap::new());
    let fallback = HashMap::from([("123".to_string(), encounter(0.0, None, Some(3)))]);

    let resolved = resolve_parse_data(Some(&plugin), Some(&fallback), None, 123, None);

    assert_eq!(resolved.source, ParseSource::ReportParse);
    assert!(resolved.originally_hidden);
    assert_eq!(resolved.primary_percentile, None);
    assert_eq!(resolved.primary_color_class, "parse-none");
    assert_eq!(resolved.primary_clear_count, Some(3));
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
fn resolve_parse_data_keeps_visible_plugin_data_over_newer_report_parse() {
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

    assert_eq!(resolved.source, ParseSource::Plugin);
    assert_eq!(resolved.primary_percentile, Some(72));
    assert_eq!(resolved.primary_boss_percentage, None);
    assert_eq!(resolved.primary_clear_count, Some(1));
    assert!(!resolved.hidden);
    assert!(!resolved.originally_hidden);
}

#[test]
fn resolve_parse_data_treats_zero_without_clear_count_as_no_data() {
    let plugin = zone_cache(
        false,
        false,
        HashMap::from([("123".to_string(), encounter(0.0, None, None))]),
    );

    let resolved = resolve_parse_data(Some(&plugin), None, None, 123, None);

    assert_eq!(resolved.source, ParseSource::Plugin);
    assert_eq!(resolved.primary_percentile, None);
    assert_eq!(resolved.primary_color_class, "parse-none");
}

#[test]
fn resolve_parse_data_keeps_zero_percentile_when_clear_count_is_positive() {
    let plugin = zone_cache(
        false,
        false,
        HashMap::from([("123".to_string(), encounter(0.0, None, Some(1)))]),
    );

    let resolved = resolve_parse_data(Some(&plugin), None, None, 123, None);

    assert_eq!(resolved.source, ParseSource::Plugin);
    assert_eq!(resolved.primary_percentile, Some(0));
    assert_eq!(resolved.primary_clear_count, Some(1));
}

#[test]
fn resolve_parse_data_keeps_better_plugin_parse_over_newer_zero_report_parse() {
    let plugin_fetched_at = Utc::now() - TimeDelta::try_minutes(30).unwrap();
    let report_updated_at = plugin_fetched_at + TimeDelta::try_minutes(10).unwrap();
    let plugin = zone_cache_at(
        plugin_fetched_at,
        false,
        false,
        HashMap::from([("123".to_string(), encounter(97.4, None, Some(8)))]),
    );
    let fallback = HashMap::from([("123".to_string(), encounter(0.0, None, Some(1)))]);

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
