use std::collections::HashMap;

use super::{build_fflogs_result_encounter_map, fflogs_exact_home_world_only};
use warp::http::{HeaderMap, HeaderValue};

#[test]
fn fflogs_job_mode_header_requests_exact_home_world_only() {
    let mut headers = HeaderMap::new();
    assert!(!fflogs_exact_home_world_only(&headers));

    headers.insert(
        super::FFLOGS_JOB_MODE_HEADER,
        HeaderValue::from_static(" exact-only "),
    );
    assert!(fflogs_exact_home_world_only(&headers));

    headers.insert(
        super::FFLOGS_JOB_MODE_HEADER,
        HeaderValue::from_static("candidate-capable"),
    );
    assert!(!fflogs_exact_home_world_only(&headers));
}

#[test]
fn fflogs_result_encounter_map_skips_zero_percentile_without_clear_evidence() {
    let encounters = HashMap::from([(101, 0.0)]);
    let boss_percentages = HashMap::new();
    let clear_counts = HashMap::from([(101, 0)]);

    let encounter_map =
        build_fflogs_result_encounter_map(encounters, boss_percentages, clear_counts);

    assert!(encounter_map.is_empty());
}

#[test]
fn fflogs_result_encounter_map_keeps_zero_percentile_with_positive_clear_count() {
    let encounters = HashMap::from([(101, 0.0)]);
    let boss_percentages = HashMap::new();
    let clear_counts = HashMap::from([(101, 1)]);

    let encounter_map =
        build_fflogs_result_encounter_map(encounters, boss_percentages, clear_counts);

    let encounter = encounter_map.get("101").expect("encounter should be kept");
    assert_eq!(encounter.percentile, 0.0);
    assert_eq!(encounter.clear_count, Some(1));
}
