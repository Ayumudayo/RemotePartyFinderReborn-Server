use super::identities::{build_identity_compare_and_set_filter, build_identity_upsert_update};
use super::is_duplicate_key_error;
use super::parse_cache::zone_cache_find_options;
use super::players::{
    build_player_upsert_documents, dedupe_players_by_preferred_order, player_lookup_find_options,
    PreparedPlayerUpsert,
};
use super::report_parse::{
    report_parse_summary_find_options, report_parse_summary_identity_filter,
};
use chrono::Utc;
use mongodb::bson::doc;

#[test]
fn identity_player_upsert_sets_shared_freshness_metadata() {
    let now = chrono::DateTime::parse_from_rfc3339("2026-04-12T12:30:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let prepared = PreparedPlayerUpsert {
        content_id: 1001,
        name: "Known Player".to_string(),
        home_world: 74,
        current_world: 79,
        account_id: "123".to_string(),
    };

    let (set_doc, set_on_insert_doc) = build_player_upsert_documents(&prepared, now);

    assert_eq!(set_doc.get_datetime("last_seen").unwrap().to_chrono(), now);
    assert_eq!(
        set_doc
            .get_datetime("identity_observed_at")
            .unwrap()
            .to_chrono(),
        now
    );
    assert_eq!(set_on_insert_doc.get_i64("content_id").unwrap(), 1001);
}

#[test]
fn identity_compare_and_set_filter_uses_previously_read_freshness() {
    let freshness = chrono::DateTime::parse_from_rfc3339("2026-04-12T12:30:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let existing = doc! {
        "content_id": 6006i64,
        "identity_observed_at": freshness,
    };

    let filter = build_identity_compare_and_set_filter(6006, Some(&existing));

    assert_eq!(filter.get_i64("content_id").unwrap(), 6006);
    assert_eq!(
        filter
            .get_datetime("identity_observed_at")
            .unwrap()
            .to_chrono(),
        freshness
    );
}

#[test]
fn identity_compare_and_set_filter_falls_back_to_legacy_last_seen_when_freshness_missing() {
    let last_seen = chrono::DateTime::parse_from_rfc3339("2026-04-12T12:30:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let existing = doc! {
        "content_id": 7007i64,
        "last_seen": last_seen,
    };

    let filter = build_identity_compare_and_set_filter(7007, Some(&existing));

    assert_eq!(filter.get_i64("content_id").unwrap(), 7007);
    assert_eq!(
        filter.get_datetime("last_seen").unwrap().to_chrono(),
        last_seen
    );
    assert_eq!(filter.get_array("$or").unwrap().len(), 2);
}

#[test]
fn identity_upsert_update_keeps_content_id_out_of_set_clause() {
    let observed_at = chrono::DateTime::parse_from_rfc3339("2026-04-12T12:30:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let merged = crate::player::IdentityMergeResult {
        player: crate::player::Player {
            content_id: 8008,
            name: "Known Player".to_string(),
            home_world: 74,
            current_world: 79,
            last_seen: observed_at,
            seen_count: 2,
            account_id: "123".to_string(),
        },
        identity_observed_at: observed_at,
        world_name: "Cerberus".to_string(),
        source: "plugin".to_string(),
        applied_incoming_identity: true,
    };

    let update = build_identity_upsert_update(8008, &merged);
    let set_doc = update.get_document("$set").unwrap();
    let set_on_insert_doc = update.get_document("$setOnInsert").unwrap();

    assert!(set_doc.get("content_id").is_none());
    assert_eq!(set_on_insert_doc.get_i64("content_id").unwrap(), 8008);
}

#[test]
fn identity_duplicate_key_errors_are_retryable() {
    let write_error: mongodb::error::WriteError = serde_json::from_str(
        r#"{"code":11000,"codeName":"DuplicateKey","errmsg":"E11000 duplicate key error"}"#,
    )
    .unwrap();
    let error = mongodb::error::Error::from(mongodb::error::ErrorKind::Write(
        mongodb::error::WriteFailure::WriteError(write_error),
    ));

    assert!(is_duplicate_key_error(&error));
}

#[test]
fn player_lookup_sort_matches_duplicate_cleanup_preference() {
    let options = player_lookup_find_options();

    assert_eq!(
        options.sort.unwrap(),
        doc! {
            "content_id": 1,
            "identity_observed_at": -1,
            "last_seen": -1,
            "seen_count": -1,
            "_id": 1,
        }
    );
}

#[test]
fn player_lookup_dedupe_keeps_first_preferred_content_id() {
    let now = Utc::now();
    let players = vec![
        crate::player::Player {
            content_id: 9009,
            name: "Preferred".to_string(),
            home_world: 74,
            current_world: 74,
            last_seen: now,
            seen_count: 7,
            account_id: "1".to_string(),
        },
        crate::player::Player {
            content_id: 9009,
            name: "Stale".to_string(),
            home_world: 80,
            current_world: 80,
            last_seen: now - chrono::TimeDelta::days(1),
            seen_count: 1,
            account_id: "2".to_string(),
        },
        crate::player::Player {
            content_id: 9010,
            name: "Other".to_string(),
            home_world: 75,
            current_world: 75,
            last_seen: now,
            seen_count: 1,
            account_id: "3".to_string(),
        },
    ];

    let deduped = dedupe_players_by_preferred_order(players);

    assert_eq!(deduped.len(), 2);
    assert_eq!(deduped[0].content_id, 9009);
    assert_eq!(deduped[0].name, "Preferred");
    assert_eq!(deduped[1].content_id, 9010);
}

#[test]
fn zone_cache_lookup_projects_only_requested_zone() {
    let options = zone_cache_find_options("73:101:1");
    let projection = options.projection.expect("projection should be set");

    assert_eq!(projection.get_i32("content_id").unwrap(), 1);
    assert_eq!(projection.get_i32("zones.73:101:1").unwrap(), 1);
    assert_eq!(projection.len(), 2);
}

#[test]
fn report_parse_summary_filter_targets_identity_index_only() {
    let identities = vec![
        crate::report_parse::ReportParseIdentityKey::new("Alice Example", 74),
        crate::report_parse::ReportParseIdentityKey::new("Alice Example", 74),
        crate::report_parse::ReportParseIdentityKey::new("Bob Example", 80),
    ];

    let filter = report_parse_summary_identity_filter(&identities)
        .expect("non-empty identities should produce a filter");

    assert_eq!(
        filter,
        doc! {
            "$or": [
                { "normalized_name": "alice example", "home_world": 74i32 },
                { "normalized_name": "bob example", "home_world": 80i32 },
            ],
        }
    );
    assert!(!filter.contains_key("zones.73:101:1"));
}

#[test]
fn report_parse_summary_find_options_project_requested_zone_and_timeout() {
    let options = report_parse_summary_find_options("73:101:1");
    let projection = options
        .projection
        .expect("summary query should project only required fields");

    assert_eq!(projection.get_i32("normalized_name").unwrap(), 1);
    assert_eq!(projection.get_i32("display_name").unwrap(), 1);
    assert_eq!(projection.get_i32("home_world").unwrap(), 1);
    assert_eq!(projection.get_i32("updated_at").unwrap(), 1);
    assert_eq!(projection.get_i32("zones.73:101:1").unwrap(), 1);
    assert!(options.max_time.is_some());
}
