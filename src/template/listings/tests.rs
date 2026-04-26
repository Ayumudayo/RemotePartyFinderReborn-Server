
use super::{encode_fflogs_path_segment, ParseDisplay, RenderableListing, RenderableMember};
use crate::listing::{
    ConditionFlags, DutyCategory, DutyFinderSettingsFlags, DutyType, JobFlags, LootRuleFlags,
    ObjectiveFlags, PartyFinderListing, PartyFinderSlot, SearchAreaFlags,
};
use crate::listing_container::QueriedListing;
use chrono::Utc;
use sestring::SeString;

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
        identity_fallback: false,
    }
}

fn sample_listing(num_parties: u8, slots_available: u8) -> PartyFinderListing {
    PartyFinderListing {
        id: 123,
        content_id_lower: 456,
        name: SeString::parse(b"Test Name").unwrap(),
        description: SeString::parse(b"Test Description").unwrap(),
        created_world: 73,
        home_world: 73,
        current_world: 73,
        category: DutyCategory::HighEndDuty,
        duty: 1077,
        duty_type: DutyType::Normal,
        beginners_welcome: false,
        seconds_remaining: 3300,
        min_item_level: 0,
        num_parties,
        slots_available,
        last_server_restart: 0,
        objective: ObjectiveFlags::PRACTICE | ObjectiveFlags::DUTY_COMPLETION,
        conditions: ConditionFlags::DUTY_COMPLETE,
        duty_finder_settings: DutyFinderSettingsFlags::NONE,
        loot_rules: LootRuleFlags::NONE,
        search_area: SearchAreaFlags::DATA_CENTRE,
        slots: vec![PartyFinderSlot {
            accepting: JobFlags::DANCER | JobFlags::BLUE_MAGE,
        }],
        jobs_present: vec![5, 0, 0, 0, 0, 0, 0, 0],
        member_content_ids: vec![],
        member_jobs: vec![],
        leader_content_id: 0,
    }
}

fn sample_renderable_listing(members: Vec<RenderableMember>) -> RenderableListing {
    RenderableListing {
        container: QueriedListing {
            created_at: Utc::now(),
            updated_at: Utc::now(),
            updated_minute: Utc::now(),
            time_left: 1800.0,
            listing: sample_listing(3, 24),
        },
        members,
        leader_parse: ParseDisplay::default(),
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
fn member_link_is_not_generated_for_fallback_identity() {
    let mut fallback = sample_member("Sayo Shijima", 73);
    fallback.identity_fallback = true;

    assert!(fallback.fflogs_character_url().is_none());
}

#[test]
fn progress_hides_boss_hp_when_percentile_exists_and_shows_clear_count() {
    let progress =
        super::ProgressDisplay::new(Some(23), None, Some(95), None, Some(7), None, false, false);

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
        false,
        crate::parse_resolver::ParseSource::ReportParse,
    );

    assert_eq!(plugin.report_parse_badge(), None);
    assert_eq!(fallback.report_parse_badge(), Some("RP"));
}

#[test]
fn parse_display_exposes_hidden_rail_tag_for_hidden_and_report_parse() {
    let hidden_plugin = ParseDisplay::new(
        Some(95),
        "parse-orange".to_string(),
        None,
        "parse-none".to_string(),
        false,
        true,
        false,
        false,
        crate::parse_resolver::ParseSource::Plugin,
    );
    let report_parse_fallback = ParseDisplay::new(
        Some(88),
        "parse-purple".to_string(),
        None,
        "parse-none".to_string(),
        false,
        false,
        true,
        false,
        crate::parse_resolver::ParseSource::ReportParse,
    );
    let report_parse_without_hidden_origin = ParseDisplay::new(
        Some(88),
        "parse-purple".to_string(),
        None,
        "parse-none".to_string(),
        false,
        false,
        false,
        false,
        crate::parse_resolver::ParseSource::ReportParse,
    );
    let visible_plugin = ParseDisplay::new(
        Some(67),
        "parse-blue".to_string(),
        None,
        "parse-none".to_string(),
        false,
        false,
        false,
        false,
        crate::parse_resolver::ParseSource::Plugin,
    );

    assert_eq!(hidden_plugin.hidden_rail_tag_label(), None);
    assert_eq!(report_parse_fallback.hidden_rail_tag_label(), Some("HID"));
    assert_eq!(
        report_parse_without_hidden_origin.hidden_rail_tag_label(),
        None
    );
    assert_eq!(visible_plugin.hidden_rail_tag_label(), None);
    assert_eq!(hidden_plugin.hidden_rail_tag_title(), "FFLogs: Hidden");
    assert_eq!(
        report_parse_fallback.hidden_rail_tag_title(),
        "FFLogs: Originally hidden player"
    );
    assert_eq!(
        report_parse_without_hidden_origin.hidden_rail_tag_title(),
        "FFLogs: Hidden"
    );
    assert_eq!(visible_plugin.hidden_rail_tag_title(), "FFLogs: Hidden");
}

#[test]
fn renderable_listing_groups_members_by_alliance_party() {
    let mut alliance_a = sample_member("Alliance A", 73);
    alliance_a.party_index = 0;
    alliance_a.slot_index = 0;

    let mut alliance_b = sample_member("Alliance B", 73);
    alliance_b.party_index = 1;
    alliance_b.slot_index = 8;

    let mut alliance_c = sample_member("Alliance C", 73);
    alliance_c.party_index = 2;
    alliance_c.slot_index = 16;

    let listing = sample_renderable_listing(vec![alliance_c, alliance_a, alliance_b]);
    let groups = listing.alliance_member_groups();

    assert_eq!(groups.len(), 3);
    assert_eq!(groups[0].label, "Alliance A");
    assert_eq!(groups[0].members[0].player.name, "Alliance A");
    assert_eq!(groups[1].label, "Alliance B");
    assert_eq!(groups[1].members[0].player.name, "Alliance B");
    assert_eq!(groups[2].label, "Alliance C");
    assert_eq!(groups[2].members[0].player.name, "Alliance C");
}

#[test]
fn renderable_listing_groups_sparse_parties_without_relabeling() {
    let mut alliance_c = sample_member("Alliance C Only", 73);
    alliance_c.party_index = 2;
    alliance_c.slot_index = 16;

    let listing = sample_renderable_listing(vec![alliance_c]);
    let groups = listing.alliance_member_groups();

    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].label, "Alliance C");
    assert_eq!(groups[0].members[0].player.name, "Alliance C Only");
}

#[test]
fn renderable_listing_groups_ignore_out_of_range_party_indices() {
    let mut alliance_a = sample_member("Alliance A", 73);
    alliance_a.party_index = 0;
    alliance_a.slot_index = 0;

    let mut invalid = sample_member("Invalid Alliance", 73);
    invalid.party_index = 7;
    invalid.slot_index = 23;

    let listing = sample_renderable_listing(vec![invalid, alliance_a]);
    let groups = listing.alliance_member_groups();

    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].label, "Alliance A");
    assert_eq!(groups[0].members.len(), 1);
    assert_eq!(groups[0].members[0].player.name, "Alliance A");
}

#[test]
fn renderable_listing_groups_preserve_member_order_within_each_alliance() {
    let mut first = sample_member("First A", 73);
    first.party_index = 0;
    first.slot_index = 0;

    let mut second = sample_member("Second A", 73);
    second.party_index = 0;
    second.slot_index = 3;

    let mut third = sample_member("Third A", 73);
    third.party_index = 0;
    third.slot_index = 6;

    let listing = sample_renderable_listing(vec![second, first, third]);
    let groups = listing.alliance_member_groups();

    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].label, "Alliance A");
    assert_eq!(groups[0].members[0].player.name, "Second A");
    assert_eq!(groups[0].members[1].player.name, "First A");
    assert_eq!(groups[0].members[2].player.name, "Third A");
}
