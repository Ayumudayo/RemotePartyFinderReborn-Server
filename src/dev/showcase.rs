use askama::Template;
use chrono::{Duration, Utc};
use sestring::SeString;

use crate::listing::{
    ConditionFlags, DutyCategory, DutyFinderSettingsFlags, DutyType, JobFlags, LootRuleFlags,
    ObjectiveFlags, PartyFinderListing, PartyFinderSlot, SearchAreaFlags,
};
use crate::listing_container::QueriedListing;
use crate::parse_resolver::ParseSource;
use crate::player::Player;
use crate::template::listings::{
    ListingsTemplate, ParseDisplay, ProgressDisplay, RenderableListing, RenderableMember,
};

#[derive(Clone, Copy)]
struct ShowcaseDuty {
    category: DutyCategory,
    duty: u16,
}

const SHOWCASE_EXTREME_DUTY: ShowcaseDuty = ShowcaseDuty {
    category: DutyCategory::HighEndDuty,
    duty: 1077,
};

const SHOWCASE_SAVAGE_DUTY: ShowcaseDuty = ShowcaseDuty {
    category: DutyCategory::HighEndDuty,
    duty: 1069,
};

const SHOWCASE_SAVAGE_ALT_DUTY: ShowcaseDuty = ShowcaseDuty {
    category: DutyCategory::HighEndDuty,
    duty: 1071,
};

const SHOWCASE_ULTIMATE_DUTY: ShowcaseDuty = ShowcaseDuty {
    category: DutyCategory::HighEndDuty,
    duty: 1006,
};

const SHOWCASE_CHAOTIC_DUTY: ShowcaseDuty = ShowcaseDuty {
    category: DutyCategory::HighEndDuty,
    duty: 1010,
};

fn showcase_text(text: &str) -> SeString {
    SeString::parse(text.as_bytes()).expect("showcase strings should always parse")
}

fn parse_color_class(percentile: Option<u8>) -> String {
    percentile
        .map(|value| crate::fflogs::percentile_color_class(f32::from(value)).to_string())
        .unwrap_or_else(|| "parse-none".to_string())
}

fn build_parse_display(
    primary_percentile: Option<u8>,
    secondary_percentile: Option<u8>,
    has_secondary: bool,
    hidden: bool,
    originally_hidden: bool,
    estimated: bool,
    source: ParseSource,
) -> ParseDisplay {
    ParseDisplay::new(
        primary_percentile,
        parse_color_class(primary_percentile),
        secondary_percentile,
        parse_color_class(secondary_percentile),
        has_secondary,
        hidden,
        originally_hidden,
        estimated,
        source,
    )
}

fn build_progress_display(
    parse: &ParseDisplay,
    primary_boss_percentage: Option<u8>,
    secondary_boss_percentage: Option<u8>,
    primary_clear_count: Option<u16>,
    secondary_clear_count: Option<u16>,
) -> ProgressDisplay {
    ProgressDisplay::new(
        primary_boss_percentage,
        secondary_boss_percentage,
        parse.primary_percentile,
        parse.secondary_percentile,
        primary_clear_count,
        secondary_clear_count,
        parse.has_secondary,
        parse.hidden,
    )
}

fn build_player(content_id: u64, name: &str, home_world: u16) -> Player {
    Player {
        content_id,
        name: name.to_string(),
        home_world,
        current_world: home_world,
        last_seen: Utc::now() - Duration::minutes(5),
        seen_count: 3,
        account_id: "-1".to_string(),
    }
}

fn alliance_party_label(party_index: u8) -> &'static str {
    match party_index {
        0 => "Alliance A",
        1 => "Alliance B",
        2 => "Alliance C",
        3 => "Alliance D",
        4 => "Alliance E",
        5 => "Alliance F",
        _ => "Alliance ?",
    }
}

fn build_member(
    content_id: u64,
    slot_index: usize,
    party_index: u8,
    job_id: u8,
    name: &str,
    home_world: u16,
    parse: ParseDisplay,
    progress: ProgressDisplay,
) -> RenderableMember {
    RenderableMember {
        job_id,
        player: build_player(content_id, name, home_world),
        parse,
        progress,
        slot_index,
        party_index,
        party_header: None,
    }
}

fn build_listing(
    id: u32,
    name: &str,
    description: &str,
    showcase_duty: ShowcaseDuty,
    num_parties: u8,
    slots_available: u8,
    mut members: Vec<RenderableMember>,
    leader_parse: ParseDisplay,
) -> RenderableListing {
    let slots_len = usize::from(slots_available);
    let mut jobs_present = vec![0; slots_len];
    let mut member_content_ids = vec![0_i64; slots_len];
    let mut member_jobs = vec![0; slots_len];

    for member in &mut members {
        if member.slot_index >= slots_len {
            continue;
        }

        if num_parties >= 3 && member.slot_index % 8 == 0 {
            member.party_header = Some(alliance_party_label(member.party_index));
        }

        jobs_present[member.slot_index] = member.job_id;
        member_content_ids[member.slot_index] = member.player.content_id as i64;
        member_jobs[member.slot_index] = member.job_id;
    }

    let open_slot = PartyFinderSlot {
        accepting: JobFlags::PALADIN
            | JobFlags::WHITE_MAGE
            | JobFlags::BLACK_MAGE
            | JobFlags::DANCER,
    };

    let listing = PartyFinderListing {
        id,
        content_id_lower: id.saturating_mul(10),
        name: showcase_text(name),
        description: showcase_text(description),
        created_world: 73,
        home_world: 73,
        current_world: 73,
        category: showcase_duty.category,
        duty: showcase_duty.duty,
        duty_type: DutyType::Normal,
        beginners_welcome: true,
        seconds_remaining: 1800,
        min_item_level: 730,
        num_parties,
        slots_available,
        last_server_restart: 0,
        objective: ObjectiveFlags::PRACTICE | ObjectiveFlags::DUTY_COMPLETION,
        conditions: ConditionFlags::DUTY_COMPLETE,
        duty_finder_settings: DutyFinderSettingsFlags::NONE,
        loot_rules: LootRuleFlags::NONE,
        search_area: SearchAreaFlags::DATA_CENTRE,
        slots: (0..slots_len)
            .map(|_| PartyFinderSlot {
                accepting: open_slot.accepting,
            })
            .collect(),
        jobs_present,
        member_content_ids,
        member_jobs,
        leader_content_id: members
            .first()
            .map(|member| member.player.content_id)
            .unwrap_or(0),
    };

    let now = Utc::now();
    let container = QueriedListing {
        created_at: now - Duration::minutes(25),
        updated_at: now - Duration::minutes(3),
        updated_minute: now - Duration::minutes(3),
        time_left: 1800.0,
        listing,
    };

    RenderableListing {
        container,
        members,
        leader_parse,
    }
}

fn build_showcase_listings() -> Vec<RenderableListing> {
    let hidden_single =
        build_parse_display(None, None, false, true, false, false, ParseSource::Plugin);
    let hidden_dual = build_parse_display(
        Some(97),
        Some(88),
        true,
        true,
        false,
        false,
        ParseSource::Plugin,
    );
    let fallback_parse = build_parse_display(
        Some(91),
        None,
        false,
        false,
        true,
        false,
        ParseSource::ReportParse,
    );
    let estimated_parse = build_parse_display(
        Some(52),
        None,
        false,
        false,
        false,
        true,
        ParseSource::Plugin,
    );
    let single_parse = build_parse_display(
        Some(67),
        None,
        false,
        false,
        false,
        false,
        ParseSource::Plugin,
    );
    let dual_parse = build_parse_display(
        Some(45),
        Some(82),
        true,
        false,
        false,
        false,
        ParseSource::Plugin,
    );
    let no_parse = build_parse_display(
        None,
        None,
        false,
        false,
        false,
        false,
        ParseSource::None,
    );

    let hidden_listing = build_listing(
        9_001,
        "Section 1: HID and unknown job",
        "Hidden FFLogs states, including an unknown job slot and a hidden dual-parse member.",
        SHOWCASE_EXTREME_DUTY,
        1,
        8,
        vec![
            build_member(
                1_001,
                0,
                0,
                0,
                "Mystery Raider",
                73,
                hidden_single.clone(),
                build_progress_display(&hidden_single, None, None, None, None),
            ),
            build_member(
                1_002,
                1,
                0,
                19,
                "Silent Paladin",
                73,
                hidden_dual.clone(),
                build_progress_display(&hidden_dual, None, None, None, None),
            ),
        ],
        hidden_single.clone(),
    );

    let fallback_listing = build_listing(
        9_002,
        "Section 2: HID fallback and clears",
        "Report-parse fallback data should reuse the HID right-rail tag and clear-count coverage.",
        SHOWCASE_SAVAGE_DUTY,
        1,
        8,
        vec![
            build_member(
                2_001,
                0,
                0,
                24,
                "Fallback Hero",
                73,
                fallback_parse.clone(),
                build_progress_display(&fallback_parse, None, None, Some(5), None),
            ),
            build_member(
                2_002,
                1,
                0,
                28,
                "Steady Scholar",
                73,
                single_parse.clone(),
                build_progress_display(&single_parse, None, None, None, None),
            ),
        ],
        build_parse_display(
            Some(88),
            None,
            false,
            false,
            true,
            false,
            ParseSource::ReportParse,
        ),
    );

    let estimated_listing = build_listing(
        9_003,
        "Section 3: Estimated match and boss HP",
        "Estimated matching should render the question mark while uncleared progress keeps a boss HP marker.",
        SHOWCASE_SAVAGE_ALT_DUTY,
        1,
        8,
        vec![
            build_member(
                3_001,
                0,
                0,
                22,
                "Guessed Dragoon",
                73,
                estimated_parse.clone(),
                build_progress_display(&estimated_parse, Some(17), None, None, None),
            ),
            build_member(
                3_002,
                1,
                0,
                20,
                "Unparsed Monk",
                73,
                no_parse.clone(),
                build_progress_display(&no_parse, Some(17), None, None, None),
            ),
        ],
        build_parse_display(
            Some(94),
            None,
            false,
            false,
            false,
            true,
            ParseSource::Plugin,
        ),
    );

    let alliance_listing = build_listing(
        9_004,
        "Section 4: Alliance and dual parses",
        "Alliance dividers should separate crowded parties cleanly while mixed parse states still stay aligned.",
        SHOWCASE_CHAOTIC_DUTY,
        3,
        24,
        vec![
            build_member(
                4_001,
                0,
                0,
                21,
                "Alliance Vanguard",
                73,
                dual_parse.clone(),
                build_progress_display(&dual_parse, None, None, None, Some(2)),
            ),
            build_member(
                4_004,
                1,
                0,
                19,
                "Alliance Bulwark",
                73,
                hidden_single.clone(),
                build_progress_display(&hidden_single, None, None, None, None),
            ),
            build_member(
                4_005,
                2,
                0,
                24,
                "Alliance Oracle",
                73,
                fallback_parse.clone(),
                build_progress_display(&fallback_parse, None, None, Some(4), None),
            ),
            build_member(
                4_006,
                3,
                0,
                38,
                "Alliance Harrier",
                73,
                single_parse.clone(),
                build_progress_display(&single_parse, None, None, None, None),
            ),
            build_member(
                4_002,
                8,
                1,
                33,
                "Alliance Support",
                73,
                single_parse.clone(),
                build_progress_display(&single_parse, None, None, None, None),
            ),
            build_member(
                4_007,
                9,
                1,
                28,
                "Alliance Scholar",
                73,
                no_parse.clone(),
                build_progress_display(&no_parse, Some(23), None, None, None),
            ),
            build_member(
                4_008,
                10,
                1,
                22,
                "Alliance Dragoon",
                73,
                estimated_parse.clone(),
                build_progress_display(&estimated_parse, Some(8), None, None, None),
            ),
            build_member(
                4_003,
                16,
                2,
                38,
                "Alliance Reserve",
                73,
                no_parse.clone(),
                build_progress_display(&no_parse, None, None, None, None),
            ),
            build_member(
                4_009,
                17,
                2,
                37,
                "Alliance Sentinel",
                73,
                single_parse.clone(),
                build_progress_display(&single_parse, None, None, None, None),
            ),
            build_member(
                4_010,
                18,
                2,
                24,
                "Alliance Whisperer",
                73,
                hidden_dual.clone(),
                build_progress_display(&hidden_dual, None, None, None, None),
            ),
        ],
        build_parse_display(
            Some(70),
            Some(83),
            true,
            false,
            false,
            false,
            ParseSource::Plugin,
        ),
    );

    let empty_members_listing = build_listing(
        9_005,
        "Section 5: Empty member detail",
        "A listing with no enriched member information should show the empty-state copy.",
        SHOWCASE_ULTIMATE_DUTY,
        1,
        8,
        Vec::new(),
        no_parse,
    );

    vec![
        hidden_listing,
        fallback_listing,
        estimated_listing,
        alliance_listing,
        empty_members_listing,
    ]
}

pub fn render_showcase_html() -> anyhow::Result<String> {
    let template = ListingsTemplate {
        containers: build_showcase_listings(),
        lang: crate::ffxiv::Language::English,
    };

    Ok(template.render()?)
}

pub fn showcase_output_relative_path() -> &'static str {
    "output/showcase/listings.html"
}

#[cfg(test)]
mod tests {
    use super::{render_showcase_html, showcase_output_relative_path};

    fn member_row_fragment<'a>(html: &'a str, member_name: &str) -> &'a str {
        let name_index = html
            .find(member_name)
            .expect("member name should exist in rendered HTML");
        let start = html[..name_index]
            .rfind("<li class=\"member-row")
            .expect("member row start");
        let end = html[name_index..]
            .find("</li>")
            .map(|offset| name_index + offset + "</li>".len())
            .expect("member row end");
        &html[start..end]
    }

    fn creator_row_fragment<'a>(html: &'a str, listing_name: &str) -> &'a str {
        let name_index = html
            .find(listing_name)
            .expect("listing name should exist in rendered HTML");
        let start = html[..name_index]
            .rfind("<div class=\"item creator\">")
            .expect("creator row start");
        let end = html[name_index..]
            .find("</div>")
            .map(|offset| name_index + offset + "</div>".len())
            .expect("creator row end");
        &html[start..end]
    }

    #[test]
    fn showcase_output_relative_path_is_stable() {
        assert_eq!(showcase_output_relative_path(), "output/showcase/listings.html");
    }

    #[test]
    fn fallback_rows_render_hidden_tag_in_right_rail() {
        let html = render_showcase_html().expect("showcase should render");

        let fallback_row = member_row_fragment(&html, "Fallback Hero");
        assert!(fallback_row.contains(r#"class="member-job""#));
        assert!(fallback_row.contains(r#"class="member-parse""#));
        assert!(fallback_row.contains(r#"class="member-tags""#));
        assert!(fallback_row.contains(
            r#"class="tag tag-hidden" title="FFLogs: Originally hidden player">HID"#
        ));
        assert!(!fallback_row.contains(">RP</span>"));
        assert!(fallback_row.contains(r#"class="member-link-slot""#));
    }

    #[test]
    fn alliance_rows_keep_compact_member_subcontainers() {
        let html = render_showcase_html().expect("showcase should render");

        let alliance_row = member_row_fragment(&html, "Alliance Vanguard");
        assert!(alliance_row.contains(r#"class="member-job""#));
        assert!(alliance_row.contains(r#"class="member-parse""#));
        assert!(alliance_row.contains(r#"class="parse-dual""#));
        assert!(alliance_row.contains(r#"class="member-info""#));
        assert!(alliance_row.contains(r#"class="member-tags""#));
        assert!(alliance_row.contains(r#"class="member-link-slot""#));
    }

    #[test]
    fn hidden_dual_rows_keep_parse_dual_inside_member_parse_container() {
        let html = render_showcase_html().expect("showcase should render");

        let hidden_dual_row = member_row_fragment(&html, "Silent Paladin");
        assert!(hidden_dual_row.contains(r#"class="member-parse""#));
        assert!(hidden_dual_row.contains(r#"class="parse-dual parse-dual-hidden""#));
        assert!(!hidden_dual_row.contains("parse-hidden-spacer"));
        assert!(!hidden_dual_row.contains(r#"<span class="member-parse">"#));
    }

    #[test]
    fn render_showcase_html_contains_all_marker_states() {
        let html = render_showcase_html().expect("showcase should render");

        let hidden_row = member_row_fragment(&html, "Mystery Raider");
        assert!(hidden_row.contains(r#"class="parse parse-hidden" title="FFLogs: Hidden">HID"#));
        assert!(!hidden_row.contains(r#"class="tag tag-hidden" title="FFLogs: Hidden">HID"#));

        let estimated_row = member_row_fragment(&html, "Guessed Dragoon");
        assert!(estimated_row.contains(r#"class="est" title="Estimated match (may be wrong)">?"#));

        let fallback_row = member_row_fragment(&html, "Fallback Hero");
        assert!(fallback_row.contains(r#"class="tag tag-clear" title="Clears: 5">✅5</span>"#));

        let boss_hp_row = member_row_fragment(&html, "Unparsed Monk");
        assert!(boss_hp_row.contains(
            r#"class="tag tag-boss" title="Final Boss HP: 17%">17%</span>"#
        ));
    }

    #[test]
    fn creator_rows_mirror_member_marker_states() {
        let html = render_showcase_html().expect("showcase should render");

        let hidden_creator = creator_row_fragment(&html, "Section 1: HID and unknown job");
        assert!(hidden_creator.contains(r#"class="parse parse-hidden" title="FFLogs: Hidden">HID"#));
        assert!(!hidden_creator.contains(r#"class="tag tag-hidden" title="FFLogs: Hidden">HID"#));

        let fallback_creator = creator_row_fragment(&html, "Section 2: HID fallback and clears");
        assert!(fallback_creator.contains(
            r#"class="tag tag-hidden" title="FFLogs: Originally hidden player">HID"#
        ));
        assert!(!fallback_creator.contains(">RP</span>"));

        let estimated_creator = creator_row_fragment(&html, "Section 3: Estimated match and boss HP");
        assert!(estimated_creator.contains(r#"class="est" title="Estimated match (may be wrong)">?"#));
    }

    #[test]
    fn showcase_contains_alliance_and_empty_member_examples() {
        let html = render_showcase_html().expect("showcase should render");

        assert!(html.contains(r#"class="listing listing-alliance-wide""#));
        assert!(html.contains(r#"class="party party-alliance""#));
        assert!(html.contains(r#"class="party-slots party-slots-24""#));
        assert!(html.contains(r#"class="alliance-columns""#));
        assert!(html.contains(r#"class="alliance-column""#));
        assert!(html.contains(r#"class="alliance-heading">Alliance B</div>"#));
        assert!(!html.contains(r#"<li class="party-divider">Alliance B</li>"#));
        assert!(html.contains("No information available for other members"));
    }

    #[test]
    fn showcase_fixture_listings_are_high_end_visible_by_default() {
        let html = render_showcase_html().expect("showcase should render");

        assert!(html.contains(r#"data-high-end="true""#));
        assert!(html.contains(r#"data-duty-id="1077""#));
        assert!(html.contains(r#"data-duty-id="1010""#));
    }
}
