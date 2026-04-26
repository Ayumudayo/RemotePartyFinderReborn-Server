use sestring::SeString;

use super::{
    ConditionFlags, DutyCategory, DutyFinderSettingsFlags, DutyType, JobFlags, LootRuleFlags,
    ObjectiveFlags, PartyFinderListing, PartyFinderSlot, SearchAreaFlags,
};

fn sample_listing() -> PartyFinderListing {
    PartyFinderListing {
        id: 166_086,
        content_id_lower: 40_205_661,
        name: SeString::parse(b"Yuki Coffee").unwrap(),
        description: SeString::parse(b"Test Description").unwrap(),
        created_world: 44,
        home_world: 44,
        current_world: 44,
        category: DutyCategory::HighEndDuty,
        duty: 1010,
        duty_type: DutyType::Normal,
        beginners_welcome: false,
        seconds_remaining: 1667,
        min_item_level: 0,
        num_parties: 3,
        slots_available: 24,
        last_server_restart: 1_774_336_035,
        objective: ObjectiveFlags::DUTY_COMPLETION,
        conditions: ConditionFlags::DUTY_COMPLETE,
        duty_finder_settings: DutyFinderSettingsFlags::NONE,
        loot_rules: LootRuleFlags::NONE,
        search_area: SearchAreaFlags::DATA_CENTRE,
        slots: vec![
            PartyFinderSlot {
                accepting: JobFlags::all() - JobFlags::GLADIATOR,
            },
            PartyFinderSlot {
                accepting: JobFlags::all() - JobFlags::GLADIATOR,
            },
            PartyFinderSlot {
                accepting: JobFlags::all() - JobFlags::GLADIATOR,
            },
            PartyFinderSlot {
                accepting: JobFlags::empty(),
            },
            PartyFinderSlot {
                accepting: JobFlags::empty(),
            },
            PartyFinderSlot {
                accepting: JobFlags::empty(),
            },
            PartyFinderSlot {
                accepting: JobFlags::empty(),
            },
            PartyFinderSlot {
                accepting: JobFlags::empty(),
            },
        ],
        jobs_present: vec![0, 0, 0, 0, 0, 0, 0, 0],
        member_content_ids: vec![
            1, 0, 2, 0, 3, 0, 4, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 7, 8, 9, 0, 10,
        ],
        member_jobs: vec![
            37, 0, 24, 0, 22, 0, 31, 0, 20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 33, 28, 34, 22, 0, 38,
        ],
        leader_content_id: 5,
    }
}

#[test]
fn slots_use_member_jobs_overlay_for_alliance_listings() {
    let listing = sample_listing();

    let slots = listing.slots();

    assert_eq!(slots.len(), 24);
    assert!(matches!(slots[0], Ok(_)));
    assert!(matches!(slots[1], Err(_)));
    assert!(matches!(slots[8], Ok(_)));
    assert!(matches!(slots[17], Err(_)));
    assert!(matches!(slots[18], Ok(_)));
    assert!(matches!(slots[23], Ok(_)));
}

#[test]
fn slots_filled_counts_member_jobs_overlay_for_alliance_listings() {
    let listing = sample_listing();

    assert_eq!(listing.slots_filled(), 10);
}
