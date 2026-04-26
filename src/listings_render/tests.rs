use std::collections::HashMap;

use chrono::Utc;

use super::{bounded_listing_lookup, resolve_member_player};

#[tokio::test]
async fn bounded_listing_lookup_returns_default_on_timeout() {
    let result: Vec<i32> = bounded_listing_lookup(
        "test slow lookup",
        std::time::Duration::from_millis(5),
        async {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            Ok::<_, anyhow::Error>(vec![1, 2, 3])
        },
    )
    .await;

    assert!(result.is_empty());
}

#[test]
fn resolve_member_player_uses_existing_player_when_present() {
    let uid = 101u64;
    let mut players = HashMap::new();
    players.insert(
        uid,
        crate::player::Player {
            content_id: uid,
            name: "Known Player".to_string(),
            home_world: 73,
            current_world: 73,
            last_seen: Utc::now(),
            seen_count: 5,
            account_id: "123".to_string(),
        },
    );

    let (player, used_fallback) = resolve_member_player(&players, uid, true, "Leader", 74, 75);

    assert!(!used_fallback);
    assert_eq!(player.name, "Known Player");
    assert_eq!(player.home_world, 73);
}

#[test]
fn resolve_member_player_falls_back_to_leader_metadata_when_missing() {
    let players: HashMap<u64, crate::player::Player> = HashMap::new();

    let (player, used_fallback) = resolve_member_player(&players, 202, true, "Leader Name", 79, 80);

    assert!(used_fallback);
    assert_eq!(player.name, "Leader Name");
    assert_eq!(player.home_world, 79);
    assert_eq!(player.current_world, 80);
}

#[test]
fn resolve_member_player_keeps_unknown_for_non_leader_missing_player() {
    let players: HashMap<u64, crate::player::Player> = HashMap::new();

    let (player, used_fallback) =
        resolve_member_player(&players, 303, false, "Leader Name", 79, 80);

    assert!(!used_fallback);
    assert_eq!(player.name, "Unknown Member");
    assert_eq!(player.home_world, 0);
}

#[test]
fn identity_upsert_updates_player_lookup_used_by_listing_render() {
    let observed_at = chrono::DateTime::parse_from_rfc3339("2026-04-12T12:30:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let now = observed_at + chrono::TimeDelta::minutes(1);
    let identity = crate::player::UploadableCharacterIdentity {
        content_id: 5150,
        name: "Resolved Member".to_string(),
        home_world: 74,
        world_name: "Tonberry".to_string(),
        source: "chara_card".to_string(),
        observed_at,
    };

    let merged = crate::player::merge_identity_into_player(None, None, &identity, now);
    let mut players = HashMap::new();
    players.insert(merged.player.content_id, merged.player.clone());

    let (player, used_fallback) =
        resolve_member_player(&players, identity.content_id, false, "Leader", 79, 79);

    assert!(merged.applied_incoming_identity);
    assert!(!used_fallback);
    assert_eq!(player.name, "Resolved Member");
    assert_eq!(player.home_world, 74);
}
