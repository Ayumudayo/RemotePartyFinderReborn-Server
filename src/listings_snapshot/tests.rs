use super::*;
use chrono::Utc;
use mongodb::options::ReturnDocument;
use serde_json::json;

#[test]
fn snapshot_etag_is_stable_for_identical_final_body() {
    let body = br#"{"revision":7,"listings":[]}"#;

    let first = compute_snapshot_etag(body);
    let second = compute_snapshot_etag(body);

    assert_eq!(first, second);
    assert!(first.starts_with("sha256-"));
}

#[test]
fn payload_hash_is_stable_for_identical_listings_payload() {
    let listings = serde_json::to_vec(&json!([
        { "listing": { "duty_id": 1010 } }
    ]))
    .expect("listings json should serialize");

    let first = compute_payload_hash(&listings);
    let second = compute_payload_hash(&listings);

    assert_eq!(first, second);
    assert!(first.starts_with("sha256-"));
}

#[test]
fn payload_hash_is_independent_of_outer_revision() {
    let listings = json!([{ "listing": { "duty_id": 1010 } }]);
    let revision_one = serde_json::to_vec(&json!({ "revision": 1, "listings": listings }))
        .expect("snapshot json should serialize");
    let revision_two = serde_json::to_vec(&json!({ "revision": 2, "listings": listings }))
        .expect("snapshot json should serialize");
    let listings_json = serde_json::to_vec(&listings).expect("listings json should serialize");

    assert_ne!(
        compute_snapshot_etag(&revision_one),
        compute_snapshot_etag(&revision_two)
    );
    assert_eq!(
        compute_payload_hash(&listings_json),
        compute_payload_hash(&listings_json)
    );
}

#[test]
fn gzip_roundtrip_preserves_body() {
    let body = br#"{"revision":7,"listings":[{"id":1}]}"#;

    let compressed = gzip_body(body).expect("body should gzip");
    let decompressed = gunzip_body(&compressed).expect("body should gunzip");

    assert_eq!(decompressed, body);
}

#[test]
fn materialized_doc_conversion_preserves_metadata_and_decodes_body() {
    let payload = BuiltListingsPayload {
        listings: Vec::new(),
        payload_hash: compute_payload_hash(b"[]"),
    };
    let built = serialize_snapshot(42, payload).expect("snapshot should serialize");
    let etag = built.etag.clone();
    let payload_hash = built.payload_hash.clone();

    let doc = materialized_doc_from_snapshot("current", built, 41)
        .expect("materialized snapshot should encode");
    let cached =
        cached_snapshot_from_materialized_doc(&doc).expect("materialized snapshot should decode");

    assert_eq!(doc.id, "current");
    assert_eq!(doc.revision, 42);
    assert_eq!(doc.source_revision, 41);
    assert_eq!(doc.etag, etag);
    assert_eq!(doc.payload_hash, payload_hash);
    assert_eq!(doc.content_type, "application/json; charset=utf-8");
    assert_eq!(doc.content_encoding, "gzip");
    assert!(!doc.body_gzip.bytes.is_empty());

    assert_eq!(cached.revision, 42);
    assert_eq!(cached.etag, Some(etag));
    assert_eq!(cached.content_encoding, Some("gzip".to_string()));
    assert_eq!(cached.body.as_ref(), doc.body_gzip.bytes.as_slice());
    assert_eq!(
        gunzip_body(cached.body.as_ref()).expect("cached body should decode"),
        br#"{"revision":42,"listings":[]}"#
    );
}

#[test]
fn source_revision_increment_update_targets_current_document() {
    let (filter, update, options) = listing_source_revision_increment_update("current", Utc::now());

    assert_eq!(filter, mongodb::bson::doc! { "_id": "current" });
    assert_eq!(
        update.get_document("$inc").unwrap(),
        &mongodb::bson::doc! { "revision": 1_i64 }
    );
    assert!(update
        .get_document("$set")
        .unwrap()
        .contains_key("updated_at"));
    assert_eq!(
        update.get_document("$setOnInsert").unwrap(),
        &mongodb::bson::doc! { "_id": "current" }
    );
    assert_eq!(options.upsert, Some(true));
}

#[test]
fn materialized_revision_allocate_update_increments_requested_document() {
    let (filter, update, options) = materialized_revision_allocate_update("current", Utc::now());

    assert_eq!(filter, mongodb::bson::doc! { "_id": "current" });
    assert_eq!(
        update.get_document("$inc").unwrap(),
        &mongodb::bson::doc! { "revision": 1_i64 }
    );
    assert!(update
        .get_document("$set")
        .unwrap()
        .contains_key("updated_at"));
    assert_eq!(
        update.get_document("$setOnInsert").unwrap(),
        &mongodb::bson::doc! { "_id": "current" }
    );
    assert_eq!(options.upsert, Some(true));
    assert!(matches!(
        options.return_document,
        Some(ReturnDocument::After)
    ));
}

#[test]
fn materialized_revision_seed_update_keeps_counter_at_least_current_snapshot() {
    let now = Utc::now();
    let (filter, update, options) = materialized_revision_seed_update("current", now, 42);

    assert_eq!(filter, mongodb::bson::doc! { "_id": "current" });
    assert_eq!(
        update.get_document("$max").unwrap(),
        &mongodb::bson::doc! { "revision": 42_i64 }
    );
    assert_eq!(
        update.get_document("$set").unwrap(),
        &mongodb::bson::doc! {
            "updated_at": mongodb::bson::DateTime::from_chrono(now)
        }
    );
    assert_eq!(
        update.get_document("$setOnInsert").unwrap(),
        &mongodb::bson::doc! { "_id": "current" }
    );
    assert_eq!(options.upsert, Some(true));
}

#[test]
fn worker_lease_filter_allows_absent_expired_or_same_owner() {
    let now = Utc::now();
    let expires_at = now + chrono::TimeDelta::seconds(120);

    let (filter, update, options) =
        snapshot_worker_lease_acquire_update("current", "owner-a", now, expires_at);

    assert_eq!(
        filter,
        mongodb::bson::doc! {
            "_id": "current",
            "$or": [
                { "expires_at": { "$lte": mongodb::bson::DateTime::from_chrono(now) } },
                { "owner_id": "owner-a" },
            ],
        }
    );
    assert_eq!(
        update,
        mongodb::bson::doc! {
            "$set": {
                "owner_id": "owner-a",
                "expires_at": mongodb::bson::DateTime::from_chrono(expires_at),
                "updated_at": mongodb::bson::DateTime::from_chrono(now),
            },
            "$setOnInsert": { "_id": "current" },
        }
    );
    assert_eq!(options.upsert, Some(true));
}

#[test]
fn worker_lease_renew_filter_requires_same_owner_and_active_lease() {
    let now = Utc::now();
    let expires_at = now + chrono::TimeDelta::seconds(120);

    let (filter, update, options) =
        snapshot_worker_lease_renew_update("current", "owner-a", now, expires_at);

    assert_eq!(
        filter,
        mongodb::bson::doc! {
            "_id": "current",
            "owner_id": "owner-a",
            "expires_at": { "$gt": mongodb::bson::DateTime::from_chrono(now) },
        }
    );
    assert_eq!(
        update,
        mongodb::bson::doc! {
            "$set": {
                "expires_at": mongodb::bson::DateTime::from_chrono(expires_at),
                "updated_at": mongodb::bson::DateTime::from_chrono(now),
            },
        }
    );
    assert_eq!(options.upsert, Some(false));
}

#[test]
fn worker_lease_predicate_allows_absent_expired_or_same_owner_and_blocks_active_other_owner() {
    let now = Utc::now();
    let expired_other_owner = ListingSnapshotWorkerLeaseDoc {
        id: "current".to_string(),
        owner_id: "owner-b".to_string(),
        expires_at: now - chrono::TimeDelta::seconds(1),
        updated_at: now,
    };
    let active_same_owner = ListingSnapshotWorkerLeaseDoc {
        id: "current".to_string(),
        owner_id: "owner-a".to_string(),
        expires_at: now + chrono::TimeDelta::seconds(120),
        updated_at: now,
    };
    let active_other_owner = ListingSnapshotWorkerLeaseDoc {
        id: "current".to_string(),
        owner_id: "owner-b".to_string(),
        expires_at: now + chrono::TimeDelta::seconds(120),
        updated_at: now,
    };

    assert!(snapshot_worker_lease_can_acquire(None, "owner-a", now));
    assert!(snapshot_worker_lease_can_acquire(
        Some(&expired_other_owner),
        "owner-a",
        now
    ));
    assert!(snapshot_worker_lease_can_acquire(
        Some(&active_same_owner),
        "owner-a",
        now
    ));
    assert!(!snapshot_worker_lease_can_acquire(
        Some(&active_other_owner),
        "owner-a",
        now
    ));
}

#[test]
fn materialized_snapshot_cas_filter_rejects_stale_or_same_payload_overwrites() {
    let filter = materialized_snapshot_cas_filter("current", 10, "sha256-new");

    assert_eq!(
        filter,
        mongodb::bson::doc! {
            "_id": "current",
            "$and": [
                {
                    "$or": [
                        { "revision": { "$lt": 10_i64 } },
                        { "revision": { "$exists": false } },
                    ],
                },
                {
                    "$or": [
                        { "payload_hash": { "$ne": "sha256-new" } },
                        { "payload_hash": { "$exists": false } },
                    ],
                },
            ],
        }
    );
}
