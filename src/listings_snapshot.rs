use crate::listing::{ConditionFlags, ObjectiveFlags, PartyFinderListing, SearchAreaFlags};
use crate::listing_container::QueriedListing;
use crate::template::listings::{
    ParseDisplay, ProgressDisplay, RenderableListing, RenderableMember,
};
use crate::web::handlers::build_listings_template;
use crate::web::{CachedListingsSnapshot, State};
use anyhow::Context;
use chrono::{DateTime, Utc};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use mongodb::{
    bson::{doc, spec::BinarySubtype, Binary, Document},
    options::{FindOneAndUpdateOptions, ReturnDocument, UpdateOptions},
    Collection,
};
use serde::{Deserialize, Serialize};
use sestring::{Payload, SeString};
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use std::sync::Arc;

#[derive(Debug, Serialize)]
pub struct ApiListingsSnapshot {
    pub revision: i64,
    pub listings: Vec<ApiReadableListingContainer>,
}

#[derive(Debug)]
pub struct BuiltListingsPayload {
    pub listings: Vec<ApiReadableListingContainer>,
    pub payload_hash: String,
}

#[derive(Debug)]
pub struct BuiltListingsSnapshot {
    pub revision: i64,
    pub body: Vec<u8>,
    pub etag: String,
    pub payload_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializedListingsSnapshotDoc {
    #[serde(rename = "_id")]
    pub id: String,
    pub revision: i64,
    pub source_revision: i64,
    pub etag: String,
    pub payload_hash: String,
    pub content_type: String,
    pub content_encoding: String,
    #[serde(with = "mongodb::bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub generated_at: DateTime<Utc>,
    pub body_gzip: Binary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListingSourceStateDoc {
    #[serde(rename = "_id")]
    pub id: String,
    pub revision: i64,
    #[serde(with = "mongodb::bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListingSnapshotRevisionStateDoc {
    #[serde(rename = "_id")]
    pub id: String,
    pub revision: i64,
    #[serde(with = "mongodb::bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListingSnapshotWorkerLeaseDoc {
    #[serde(rename = "_id")]
    pub id: String,
    pub owner_id: String,
    #[serde(with = "mongodb::bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub expires_at: DateTime<Utc>,
    #[serde(with = "mongodb::bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub updated_at: DateTime<Utc>,
}

pub async fn build_listings_payload(state: Arc<State>) -> anyhow::Result<BuiltListingsPayload> {
    let listings = build_listings_template(state, None)
        .await
        .containers
        .into_iter()
        .map(ApiReadableListingContainer::from_renderable)
        .collect::<Vec<_>>();
    let listings_json =
        serde_json::to_vec(&listings).context("failed to serialize listings payload")?;
    let payload_hash = compute_payload_hash(&listings_json);

    Ok(BuiltListingsPayload {
        listings,
        payload_hash,
    })
}

pub fn serialize_snapshot(
    revision: i64,
    payload: BuiltListingsPayload,
) -> anyhow::Result<BuiltListingsSnapshot> {
    let snapshot = ApiListingsSnapshot {
        revision,
        listings: payload.listings,
    };
    let body = serde_json::to_vec(&snapshot).context("failed to serialize listings snapshot")?;
    let etag = compute_snapshot_etag(&body);

    Ok(BuiltListingsSnapshot {
        revision,
        body,
        etag,
        payload_hash: payload.payload_hash,
    })
}

pub fn compute_snapshot_etag(body: &[u8]) -> String {
    hash_bytes(body)
}

pub fn compute_payload_hash(listings_json: &[u8]) -> String {
    hash_bytes(listings_json)
}

pub fn gzip_body(body: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(body)
        .context("failed to write gzip body")?;
    encoder.finish().context("failed to finish gzip body")
}

pub fn gunzip_body(body: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut decoder = GzDecoder::new(body);
    let mut decoded = Vec::new();
    decoder
        .read_to_end(&mut decoded)
        .context("failed to read gzip body")?;
    Ok(decoded)
}

pub fn materialized_doc_from_snapshot(
    document_id: &str,
    snapshot: BuiltListingsSnapshot,
    source_revision: i64,
) -> anyhow::Result<MaterializedListingsSnapshotDoc> {
    let body_gzip = gzip_body(&snapshot.body)?;

    Ok(MaterializedListingsSnapshotDoc {
        id: document_id.to_string(),
        revision: snapshot.revision,
        source_revision,
        etag: snapshot.etag,
        payload_hash: snapshot.payload_hash,
        content_type: "application/json; charset=utf-8".to_string(),
        content_encoding: "gzip".to_string(),
        generated_at: Utc::now(),
        body_gzip: Binary {
            subtype: BinarySubtype::Generic,
            bytes: body_gzip,
        },
    })
}

pub async fn load_listing_source_state(
    collection: &Collection<ListingSourceStateDoc>,
    id: &str,
) -> anyhow::Result<Option<ListingSourceStateDoc>> {
    collection
        .find_one(doc! { "_id": id }, None)
        .await
        .context("failed to load listing source state")
}

pub async fn load_current_materialized_doc(
    collection: &Collection<MaterializedListingsSnapshotDoc>,
    id: &str,
) -> anyhow::Result<Option<MaterializedListingsSnapshotDoc>> {
    collection
        .find_one(doc! { "_id": id }, None)
        .await
        .context("failed to load current materialized listings snapshot document")
}

pub fn cached_snapshot_from_materialized_doc(
    doc: &MaterializedListingsSnapshotDoc,
) -> anyhow::Result<CachedListingsSnapshot> {
    Ok(CachedListingsSnapshot {
        revision: doc
            .revision
            .try_into()
            .context("materialized snapshot revision must be non-negative")?,
        body: doc.body_gzip.bytes.clone().into(),
        etag: Some(doc.etag.clone()),
        content_encoding: (!doc.content_encoding.trim().is_empty())
            .then(|| doc.content_encoding.clone()),
    })
}

pub async fn load_current_materialized_snapshot(
    state: &State,
) -> anyhow::Result<Option<CachedListingsSnapshot>> {
    let collection = state.listings_snapshot_collection();
    let doc = load_current_materialized_doc(&collection, &state.listings_snapshot_document_id)
        .await
        .context("failed to load current materialized listings snapshot")?;

    doc.as_ref()
        .map(cached_snapshot_from_materialized_doc)
        .transpose()
}

pub fn listing_source_revision_increment_update(
    document_id: &str,
    updated_at: DateTime<Utc>,
) -> (Document, Document, UpdateOptions) {
    let filter = doc! { "_id": document_id };
    let update = doc! {
        "$inc": { "revision": 1_i64 },
        "$set": { "updated_at": mongodb::bson::DateTime::from_chrono(updated_at) },
        "$setOnInsert": { "_id": document_id },
    };
    let options = UpdateOptions::builder().upsert(true).build();

    (filter, update, options)
}

pub async fn increment_listing_source_revision(state: &State) -> anyhow::Result<i64> {
    let (filter, update, _) = listing_source_revision_increment_update(
        &state.listing_source_state_document_id,
        Utc::now(),
    );
    let options = FindOneAndUpdateOptions::builder()
        .upsert(true)
        .return_document(ReturnDocument::After)
        .build();

    let doc = state
        .listing_source_state_collection()
        .find_one_and_update(filter, update, options)
        .await
        .context("failed to increment listing source revision")?
        .context("listing source revision update returned no document")?;

    Ok(doc.revision)
}

pub fn materialized_revision_allocate_update(
    document_id: &str,
    updated_at: DateTime<Utc>,
) -> (Document, Document, FindOneAndUpdateOptions) {
    let filter = doc! { "_id": document_id };
    let update = doc! {
        "$inc": { "revision": 1_i64 },
        "$set": { "updated_at": mongodb::bson::DateTime::from_chrono(updated_at) },
        "$setOnInsert": { "_id": document_id },
    };
    let options = FindOneAndUpdateOptions::builder()
        .upsert(true)
        .return_document(ReturnDocument::After)
        .build();

    (filter, update, options)
}

pub fn materialized_revision_seed_update(
    document_id: &str,
    updated_at: DateTime<Utc>,
    minimum_revision: i64,
) -> (Document, Document, UpdateOptions) {
    let filter = doc! { "_id": document_id };
    let update = doc! {
        "$max": { "revision": minimum_revision },
        "$set": { "updated_at": mongodb::bson::DateTime::from_chrono(updated_at) },
        "$setOnInsert": { "_id": document_id },
    };
    let options = UpdateOptions::builder().upsert(true).build();

    (filter, update, options)
}

pub async fn ensure_materialized_revision_at_least(
    collection: &Collection<ListingSnapshotRevisionStateDoc>,
    id: &str,
    minimum_revision: i64,
) -> anyhow::Result<()> {
    let (filter, update, options) =
        materialized_revision_seed_update(id, Utc::now(), minimum_revision.max(0));
    collection
        .update_one(filter, update, options)
        .await
        .context("failed to seed materialized listings snapshot revision")?;

    Ok(())
}

pub async fn allocate_materialized_revision(
    collection: &Collection<ListingSnapshotRevisionStateDoc>,
    id: &str,
) -> anyhow::Result<i64> {
    let (filter, update, options) = materialized_revision_allocate_update(id, Utc::now());
    let doc = collection
        .find_one_and_update(filter, update, options)
        .await
        .context("failed to allocate materialized listings snapshot revision")?
        .context("materialized listings snapshot revision allocation returned no document")?;

    Ok(doc.revision)
}

pub fn snapshot_worker_lease_acquire_update(
    document_id: &str,
    owner_id: &str,
    now: DateTime<Utc>,
    expires_at: DateTime<Utc>,
) -> (Document, Document, UpdateOptions) {
    let filter = doc! {
        "_id": document_id,
        "$or": [
            { "expires_at": { "$lte": mongodb::bson::DateTime::from_chrono(now) } },
            { "owner_id": owner_id },
        ],
    };
    let update = doc! {
        "$set": {
            "owner_id": owner_id,
            "expires_at": mongodb::bson::DateTime::from_chrono(expires_at),
            "updated_at": mongodb::bson::DateTime::from_chrono(now),
        },
        "$setOnInsert": { "_id": document_id },
    };
    let options = UpdateOptions::builder().upsert(true).build();

    (filter, update, options)
}

pub fn snapshot_worker_lease_renew_update(
    document_id: &str,
    owner_id: &str,
    now: DateTime<Utc>,
    expires_at: DateTime<Utc>,
) -> (Document, Document, UpdateOptions) {
    let filter = doc! {
        "_id": document_id,
        "owner_id": owner_id,
        "expires_at": { "$gt": mongodb::bson::DateTime::from_chrono(now) },
    };
    let update = doc! {
        "$set": {
            "expires_at": mongodb::bson::DateTime::from_chrono(expires_at),
            "updated_at": mongodb::bson::DateTime::from_chrono(now),
        },
    };
    let options = UpdateOptions::builder().upsert(false).build();

    (filter, update, options)
}

pub fn snapshot_worker_lease_can_acquire(
    current: Option<&ListingSnapshotWorkerLeaseDoc>,
    owner_id: &str,
    now: DateTime<Utc>,
) -> bool {
    match current {
        Some(lease) => lease.expires_at <= now || lease.owner_id == owner_id,
        None => true,
    }
}

fn is_duplicate_key_error(error: &mongodb::error::Error) -> bool {
    error.to_string().contains("E11000") || error.to_string().contains("duplicate key")
}

pub async fn try_acquire_snapshot_worker_lease(
    collection: &Collection<ListingSnapshotWorkerLeaseDoc>,
    id: &str,
    owner_id: &str,
    now: DateTime<Utc>,
    lease_ttl_seconds: i64,
) -> anyhow::Result<bool> {
    let expires_at = now + chrono::TimeDelta::seconds(lease_ttl_seconds.max(1));
    let (filter, update, options) =
        snapshot_worker_lease_acquire_update(id, owner_id, now, expires_at);

    match collection.update_one(filter, update, options).await {
        Ok(result) => Ok(result.matched_count == 1 || result.upserted_id.is_some()),
        Err(error) if is_duplicate_key_error(&error) => Ok(false),
        Err(error) => Err(error).context("failed to acquire listings snapshot worker lease"),
    }
}

pub async fn try_renew_snapshot_worker_lease(
    collection: &Collection<ListingSnapshotWorkerLeaseDoc>,
    id: &str,
    owner_id: &str,
    now: DateTime<Utc>,
    lease_ttl_seconds: i64,
) -> anyhow::Result<bool> {
    let expires_at = now + chrono::TimeDelta::seconds(lease_ttl_seconds.max(1));
    let (filter, update, options) =
        snapshot_worker_lease_renew_update(id, owner_id, now, expires_at);

    collection
        .update_one(filter, update, options)
        .await
        .map(|result| result.matched_count == 1)
        .context("failed to renew listings snapshot worker lease")
}

pub fn materialized_snapshot_cas_filter(
    document_id: &str,
    revision: i64,
    payload_hash: &str,
) -> Document {
    doc! {
        "_id": document_id,
        "$and": [
            {
                "$or": [
                    { "revision": { "$lt": revision } },
                    { "revision": { "$exists": false } },
                ],
            },
            {
                "$or": [
                    { "payload_hash": { "$ne": payload_hash } },
                    { "payload_hash": { "$exists": false } },
                ],
            },
        ],
    }
}

pub fn materialized_snapshot_cas_update(
    document_id: &str,
    built: BuiltListingsSnapshot,
    source_revision: i64,
) -> anyhow::Result<(Document, Document, UpdateOptions)> {
    let filter = materialized_snapshot_cas_filter(document_id, built.revision, &built.payload_hash);
    let doc = materialized_doc_from_snapshot(document_id, built, source_revision)?;
    let mut set_doc = mongodb::bson::to_document(&doc)
        .context("failed to serialize materialized listings snapshot document")?;
    set_doc.remove("_id");
    let update = doc! {
        "$set": set_doc,
        "$setOnInsert": { "_id": document_id },
    };
    let options = UpdateOptions::builder().upsert(true).build();

    Ok((filter, update, options))
}

pub async fn try_write_materialized_snapshot_cas(
    collection: &Collection<MaterializedListingsSnapshotDoc>,
    id: &str,
    built: BuiltListingsSnapshot,
    source_revision: i64,
) -> anyhow::Result<bool> {
    let (filter, update, options) = materialized_snapshot_cas_update(id, built, source_revision)?;
    match collection.update_one(filter, update, options).await {
        Ok(result) => Ok(result.matched_count == 1 || result.upserted_id.is_some()),
        Err(error) if is_duplicate_key_error(&error) => Ok(false),
        Err(error) => Err(error).context("failed to write materialized listings snapshot"),
    }
}

fn hash_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!(
        "sha256-{}",
        base64::encode_config(digest, base64::URL_SAFE_NO_PAD)
    )
}

fn sestring_payloads(value: &SeString) -> Vec<ApiSeStringPayload> {
    value
        .0
        .iter()
        .filter_map(|payload| match payload {
            Payload::Text(text) => Some(ApiSeStringPayload::Text {
                text: text.0.clone(),
            }),
            Payload::AutoTranslate(auto_translate) => Some(ApiSeStringPayload::AutoTranslate {
                group: auto_translate.group,
                key: auto_translate.key,
            }),
            _ => None,
        })
        .collect()
}

fn description_badges(listing: &PartyFinderListing) -> (&'static str, Vec<&'static str>) {
    let mut colour_class = "";
    let mut badges = Vec::new();

    if listing.objective.contains(ObjectiveFlags::PRACTICE) {
        badges.push("practice");
        colour_class = "desc-green";
    }

    if listing.objective.contains(ObjectiveFlags::DUTY_COMPLETION) {
        badges.push("duty_completion");
        colour_class = "desc-blue";
    }

    if listing.objective.contains(ObjectiveFlags::LOOT) {
        badges.push("loot");
        colour_class = "desc-yellow";
    }

    if listing.conditions.contains(ConditionFlags::DUTY_COMPLETE) {
        badges.push("duty_complete");
    }

    if listing
        .conditions
        .contains(ConditionFlags::DUTY_COMPLETE_WEEKLY_REWARD_UNCLAIMED)
    {
        badges.push("weekly_reward_unclaimed");
    }

    if listing.conditions.contains(ConditionFlags::DUTY_INCOMPLETE) {
        badges.push("duty_incomplete");
    }

    if listing
        .search_area
        .contains(SearchAreaFlags::ONE_PLAYER_PER_JOB)
    {
        badges.push("one_player_per_job");
    }

    (colour_class, badges)
}

fn role_class_from_class_job(class_job: &ffxiv_types::jobs::ClassJob) -> &'static str {
    use ffxiv_types::Role;

    match class_job.role() {
        Some(Role::Tank) => "tank",
        Some(Role::Healer) => "healer",
        Some(Role::Dps) => "dps",
        None => "",
    }
}

fn build_display_slots(listing: &PartyFinderListing) -> Vec<ApiDisplaySlot> {
    listing
        .slots()
        .into_iter()
        .map(|slot| match slot {
            Ok(class_job) => ApiDisplaySlot {
                filled: true,
                role_class: role_class_from_class_job(&class_job).to_string(),
                title: class_job.code().to_string(),
                icon_code: Some(class_job.code().to_string()),
            },
            Err((role_class, title)) => ApiDisplaySlot {
                filled: false,
                role_class,
                title,
                icon_code: None,
            },
        })
        .collect()
}

/// A render-oriented JSON shape for client-side listings rendering.
#[derive(Debug, Serialize)]
pub struct ApiReadableListingContainer {
    pub time_left_seconds: i64,
    pub updated_at_timestamp: i64,
    pub listing: ApiReadableListing,
}

impl ApiReadableListingContainer {
    pub(crate) fn from_renderable(value: RenderableListing) -> Self {
        let RenderableListing {
            container,
            members,
            leader_parse,
        } = value;
        let QueriedListing {
            created_at: _,
            updated_at,
            updated_minute: _,
            time_left,
            listing,
        } = container;
        let time_left_seconds = time_left as i64;
        let updated_at_timestamp = updated_at.timestamp();

        Self {
            time_left_seconds,
            updated_at_timestamp,
            listing: ApiReadableListing::from_parts(listing, members, leader_parse),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ApiReadableListing {
    pub creator_name: Vec<ApiSeStringPayload>,
    pub description: Vec<ApiSeStringPayload>,
    pub duty_id: u16,
    pub duty_type: u8,
    pub category: u32,
    pub created_world: ApiReadableWorld,
    pub home_world: ApiReadableWorld,
    pub data_centre: Option<&'static str>,
    pub min_item_level: u16,
    pub num_parties: u8,
    pub slot_count: u8,
    pub slots_filled_count: usize,
    pub high_end: bool,
    pub cross_world: bool,
    pub content_kind: u32,
    pub joinable_roles: u32,
    pub objective_bits: u32,
    pub conditions_bits: u32,
    pub search_area_bits: u32,
    pub description_badge_class: &'static str,
    pub description_badges: Vec<&'static str>,
    pub display_slots: Vec<ApiDisplaySlot>,
    pub members: Vec<ApiReadableMember>,
    pub leader_parse: ApiParseDisplay,
    pub is_alliance_view: bool,
}

impl ApiReadableListing {
    pub(crate) fn from_parts(
        value: PartyFinderListing,
        members: Vec<RenderableMember>,
        leader_parse: ParseDisplay,
    ) -> Self {
        let (description_badge_class, description_badges) = description_badges(&value);
        let is_alliance_view =
            value.num_parties >= 3 || members.iter().any(|member| member.party_index > 0);

        Self {
            creator_name: sestring_payloads(&value.name),
            description: sestring_payloads(&value.description),
            duty_id: value.duty,
            duty_type: value.duty_type as u8,
            category: value.category as u32,
            created_world: value.created_world.into(),
            home_world: value.home_world.into(),
            data_centre: value.data_centre_name(),
            min_item_level: value.min_item_level,
            num_parties: value.num_parties,
            slot_count: value.slots_available,
            slots_filled_count: value.slots_filled(),
            high_end: value.high_end(),
            cross_world: value.is_cross_world(),
            content_kind: value.content_kind(),
            joinable_roles: value.joinable_roles(),
            objective_bits: value.objective.bits() as u32,
            conditions_bits: value.conditions.bits() as u32,
            search_area_bits: value.search_area.bits() as u32,
            description_badge_class,
            description_badges,
            display_slots: build_display_slots(&value),
            members: members.into_iter().map(ApiReadableMember::from).collect(),
            leader_parse: leader_parse.into(),
            is_alliance_view,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ApiReadableMember {
    pub name: String,
    pub home_world: ApiReadableWorld,
    pub job_id: u8,
    pub job_code: Option<&'static str>,
    pub role_class: &'static str,
    pub parse: ApiParseDisplay,
    pub progress: ApiProgressDisplay,
    pub slot_index: usize,
    pub party_index: u8,
    pub fflogs_character_url: Option<String>,
}

impl From<RenderableMember> for ApiReadableMember {
    fn from(value: RenderableMember) -> Self {
        let job_code = value.job_code();
        let role_class = value.role_class();
        let fflogs_character_url = value.fflogs_character_url();

        Self {
            name: value.player.name,
            home_world: value.player.home_world.into(),
            job_id: value.job_id,
            job_code,
            role_class,
            parse: value.parse.into(),
            progress: value.progress.into(),
            slot_index: value.slot_index,
            party_index: value.party_index,
            fflogs_character_url,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ApiParseDisplay {
    pub primary_percentile: Option<u8>,
    pub primary_color_class: String,
    pub secondary_percentile: Option<u8>,
    pub secondary_color_class: String,
    pub has_secondary: bool,
    pub hidden: bool,
    pub originally_hidden: bool,
    pub estimated: bool,
}

impl From<ParseDisplay> for ApiParseDisplay {
    fn from(value: ParseDisplay) -> Self {
        Self {
            primary_percentile: value.primary_percentile,
            primary_color_class: value.primary_color_class,
            secondary_percentile: value.secondary_percentile,
            secondary_color_class: value.secondary_color_class,
            has_secondary: value.has_secondary,
            hidden: value.hidden,
            originally_hidden: value.originally_hidden,
            estimated: value.estimated,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ApiProgressDisplay {
    pub final_boss_percentage: Option<u8>,
    pub final_clear_count: Option<u16>,
}

impl From<ProgressDisplay> for ApiProgressDisplay {
    fn from(value: ProgressDisplay) -> Self {
        Self {
            final_boss_percentage: value.final_boss_percentage,
            final_clear_count: value.final_clear_count,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ApiSeStringPayload {
    Text { text: String },
    AutoTranslate { group: u8, key: u32 },
}

#[derive(Debug, Serialize)]
pub struct ApiReadableWorld {
    pub name: &'static str,
}

impl From<u16> for ApiReadableWorld {
    fn from(value: u16) -> Self {
        Self {
            name: crate::ffxiv::WORLDS
                .get(&(value as u32))
                .map(|w| w.as_str())
                .unwrap_or("Unknown"),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ApiDisplaySlot {
    pub filled: bool,
    pub role_class: String,
    pub title: String,
    pub icon_code: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let cached = cached_snapshot_from_materialized_doc(&doc)
            .expect("materialized snapshot should decode");

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
        let (filter, update, options) =
            listing_source_revision_increment_update("current", Utc::now());

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
        let (filter, update, options) =
            materialized_revision_allocate_update("current", Utc::now());

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
}
