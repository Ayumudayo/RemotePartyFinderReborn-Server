use std::{collections::HashMap, time::Duration};

use futures_util::StreamExt;
use mongodb::{
    bson::{doc, Document},
    options::FindOptions,
    Collection,
};

pub use crate::report_parse::{
    ReportParseIdentityKey, ReportParseSummaryDoc, ReportParseZoneSummary,
};

const REPORT_PARSE_SUMMARY_LOOKUP_MAX_TIME: Duration = Duration::from_secs(2);
pub async fn get_report_parse_summaries_by_zone(
    collection: Collection<ReportParseSummaryDoc>,
    zone_key: &str,
    identities: &[ReportParseIdentityKey],
) -> anyhow::Result<HashMap<ReportParseIdentityKey, ReportParseZoneSummary>> {
    if zone_key.trim().is_empty() || identities.is_empty() {
        return Ok(HashMap::new());
    }

    let Some(filter) = report_parse_summary_identity_filter(identities) else {
        return Ok(HashMap::new());
    };
    let options = report_parse_summary_find_options(zone_key);
    let cursor = collection.find(filter, Some(options)).await?;

    let docs = cursor
        .filter_map(async |result| result.ok())
        .collect::<Vec<_>>()
        .await;

    let mut summaries = HashMap::new();
    for doc in docs {
        if let Some(zone_summary) = doc.zones.get(zone_key) {
            summaries.insert(
                ReportParseIdentityKey::from_summary(&doc),
                zone_summary.clone(),
            );
        }
    }

    Ok(summaries)
}

pub(super) fn report_parse_summary_identity_filter(
    identities: &[ReportParseIdentityKey],
) -> Option<Document> {
    let mut unique_identities = identities.to_vec();
    unique_identities.sort_by(|a, b| {
        a.normalized_name
            .cmp(&b.normalized_name)
            .then_with(|| a.home_world.cmp(&b.home_world))
    });
    unique_identities.dedup();
    if unique_identities.is_empty() {
        return None;
    }

    let filters = unique_identities
        .iter()
        .map(|identity| {
            doc! {
                "normalized_name": &identity.normalized_name,
                "home_world": identity.home_world as i32,
            }
        })
        .collect::<Vec<_>>();

    Some(doc! { "$or": filters })
}

pub(super) fn report_parse_summary_find_options(zone_key: &str) -> FindOptions {
    FindOptions::builder()
        .projection(doc! {
            "normalized_name": 1,
            "display_name": 1,
            "home_world": 1,
            "updated_at": 1,
            format!("zones.{}", zone_key): 1,
        })
        .max_time(REPORT_PARSE_SUMMARY_LOOKUP_MAX_TIME)
        .build()
}
