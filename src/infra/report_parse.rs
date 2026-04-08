use std::collections::HashMap;

use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::fflogs::EncounterParse;

pub const REPORT_PARSE_SOURCE: &str = "report_parse";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReportParseIdentityKey {
    pub normalized_name: String,
    pub home_world: u16,
}

impl ReportParseIdentityKey {
    pub fn new(name: &str, home_world: u16) -> Self {
        Self {
            normalized_name: normalize_character_name(name),
            home_world,
        }
    }

    pub fn from_summary(doc: &ReportParseSummaryDoc) -> Self {
        Self {
            normalized_name: doc.normalized_name.clone(),
            home_world: doc.home_world,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportParseSummaryDoc {
    pub normalized_name: String,
    pub display_name: String,
    pub home_world: u16,
    pub zone_key: String,
    pub zone_id: u32,
    pub difficulty_id: i32,
    pub partition: i32,
    #[serde(default)]
    pub encounters: HashMap<String, EncounterParse>,
    pub source: String,
    #[serde(with = "mongodb::bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub first_seen_report_at: chrono::DateTime<Utc>,
    #[serde(with = "mongodb::bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub last_seen_report_at: chrono::DateTime<Utc>,
    #[serde(with = "mongodb::bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub updated_at: chrono::DateTime<Utc>,
}

pub fn normalize_character_name(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::normalize_character_name;

    #[test]
    fn normalize_character_name_trims_and_lowercases() {
        assert_eq!(normalize_character_name("  Alice Example  "), "alice example");
    }
}
