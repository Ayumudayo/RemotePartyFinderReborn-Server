use std::collections::HashMap;

use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::fflogs::EncounterParse;

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
pub struct ReportParseZoneSummary {
    pub zone_id: u32,
    pub difficulty_id: i32,
    pub partition: i32,
    #[serde(default)]
    pub encounters: HashMap<String, EncounterParse>,
    #[serde(with = "mongodb::bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub first_seen_report_at: chrono::DateTime<Utc>,
    #[serde(with = "mongodb::bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub last_seen_report_at: chrono::DateTime<Utc>,
    #[serde(with = "mongodb::bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub updated_at: chrono::DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportParseSummaryDoc {
    pub normalized_name: String,
    pub display_name: String,
    pub home_world: u16,
    #[serde(default)]
    pub zones: HashMap<String, ReportParseZoneSummary>,
    #[serde(with = "mongodb::bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub updated_at: chrono::DateTime<Utc>,
}

pub fn normalize_character_name(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use mongodb::bson::{self, doc};
    use std::collections::HashMap;

    use super::{
        normalize_character_name, ReportParseIdentityKey, ReportParseSummaryDoc,
        ReportParseZoneSummary,
    };
    use crate::fflogs::EncounterParse;
    #[test]
    fn normalize_character_name_trims_and_lowercases() {
        assert_eq!(
            normalize_character_name("  Alice Example  "),
            "alice example"
        );
    }

    #[test]
    fn nested_zone_summary_document_deserializes_into_single_character_identity() {
        let now = Utc::now();
        let document = doc! {
            "normalized_name": "alice example",
            "display_name": "Alice Example",
            "home_world": 74,
            "zones": {
                "73:101:1": {
                    "zone_id": 73,
                    "difficulty_id": 101,
                    "partition": 1,
                    "encounters": {
                        "101": {
                            "percentile": 95.2,
                            "job_id": 0,
                            "boss_percentage": bson::Bson::Null,
                            "clear_count": 3,
                        }
                    },
                    "first_seen_report_at": bson::DateTime::from_chrono(now),
                    "last_seen_report_at": bson::DateTime::from_chrono(now),
                    "updated_at": bson::DateTime::from_chrono(now),
                },
                "72:100:1": {
                    "zone_id": 72,
                    "difficulty_id": 100,
                    "partition": 1,
                    "encounters": {
                        "1083": {
                            "percentile": -1.0,
                            "job_id": 0,
                            "boss_percentage": 12.4,
                            "clear_count": bson::Bson::Null,
                        }
                    },
                    "first_seen_report_at": bson::DateTime::from_chrono(now),
                    "last_seen_report_at": bson::DateTime::from_chrono(now),
                    "updated_at": bson::DateTime::from_chrono(now),
                }
            },
            "updated_at": bson::DateTime::from_chrono(now),
        };

        let decoded: ReportParseSummaryDoc =
            bson::from_document(document).expect("nested-zone summary should deserialize");
        let identity = ReportParseIdentityKey::from_summary(&decoded);

        assert_eq!(identity.normalized_name, "alice example");
        assert_eq!(identity.home_world, 74);
        assert_eq!(decoded.zones.len(), 2);
        assert_eq!(decoded.zones["73:101:1"].zone_id, 73);
    }

    #[test]
    fn nested_zone_summary_serializes_encounters_under_zones() {
        let now = Utc::now();
        let doc = ReportParseSummaryDoc {
            normalized_name: "alice example".to_string(),
            display_name: "Alice Example".to_string(),
            home_world: 74,
            zones: HashMap::from([(
                "73:101:1".to_string(),
                ReportParseZoneSummary {
                    zone_id: 73,
                    difficulty_id: 101,
                    partition: 1,
                    encounters: HashMap::from([(
                        "101".to_string(),
                        EncounterParse {
                            percentile: 95.2,
                            job_id: 0,
                            boss_percentage: None,
                            clear_count: Some(3),
                        },
                    )]),
                    first_seen_report_at: now,
                    last_seen_report_at: now,
                    updated_at: now,
                },
            )]),
            updated_at: now,
        };

        let encoded = bson::to_document(&doc).expect("summary should serialize");

        assert!(encoded.get("zone_key").is_none());
        assert!(encoded
            .get_document("zones")
            .unwrap()
            .contains_key("73:101:1"));
    }
}
