use crate::listing::PartyFinderListing;
use chrono::{DateTime, Duration, TimeDelta, Utc};
use chrono_humanize::HumanTime;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct ListingContainer {
    #[serde(with = "mongodb::bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub created_at: DateTime<Utc>,
    #[serde(with = "mongodb::bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub updated_at: DateTime<Utc>,
    pub listing: PartyFinderListing,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct QueriedListing {
    #[serde(with = "mongodb::bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub created_at: DateTime<Utc>,
    #[serde(with = "mongodb::bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub updated_at: DateTime<Utc>,
    #[serde(with = "mongodb::bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    pub updated_minute: DateTime<Utc>,
    pub time_left: f64,
    pub listing: PartyFinderListing,
}

impl QueriedListing {
    pub fn human_time_left(&self) -> HumanTime {
        HumanTime::from(
            TimeDelta::try_milliseconds((self.time_left * 1000f64) as i64)
                .unwrap_or(TimeDelta::zero()),
        )
    }

    pub fn since_updated(&self) -> Duration {
        Utc::now() - self.updated_at
    }

    pub fn human_since_updated(&self) -> HumanTime {
        HumanTime::from(-self.since_updated())
    }

    /// JavaScript에서 시간을 처리하기 위한 updated_at Unix timestamp (초 단위)
    pub fn updated_at_timestamp(&self) -> i64 {
        self.updated_at.timestamp()
    }

    /// JavaScript에서 시간을 처리하기 위한 남은 시간 (초 단위)
    pub fn time_left_seconds(&self) -> i64 {
        self.time_left as i64
    }
}
