use crate::ffxiv::Language;
use crate::listing::{DutyCategory, DutyType};
use crate::web::State;
use anyhow::Result;
use chrono::{TimeDelta, Utc};
use futures_util::TryStreamExt;
use mongodb::bson::{doc, Document};
use mongodb::options::AggregateOptions;
use serde::{Deserialize, Deserializer};
use sestring::SeString;
use std::borrow::Cow;
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize)]
pub struct CachedStatistics {
    pub all_time: Statistics,
    pub seven_days: Statistics,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Aliases {
    #[serde(deserialize_with = "alias_de")]
    pub aliases: HashMap<u32, Alias>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Statistics {
    pub count: Vec<Count>,
    #[serde(default)]
    pub aliases: HashMap<u32, Alias>,
    pub duties: Vec<DutyInfo>,
    pub hosts: Vec<HostInfo>,
    pub hours: Vec<HourInfo>,
    pub days: Vec<DayInfo>,
}

fn alias_de<'de, D>(de: D) -> std::result::Result<HashMap<u32, Alias>, D::Error>
where
    D: Deserializer<'de>,
{
    let aliases: Vec<AliasInfo> = Deserialize::deserialize(de)?;
    let map = aliases
        .into_iter()
        .map(|info| (info.content_id, info.alias))
        .collect();
    Ok(map)
}

impl Statistics {
    pub fn num_listings(&self) -> usize {
        if self.count.is_empty() {
            return 0;
        }

        self.count[0].count
    }

    pub fn player_name(&self, cid: &u32) -> Cow<'_, str> {
        let alias = match self.aliases.get(cid) {
            Some(a) => a,
            None => return "<unknown>".into(),
        };

        let world = match crate::ffxiv::WORLDS.get(&alias.home_world) {
            Some(world) => world.name(),
            None => "<unknown>",
        };

        format!("{} @ {}", alias.name.text(), world).into()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Count {
    pub count: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AliasInfo {
    #[serde(rename = "_id")]
    pub content_id: u32,
    pub alias: Alias,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Alias {
    #[serde(with = "crate::base64_sestring")]
    pub name: SeString,
    pub home_world: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DutyInfo {
    #[serde(rename = "_id")]
    pub info: (u8, u32, u16),
    pub count: usize,
}

impl DutyInfo {
    pub fn name(&self, lang: &Language) -> Cow<'_, str> {
        let kind = match DutyType::from_u8(self.info.0) {
            Some(k) => k,
            None => return Cow::from("<unknown>"),
        };
        let category = match DutyCategory::from_u32(self.info.1) {
            Some(c) => c,
            None => return Cow::from("<unknown>"),
        };
        crate::ffxiv::duty_name(kind, category, self.info.2, *lang)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct HostInfo {
    #[serde(rename = "_id")]
    pub created_world: u32,
    pub count: usize,
    pub content_ids: Vec<HostInfoInfo>,
}

impl HostInfo {
    pub fn num_other(&self) -> usize {
        let top15: usize = self.content_ids.iter().map(|info| info.count).sum();
        self.count - top15
    }

    pub fn world_name(&self) -> &'static str {
        match crate::ffxiv::WORLDS.get(&self.created_world) {
            Some(world) => world.name(),
            None => "<unknown>",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct HostInfoInfo {
    pub content_id: u32,
    pub count: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HourInfo {
    #[serde(rename = "_id")]
    pub hour: u8,
    pub count: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DayInfo {
    #[serde(rename = "_id")]
    pub day: u8,
    pub count: usize,
}

impl DayInfo {
    pub fn name(&self) -> &'static str {
        match self.day {
            1 => "Sunday",
            2 => "Monday",
            3 => "Tuesday",
            4 => "Wednesday",
            5 => "Thursday",
            6 => "Friday",
            7 => "Saturday",
            _ => "<unknown>",
        }
    }
}

lazy_static::lazy_static! {
    static ref QUERY: [Document; 2] = [
        doc! {
            "$match": {
                // filter private pfs
                "listing.search_area": { "$bitsAllClear": 2 },
            }
        },
        doc! {
            "$facet": {
                "count": [
                    {
                        "$count": "count",
                    },
                ],
                "duties": [
                    {
                        "$group": {
                            "_id": [
                                "$listing.duty_type",
                                "$listing.category",
                                "$listing.duty",
                            ],
                            "count": {
                                "$sum": 1
                            },
                        }
                    },
                    {
                        "$sort": {
                            "count": -1,
                        }
                    }
                ],
                "hosts": [
                    {
                        "$group": {
                            "_id": {
                                "world": "$listing.created_world",
                                "content_id": "$listing.content_id_lower",
                            },
                            "count": { "$sum": 1 },
                        }
                    },
                    {
                        "$sort": {
                            "count": -1,
                        }
                    },
                    {
                        "$group": {
                            "_id": "$_id.world",
                            "count": {
                                "$sum": "$count",
                            },
                            "content_ids": {
                                "$push": {
                                    "content_id": "$_id.content_id",
                                    "count": "$count",
                                }
                            }
                        }
                    },
                    {
                        "$addFields": {
                            "content_ids": {
                                "$slice": ["$content_ids", 0, 15],
                            },
                        }
                    },
                    {
                        "$sort": { "count": -1 }
                    },
                ],
                "hours": [
                    {
                        "$group": {
                            "_id": {
                                "$hour": "$created_at",
                            },
                            "count": {
                                "$sum": 1
                            },
                        }
                    },
                    {
                        "$sort": {
                            "_id": 1,
                        }
                    }
                ],
                "days": [
                    {
                        "$group": {
                            "_id": {
                                "$dayOfWeek": "$created_at",
                            },
                            "count": {
                                "$sum": 1
                            },
                        }
                    },
                    {
                        "$sort": {
                            "_id": 1,
                        }
                    }
                ],
            }
        },
    ];

    static ref ALIASES_QUERY: [Document; 1] = [
        doc! {
            "$facet": {
                "aliases": [
                    {
                        "$sort": {
                            "created_at": -1,
                        }
                    },
                    {
                        "$group": {
                            "_id": "$listing.content_id_lower",
                            "alias": {
                                "$first": {
                                    "name": "$listing.name",
                                    "home_world": "$listing.home_world",
                                },
                            },
                        }
                    }
                ],
            },
        },
    ];
}

pub async fn get_stats(state: &State) -> Result<Statistics> {
    get_stats_internal(state, QUERY.iter().cloned()).await
}

pub async fn get_stats_seven_days(state: &State) -> Result<Statistics> {
    let last_week = Utc::now() - TimeDelta::try_days(7).unwrap();

    let mut docs = QUERY.to_vec();
    docs.insert(
        0,
        doc! {
            "$match": {
                "created_at": {
                    "$gte": last_week,
                },
            },
        },
    );

    get_stats_internal(state, docs).await
}

async fn get_stats_internal(
    state: &State,
    docs: impl IntoIterator<Item = Document>,
) -> Result<Statistics> {
    let mut cursor = state
        .collection()
        .aggregate(
            docs,
            AggregateOptions::builder().allow_disk_use(true).build(),
        )
        .await?;
    let doc = cursor.try_next().await?;
    let doc = doc.ok_or_else(|| anyhow::anyhow!("missing document"))?;
    let mut stats: Statistics = mongodb::bson::from_document(doc)?;

    let ids: Vec<u32> = stats
        .hosts
        .iter()
        .flat_map(|host| host.content_ids.iter().map(|entry| entry.content_id))
        .collect();
    let mut aliases_query: Vec<Document> = ALIASES_QUERY.iter().cloned().collect();
    aliases_query.insert(
        0,
        doc! {
            "$match": {
                "listing.content_id_lower": {
                    "$in": ids,
                }
            }
        },
    );
    let mut cursor = state
        .collection()
        .aggregate(
            aliases_query,
            AggregateOptions::builder().allow_disk_use(true).build(),
        )
        .await?;
    let doc = cursor.try_next().await?;
    let doc = doc.ok_or_else(|| anyhow::anyhow!("missing document"))?;
    let aliases: Aliases = mongodb::bson::from_document(doc)?;

    stats.aliases = aliases.aliases;

    Ok(stats)
}
