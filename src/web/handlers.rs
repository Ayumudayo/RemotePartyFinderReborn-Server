use std::{convert::Infallible, sync::Arc};
use warp::Reply;

use super::State;
use crate::{
    ffxiv::Language, template::listings::ListingsTemplate, template::stats::StatsTemplate,
};

pub async fn listings_handler(
    _state: Arc<State>,
    codes: Option<String>,
) -> std::result::Result<impl Reply, Infallible> {
    Ok(ListingsTemplate::new(
        Vec::new(),
        Language::from_codes(codes.as_deref()),
    ))
}

pub async fn stats_handler(
    state: Arc<State>,
    codes: Option<String>,
    seven_days: bool,
) -> std::result::Result<impl Reply, Infallible> {
    let lang = Language::from_codes(codes.as_deref());
    let stats = state.stats.read().await.clone();
    Ok(match stats {
        Some(stats) => StatsTemplate {
            stats: if seven_days {
                stats.seven_days
            } else {
                stats.all_time
            },
            lang,
        }
        .into_response(),
        None => "Stats haven't been calculated yet. Please wait :(".into_response(),
    })
}
