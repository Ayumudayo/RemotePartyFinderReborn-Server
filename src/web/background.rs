use std::{sync::Arc, time::Duration};

use crate::stats::CachedStatistics;
use super::State;

pub fn spawn_stats_task(state: Arc<State>) {
    let stats_state = Arc::clone(&state);
    tokio::task::spawn(async move {
        loop {
            let all_time = match crate::stats::get_stats(&*stats_state).await {
                Ok(stats) => stats,
                Err(e) => {
                    tracing::error!("error generating stats: {:#?}", e);
                    continue;
                }
            };

            let seven_days = match crate::stats::get_stats_seven_days(&*stats_state).await {
                Ok(stats) => stats,
                Err(e) => {
                    tracing::error!("error generating stats: {:#?}", e);
                    continue;
                }
            };

            *stats_state.stats.write().await = Some(CachedStatistics {
                all_time,
                seven_days,
            });

            tokio::time::sleep(Duration::from_secs(60 * 60 * 12)).await;
        }
    });
}

