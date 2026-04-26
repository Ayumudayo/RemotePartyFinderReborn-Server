use std::sync::Arc;

use crate::web::State;
use crate::ws::WsApiClient;
use warp::filters::BoxedFilter;
use warp::{Filter, Reply};
pub(super) fn ws(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route =
        warp::path("ws")
            .and(warp::ws())
            .and(warp::path::end())
            .map(move |ws: warp::ws::Ws| {
                let state = Arc::clone(&state);
                ws.on_upgrade(move |websocket| async move {
                    WsApiClient::run(state, websocket).await;
                })
            });

    warp::get().and(route).boxed()
}
