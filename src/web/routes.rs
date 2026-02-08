
use std::sync::Arc;
use warp::{filters::BoxedFilter, http::Uri, Filter, Reply};

use crate::listing::PartyFinderListing;
use crate::player::UploadablePlayer;
use super::handlers;
use super::State;

pub fn router(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    index()
        .or(listings(Arc::clone(&state)))
        .or(contribute(Arc::clone(&state)))
        .or(contribute_multiple(Arc::clone(&state)))
        .or(contribute_players(Arc::clone(&state)))

        .or(contribute_detail(Arc::clone(&state)))
        .or(contribute_fflogs_jobs(Arc::clone(&state)))
        .or(contribute_fflogs_results(Arc::clone(&state)))
        .or(stats(Arc::clone(&state)))
        .or(stats_seven_days(Arc::clone(&state)))
        .or(assets())
        .or(crate::api::api(Arc::clone(&state)))
        .boxed()
}

fn index() -> BoxedFilter<(impl Reply,)> {
    let route = warp::path::end().map(|| warp::redirect(Uri::from_static("/listings")));
    warp::get().and(route).boxed()
}

fn listings(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route = warp::path("listings")
        .and(warp::path::end())
        .and(
            warp::cookie::<String>("lang")
                .or(warp::header::<String>("accept-language"))
                .unify()
                .map(Some)
                .or(warp::any().map(|| None))
                .unify(),
        )
        .and_then(move |codes: Option<String>| handlers::listings_handler(Arc::clone(&state), codes));

    warp::get().and(route).boxed()
}

fn stats(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route = warp::path("stats")
        .and(warp::path::end())
        .and(
            warp::cookie::<String>("lang")
                .or(warp::header::<String>("accept-language"))
                .unify()
                .map(Some)
                .or(warp::any().map(|| None))
                .unify(),
        )
        .and_then(move |codes: Option<String>| handlers::stats_handler(Arc::clone(&state), codes, false));

    warp::get().and(route).boxed()
}

fn stats_seven_days(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route = warp::path("stats")
        .and(warp::path("7days"))
        .and(warp::path::end())
        .and(
            warp::cookie::<String>("lang")
                .or(warp::header::<String>("accept-language"))
                .unify()
                .map(Some)
                .or(warp::any().map(|| None))
                .unify(),
        )
        .and_then(move |codes: Option<String>| handlers::stats_handler(Arc::clone(&state), codes, true));

    warp::get().and(route).boxed()
}

fn contribute(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route = warp::path("contribute")
        .and(warp::path::end())
        .and(warp::body::json())
        .and_then(move |listing: PartyFinderListing| handlers::contribute_handler(Arc::clone(&state), listing));
    warp::post().and(route).boxed()
}

fn contribute_multiple(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route = warp::path("contribute")
        .and(warp::path("multiple"))
        .and(warp::path::end())
        .and(warp::body::json())
        .and_then(move |listings: Vec<PartyFinderListing>| handlers::contribute_multiple_handler(Arc::clone(&state), listings));
    warp::post().and(route).boxed()
}

fn contribute_players(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route = warp::path("contribute")
        .and(warp::path("players"))
        .and(warp::path::end())
        .and(warp::body::json())
        .and_then(move |players: Vec<UploadablePlayer>| handlers::contribute_players_handler(Arc::clone(&state), players));
    warp::post().and(route).boxed()
}



fn contribute_detail(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route = warp::path("contribute")
        .and(warp::path("detail"))
        .and(warp::path::end())
        .and(warp::body::json())
        .and_then(move |detail: handlers::UploadablePartyDetail| handlers::contribute_detail_handler(Arc::clone(&state), detail));
    warp::post().and(route).boxed()
}

fn contribute_fflogs_jobs(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route = warp::path("contribute")
        .and(warp::path("fflogs"))
        .and(warp::path("jobs"))
        .and(warp::path::end())
        .and_then(move || handlers::contribute_fflogs_jobs_handler(Arc::clone(&state)));
    warp::get().and(route).boxed()
}

fn contribute_fflogs_results(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route = warp::path("contribute")
        .and(warp::path("fflogs"))
        .and(warp::path("results"))
        .and(warp::path::end())
        .and(warp::body::json())
        .and_then(move |results: Vec<handlers::ParseResult>| handlers::contribute_fflogs_results_handler(Arc::clone(&state), results));
    warp::post().and(route).boxed()
}



fn assets() -> BoxedFilter<(impl Reply,)> {
    warp::get()
        .and(warp::path("assets"))
        .and(
            icons()
                .or(minireset())
                .or(common_css())
                .or(listings_css())
                .or(listings_js())
                .or(stats_css())
                .or(stats_js())
                .or(d3())
                .or(pico())
                .or(common_js())
                .or(list_js())
                .or(translations_js()),
        )
        .boxed()
}

fn icons() -> BoxedFilter<(impl Reply,)> {
    warp::path("icons.svg")
        .and(warp::path::end())
        .and(warp::fs::file("./assets/icons.svg"))
        .boxed()
}

fn minireset() -> BoxedFilter<(impl Reply,)> {
    warp::path("minireset.css")
        .and(warp::path::end())
        .and(warp::fs::file("./assets/minireset.css"))
        .boxed()
}

fn common_css() -> BoxedFilter<(impl Reply,)> {
    warp::path("common.css")
        .and(warp::path::end())
        .and(warp::fs::file("./assets/common.css"))
        .boxed()
}

fn listings_css() -> BoxedFilter<(impl Reply,)> {
    warp::path("listings.css")
        .and(warp::path::end())
        .and(warp::fs::file("./assets/listings.css"))
        .boxed()
}

fn listings_js() -> BoxedFilter<(impl Reply,)> {
    warp::path("listings.js")
        .and(warp::path::end())
        .and(warp::fs::file("./assets/listings.js"))
        .boxed()
}

fn stats_css() -> BoxedFilter<(impl Reply,)> {
    warp::path("stats.css")
        .and(warp::path::end())
        .and(warp::fs::file("./assets/stats.css"))
        .boxed()
}

fn stats_js() -> BoxedFilter<(impl Reply,)> {
    warp::path("stats.js")
        .and(warp::path::end())
        .and(warp::fs::file("./assets/stats.js"))
        .boxed()
}

fn d3() -> BoxedFilter<(impl Reply,)> {
    warp::path("d3.js")
        .and(warp::path::end())
        .and(warp::fs::file("./assets/d3.v7.min.js"))
        .boxed()
}

fn pico() -> BoxedFilter<(impl Reply,)> {
    warp::path("pico.css")
        .and(warp::path::end())
        .and(warp::fs::file("./assets/pico.min.css"))
        .boxed()
}

fn common_js() -> BoxedFilter<(impl Reply,)> {
    warp::path("common.js")
        .and(warp::path::end())
        .and(warp::fs::file("./assets/common.js"))
        .boxed()
}

fn list_js() -> BoxedFilter<(impl Reply,)> {
    warp::path("list.js")
        .and(warp::path::end())
        .and(warp::fs::file("./assets/list.min.js"))
        .boxed()
}

fn translations_js() -> BoxedFilter<(impl Reply,)> {
    warp::path("translations.js")
        .and(warp::path::end())
        .and(warp::fs::file("./assets/translations.js"))
        .boxed()
}
