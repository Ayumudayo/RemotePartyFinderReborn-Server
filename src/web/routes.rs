use std::sync::Arc;
use warp::{filters::BoxedFilter, http::Uri, Filter, Reply};

use super::State;
use super::{contribute, fflogs_ingest, handlers};
use crate::listing::PartyFinderListing;
use crate::player::{UploadableCharacterIdentity, UploadablePlayer};

pub fn router(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    index()
        .or(listings(Arc::clone(&state)))
        .or(contribute(Arc::clone(&state)))
        .or(contribute_multiple(Arc::clone(&state)))
        .or(contribute_players(Arc::clone(&state)))
        .or(contribute_character_identity(Arc::clone(&state)))
        .or(contribute_detail(Arc::clone(&state)))
        .or(contribute_fflogs_jobs(Arc::clone(&state)))
        .or(contribute_fflogs_results(Arc::clone(&state)))
        .or(contribute_fflogs_leases_abandon(Arc::clone(&state)))
        .or(stats(Arc::clone(&state)))
        .or(stats_seven_days(Arc::clone(&state)))
        .or(assets())
        .or(crate::api::internal_routes(Arc::clone(&state)))
        .or(crate::api::api(Arc::clone(&state)))
        .boxed()
}

fn language_codes() -> BoxedFilter<(Option<String>,)> {
    warp::cookie::<String>("lang")
        .or(warp::header::<String>("accept-language"))
        .unify()
        .map(Some)
        .or(warp::any().map(|| None))
        .unify()
        .boxed()
}

fn index() -> BoxedFilter<(impl Reply,)> {
    let route = warp::path::end().map(|| warp::redirect(Uri::from_static("/listings")));
    warp::get().and(route).boxed()
}

fn listings(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route = warp::path("listings")
        .and(warp::path::end())
        .and(language_codes())
        .and_then(move |codes: Option<String>| {
            handlers::listings_handler(Arc::clone(&state), codes)
        });

    warp::get().and(route).boxed()
}

fn stats(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route = warp::path("stats")
        .and(warp::path::end())
        .and(language_codes())
        .and_then(move |codes: Option<String>| {
            handlers::stats_handler(Arc::clone(&state), codes, false)
        });

    warp::get().and(route).boxed()
}

fn stats_seven_days(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route = warp::path("stats")
        .and(warp::path("7days"))
        .and(warp::path::end())
        .and(language_codes())
        .and_then(move |codes: Option<String>| {
            handlers::stats_handler(Arc::clone(&state), codes, true)
        });

    warp::get().and(route).boxed()
}

fn contribute(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route = warp::path("contribute")
        .and(warp::path::end())
        .and(warp::header::headers_cloned())
        .and(warp::addr::remote())
        .and(warp::body::content_length_limit(
            state.max_body_bytes_contribute,
        ))
        .and(warp::body::json())
        .and_then(move |headers, remote_addr, listing: PartyFinderListing| {
            contribute::contribute_handler(Arc::clone(&state), headers, remote_addr, listing)
        });
    warp::post().and(route).boxed()
}

fn contribute_multiple(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route = warp::path("contribute")
        .and(warp::path("multiple"))
        .and(warp::path::end())
        .and(warp::header::headers_cloned())
        .and(warp::addr::remote())
        .and(warp::body::content_length_limit(
            state.max_body_bytes_multiple,
        ))
        .and(warp::body::json())
        .and_then(
            move |headers, remote_addr, listings: Vec<PartyFinderListing>| {
                contribute::contribute_multiple_handler(
                    Arc::clone(&state),
                    headers,
                    remote_addr,
                    listings,
                )
            },
        );
    warp::post().and(route).boxed()
}

fn contribute_players(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route = warp::path("contribute")
        .and(warp::path("players"))
        .and(warp::path::end())
        .and(warp::header::headers_cloned())
        .and(warp::addr::remote())
        .and(warp::body::content_length_limit(
            state.max_body_bytes_players,
        ))
        .and(warp::body::json())
        .and_then(
            move |headers, remote_addr, players: Vec<UploadablePlayer>| {
                contribute::contribute_players_handler(
                    Arc::clone(&state),
                    headers,
                    remote_addr,
                    players,
                )
            },
        );
    warp::post().and(route).boxed()
}

fn contribute_character_identity(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route = warp::path("contribute")
        .and(warp::path("character-identity"))
        .and(warp::path::end())
        .and(warp::header::headers_cloned())
        .and(warp::addr::remote())
        .and(warp::body::content_length_limit(
            state.max_body_bytes_players,
        ))
        .and(warp::body::json())
        .and_then(
            move |headers, remote_addr, identities: Vec<UploadableCharacterIdentity>| {
                contribute::contribute_character_identity_handler(
                    Arc::clone(&state),
                    headers,
                    remote_addr,
                    identities,
                )
            },
        );
    warp::post().and(route).boxed()
}

fn contribute_detail(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route = warp::path("contribute")
        .and(warp::path("detail"))
        .and(warp::path::end())
        .and(warp::header::headers_cloned())
        .and(warp::addr::remote())
        .and(warp::body::content_length_limit(
            state.max_body_bytes_detail,
        ))
        .and(warp::body::json())
        .and_then(
            move |headers, remote_addr, detail: contribute::UploadablePartyDetail| {
                contribute::contribute_detail_handler(
                    Arc::clone(&state),
                    headers,
                    remote_addr,
                    detail,
                )
            },
        );
    warp::post().and(route).boxed()
}

fn contribute_fflogs_jobs(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route = warp::path("contribute")
        .and(warp::path("fflogs"))
        .and(warp::path("jobs"))
        .and(warp::path::end())
        .and(warp::header::headers_cloned())
        .and(warp::addr::remote())
        .and_then(move |headers, remote_addr| {
            fflogs_ingest::contribute_fflogs_jobs_handler(Arc::clone(&state), headers, remote_addr)
        });
    warp::get().and(route).boxed()
}

fn contribute_fflogs_results(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route = warp::path("contribute")
        .and(warp::path("fflogs"))
        .and(warp::path("results"))
        .and(warp::path::end())
        .and(warp::header::headers_cloned())
        .and(warp::addr::remote())
        .and(warp::body::content_length_limit(
            state.max_body_bytes_fflogs_results,
        ))
        .and(warp::body::json())
        .and_then(
            move |headers, remote_addr, results: Vec<fflogs_ingest::ParseResult>| {
                fflogs_ingest::contribute_fflogs_results_handler(
                    Arc::clone(&state),
                    headers,
                    remote_addr,
                    results,
                )
            },
        );
    warp::post().and(route).boxed()
}

fn contribute_fflogs_leases_abandon(state: Arc<State>) -> BoxedFilter<(impl Reply,)> {
    let route = warp::path("contribute")
        .and(warp::path("fflogs"))
        .and(warp::path("leases"))
        .and(warp::path("abandon"))
        .and(warp::path::end())
        .and(warp::header::headers_cloned())
        .and(warp::addr::remote())
        .and(warp::body::content_length_limit(
            state.max_body_bytes_fflogs_results,
        ))
        .and(warp::body::json())
        .and_then(
            move |headers, remote_addr, leases: Vec<fflogs_ingest::AbandonFflogsLease>| {
                fflogs_ingest::contribute_fflogs_leases_abandon_handler(
                    Arc::clone(&state),
                    headers,
                    remote_addr,
                    leases,
                )
            },
        );
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
                .or(listing_data_js())
                .or(listings_js())
                .or(stats_css())
                .or(stats_js())
                .or(d3())
                .or(pico())
                .or(common_js())
                .or(translations_js()),
        )
        .boxed()
}

fn immutable_asset_cache_control() -> &'static str {
    "public, max-age=31536000, immutable"
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

fn listing_data_js() -> BoxedFilter<(impl Reply,)> {
    warp::path("listing-data.js")
        .and(warp::path::end())
        .and(warp::fs::file("./assets/listing-data.js"))
        .map(|file| {
            warp::reply::with_header(file, "Cache-Control", immutable_asset_cache_control())
        })
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

fn translations_js() -> BoxedFilter<(impl Reply,)> {
    warp::path("translations.js")
        .and(warp::path::end())
        .and(warp::fs::file("./assets/translations.js"))
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::{immutable_asset_cache_control, router};
    use crate::web::test_support::{sample_listing, state_with_signature_required};
    use warp::http::StatusCode;

    #[test]
    fn immutable_asset_cache_control_is_long_lived() {
        assert_eq!(
            immutable_asset_cache_control(),
            "public, max-age=31536000, immutable"
        );
    }

    #[tokio::test]
    async fn contribute_routes_bind_request_bodies_before_authorization() {
        let route = router(state_with_signature_required().await);
        let listing = serde_json::to_string(&sample_listing()).unwrap();

        for (path, body) in [
            ("/contribute", listing),
            ("/contribute/multiple", "[]".to_string()),
            ("/contribute/players", "[]".to_string()),
            ("/contribute/character-identity", "[]".to_string()),
            (
                "/contribute/detail",
                r#"{"listing_id":1,"leader_content_id":2,"leader_name":"Leader","home_world":73,"member_content_ids":[2],"member_jobs":[37],"slot_flags":["0x0000000000000001"]}"#
                    .to_string(),
            ),
        ] {
            let response = warp::test::request()
                .method("POST")
                .path(path)
                .header("content-type", "application/json")
                .body(body)
                .reply(&route)
                .await;

            assert_eq!(
                response.status(),
                StatusCode::UNAUTHORIZED,
                "route {path} should reach authorization after JSON binding"
            );
        }
    }

    #[tokio::test]
    async fn fflogs_routes_bind_request_bodies_before_authorization() {
        let route = router(state_with_signature_required().await);

        let jobs_response = warp::test::request()
            .method("GET")
            .path("/contribute/fflogs/jobs")
            .reply(&route)
            .await;
        assert_eq!(
            jobs_response.status(),
            StatusCode::UNAUTHORIZED,
            "FFLogs jobs route should reach authorization"
        );

        for (path, body) in [
            (
                "/contribute/fflogs/results",
                r#"[{"content_id":1,"zone_id":73,"difficulty_id":101,"partition":1,"encounters":{"1":95.0},"boss_percentages":{},"clear_counts":{"1":1},"is_hidden":false,"is_estimated":false,"matched_server":"Chocobo","lease_token":"lease"}]"#,
            ),
            (
                "/contribute/fflogs/leases/abandon",
                r#"[{"content_id":1,"zone_id":73,"difficulty_id":101,"partition":1,"lease_token":"lease","reason":"test"}]"#,
            ),
        ] {
            let response = warp::test::request()
                .method("POST")
                .path(path)
                .header("content-type", "application/json")
                .body(body)
                .reply(&route)
                .await;

            assert_eq!(
                response.status(),
                StatusCode::UNAUTHORIZED,
                "route {path} should reach authorization after JSON binding"
            );
        }
    }
}
