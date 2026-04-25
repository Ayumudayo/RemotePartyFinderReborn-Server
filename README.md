# Remote Party Finder Reborn - Server

A Rust backend server that aggregates Party Finder listings, integrates with FFLogs, and serves a modern web interface.

## Disclaimer

This is a fork of the [original Remote Party Finder project](https://github.com/zeroeightysix/remote-party-finder)'s server part by zeroeightysix.

**Note**: This project is a proof of concept created for personal interest. There are no plans for a public release.

## Key Features

- **Web Interface**: A responsive, clean web UI to view current Party Finder listings.
- **FFLogs Integration**: Receive parse data (Best Perfs) from clients and cache for display party members' Best Perfs.
- **Real-time Updates**: Receives data from connected Dalamud plugins.

## Architecture

The server is built with:
- **Language**: Rust
- **Web Framework**: Warp
- **Database**: MongoDB
- **Templating**: Askama (HTML templates)

## Setup & Usage

### Prerequisites

- **Rust** (Stable toolchain)
- **MongoDB** (Running instance)

### Configuration

1.  Copy `config.example.toml` to `config.toml`.
2.  Edit `config.toml` with your environment details:

    ```toml
    [web]

    # Network
    host = "127.0.0.1:8000"

    # FFLogs job scheduling
    fflogs_jobs_limit = 20
    fflogs_hidden_cache_ttl_hours = 24

    # Console monitor snapshot (seconds, 0 to disable; default off for low-noise console)
    monitor_snapshot_interval_seconds = 0

    # MongoDB write concurrency
    listing_upsert_concurrency = 16
    player_upsert_concurrency = 32

    # Request body size limits (bytes)
    max_body_bytes_contribute = 262144
    max_body_bytes_multiple = 1048576
    max_body_bytes_players = 524288
    max_body_bytes_detail = 131072
    max_body_bytes_fflogs_results = 524288

    # Batch size limits (items per request)
    max_multiple_batch_size = 200
    max_players_batch_size = 100
    max_fflogs_results_batch_size = 100
    max_detail_member_count = 48

    # Per-client rate limits (requests per minute)
    ingest_rate_limit_contribute_per_minute = 120
    ingest_rate_limit_multiple_per_minute = 60
    ingest_rate_limit_players_per_minute = 120
    ingest_rate_limit_detail_per_minute = 120
    ingest_rate_limit_fflogs_jobs_per_minute = 30
    ingest_rate_limit_fflogs_results_per_minute = 60

    # Signature/replay protection
    # Keep false for public plugin distribution; true is for controlled/private deployments.
    ingest_require_signature = false
    ingest_shared_secret = "rpf-reborn-public-ingest-v1"
    ingest_clock_skew_seconds = 300
    ingest_nonce_ttl_seconds = 300

    # Protected endpoint capabilities
    # Keep false during rollout, then enable after upgraded plugin builds are deployed.
    ingest_require_capabilities_for_protected_endpoints = false
    ingest_capability_secret = ""
    ingest_capability_session_ttl_seconds = 43200
    ingest_capability_detail_ttl_seconds = 900

    [mongo]
    url = "mongodb://localhost:27017/remote-party-finder"
    ```

For signature-enforced operation, start from `config.secure.example.toml` instead.
Set `ingest_shared_secret` to a strong random value and use the same value in the plugin's `IngestSharedSecret`.

Recommended mode for public plugin distribution is `ingest_require_signature = false`.
Shared-secret signatures are suitable for controlled/private deployments only; they are not strong client authentication for openly distributed binaries.
Protected endpoint capabilities are designed for a staged rollout: keep
`ingest_require_capabilities_for_protected_endpoints = false` until updated plugin builds are live, then enable it with a separate `ingest_capability_secret`.

### Materialized Listings Deployment

For low-resource hosting, run the web server in materialized snapshot mode and run
`listings_snapshot_worker` on a separate machine. The worker writes the compressed
snapshot into MongoDB and sends a signed refresh request to the web server.

Koyeb-style container deployments can override `config.toml` with environment
variables instead of baking secrets into the image:

```text
RPF_LISTINGS_SNAPSHOT_SOURCE=materialized
RPF_SNAPSHOT_REFRESH_SHARED_SECRET=<same-long-random-secret-as-worker>
RPF_SNAPSHOT_REFRESH_CLIENT_ID=listings-snapshot-worker
RPF_MATERIALIZED_SNAPSHOT_RECONCILE_INTERVAL_SECONDS=5
```

Use `MONGODB_URI`, `MONGO_URL`, or `RPF_MONGO_URL` when the MongoDB URL should be
injected from the host environment. The Pi-side worker must use the same MongoDB
URL, `snapshot_refresh_shared_secret`, `snapshot_refresh_client_id`, and
`listings_snapshot_document_id`, with `snapshot_worker.refresh_url` pointing at:

```text
https://<web-host>/internal/listings/snapshot/refresh
```

Supported environment overrides:

| Variable | Applies to | Type | Overrides |
| --- | --- | --- | --- |
| `PORT` | Server | TCP port | `web.host` as `0.0.0.0:<PORT>` |
| `RPF_WEB_HOST` | Server | Socket address | `web.host`; takes precedence over `PORT` |
| `MONGODB_URI` | Server, worker | String | `mongo.url` |
| `MONGO_URL` | Server, worker | String | `mongo.url` |
| `RPF_MONGO_URL` | Server, worker | String | `mongo.url` |
| `RPF_LISTINGS_SNAPSHOT_SOURCE` | Server, worker | `inline` or `materialized` | `web.listings_snapshot_source` |
| `RPF_LISTINGS_SNAPSHOT_COLLECTION` | Server, worker | String | `web.listings_snapshot_collection` |
| `RPF_LISTINGS_SNAPSHOT_DOCUMENT_ID` | Server, worker | String | `web.listings_snapshot_document_id` |
| `RPF_LISTING_SOURCE_STATE_COLLECTION` | Server, worker | String | `web.listing_source_state_collection` |
| `RPF_LISTING_SOURCE_STATE_DOCUMENT_ID` | Server, worker | String | `web.listing_source_state_document_id` |
| `RPF_LISTING_SNAPSHOT_REVISION_STATE_COLLECTION` | Server, worker | String | `web.listing_snapshot_revision_state_collection` |
| `RPF_LISTING_SNAPSHOT_WORKER_LEASE_COLLECTION` | Worker | String | `web.listing_snapshot_worker_lease_collection` |
| `RPF_MATERIALIZED_SNAPSHOT_RECONCILE_INTERVAL_SECONDS` | Server | Unsigned integer | `web.materialized_snapshot_reconcile_interval_seconds` |
| `RPF_SNAPSHOT_REFRESH_SHARED_SECRET` | Server, worker | String | `web.snapshot_refresh_shared_secret` |
| `RPF_SNAPSHOT_REFRESH_CLIENT_ID` | Server, worker | String | `web.snapshot_refresh_client_id` |
| `RPF_SNAPSHOT_REFRESH_CLOCK_SKEW_SECONDS` | Server, worker | Unsigned integer | `web.snapshot_refresh_clock_skew_seconds` |
| `RPF_SNAPSHOT_REFRESH_NONCE_TTL_SECONDS` | Server | Unsigned integer | `web.snapshot_refresh_nonce_ttl_seconds` |
| `RPF_SNAPSHOT_WORKER_ENABLED` | Worker | Boolean | `snapshot_worker.enabled` |
| `RPF_SNAPSHOT_WORKER_TICK_SECONDS` | Worker | Unsigned integer | `snapshot_worker.tick_seconds` |
| `RPF_SNAPSHOT_WORKER_FORCE_REBUILD_INTERVAL_SECONDS` | Worker | Unsigned integer | `snapshot_worker.force_rebuild_interval_seconds` |
| `RPF_SNAPSHOT_WORKER_LEASE_TTL_SECONDS` | Worker | Unsigned integer | `snapshot_worker.lease_ttl_seconds` |
| `RPF_SNAPSHOT_WORKER_OWNER_ID` | Worker | String | `snapshot_worker.owner_id` |
| `RPF_SNAPSHOT_WORKER_REFRESH_URL` | Worker | URL | `snapshot_worker.refresh_url` |
| `RPF_SNAPSHOT_WORKER_LOG_FILTER` | Worker | Tracing filter | `snapshot_worker.log_filter` |

Boolean values accept `1`, `true`, `yes`, `on`, `0`, `false`, `no`, and `off`.
Only the variables listed above are currently supported as environment
overrides; other tuning values must still be set in `config.toml`.

### Recommended Presets

Tune these four values together based on server capacity and contributor count:

| Preset | `fflogs_jobs_limit` | `fflogs_hidden_cache_ttl_hours` | `listing_upsert_concurrency` | `player_upsert_concurrency` |
| --- | ---: | ---: | ---: | ---: |
| Low-load / single-node | 10 | 24 | 8 | 16 |
| Balanced (default) | 20 | 24 | 16 | 32 |
| High-throughput | 30 | 12 | 24 | 48 |

- Higher `fflogs_jobs_limit` increases refresh speed but raises FFLogs/API pressure.
- Lower `fflogs_hidden_cache_ttl_hours` re-checks hidden characters sooner but increases query volume.
- `monitor_snapshot_interval_seconds` controls periodic one-line `[MONITOR]` logs for Koyeb console observability (default `0` = off).
- Increase upsert concurrency only if MongoDB has headroom; otherwise keep defaults.
- `ingest_require_signature = true` enables HMAC+timestamp+nonce verification for `/contribute*` requests.
- `ingest_require_capabilities_for_protected_endpoints = true` requires server-issued `X-RPF-Capability` tokens for `/contribute/detail` and `/contribute/fflogs/*`.
- `ingest_capability_session_ttl_seconds` controls the FFLogs worker token lifetime; `ingest_capability_detail_ttl_seconds` controls per-listing detail upload lifetime.
- `ingest_rate_limit_*` limits are keyed by remote IP when available (fallback: `X-RPF-Client-Id`) and return `429` with `Retry-After`.
- Restart the server after editing `config.toml`.

### Legacy `account_id` Migration

`players.account_id` is stored as `String` by design. If legacy documents still contain numeric values, the server can now read both formats, but you should normalize old records once.

Run this in Mongo shell:

```javascript
db.players.updateMany(
  { account_id: { $type: ["int", "long", "double", "decimal"] } },
  [
    {
      $set: {
        account_id: { $toString: "$account_id" }
      }
    }
  ]
)
```

### Running the Server

```bash
# Debug run
cargo run

# Release build and run
cargo run --release
```

The server listens on `http://127.0.0.1:8000` by default.

### Validation

```bash
# Type check on stable
CARGO_INCREMENTAL=0 cargo +stable check

# Targeted regression tests used by recent listings/member fixes
CARGO_INCREMENTAL=0 cargo +stable test resolve_member_player
```

### Docker

`Dockerfile` uses a stable Rust builder image (`rust:bookworm`).

## License

No open-source license has been declared for this repository at this time.

Please read `LEGAL_NOTICE.md` for provenance and policy details.
